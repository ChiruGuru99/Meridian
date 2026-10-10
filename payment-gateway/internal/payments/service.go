package payments

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	ledgerv1 "meridian/gateway/gen/ledgerv1"
	"meridian/gateway/internal/ledger"
)

// Ledger is the subset of the ledger client the domain uses (a fake implements it in tests).
type Ledger interface {
	Post(ctx context.Context, req *ledgerv1.PostTransactionRequest) (*ledgerv1.Transaction, error)
	Reverse(ctx context.Context, key, txnID, reason string) (*ledgerv1.Transaction, error)
	CreateAccount(ctx context.Context, req *ledgerv1.CreateAccountRequest) (*ledgerv1.Account, error)
	GetAccount(ctx context.Context, id string) (*ledgerv1.Account, error)
}

type Service struct {
	DB     *pgxpool.Pool
	Ledger Ledger
	Log    *slog.Logger
}

// Currencies the platform's system accounts are opened in (see ledger migration V1).
var supportedCurrencies = map[string]bool{"EUR": true, "USD": true, "GBP": true}

// MaxAmount mirrors the ledger's bound: 10^15 minor units.
const MaxAmount int64 = 1_000_000_000_000_000

var (
	transitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_state_transitions_total", Help: "Resource state transitions.",
	}, []string{"kind", "status"})
	idempotentReplays = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_idempotent_replays_total", Help: "Requests answered from a stored idempotent response.",
	})
	ledgerCalls = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "gateway_ledger_call_duration_seconds", Help: "Ledger gRPC call latency.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"op", "outcome"})
	driveFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_drive_failures_total", Help: "Retryable failures while driving an in-flight resource.",
	}, []string{"kind"})
)

func newID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

func validMoney(m Money) *Problem {
	if !supportedCurrencies[m.Currency] {
		return problem(422, "unsupported_currency", "currency %q is not supported (EUR, USD, GBP)", m.Currency)
	}
	if m.AmountMinor <= 0 || m.AmountMinor > MaxAmount {
		return problem(422, "invalid_amount", "amount_minor must be between 1 and %d", MaxAmount)
	}
	return nil
}

// ownsAccount enforces that a client can only debit accounts it opened.
func ownsAccount(ctx context.Context, tx pgx.Tx, clientID, accountID string) (bool, error) {
	var ok bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM client_accounts WHERE account_id = $1 AND client_id = $2 AND status = 'ACTIVE')`,
		accountID, clientID).Scan(&ok)
	return ok, err
}

func (s *Service) post(ctx context.Context, op, key, typ, desc string, postings []*ledgerv1.Posting, meta map[string]string) (*ledgerv1.Transaction, error) {
	start := time.Now()
	t, err := s.Ledger.Post(ctx, &ledgerv1.PostTransactionRequest{IdempotencyKey: key, Type: typ, Description: desc, Postings: postings, Metadata: meta})
	ledgerCalls.WithLabelValues(op, outcome(err)).Observe(time.Since(start).Seconds())
	return t, err
}

func outcome(err error) string {
	if err == nil {
		return "ok"
	}
	if _, ok := ledger.AsRejection(err); ok {
		return "rejected"
	}
	return "error"
}

// Drive resolves an in-flight resource. It is idempotent and safe to call concurrently:
// ledger keys are deterministic and the final state change re-checks the state under a row lock.
func (s *Service) Drive(ctx context.Context, kind, id string) error {
	var err error
	switch kind {
	case "payment":
		err = s.drivePayment(ctx, id)
	case "transfer":
		err = s.driveTransfer(ctx, id)
	case "deposit":
		err = s.driveDeposit(ctx, id)
	case "account":
		err = s.driveAccount(ctx, id)
	default:
		return fmt.Errorf("unknown resource kind %q", kind)
	}
	if err != nil {
		driveFailures.WithLabelValues(kind).Inc()
		// Push the next recovery attempt back so a down dependency is not hammered.
		table := map[string]string{"payment": "payments", "transfer": "transfers", "deposit": "deposits", "account": "client_accounts"}[kind]
		_, _ = s.DB.Exec(context.WithoutCancel(ctx), `UPDATE `+table+` SET updated_at = now() WHERE `+idColumn(kind)+` = $1`, id)
	}
	return err
}

func idColumn(kind string) string {
	if kind == "account" {
		return "account_id"
	}
	return "id"
}

// ---------------------------------------------------------------- accounts

type CurrencyLimit struct {
	Currency            string `json:"currency"`
	OverdraftLimitMinor int64  `json:"overdraft_limit_minor"`
}

type CreateAccountRequest struct {
	Name       string          `json:"name"`
	Currencies []CurrencyLimit `json:"currencies"`
}

func (s *Service) BeginCreateAccount(clientID string, req CreateAccountRequest) BeginFunc {
	return func(ctx context.Context, tx pgx.Tx) (*Response, string, string, error) {
		if strings.TrimSpace(req.Name) == "" || len(req.Name) > 200 {
			return nil, "", "", problem(400, "invalid_name", "name must be 1..200 characters")
		}
		if len(req.Currencies) == 0 {
			return nil, "", "", problem(400, "invalid_currencies", "at least one currency is required")
		}
		seen := map[string]bool{}
		for _, c := range req.Currencies {
			if !supportedCurrencies[c.Currency] || seen[c.Currency] {
				return nil, "", "", problem(422, "unsupported_currency", "currency %q is unsupported or duplicated", c.Currency)
			}
			if c.OverdraftLimitMinor < 0 || c.OverdraftLimitMinor > MaxAmount {
				return nil, "", "", problem(422, "invalid_overdraft_limit", "overdraft_limit_minor must be between 0 and %d", MaxAmount)
			}
			seen[c.Currency] = true
		}
		id := newID("acc_")
		_, err := tx.Exec(ctx, `INSERT INTO client_accounts (account_id, client_id, name, currencies, status) VALUES ($1, $2, $3, $4, 'PENDING')`,
			id, clientID, req.Name, req.Currencies)
		return nil, "account", id, err
	}
}

func (s *Service) driveAccount(ctx context.Context, id string) error {
	var name, status string
	var currencies []CurrencyLimit
	if err := s.DB.QueryRow(ctx, `SELECT name, currencies, status FROM client_accounts WHERE account_id = $1`, id).Scan(&name, &currencies, &status); err != nil {
		return err
	}
	if status != "PENDING" {
		return nil
	}
	req := &ledgerv1.CreateAccountRequest{Id: id, Name: name, Type: ledgerv1.AccountType_LIABILITY}
	for _, c := range currencies {
		limit := c.OverdraftLimitMinor
		req.Currencies = append(req.Currencies, &ledgerv1.CurrencyLimit{Currency: c.Currency, OverdraftLimit: &limit})
	}
	acct, err := s.Ledger.CreateAccount(ctx, req)
	rej, rejected := ledger.AsRejection(err)
	if err != nil && !rejected {
		return err
	}
	return pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		next := "ACTIVE"
		if rejected {
			next = "FAILED"
		}
		tag, err := tx.Exec(ctx, `UPDATE client_accounts SET status = $2, updated_at = now() WHERE account_id = $1 AND status = 'PENDING'`, id, next)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		if rejected {
			return completeIdempotencyProblem(ctx, tx, "account", id, problem(422, "account_rejected", "%s", rej.Message))
		}
		return completeIdempotency(ctx, tx, "account", id, 201, AccountView(acct))
	})
}

// AccountView is the public JSON shape of a ledger account.
func AccountView(a *ledgerv1.Account) map[string]any {
	balances := []map[string]any{}
	for _, b := range a.Balances {
		balances = append(balances, map[string]any{"currency": b.Currency, "balance_minor": b.Balance, "overdraft_limit_minor": b.OverdraftLimit})
	}
	return map[string]any{"id": a.Id, "name": a.Name, "balances": balances}
}

// ---------------------------------------------------------------- deposits

type DepositRequest struct {
	AccountID string `json:"account_id"`
	Amount    Money  `json:"amount"`
	Reference string `json:"reference"`
}

// BeginDeposit simulates inbound funds from the partner bank (in production these arrive as
// bank notifications, not client API calls).
func (s *Service) BeginDeposit(clientID string, req DepositRequest) BeginFunc {
	return func(ctx context.Context, tx pgx.Tx) (*Response, string, string, error) {
		if p := validMoney(req.Amount); p != nil {
			return nil, "", "", p
		}
		if len(req.Reference) > 140 {
			return nil, "", "", problem(400, "invalid_reference", "reference must be at most 140 characters")
		}
		owned, err := ownsAccount(ctx, tx, clientID, req.AccountID)
		if err != nil {
			return nil, "", "", err
		}
		if !owned {
			return nil, "", "", problem(404, "account_not_found", "account %q not found", req.AccountID)
		}
		id := newID("dep_")
		_, err = tx.Exec(ctx, `INSERT INTO deposits (id, client_id, account_id, currency, amount, reference, status) VALUES ($1,$2,$3,$4,$5,$6,'PENDING')`,
			id, clientID, req.AccountID, req.Amount.Currency, req.Amount.AmountMinor, req.Reference)
		return nil, "deposit", id, err
	}
}

func (s *Service) driveDeposit(ctx context.Context, id string) error {
	d, err := s.GetDeposit(ctx, s.DB, id)
	if err != nil || d.Status != "PENDING" {
		return err
	}
	txn, err := s.post(ctx, "deposit", "deposit:"+id, "DEPOSIT", "Inbound funds "+d.Reference,
		ledger.Transfer(ledger.Nostro, d.AccountID, d.Amount.Currency, d.Amount.AmountMinor),
		map[string]string{"deposit_id": id, "reference": d.Reference})
	rej, rejected := ledger.AsRejection(err)
	if err != nil && !rejected {
		return err
	}
	return pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		cur, err := s.GetDeposit(ctx, tx, id, true)
		if err != nil || cur.Status != "PENDING" {
			return err
		}
		if rejected {
			cur.Status, cur.FailureReason = "FAILED", &rej.Reason
		} else {
			cur.Status, cur.LedgerTxnID = "COMPLETED", &txn.Id
		}
		if _, err := tx.Exec(ctx, `UPDATE deposits SET status = $2, ledger_txn_id = $3, failure_reason = $4, updated_at = now() WHERE id = $1`,
			id, cur.Status, cur.LedgerTxnID, cur.FailureReason); err != nil {
			return err
		}
		transitions.WithLabelValues("deposit", cur.Status).Inc()
		if err := writeEvent(ctx, tx, "deposit", id, "deposit."+strings.ToLower(cur.Status), cur); err != nil {
			return err
		}
		if rejected {
			return completeIdempotencyProblem(ctx, tx, "deposit", id, problem(422, "deposit_rejected", "%s", rej.Message))
		}
		return completeIdempotency(ctx, tx, "deposit", id, 201, cur)
	})
}

// ---------------------------------------------------------------- IBAN

var ibanShape = regexp.MustCompile(`^[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}$`)

// ValidIBAN checks shape and the ISO 13616 mod-97 checksum.
func ValidIBAN(iban string) bool {
	if !ibanShape.MatchString(iban) {
		return false
	}
	rearranged := iban[4:] + iban[:4]
	var digits strings.Builder
	for _, r := range rearranged {
		if r >= 'A' && r <= 'Z' {
			digits.WriteString(fmt.Sprint(int(r-'A') + 10))
		} else {
			digits.WriteRune(r)
		}
	}
	n, ok := new(big.Int).SetString(digits.String(), 10)
	return ok && new(big.Int).Mod(n, big.NewInt(97)).Int64() == 1
}
