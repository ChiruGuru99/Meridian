package payments

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"meridian/gateway/internal/ledger"
	"meridian/gateway/internal/outbox"
)

var compensations = promauto.NewCounter(prometheus.CounterOpts{
	Name: "gateway_saga_compensations_total", Help: "Payout sagas compensated after a rail rejection.",
})

type Destination struct {
	Type      string `json:"type"` // internal | external
	AccountID string `json:"account_id,omitempty"`
	IBAN      string `json:"iban,omitempty"`
	Name      string `json:"name,omitempty"`
}

type TransferRequest struct {
	SourceAccountID string      `json:"source_account_id"`
	Destination     Destination `json:"destination"`
	Amount          Money       `json:"amount"`
	Reference       string      `json:"reference"`
}

// PayoutInstruction is what the rail receives (topic rail.payout-instructions.v1).
type PayoutInstruction struct {
	TransferID      string `json:"transfer_id"`
	AmountMinor     int64  `json:"amount_minor"`
	Currency        string `json:"currency"`
	BeneficiaryIBAN string `json:"beneficiary_iban"`
	BeneficiaryName string `json:"beneficiary_name"`
	Reference       string `json:"reference"`
}

// PayoutResult is the rail's answer (topic rail.payout-results.v1).
type PayoutResult struct {
	TransferID    string `json:"transfer_id"`
	Status        string `json:"status"` // SETTLED | REJECTED
	ReasonCode    string `json:"reason_code,omitempty"`
	Reason        string `json:"reason,omitempty"`
	RailReference string `json:"rail_reference"`
}

func (s *Service) BeginTransfer(clientID string, req TransferRequest) BeginFunc {
	return func(ctx context.Context, tx pgx.Tx) (*Response, string, string, error) {
		if p := validMoney(req.Amount); p != nil {
			return nil, "", "", p
		}
		if len(req.Reference) > 140 {
			return nil, "", "", problem(400, "invalid_reference", "reference must be at most 140 characters")
		}
		owned, err := ownsAccount(ctx, tx, clientID, req.SourceAccountID)
		if err != nil {
			return nil, "", "", err
		}
		if !owned {
			return nil, "", "", problem(404, "account_not_found", "source account %q not found", req.SourceAccountID)
		}
		var dest, iban, name *string
		switch req.Destination.Type {
		case "internal":
			if req.Destination.AccountID == "" || req.Destination.AccountID == req.SourceAccountID ||
				strings.HasPrefix(req.Destination.AccountID, "SYS-") {
				return nil, "", "", problem(400, "invalid_destination", "destination.account_id is required, must differ from the source and must not be a system account")
			}
			dest = &req.Destination.AccountID
		case "external":
			if !ValidIBAN(req.Destination.IBAN) {
				return nil, "", "", problem(400, "invalid_iban", "destination.iban is not a valid IBAN")
			}
			if strings.TrimSpace(req.Destination.Name) == "" || len(req.Destination.Name) > 140 {
				return nil, "", "", problem(400, "invalid_beneficiary", "destination.name must be 1..140 characters")
			}
			iban, name = &req.Destination.IBAN, &req.Destination.Name
		default:
			return nil, "", "", problem(400, "invalid_destination", "destination.type must be internal or external")
		}
		id := newID("trf_")
		_, err = tx.Exec(ctx, `
			INSERT INTO transfers (id, client_id, kind, source_account_id, destination_account_id, beneficiary_iban, beneficiary_name,
			                       currency, amount, reference, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			id, clientID, req.Destination.Type, req.SourceAccountID, dest, iban, name, req.Amount.Currency, req.Amount.AmountMinor,
			req.Reference, TReserving)
		if err != nil {
			return nil, "", "", err
		}
		transitions.WithLabelValues("transfer", string(TReserving)).Inc()
		return nil, "transfer", id, nil
	}
}

// driveTransfer executes the current step of the transfer saga.
func (s *Service) driveTransfer(ctx context.Context, id string) error {
	t, err := s.GetTransfer(ctx, s.DB, id)
	if err != nil {
		return err
	}
	cur, amt := t.Amount.Currency, t.Amount.AmountMinor
	meta := map[string]string{"transfer_id": id}
	var txnID string
	var rej *ledger.Rejection
	switch t.Status {
	case TReserving:
		var key, typ string
		var credit string
		if t.Kind == "internal" {
			key, typ, credit = "transfer:"+id, "TRANSFER", *t.DestinationAccountID
		} else {
			key, typ, credit = "transfer:"+id+":reserve", "PAYOUT_RESERVE", ledger.PayoutClearing
		}
		txn, err := s.post(ctx, "transfer_reserve", key, typ, t.Reference, ledger.Transfer(t.SourceAccountID, credit, cur, amt), meta)
		if r, ok := ledger.AsRejection(err); ok {
			rej = r
		} else if err != nil {
			return err
		} else {
			txnID = txn.Id
		}
	case TSettling:
		meta["rail_reference"] = deref(t.RailReference)
		txn, err := s.post(ctx, "transfer_settle", "transfer:"+id+":settle", "PAYOUT_SETTLE", t.Reference,
			ledger.Transfer(ledger.PayoutClearing, ledger.Nostro, cur, amt), meta)
		if err != nil {
			return err // clearing always covers the reserve; a rejection here is a fault for operators, keep retrying
		}
		txnID = txn.Id
	case TCompensating:
		start := time.Now()
		txn, err := s.Ledger.Reverse(ctx, "transfer:"+id+":compensate", deref(t.ReserveTxnID), "payout rejected: "+deref(t.FailureReason))
		ledgerCalls.WithLabelValues("transfer_compensate", outcome(err)).Observe(time.Since(start).Seconds())
		if err != nil {
			return err
		}
		txnID = txn.Id
	default:
		return nil
	}

	return pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		locked, err := s.GetTransfer(ctx, tx, id, true)
		if err != nil || locked.Status != t.Status {
			return err
		}
		var next Status
		var code int
		switch {
		case t.Status == TReserving && rej != nil:
			next, code = TFailed, 402
			reason := strings.ToLower(rej.Reason)
			_, err = tx.Exec(ctx, `UPDATE transfers SET failure_reason = $2 WHERE id = $1`, id, reason)
		case t.Status == TReserving && t.Kind == "internal":
			next, code = TCompleted, 201
			_, err = tx.Exec(ctx, `UPDATE transfers SET reserve_txn_id = $2 WHERE id = $1`, id, txnID)
		case t.Status == TReserving:
			next, code = TSubmitted, 202
			if _, err = tx.Exec(ctx, `UPDATE transfers SET reserve_txn_id = $2 WHERE id = $1`, id, txnID); err == nil {
				// The instruction is written in the same transaction as SUBMITTED: the rail gets it iff we consider it sent.
				err = outbox.Write(ctx, tx, outbox.TopicPayoutInstructions, "transfer", id, "payout.instruction", PayoutInstruction{
					TransferID: id, AmountMinor: amt, Currency: cur, BeneficiaryIBAN: deref(t.BeneficiaryIBAN),
					BeneficiaryName: deref(t.BeneficiaryName), Reference: t.Reference,
				})
			}
		case t.Status == TSettling:
			next = TCompleted
			_, err = tx.Exec(ctx, `UPDATE transfers SET settle_txn_id = $2 WHERE id = $1`, id, txnID)
		case t.Status == TCompensating:
			next = TCompensated
			compensations.Inc()
			_, err = tx.Exec(ctx, `UPDATE transfers SET compensation_txn_id = $2 WHERE id = $1`, id, txnID)
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE transfers SET status = $2, version = version + 1, updated_at = now() WHERE id = $1`, id, next); err != nil {
			return err
		}
		transitions.WithLabelValues("transfer", string(next)).Inc()
		updated, err := s.GetTransfer(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := writeEvent(ctx, tx, "transfer", id, "transfer."+strings.ToLower(string(next)), updated); err != nil {
			return err
		}
		if code == 0 {
			return nil // saga steps after SUBMITTED are not tied to a client request
		}
		return completeIdempotency(ctx, tx, "transfer", id, code, updated)
	})
}

// PayoutResultConsumer is the inbox name for rail results.
const PayoutResultConsumer = "gateway.payout-results"

// HandlePayoutResult applies a rail result exactly once: the message id is recorded in the inbox
// in the same transaction as the saga's state change, so a redelivered message is a no-op.
func (s *Service) HandlePayoutResult(ctx context.Context, messageID string, res PayoutResult) error {
	advance := false
	err := pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO inbox (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, PayoutResultConsumer, messageID)
		if err != nil || tag.RowsAffected() == 0 {
			return err // duplicate delivery
		}
		t, err := s.GetTransfer(ctx, tx, res.TransferID, true)
		if errors.Is(err, ErrNotFound) {
			s.Log.Warn("payout result for unknown transfer", "transfer", res.TransferID)
			return nil
		}
		if err != nil || t.Status != TSubmitted {
			return err
		}
		next := TSettling
		var reason *string
		if res.Status != "SETTLED" {
			next = TCompensating
			r := strings.TrimSpace(res.ReasonCode + " " + res.Reason)
			reason = &r
		}
		if _, err := tx.Exec(ctx, `
			UPDATE transfers SET status = $2, rail_reference = $3, failure_reason = $4, version = version + 1, updated_at = now()
			WHERE id = $1`, t.ID, next, res.RailReference, reason); err != nil {
			return err
		}
		transitions.WithLabelValues("transfer", string(next)).Inc()
		advance = true
		updated, err := s.GetTransfer(ctx, tx, t.ID)
		if err != nil {
			return err
		}
		return writeEvent(ctx, tx, "transfer", t.ID, "transfer."+strings.ToLower(string(next)), updated)
	})
	if err != nil || !advance {
		return err
	}
	if err := s.Drive(ctx, "transfer", res.TransferID); err != nil {
		s.Log.Warn("saga step failed, recovery will retry", "transfer", res.TransferID, "err", err)
	}
	return nil
}

const transferColumns = `id, client_id, kind, source_account_id, destination_account_id, beneficiary_iban, beneficiary_name,
	currency, amount, reference, status, reserve_txn_id, settle_txn_id, compensation_txn_id, rail_reference, failure_reason,
	version, created_at, updated_at`

func scanTransfer(row pgx.Row) (*Transfer, error) {
	var t Transfer
	err := row.Scan(&t.ID, &t.ClientID, &t.Kind, &t.SourceAccountID, &t.DestinationAccountID, &t.BeneficiaryIBAN, &t.BeneficiaryName,
		&t.Amount.Currency, &t.Amount.AmountMinor, &t.Reference, &t.Status, &t.ReserveTxnID, &t.SettleTxnID, &t.CompensationTxnID,
		&t.RailReference, &t.FailureReason, &t.Version, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &t, err
}

func (s *Service) GetTransfer(ctx context.Context, q querier, id string, forUpdate ...bool) (*Transfer, error) {
	sql := `SELECT ` + transferColumns + ` FROM transfers WHERE id = $1`
	if len(forUpdate) > 0 && forUpdate[0] {
		sql += ` FOR UPDATE`
	}
	return scanTransfer(q.QueryRow(ctx, sql, id))
}

func (s *Service) ListTransfers(ctx context.Context, status string, limit int) ([]*Transfer, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+transferColumns+` FROM transfers WHERE ($1 = '' OR status = $1) ORDER BY created_at DESC LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Transfer{}
	for rows.Next() {
		t, err := scanTransfer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Service) GetDeposit(ctx context.Context, q querier, id string, forUpdate ...bool) (*Deposit, error) {
	sql := `SELECT id, client_id, account_id, currency, amount, reference, status, ledger_txn_id, failure_reason, created_at, updated_at
		FROM deposits WHERE id = $1`
	if len(forUpdate) > 0 && forUpdate[0] {
		sql += ` FOR UPDATE`
	}
	var d Deposit
	err := q.QueryRow(ctx, sql, id).Scan(&d.ID, &d.ClientID, &d.AccountID, &d.Amount.Currency, &d.Amount.AmountMinor, &d.Reference,
		&d.Status, &d.LedgerTxnID, &d.FailureReason, &d.CreatedAt, &d.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &d, err
}

// TimelineEvent is one row of a resource's audit trail (the retained outbox).
type TimelineEvent struct {
	ID          int64           `json:"id"`
	Topic       string          `json:"topic"`
	EventType   string          `json:"event_type"`
	CreatedAt   string          `json:"created_at"`
	PublishedAt *string         `json:"published_at"`
	Payload     json.RawMessage `json:"payload"`
}

func (s *Service) Timeline(ctx context.Context, aggregateID string) ([]TimelineEvent, error) {
	rows, err := s.DB.Query(ctx, `
		SELECT id, topic, event_type, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
		       to_char(published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'), payload
		FROM outbox WHERE aggregate_id = $1 ORDER BY id`, aggregateID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (TimelineEvent, error) {
		var e TimelineEvent
		err := r.Scan(&e.ID, &e.Topic, &e.EventType, &e.CreatedAt, &e.PublishedAt, &e.Payload)
		return e, err
	})
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
