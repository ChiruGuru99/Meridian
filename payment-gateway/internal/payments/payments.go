package payments

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	ledgerv1 "meridian/gateway/gen/ledgerv1"
	"meridian/gateway/internal/ledger"
	"meridian/gateway/internal/outbox"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

var ErrNotFound = errors.New("not found")

func writeEvent(ctx context.Context, tx pgx.Tx, kind, id, eventType string, data any) error {
	return outbox.Write(ctx, tx, outbox.TopicPaymentEvents, kind, id, eventType, data)
}

type CreatePaymentRequest struct {
	PayerAccountID string `json:"payer_account_id"`
	PayeeAccountID string `json:"payee_account_id"`
	Amount         Money  `json:"amount"`
	Description    string `json:"description"`
}

// AmountRequest is the optional body of capture and refund. Zero means "the full remaining amount".
type AmountRequest struct {
	AmountMinor int64 `json:"amount_minor"`
}

func (s *Service) BeginCreatePayment(clientID string, req CreatePaymentRequest) BeginFunc {
	return func(ctx context.Context, tx pgx.Tx) (*Response, string, string, error) {
		if p := validMoney(req.Amount); p != nil {
			return nil, "", "", p
		}
		if req.PayerAccountID == "" || req.PayeeAccountID == "" || req.PayerAccountID == req.PayeeAccountID {
			return nil, "", "", problem(400, "invalid_accounts", "payer_account_id and payee_account_id are required and must differ")
		}
		if len(req.Description) > 500 {
			return nil, "", "", problem(400, "invalid_description", "description must be at most 500 characters")
		}
		payerClient, payerCurrency, payerFound, err := activeAccountForCurrency(ctx, tx, req.PayerAccountID, req.Amount.Currency)
		if err != nil {
			return nil, "", "", err
		}
		if !payerFound || payerClient != clientID {
			return nil, "", "", problem(404, "account_not_found", "payer account %q not found", req.PayerAccountID)
		}
		if !payerCurrency {
			return nil, "", "", problem(422, "account_currency_not_enabled", "payer account %q does not support %s", req.PayerAccountID, req.Amount.Currency)
		}
		_, payeeCurrency, payeeFound, err := activeAccountForCurrency(ctx, tx, req.PayeeAccountID, req.Amount.Currency)
		if err != nil {
			return nil, "", "", err
		}
		if !payeeFound {
			return nil, "", "", problem(404, "account_not_found", "payee account %q not found", req.PayeeAccountID)
		}
		if !payeeCurrency {
			return nil, "", "", problem(422, "account_currency_not_enabled", "payee account %q does not support %s", req.PayeeAccountID, req.Amount.Currency)
		}
		id := newID("pay_")
		_, err = tx.Exec(ctx, `
			INSERT INTO payments (id, client_id, payer_account_id, payee_account_id, currency, amount, status, description)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			id, clientID, req.PayerAccountID, req.PayeeAccountID, req.Amount.Currency, req.Amount.AmountMinor, Created, req.Description)
		if err != nil {
			return nil, "", "", err
		}
		p, err := s.GetPayment(ctx, tx, id)
		if err != nil {
			return nil, "", "", err
		}
		transitions.WithLabelValues("payment", string(Created)).Inc()
		if err := writeEvent(ctx, tx, "payment", id, "payment.created", p); err != nil {
			return nil, "", "", err
		}
		return jsonResponse(201, p), "payment", id, nil
	}
}

// BeginPaymentOp moves a payment into the in-flight state of op (under a row lock) and hands it to the driver.
func (s *Service) BeginPaymentOp(clientID, paymentID string, op Op, amount int64) BeginFunc {
	return func(ctx context.Context, tx pgx.Tx) (*Response, string, string, error) {
		p, err := s.GetPayment(ctx, tx, paymentID, true)
		allowed := false
		if err == nil {
			allowed, err = s.paymentOpAllowed(ctx, tx, clientID, p, op)
		}
		if errors.Is(err, ErrNotFound) || (err == nil && !allowed) {
			return nil, "", "", problem(404, "payment_not_found", "payment %q not found", paymentID)
		}
		if err != nil {
			return nil, "", "", err
		}
		next, err := Begin(p.Status, op)
		if err != nil {
			return nil, "", "", problem(409, "invalid_state", "%s", err.Error())
		}
		var pending *int64
		var refundID *string
		switch op {
		case OpCapture:
			c := p.Amount.AmountMinor
			if amount != 0 {
				c = amount
			}
			if c <= 0 || c > p.Amount.AmountMinor {
				return nil, "", "", problem(422, "invalid_amount", "capture amount must be between 1 and the authorised %d", p.Amount.AmountMinor)
			}
			pending = &c
		case OpRefund:
			refundable := p.CapturedAmount - p.RefundedAmount
			r := refundable
			if amount != 0 {
				r = amount
			}
			if r <= 0 || r > refundable {
				return nil, "", "", problem(422, "invalid_amount", "refund amount must be between 1 and the refundable %d", refundable)
			}
			id := newID("ref_")
			if _, err := tx.Exec(ctx, `INSERT INTO refunds (id, payment_id, amount, status) VALUES ($1, $2, $3, 'PENDING')`, id, paymentID, r); err != nil {
				return nil, "", "", err
			}
			pending, refundID = &r, &id
		}
		if _, err := tx.Exec(ctx, `
			UPDATE payments SET status = $2, pending_amount = $3, pending_refund_id = $4, version = version + 1, updated_at = now()
			WHERE id = $1`, paymentID, next, pending, refundID); err != nil {
			return nil, "", "", err
		}
		transitions.WithLabelValues("payment", string(next)).Inc()
		if !next.InFlight() {
			// CREATED -> CANCELLED: nothing to post to the ledger.
			p, err := s.GetPayment(ctx, tx, paymentID)
			if err != nil {
				return nil, "", "", err
			}
			if err := writeEvent(ctx, tx, "payment", paymentID, "payment."+strings.ToLower(string(next)), p); err != nil {
				return nil, "", "", err
			}
			return jsonResponse(200, p), "payment", paymentID, nil
		}
		return nil, "payment", paymentID, nil
	}
}

func (s *Service) paymentOpAllowed(ctx context.Context, q querier, clientID string, p *Payment, op Op) (bool, error) {
	accountID := p.PayerAccountID
	if op == OpCapture || op == OpRefund {
		accountID = p.PayeeAccountID
	}
	return ownsAccount(ctx, q, clientID, accountID)
}

// paymentPostings is the accounting for each in-flight payment state (see DESIGN.md §4).
func paymentPostings(p *Payment) (op, key string, postings []*ledgerv1.Posting) {
	cur, a := p.Amount.Currency, p.Amount.AmountMinor
	switch p.Status {
	case Authorising:
		return "authorise", "payment:" + p.ID + ":authorise", ledger.Transfer(p.PayerAccountID, ledger.AuthHolds, cur, a)
	case Capturing:
		c := *p.PendingAmount
		postings = []*ledgerv1.Posting{
			{AccountId: ledger.AuthHolds, Currency: cur, Amount: a},
			{AccountId: p.PayeeAccountID, Currency: cur, Amount: -c},
		}
		if a > c {
			// Partial capture releases the remainder of the hold back to the payer in the same transaction.
			postings = append(postings, &ledgerv1.Posting{AccountId: p.PayerAccountID, Currency: cur, Amount: -(a - c)})
		}
		return "capture", "payment:" + p.ID + ":capture", postings
	case Cancelling:
		return "cancel", "payment:" + p.ID + ":cancel", ledger.Transfer(ledger.AuthHolds, p.PayerAccountID, cur, a)
	case Refunding:
		return "refund", "payment:" + p.ID + ":refund:" + *p.PendingRefundID, ledger.Transfer(p.PayeeAccountID, p.PayerAccountID, cur, *p.PendingAmount)
	}
	return "", "", nil
}

func (s *Service) drivePayment(ctx context.Context, id string) error {
	p, err := s.GetPayment(ctx, s.DB, id)
	if err != nil || !p.Status.InFlight() {
		return err
	}
	op, key, postings := paymentPostings(p)
	txn, err := s.post(ctx, op, key, "PAYMENT_"+strings.ToUpper(op), p.Description, postings, map[string]string{"payment_id": id})
	rej, rejected := ledger.AsRejection(err)
	if err != nil && !rejected {
		return err
	}
	return pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		cur, err := s.GetPayment(ctx, tx, id, true)
		if err != nil || cur.Status != p.Status {
			return err // already resolved by a concurrent driver
		}
		accepted := !rejected
		captured, refunded := cur.CapturedAmount, cur.RefundedAmount
		switch {
		case cur.Status == Capturing && accepted:
			captured = *cur.PendingAmount
		case cur.Status == Refunding && accepted:
			refunded += *cur.PendingAmount
		}
		next, err := Complete(cur.Status, accepted, cur.RefundedAmount, captured-refunded)
		if err != nil {
			return err
		}
		var reason *string
		if rejected {
			r := strings.ToLower(rej.Reason)
			reason = &r
		}
		if cur.Status == Refunding {
			refundStatus := "SUCCEEDED"
			var txnID *string
			if accepted {
				txnID = &txn.Id
			} else {
				refundStatus = "FAILED"
			}
			if _, err := tx.Exec(ctx, `UPDATE refunds SET status = $2, ledger_txn_id = $3, failure_reason = $4 WHERE id = $1`,
				*cur.PendingRefundID, refundStatus, txnID, reason); err != nil {
				return err
			}
		}
		declineReason := cur.DeclineReason
		if next == Declined {
			declineReason = reason
		}
		if _, err := tx.Exec(ctx, `
			UPDATE payments SET status = $2, captured_amount = $3, refunded_amount = $4, decline_reason = $5,
			       pending_amount = NULL, pending_refund_id = NULL, version = version + 1, updated_at = now()
			WHERE id = $1`, id, next, captured, refunded, declineReason); err != nil {
			return err
		}
		transitions.WithLabelValues("payment", string(next)).Inc()
		updated, err := s.GetPayment(ctx, tx, id)
		if err != nil {
			return err
		}
		eventType := "payment." + strings.ToLower(string(next))
		if rejected && cur.Status == Refunding {
			eventType = "payment.refund_failed"
		}
		if err := writeEvent(ctx, tx, "payment", id, eventType, updated); err != nil {
			return err
		}
		code := 200
		switch {
		case rejected && (cur.Status == Authorising || cur.Status == Refunding):
			code = 402 // declined by funds check; body still carries the payment
		case rejected:
			// Capture and void cannot fail on funds (the hold covers them); anything else is a ledger-side fault.
			s.Log.Error("ledger rejected a hold settlement", "payment", id, "op", op, "reason", rej.Reason)
			return completeIdempotencyProblem(ctx, tx, "payment", id, problem(422, "ledger_rejected", "%s", rej.Message))
		}
		return completeIdempotency(ctx, tx, "payment", id, code, updated)
	})
}

const paymentColumns = `id, client_id, payer_account_id, payee_account_id, currency, amount, captured_amount, refunded_amount,
	status, pending_amount, pending_refund_id, description, decline_reason, version, created_at, updated_at`

func scanPayment(row pgx.Row) (*Payment, error) {
	var p Payment
	err := row.Scan(&p.ID, &p.ClientID, &p.PayerAccountID, &p.PayeeAccountID, &p.Amount.Currency, &p.Amount.AmountMinor,
		&p.CapturedAmount, &p.RefundedAmount, &p.Status, &p.PendingAmount, &p.PendingRefundID, &p.Description,
		&p.DeclineReason, &p.Version, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &p, err
}

// GetPayment loads a payment with its refunds; forUpdate takes the row lock (inside a transaction).
func (s *Service) GetPayment(ctx context.Context, q querier, id string, forUpdate ...bool) (*Payment, error) {
	sql := `SELECT ` + paymentColumns + ` FROM payments WHERE id = $1`
	if len(forUpdate) > 0 && forUpdate[0] {
		sql += ` FOR UPDATE`
	}
	p, err := scanPayment(q.QueryRow(ctx, sql, id))
	if err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx, `SELECT id, amount, status, ledger_txn_id, failure_reason, created_at FROM refunds WHERE payment_id = $1 ORDER BY created_at, id`, id)
	if err != nil {
		return nil, err
	}
	p.Refunds, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Refund, error) {
		var x Refund
		err := r.Scan(&x.ID, &x.Amount, &x.Status, &x.LedgerTxnID, &x.FailureReason, &x.CreatedAt)
		return x, err
	})
	return p, err
}

// CanAccessPayment permits the payer and payee clients to read the payment, but no other client.
func (s *Service) CanAccessPayment(ctx context.Context, q querier, clientID string, p *Payment) (bool, error) {
	ownedPayer, err := ownsAccount(ctx, q, clientID, p.PayerAccountID)
	if err != nil || ownedPayer {
		return ownedPayer, err
	}
	return ownsAccount(ctx, q, clientID, p.PayeeAccountID)
}

// ListPayments is the ops-console query (all clients).
func (s *Service) ListPayments(ctx context.Context, status string, limit int) ([]*Payment, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+paymentColumns+` FROM payments WHERE ($1 = '' OR status = $1) ORDER BY created_at DESC LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Payment{}
	for rows.Next() {
		p, err := scanPayment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
