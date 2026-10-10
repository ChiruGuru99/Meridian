// Package payments holds the payments domain: state machines, the operations that drive them
// through the ledger, the payout saga and crash recovery.
package payments

import (
	"errors"
	"fmt"
	"time"
)

type Status string

const (
	Created           Status = "CREATED"
	Authorising       Status = "AUTHORISING"
	Authorised        Status = "AUTHORISED"
	Declined          Status = "DECLINED"
	Capturing         Status = "CAPTURING"
	Captured          Status = "CAPTURED"
	Cancelling        Status = "CANCELLING"
	Cancelled         Status = "CANCELLED"
	Refunding         Status = "REFUNDING"
	PartiallyRefunded Status = "PARTIALLY_REFUNDED"
	Refunded          Status = "REFUNDED"
)

type Op string

const (
	OpAuthorise Op = "authorise"
	OpCapture   Op = "capture"
	OpCancel    Op = "cancel"
	OpRefund    Op = "refund"
)

// ErrInvalidTransition means the operation is not allowed in the payment's current state.
var ErrInvalidTransition = errors.New("invalid state transition")

// InFlight lists the states that a driver must still resolve by calling the ledger.
var InFlight = []Status{Authorising, Capturing, Cancelling, Refunding}

func (s Status) InFlight() bool {
	for _, x := range InFlight {
		if s == x {
			return true
		}
	}
	return false
}

// Begin is the payment state machine's entry edge: the state an operation moves the payment into.
// CREATED -> cancel needs no ledger effect and goes straight to CANCELLED.
func Begin(s Status, op Op) (Status, error) {
	switch {
	case op == OpAuthorise && s == Created:
		return Authorising, nil
	case op == OpCapture && s == Authorised:
		return Capturing, nil
	case op == OpCancel && s == Created:
		return Cancelled, nil
	case op == OpCancel && s == Authorised:
		return Cancelling, nil
	case op == OpRefund && (s == Captured || s == PartiallyRefunded):
		return Refunding, nil
	}
	return "", fmt.Errorf("%w: cannot %s a payment in state %s", ErrInvalidTransition, op, s)
}

// Complete is the exit edge of an in-flight state, given whether the ledger accepted the posting.
// remaining is the refundable amount left after a successful refund (ignored otherwise).
func Complete(s Status, accepted bool, refundedBefore, remaining int64) (Status, error) {
	switch s {
	case Authorising:
		if accepted {
			return Authorised, nil
		}
		return Declined, nil
	case Capturing:
		if accepted {
			return Captured, nil
		}
		return Authorised, nil
	case Cancelling:
		if accepted {
			return Cancelled, nil
		}
		return Authorised, nil
	case Refunding:
		switch {
		case accepted && remaining == 0:
			return Refunded, nil
		case accepted:
			return PartiallyRefunded, nil
		case refundedBefore > 0:
			return PartiallyRefunded, nil
		default:
			return Captured, nil
		}
	}
	return "", fmt.Errorf("%w: %s is not in flight", ErrInvalidTransition, s)
}

type Payment struct {
	ID              string    `json:"id"`
	ClientID        string    `json:"-"`
	PayerAccountID  string    `json:"payer_account_id"`
	PayeeAccountID  string    `json:"payee_account_id"`
	Amount          Money     `json:"amount"`
	CapturedAmount  int64     `json:"captured_amount_minor"`
	RefundedAmount  int64     `json:"refunded_amount_minor"`
	Status          Status    `json:"status"`
	PendingAmount   *int64    `json:"pending_amount_minor,omitempty"`
	PendingRefundID *string   `json:"pending_refund_id,omitempty"`
	Description     string    `json:"description"`
	DeclineReason   *string   `json:"decline_reason,omitempty"`
	Version         int64     `json:"version"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	Refunds         []Refund  `json:"refunds,omitempty"`
}

type Refund struct {
	ID            string    `json:"id"`
	Amount        int64     `json:"amount_minor"`
	Status        string    `json:"status"`
	LedgerTxnID   *string   `json:"ledger_transaction_id,omitempty"`
	FailureReason *string   `json:"failure_reason,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type Money struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

// Transfer statuses. Internal: RESERVING -> COMPLETED | FAILED.
// External (saga): RESERVING -> SUBMITTED -> SETTLING -> COMPLETED, or
// SUBMITTED -> COMPENSATING -> COMPENSATED when the rail rejects; RESERVING -> FAILED on decline.
const (
	TReserving    Status = "RESERVING"
	TSubmitted    Status = "SUBMITTED"
	TSettling     Status = "SETTLING"
	TCompleted    Status = "COMPLETED"
	TCompensating Status = "COMPENSATING"
	TCompensated  Status = "COMPENSATED"
	TFailed       Status = "FAILED"
)

var TransferInFlight = []Status{TReserving, TSettling, TCompensating}

type Transfer struct {
	ID                   string    `json:"id"`
	ClientID             string    `json:"-"`
	Kind                 string    `json:"kind"`
	SourceAccountID      string    `json:"source_account_id"`
	DestinationAccountID *string   `json:"destination_account_id,omitempty"`
	BeneficiaryIBAN      *string   `json:"beneficiary_iban,omitempty"`
	BeneficiaryName      *string   `json:"beneficiary_name,omitempty"`
	Amount               Money     `json:"amount"`
	Reference            string    `json:"reference"`
	Status               Status    `json:"status"`
	ReserveTxnID         *string   `json:"reserve_transaction_id,omitempty"`
	SettleTxnID          *string   `json:"settle_transaction_id,omitempty"`
	CompensationTxnID    *string   `json:"compensation_transaction_id,omitempty"`
	RailReference        *string   `json:"rail_reference,omitempty"`
	FailureReason        *string   `json:"failure_reason,omitempty"`
	Version              int64     `json:"version"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type Deposit struct {
	ID            string    `json:"id"`
	ClientID      string    `json:"-"`
	AccountID     string    `json:"account_id"`
	Amount        Money     `json:"amount"`
	Reference     string    `json:"reference"`
	Status        string    `json:"status"`
	LedgerTxnID   *string   `json:"ledger_transaction_id,omitempty"`
	FailureReason *string   `json:"failure_reason,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}
