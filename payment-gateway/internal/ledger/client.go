// Package ledger wraps the ledger-service gRPC API and classifies its errors.
package ledger

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ledgerv1 "meridian/gateway/gen/ledgerv1"
)

// System accounts seeded by the ledger's migrations.
const (
	Nostro         = "SYS-NOSTRO"
	AuthHolds      = "SYS-AUTH-HOLDS"
	PayoutClearing = "SYS-PAYOUT-CLEARING"
)

// Rejection is a permanent business refusal from the ledger (insufficient funds, unknown account...).
// Anything that is not a Rejection is treated as retryable.
type Rejection struct {
	Reason  string // ledger ErrorInfo reason, e.g. INSUFFICIENT_FUNDS
	Message string
}

func (r *Rejection) Error() string { return r.Reason + ": " + r.Message }

// AsRejection reports whether err is a permanent ledger rejection.
func AsRejection(err error) (*Rejection, bool) {
	var r *Rejection
	ok := errors.As(err, &r)
	return r, ok
}

type Client struct {
	conn    *grpc.ClientConn
	api     ledgerv1.LedgerServiceClient
	timeout time.Duration
}

func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, api: ledgerv1.NewLedgerServiceClient(conn), timeout: 5 * time.Second}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// Conn exposes the connection for health checks.
func (c *Client) Conn() *grpc.ClientConn { return c.conn }

func (c *Client) Post(ctx context.Context, req *ledgerv1.PostTransactionRequest) (*ledgerv1.Transaction, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	t, err := c.api.PostTransaction(ctx, req)
	return t, classify(err)
}

func (c *Client) Reverse(ctx context.Context, key, txnID, reason string) (*ledgerv1.Transaction, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	t, err := c.api.ReverseTransaction(ctx, &ledgerv1.ReverseTransactionRequest{IdempotencyKey: key, TransactionId: txnID, Reason: reason})
	return t, classify(err)
}

func (c *Client) CreateAccount(ctx context.Context, req *ledgerv1.CreateAccountRequest) (*ledgerv1.Account, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	a, err := c.api.CreateAccount(ctx, req)
	return a, classify(err)
}

func (c *Client) GetAccount(ctx context.Context, id string) (*ledgerv1.Account, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	a, err := c.api.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: id})
	return a, classify(err)
}

// classify turns business status codes into *Rejection and leaves transport failures as-is (retryable).
func classify(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition, codes.AlreadyExists:
		reason := st.Code().String()
		for _, d := range st.Details() {
			if info, ok := d.(*errdetails.ErrorInfo); ok {
				reason = info.Reason
			}
		}
		return &Rejection{Reason: reason, Message: st.Message()}
	}
	return err
}

// Transfer builds the two legs that move amount from debit to credit account.
func Transfer(debit, credit, currency string, amount int64) []*ledgerv1.Posting {
	return []*ledgerv1.Posting{
		{AccountId: debit, Currency: currency, Amount: amount},
		{AccountId: credit, Currency: currency, Amount: -amount},
	}
}
