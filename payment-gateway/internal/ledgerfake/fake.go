// Package ledgerfake is an in-memory ledger for gateway tests. It enforces the same rules the real
// ledger does (balanced postings, overdraft limits, idempotency keys, reverse-once) and can inject
// failures: transport errors before applying, or "lost responses" after applying.
package ledgerfake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	ledgerv1 "meridian/gateway/gen/ledgerv1"
	"meridian/gateway/internal/ledger"
)

type Ledger struct {
	mu        sync.Mutex
	balances  map[string]int64 // account|currency, customer-normal sign (credit increases)
	limits    map[string]*int64
	byKey     map[string]*ledgerv1.Transaction
	byID      map[string]*ledgerv1.Transaction
	reversed  map[string]bool
	Effects   int // number of transactions actually applied
	failNext  int
	loseNext  int
}

func New() *Ledger {
	l := &Ledger{balances: map[string]int64{}, limits: map[string]*int64{}, byKey: map[string]*ledgerv1.Transaction{},
		byID: map[string]*ledgerv1.Transaction{}, reversed: map[string]bool{}}
	zero := int64(0)
	for _, cur := range []string{"EUR", "USD", "GBP"} {
		l.limits[ledger.Nostro+"|"+cur] = nil // unlimited
		l.limits[ledger.AuthHolds+"|"+cur] = &zero
		l.limits[ledger.PayoutClearing+"|"+cur] = &zero
	}
	return l
}

// FailNext makes the next n calls fail with UNAVAILABLE before doing anything.
func (l *Ledger) FailNext(n int) { l.mu.Lock(); l.failNext = n; l.mu.Unlock() }

// LoseNext makes the next n successful posts apply but return UNAVAILABLE (response lost in transit).
func (l *Ledger) LoseNext(n int) { l.mu.Lock(); l.loseNext = n; l.mu.Unlock() }

// sign: the nostro is an asset (debit-normal); every other account here is credit-normal.
func sign(account string) int64 {
	if account == ledger.Nostro {
		return 1
	}
	return -1
}

func (l *Ledger) Balance(account, currency string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balances[account+"|"+currency]
}

func (l *Ledger) CreateAccount(_ context.Context, req *ledgerv1.CreateAccountRequest) (*ledgerv1.Account, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.injected(); err != nil {
		return nil, err
	}
	for _, c := range req.Currencies {
		k := req.Id + "|" + c.Currency
		if _, ok := l.limits[k]; !ok {
			l.limits[k] = c.OverdraftLimit
		}
	}
	return l.account(req.Id, req.Name), nil
}

func (l *Ledger) GetAccount(_ context.Context, id string) (*ledgerv1.Account, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.account(id, id), nil
}

func (l *Ledger) account(id, name string) *ledgerv1.Account {
	a := &ledgerv1.Account{Id: id, Name: name, Type: ledgerv1.AccountType_LIABILITY}
	for _, cur := range []string{"EUR", "USD", "GBP"} {
		if lim, ok := l.limits[id+"|"+cur]; ok {
			a.Balances = append(a.Balances, &ledgerv1.Balance{Currency: cur, Balance: l.balances[id+"|"+cur], OverdraftLimit: lim})
		}
	}
	return a
}

func (l *Ledger) Post(_ context.Context, req *ledgerv1.PostTransactionRequest) (*ledgerv1.Transaction, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.injected(); err != nil {
		return nil, err
	}
	return l.apply(req.IdempotencyKey, req.Type, req.Postings, "")
}

func (l *Ledger) Reverse(_ context.Context, key, txnID, reason string) (*ledgerv1.Transaction, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.injected(); err != nil {
		return nil, err
	}
	if t, ok := l.byKey[key]; ok {
		return t, l.lost()
	}
	orig, ok := l.byID[txnID]
	if !ok {
		return nil, &ledger.Rejection{Reason: "TRANSACTION_NOT_FOUND", Message: txnID}
	}
	if l.reversed[txnID] {
		return nil, &ledger.Rejection{Reason: "ALREADY_REVERSED", Message: txnID}
	}
	var neg []*ledgerv1.Posting
	for _, p := range orig.Postings {
		neg = append(neg, &ledgerv1.Posting{AccountId: p.AccountId, Currency: p.Currency, Amount: -p.Amount})
	}
	t, err := l.apply(key, "REVERSAL", neg, txnID)
	if err == nil || isLost(err) {
		l.reversed[txnID] = true
	}
	return t, err
}

func (l *Ledger) apply(key, typ string, postings []*ledgerv1.Posting, reverses string) (*ledgerv1.Transaction, error) {
	if t, ok := l.byKey[key]; ok {
		if !samePostings(t.Postings, postings) {
			return nil, &ledger.Rejection{Reason: "IDEMPOTENCY_KEY_REUSED", Message: key}
		}
		return t, l.lost()
	}
	sums := map[string]int64{}
	for _, p := range postings {
		if p.Amount == 0 {
			return nil, &ledger.Rejection{Reason: "INVALID_ARGUMENT", Message: "zero amount"}
		}
		sums[p.Currency] += p.Amount
	}
	for cur, s := range sums {
		if s != 0 {
			return nil, &ledger.Rejection{Reason: "UNBALANCED", Message: cur}
		}
	}
	next := map[string]int64{}
	for _, p := range postings {
		k := p.AccountId + "|" + p.Currency
		lim, ok := l.limits[k]
		if !ok {
			return nil, &ledger.Rejection{Reason: "ACCOUNT_NOT_FOUND", Message: k}
		}
		if _, seen := next[k]; !seen {
			next[k] = l.balances[k]
		}
		change := sign(p.AccountId) * p.Amount
		next[k] += change
		if change < 0 && lim != nil && next[k] < -*lim {
			return nil, &ledger.Rejection{Reason: "INSUFFICIENT_FUNDS", Message: k}
		}
	}
	for k, v := range next {
		l.balances[k] = v
	}
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	t := &ledgerv1.Transaction{Id: hex.EncodeToString(b), IdempotencyKey: key, Type: typ, ReversesId: reverses}
	for _, p := range postings {
		t.Postings = append(t.Postings, proto.Clone(p).(*ledgerv1.Posting))
	}
	l.byKey[key], l.byID[t.Id] = t, t
	l.Effects++
	return t, l.lost()
}

func (l *Ledger) injected() error {
	if l.failNext > 0 {
		l.failNext--
		return status.Error(codes.Unavailable, "injected failure")
	}
	return nil
}

func (l *Ledger) lost() error {
	if l.loseNext > 0 {
		l.loseNext--
		return status.Error(codes.Unavailable, "injected lost response")
	}
	return nil
}

func isLost(err error) bool { return status.Code(err) == codes.Unavailable }

func samePostings(a, b []*ledgerv1.Posting) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// String renders all balances (debug aid for failing tests).
func (l *Ledger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return fmt.Sprint(l.balances)
}
