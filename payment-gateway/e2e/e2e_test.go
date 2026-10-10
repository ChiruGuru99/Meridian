//go:build e2e

package e2e

import "testing"

// TestTransferEndToEnd is the thin vertical slice: gateway -> ledger (gRPC) -> Postgres.
func TestTransferEndToEnd(t *testing.T) {
	alice := openAccount(t, "Alice", 100_00)
	bob := openAccount(t, "Bob", 0)

	r := expect(t, post(t, "/v1/transfers", map[string]any{
		"source_account_id": alice,
		"destination":       map[string]any{"type": "internal", "account_id": bob},
		"amount":            money(25_00, "EUR"),
		"reference":         "rent share",
	}), 201)
	if r.Body["status"] != "COMPLETED" {
		t.Fatalf("transfer not completed: %s", r.Raw)
	}
	if got := balance(t, alice, "EUR"); got != 75_00 {
		t.Fatalf("alice EUR = %d, want 7500", got)
	}
	if got := balance(t, bob, "EUR"); got != 25_00 {
		t.Fatalf("bob EUR = %d, want 2500", got)
	}
	assertInvariants(t)
}

// TestPaymentLifecycleEndToEnd: authorise, partial capture, partial refund against the real ledger.
func TestPaymentLifecycleEndToEnd(t *testing.T) {
	payer := openAccount(t, "Payer", 200_00)
	payee := openAccount(t, "Merchant", 0)
	r := expect(t, post(t, "/v1/payments", map[string]any{"payer_account_id": payer, "payee_account_id": payee,
		"amount": money(120_00, "EUR"), "description": "order 1001"}), 201)
	id := r.Body["id"].(string)

	expect(t, post(t, "/v1/payments/"+id+"/authorise", nil), 200)
	if got := balance(t, payer, "EUR"); got != 80_00 {
		t.Fatalf("payer after authorise = %d", got)
	}
	expect(t, post(t, "/v1/payments/"+id+"/capture", map[string]any{"amount_minor": 100_00}), 200)
	expect(t, post(t, "/v1/payments/"+id+"/refunds", map[string]any{"amount_minor": 30_00}), 200)
	if p, m := balance(t, payer, "EUR"), balance(t, payee, "EUR"); p != 130_00 || m != 70_00 {
		t.Fatalf("payer=%d merchant=%d, want 13000/7000", p, m)
	}
	r = expect(t, call(t, "GET", gatewayURL+"/v1/payments/"+id, "", nil), 200)
	if r.Body["status"] != "PARTIALLY_REFUNDED" {
		t.Fatalf("status %v", r.Body["status"])
	}
	assertInvariants(t)
}

// TestIdempotencyEndToEnd: a retried request returns the stored response and moves money once.
func TestIdempotencyEndToEnd(t *testing.T) {
	a := openAccount(t, "A", 50_00)
	b := openAccount(t, "B", 0)
	body := map[string]any{"source_account_id": a, "destination": map[string]any{"type": "internal", "account_id": b},
		"amount": money(10_00, "EUR"), "reference": "retry me"}
	k := newKey()
	first := expect(t, call(t, "POST", gatewayURL+"/v1/transfers", k, body), 201)
	for i := 0; i < 3; i++ {
		again := expect(t, call(t, "POST", gatewayURL+"/v1/transfers", k, body), 201)
		if !again.Replayed || string(again.Raw) != string(first.Raw) {
			t.Fatalf("retry %d not replayed identically", i)
		}
	}
	if got := balance(t, a, "EUR"); got != 40_00 {
		t.Fatalf("source = %d after 4 identical requests, want 4000", got)
	}
	body["amount"] = money(11_00, "EUR")
	expect(t, call(t, "POST", gatewayURL+"/v1/transfers", k, body), 422)
}
