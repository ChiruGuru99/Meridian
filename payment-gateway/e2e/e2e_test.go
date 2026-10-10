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
