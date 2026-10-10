//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestPayoutSagaEndToEnd: reserve -> rail (Kafka) -> settle, and reserve -> rail rejects -> compensate.
// Then the rail's answers are re-delivered through Kafka, plus a contradictory one: nothing moves.
func TestPayoutSagaEndToEnd(t *testing.T) {
	src := openAccount(t, "Payout source", 100_00)
	ok := payoutTo(t, src, makeIBAN("DE", "370400440532013000"), 30_00)
	closed := payoutTo(t, src, makeIBAN("DE", "370400440532019999"), 20_00) // rail-sim: *9999 = closed account (AC04)

	waitFor(t, 30*time.Second, "payout settled", func() bool { return transferStatus(t, ok) == "COMPLETED" })
	waitFor(t, 30*time.Second, "payout compensated", func() bool { return transferStatus(t, closed) == "COMPENSATED" })
	r := expect(t, call(t, "GET", gatewayURL+"/v1/transfers/"+closed, "", nil), 200)
	if !strings.Contains(r.Body["failure_reason"].(string), "AC04") {
		t.Fatalf("failure reason: %v", r.Body["failure_reason"])
	}
	if got := balance(t, src, "EUR"); got != 70_00 {
		t.Fatalf("source = %d, want 7000 (30.00 paid out, 20.00 compensated)", got)
	}
	assertInvariants(t)

	// At-least-once delivery from the rail: replay its answers and add a conflicting late one.
	cl, err := kgo.NewClient(kgo.SeedBrokers(env("KAFKA_BROKERS", "localhost:19092")),
		kgo.ConsumeTopics("rail.payout-results.v1"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	found := map[string]*kgo.Record{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for len(found) < 2 && ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(rec *kgo.Record) {
			if k := string(rec.Key); k == ok || k == closed {
				found[k] = rec
			}
		})
	}
	if len(found) != 2 {
		t.Fatalf("rail results not found on the topic: %v", found)
	}
	for _, rec := range found {
		dup := &kgo.Record{Topic: rec.Topic, Key: rec.Key, Value: rec.Value}
		for i := 0; i < 2; i++ {
			if err := cl.ProduceSync(ctx, dup).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	var ev map[string]any
	_ = json.Unmarshal(found[closed].Value, &ev)
	ev["event_id"] = "forged-late-settlement"
	ev["data"].(map[string]any)["status"] = "SETTLED"
	forged, _ := json.Marshal(ev)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "rail.payout-results.v1", Key: []byte(closed), Value: forged}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second) // let the gateway consume the duplicates
	if transferStatus(t, ok) != "COMPLETED" || transferStatus(t, closed) != "COMPENSATED" {
		t.Fatal("duplicates changed a terminal saga")
	}
	if got := balance(t, src, "EUR"); got != 70_00 {
		t.Fatalf("source = %d after duplicate deliveries, want 7000", got)
	}
	assertInvariants(t)
}

func payoutTo(t *testing.T, src, iban string, amount int64) string {
	t.Helper()
	r := expect(t, post(t, "/v1/transfers", map[string]any{
		"source_account_id": src,
		"destination":       map[string]any{"type": "external", "iban": iban, "name": "Beneficiary GmbH"},
		"amount":            money(amount, "EUR"),
		"reference":         "invoice 77",
	}), 202)
	return r.Body["id"].(string)
}
