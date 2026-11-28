package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"meridian/gateway/internal/payments"
)

func TestDecide(t *testing.T) {
	tests := []struct {
		name       string
		iban       string
		rejectRate float64
		status     string
		reasonCode string
	}{
		{
			name:   "settles ordinary beneficiary",
			iban:   "DE89370400440532013000",
			status: "SETTLED",
		},
		{
			name:       "rejects closed account deterministically",
			iban:       "DE89370400440532019999",
			status:     "REJECTED",
			reasonCode: "AC04",
		},
		{
			name:       "rejects configured random failures",
			iban:       "GB82WEST12345698765432",
			rejectRate: 1,
			status:     "REJECTED",
			reasonCode: "MS03",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := payments.PayoutInstruction{
				TransferID:      "transfer-1",
				BeneficiaryIBAN: tt.iban,
			}
			event := (&rail{rejectRate: tt.rejectRate}).decide(input)

			if event.EventID == "" || event.EventType != "payout.result" {
				t.Fatalf("invalid envelope: %+v", event)
			}
			if _, err := time.Parse(time.RFC3339Nano, event.OccurredAt); err != nil {
				t.Fatalf("invalid occurred_at %q: %v", event.OccurredAt, err)
			}
			payload, err := json.Marshal(event.Data)
			if err != nil {
				t.Fatal(err)
			}
			var result payments.PayoutResult
			if err := json.Unmarshal(payload, &result); err != nil {
				t.Fatal(err)
			}
			if result.TransferID != input.TransferID || result.Status != tt.status || result.ReasonCode != tt.reasonCode {
				t.Fatalf("unexpected result: %+v", result)
			}
			if result.Status == "SETTLED" && result.RailReference == "" {
				t.Fatal("settled result must include a rail reference")
			}
		})
	}
}

func TestHandleSkipsMalformedInstructions(t *testing.T) {
	r := &rail{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, value := range []string{
		"{not-json",
		`{"data":{}}`,
	} {
		record := &kgo.Record{Offset: 12, Value: []byte(value)}
		if err := r.handle(context.Background(), record); err != nil {
			t.Fatalf("malformed instruction must be skipped, got %v", err)
		}
	}
}
