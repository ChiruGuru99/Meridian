package payments

import (
	"errors"
	"testing"

	"pgregory.net/rapid"
)

func TestBeginTransitions(t *testing.T) {
	cases := []struct {
		from Status
		op   Op
		want Status // "" = not allowed
	}{
		{Created, OpAuthorise, Authorising},
		{Created, OpCancel, Cancelled},
		{Created, OpCapture, ""},
		{Created, OpRefund, ""},
		{Authorised, OpCapture, Capturing},
		{Authorised, OpCancel, Cancelling},
		{Authorised, OpAuthorise, ""},
		{Authorised, OpRefund, ""},
		{Captured, OpRefund, Refunding},
		{Captured, OpCancel, ""},
		{PartiallyRefunded, OpRefund, Refunding},
		{Refunded, OpRefund, ""},
		{Declined, OpAuthorise, ""},
		{Cancelled, OpCapture, ""},
		{Authorising, OpCancel, ""}, // nothing may start while a ledger call is in flight
		{Capturing, OpCapture, ""},
		{Refunding, OpRefund, ""},
	}
	for _, c := range cases {
		got, err := Begin(c.from, c.op)
		if c.want == "" {
			if !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("%s %s: expected invalid transition, got %s", c.from, c.op, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%s %s: got %s %v, want %s", c.from, c.op, got, err, c.want)
		}
	}
}

func TestCompleteTransitions(t *testing.T) {
	cases := []struct {
		from                     Status
		accepted                 bool
		refundedBefore, remaining int64
		want                     Status
	}{
		{Authorising, true, 0, 0, Authorised},
		{Authorising, false, 0, 0, Declined},
		{Capturing, true, 0, 0, Captured},
		{Cancelling, true, 0, 0, Cancelled},
		{Refunding, true, 0, 50, PartiallyRefunded},
		{Refunding, true, 50, 0, Refunded},
		{Refunding, false, 0, 100, Captured},
		{Refunding, false, 30, 70, PartiallyRefunded},
	}
	for _, c := range cases {
		got, err := Complete(c.from, c.accepted, c.refundedBefore, c.remaining)
		if err != nil || got != c.want {
			t.Errorf("%+v: got %s %v", c, got, err)
		}
	}
	if _, err := Complete(Authorised, true, 0, 0); !errors.Is(err, ErrInvalidTransition) {
		t.Error("completing a stable state must fail")
	}
}

// Property: for any sequence of operations and ledger outcomes, the machine only ever moves along
// legal edges, never resolves a stable state, and terminal states absorb every operation.
func TestStateMachineProperty(t *testing.T) {
	terminal := map[Status]bool{Declined: true, Cancelled: true, Refunded: true}
	rapid.Check(t, func(t *rapid.T) {
		s := Created
		captured, refunded := int64(0), int64(0)
		const amount = 100
		steps := rapid.IntRange(1, 30).Draw(t, "steps")
		for i := 0; i < steps; i++ {
			op := rapid.SampledFrom([]Op{OpAuthorise, OpCapture, OpCancel, OpRefund}).Draw(t, "op")
			next, err := Begin(s, op)
			if terminal[s] && err == nil {
				t.Fatalf("terminal %s accepted %s", s, op)
			}
			if err != nil {
				continue
			}
			if !next.InFlight() {
				s = next
				continue
			}
			accepted := rapid.Bool().Draw(t, "accepted")
			pending := int64(0)
			switch next {
			case Capturing:
				pending = rapid.Int64Range(1, amount).Draw(t, "capture")
			case Refunding:
				pending = rapid.Int64Range(1, captured-refunded).Draw(t, "refund")
			}
			if next == Capturing && !accepted {
				accepted = true // capture of a held amount cannot be refused by funds
			}
			before := refunded
			if accepted && next == Capturing {
				captured = pending
			}
			if accepted && next == Refunding {
				refunded += pending
			}
			s, err = Complete(next, accepted, before, captured-refunded)
			if err != nil {
				t.Fatal(err)
			}
			if s.InFlight() {
				t.Fatalf("Complete returned in-flight state %s", s)
			}
			if refunded > captured || captured > amount {
				t.Fatalf("amounts out of bounds: captured %d refunded %d", captured, refunded)
			}
			if s == Refunded && refunded != captured {
				t.Fatalf("REFUNDED with %d of %d refunded", refunded, captured)
			}
		}
	})
}

func TestValidIBAN(t *testing.T) {
	for _, ok := range []string{"DE89370400440532013000", "GB82WEST12345698765432", "NL91ABNA0417164300"} {
		if !ValidIBAN(ok) {
			t.Errorf("%s should be valid", ok)
		}
	}
	for _, bad := range []string{"DE89370400440532013001", "GB82WEST1234569876543", "de89370400440532013000", "", "XX00"} {
		if ValidIBAN(bad) {
			t.Errorf("%s should be invalid", bad)
		}
	}
}
