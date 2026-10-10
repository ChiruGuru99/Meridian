package payments

import (
	"context"
	"encoding/json"
	"time"
)

func jsonResponse(code int, v any) *Response {
	b, _ := json.Marshal(v)
	return &Response{Code: code, Body: b}
}

// RecoveryAge is how long a resource may sit in an in-flight state before the recovery loop
// assumes its driver died (crash, timeout) and re-drives it.
const RecoveryAge = 5 * time.Second

// Recover periodically re-drives resources stuck in an in-flight state. Re-driving is safe
// because each ledger call carries a deterministic idempotency key.
func (s *Service) Recover(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s.RecoverOnce(ctx)
	}
}

// RecoverOnce runs one recovery pass and returns how many resources it attempted.
func (s *Service) RecoverOnce(ctx context.Context) int {
	age := RecoveryAge.String()
	queries := []struct {
		kind, sql string
		args      []any
	}{
		{"payment", `SELECT id FROM payments WHERE status = ANY($1) AND updated_at < now() - $2::interval ORDER BY updated_at LIMIT 100`,
			[]any{statusStrings(InFlight), age}},
		{"transfer", `SELECT id FROM transfers WHERE status = ANY($1) AND updated_at < now() - $2::interval ORDER BY updated_at LIMIT 100`,
			[]any{statusStrings(TransferInFlight), age}},
		{"deposit", `SELECT id FROM deposits WHERE status = 'PENDING' AND updated_at < now() - $1::interval ORDER BY updated_at LIMIT 100`,
			[]any{age}},
		{"account", `SELECT account_id FROM client_accounts WHERE status = 'PENDING' AND updated_at < now() - $1::interval ORDER BY updated_at LIMIT 100`,
			[]any{age}},
	}
	n := 0
	for _, q := range queries {
		rows, err := s.DB.Query(ctx, q.sql, q.args...)
		if err != nil {
			s.Log.Warn("recovery query failed", "kind", q.kind, "err", err)
			continue
		}
		var ids []string
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				ids = append(ids, id)
			}
		}
		rows.Close()
		for _, id := range ids {
			n++
			if err := s.Drive(ctx, q.kind, id); err != nil {
				s.Log.Warn("recovery drive failed", "kind", q.kind, "id", id, "err", err)
			} else {
				s.Log.Info("recovered in-flight resource", "kind", q.kind, "id", id)
			}
		}
	}
	return n
}

func statusStrings(ss []Status) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}
