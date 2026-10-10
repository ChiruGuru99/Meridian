// Package outbox implements the transactional outbox: events are inserted in the same DB
// transaction as the state change they describe, and a relay publishes them to Kafka.
package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	TopicPaymentEvents      = "payments.events.v1"
	TopicPayoutInstructions = "rail.payout-instructions.v1"
	TopicPayoutResults      = "rail.payout-results.v1"
)

var (
	published = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_outbox_published_total", Help: "Outbox events published to Kafka.",
	}, []string{"topic"})
	publishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_outbox_publish_errors_total", Help: "Failed publish attempts (retried).",
	})
)

// Event is the envelope every gateway event shares.
type Event struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
	Data       any    `json:"data"`
}

// Write inserts an event inside tx. The current trace context is stored so the consumer can join the trace.
func Write(ctx context.Context, tx pgx.Tx, topic, aggregateType, aggregateID, eventType string, data any) error {
	ev := Event{EventID: uuid.NewString(), EventType: eventType, OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Data: data}
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers, _ := json.Marshal(carrier)
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (topic, message_key, event_type, aggregate_type, aggregate_id, payload, headers)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		topic, aggregateID, eventType, aggregateType, aggregateID, payload, headers)
	return err
}

// Relay publishes unpublished rows in id order. A row is marked published only after the broker
// acknowledged it, so delivery is at-least-once; consumers dedupe on event_id.
type Relay struct {
	DB     *pgxpool.Pool
	Kafka  *kgo.Client
	Every  time.Duration
	Logger *slog.Logger
}

func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for {
			n, err := r.publishBatch(ctx)
			if err != nil {
				publishErrors.Inc()
				r.Logger.Warn("outbox relay paused", "err", err)
				break
			}
			if n == 0 {
				break
			}
		}
	}
}

type row struct {
	id      int64
	topic   string
	key     string
	payload []byte
	headers map[string]string
}

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	sent := 0
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, topic, message_key, payload, headers FROM outbox
			WHERE published_at IS NULL ORDER BY id LIMIT 200 FOR UPDATE SKIP LOCKED`)
		if err != nil {
			return err
		}
		batch, err := pgx.CollectRows(rows, func(rs pgx.CollectableRow) (row, error) {
			var x row
			err := rs.Scan(&x.id, &x.topic, &x.key, &x.payload, &x.headers)
			return x, err
		})
		if err != nil || len(batch) == 0 {
			return err
		}
		var ids []int64
		var publishErr error
		for _, x := range batch {
			rec := &kgo.Record{Topic: x.topic, Key: []byte(x.key), Value: x.payload}
			for k, v := range x.headers {
				rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
			}
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			publishErr = r.Kafka.ProduceSync(pctx, rec).FirstErr()
			cancel()
			if publishErr != nil {
				break // stop at the first failure to keep per-key order
			}
			ids = append(ids, x.id)
			published.WithLabelValues(x.topic).Inc()
		}
		if len(ids) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
				return err
			}
		}
		sent = len(ids)
		if publishErr != nil && sent == 0 {
			return publishErr
		}
		return nil
	})
	return sent, err
}
