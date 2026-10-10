package outbox

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"meridian/gateway/internal/db"
)

// An event leaves the outbox only once the broker has acknowledged it; while the broker is
// unreachable it stays pending, and nothing is lost.
func TestRelayPublishesOnlyAfterAck(t *testing.T) {
	ctx := context.Background()
	pg, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithDatabase("gw"), postgres.WithUsername("gw"),
		postgres.WithPassword("gw"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Terminate(ctx)
	url, _ := pg.ConnectionString(ctx, "sslmode=disable")
	pool, err := db.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	// Write an event inside a traced business transaction.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	tctx := trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: 1}))
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return Write(tctx, tx, TopicPaymentEvents, "payment", "pay_1", "payment.created", map[string]any{"id": "pay_1"})
	})
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pending := func() (n int) {
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
		return n
	}

	dead, _ := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.RecordDeliveryTimeout(2*time.Second))
	if _, err := (&Relay{DB: pool, Kafka: dead, Logger: log}).publishBatch(ctx); err == nil {
		t.Fatal("publishing to an unreachable broker must fail")
	}
	dead.Close()
	if pending() != 1 {
		t.Fatal("the event must stay pending while the broker is down")
	}

	rp, err := redpanda.Run(ctx, "redpandadata/redpanda:v26.1.18", redpanda.WithAutoCreateTopics())
	if err != nil {
		t.Fatal(err)
	}
	defer rp.Terminate(ctx)
	broker, _ := rp.KafkaSeedBroker(ctx)
	live, _ := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	defer live.Close()
	if n, err := (&Relay{DB: pool, Kafka: live, Logger: log}).publishBatch(ctx); err != nil || n != 1 {
		t.Fatalf("publishBatch = %d, %v", n, err)
	}
	if pending() != 0 {
		t.Fatal("the event must be marked published after the ack")
	}

	cons, _ := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(TopicPaymentEvents), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cons.Close()
	fctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	fetches := cons.PollFetches(fctx)
	recs := fetches.Records()
	if len(recs) != 1 || string(recs[0].Key) != "pay_1" {
		t.Fatalf("want one record keyed pay_1, got %d (%v)", len(recs), fetches.Errors())
	}
	var traceparent string
	for _, h := range recs[0].Headers {
		if h.Key == "traceparent" {
			traceparent = string(h.Value)
		}
	}
	if traceparent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("trace context not carried through the outbox: %q", traceparent)
	}
}
