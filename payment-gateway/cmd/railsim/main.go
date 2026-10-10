// Command railsim simulates an external payment rail (think SEPA credit transfers).
// It consumes payout instructions and answers each one, once, with SETTLED or REJECTED.
//
// Deterministic rule for demos: a beneficiary IBAN ending in "9999" is a closed account and is
// rejected with ISO 20022 reason AC04. RAIL_REJECT_RATE adds random rejections (MS03).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"meridian/gateway/internal/outbox"
	"meridian/gateway/internal/payments"
	"meridian/gateway/internal/telemetry"
)

var processed = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "rail_payouts_total", Help: "Payout instructions answered by the simulated rail.",
}, []string{"status", "duplicate"})

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Setup(ctx, "rail-sim")
	if err != nil {
		log.Error("telemetry", "err", err)
		os.Exit(1)
	}
	defer shutdown(context.Background())

	rejectRate, _ := strconv.ParseFloat(env("RAIL_REJECT_RATE", "0"), 64)
	latency, _ := time.ParseDuration(env("RAIL_LATENCY", "100ms"))
	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ",")
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("rail-sim"),
		kgo.ConsumeTopics(outbox.TopicPayoutInstructions),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		log.Error("kafka", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"UP"}`)) })
	go func() { _ = http.ListenAndServe(env("ADMIN_ADDR", ":8084"), mux) }()

	rail := &rail{cl: cl, rdb: rdb, log: log, rejectRate: rejectRate, latency: latency}
	log.Info("rail-sim running", "reject_rate", rejectRate, "latency", latency)
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(rec *kgo.Record) {
			for backoff := 100 * time.Millisecond; ctx.Err() == nil; backoff = min(backoff*2, 5*time.Second) {
				err := rail.handle(ctx, rec)
				if err == nil {
					return
				}
				log.Warn("instruction not answered, retrying", "err", err)
				time.Sleep(backoff)
			}
		})
		if err := cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			log.Warn("commit failed", "err", err)
		}
	}
}

type rail struct {
	cl         *kgo.Client
	rdb        *redis.Client
	log        *slog.Logger
	rejectRate float64
	latency    time.Duration
}

// handle answers an instruction. The decision is stored in Redis under the transfer id before it is
// published, so a redelivered instruction re-publishes the same answer (same event id) instead of
// paying twice.
func (r *rail) handle(ctx context.Context, rec *kgo.Record) error {
	var ev struct {
		Data payments.PayoutInstruction `json:"data"`
	}
	if err := json.Unmarshal(rec.Value, &ev); err != nil || ev.Data.TransferID == "" {
		r.log.Error("skipping malformed instruction", "offset", rec.Offset)
		return nil
	}
	ctx, span := otel.Tracer("rail-sim").Start(otel.GetTextMapPropagator().Extract(ctx, telemetry.HeaderCarrier{R: rec}),
		"rail payout", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	key := "rail:payout:" + ev.Data.TransferID
	answer, err := r.rdb.Get(ctx, key).Bytes()
	duplicate := err == nil
	if errors.Is(err, redis.Nil) {
		time.Sleep(r.latency)
		answer, err = json.Marshal(r.decide(ev.Data))
		if err != nil {
			return err
		}
		ok, err := r.rdb.SetNX(ctx, key, answer, 7*24*time.Hour).Result()
		if err != nil {
			return err
		}
		if !ok { // lost a race with another instance: use its answer
			if answer, err = r.rdb.Get(ctx, key).Bytes(); err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	}

	out := &kgo.Record{Topic: outbox.TopicPayoutResults, Key: []byte(ev.Data.TransferID), Value: answer}
	otel.GetTextMapPropagator().Inject(ctx, telemetry.HeaderCarrier{R: out})
	if err := r.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return err
	}
	var res outbox.Event
	_ = json.Unmarshal(answer, &res)
	status := "unknown"
	if m, ok := res.Data.(map[string]any); ok {
		status, _ = m["status"].(string)
	}
	processed.WithLabelValues(status, strconv.FormatBool(duplicate)).Inc()
	return nil
}

func (r *rail) decide(in payments.PayoutInstruction) outbox.Event {
	res := payments.PayoutResult{TransferID: in.TransferID, Status: "SETTLED", RailReference: "RAIL" + randHex(8)}
	switch {
	case strings.HasSuffix(in.BeneficiaryIBAN, "9999"):
		res.Status, res.ReasonCode, res.Reason = "REJECTED", "AC04", "ClosedAccountNumber"
	case mrand.Float64() < r.rejectRate:
		res.Status, res.ReasonCode, res.Reason = "REJECTED", "MS03", "NotSpecifiedReasonAgentGenerated"
	}
	return outbox.Event{EventID: randHex(16), EventType: "payout.result", OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Data: res}
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
