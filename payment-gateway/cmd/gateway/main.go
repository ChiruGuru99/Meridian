// Command gateway is Meridian's public payments API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"meridian/gateway/internal/api"
	"meridian/gateway/internal/db"
	"meridian/gateway/internal/ledger"
	"meridian/gateway/internal/outbox"
	"meridian/gateway/internal/payments"
	"meridian/gateway/internal/telemetry"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.Setup(ctx, "payment-gateway")
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	clients, err := api.ParseClients(env("API_KEYS", "dev-key-0123456789abcdef:dev:50:100"))
	if err != nil {
		return err
	}

	pool, err := db.Open(ctx, env("DATABASE_URL", "postgres://gateway:gateway@localhost:5432/gateway"))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := retry(ctx, log, "migrate", func() error { return db.Migrate(ctx, pool) }); err != nil {
		return err
	}

	ledgerClient, err := ledger.Dial(env("LEDGER_ADDR", "localhost:9091"))
	if err != nil {
		return err
	}
	defer ledgerClient.Close()

	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379"), DialTimeout: time.Second, ReadTimeout: 200 * time.Millisecond})
	defer rdb.Close()

	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ",")
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return err
	}
	defer producer.Close()
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("payment-gateway"),
		kgo.ConsumeTopics(outbox.TopicPayoutResults),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return err
	}
	defer consumer.Close()

	svc := &payments.Service{DB: pool, Ledger: ledgerClient, Log: log}
	srv := &api.Server{Svc: svc, Ledger: ledgerClient, Redis: rdb, Clients: clients, Log: log}

	go (&outbox.Relay{DB: pool, Kafka: producer, Every: 100 * time.Millisecond, Logger: log}).Run(ctx)
	go svc.Recover(ctx, time.Second)
	go consumePayoutResults(ctx, consumer, svc, log)

	public := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: srv.Public(), ReadHeaderTimeout: 5 * time.Second}
	admin := &http.Server{Addr: env("ADMIN_ADDR", ":8082"), Handler: srv.Admin(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 2)
	for _, s := range []*http.Server{public, admin} {
		go func() {
			log.Info("listening", "addr", s.Addr)
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}()
	}
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = public.Shutdown(shutdownCtx)
	_ = admin.Shutdown(shutdownCtx)
	return nil
}

// consumePayoutResults feeds rail results to the saga. Offsets are committed only after the
// effect is stored, and a failing record is retried (not skipped), so delivery is at-least-once
// and the inbox makes the effect exactly-once.
func consumePayoutResults(ctx context.Context, cl *kgo.Client, svc *payments.Service, log *slog.Logger) {
	tracer := otel.Tracer("payment-gateway")
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Warn("fetch error", "topic", t, "partition", p, "err", err)
			}
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			var ev struct {
				EventID string                `json:"event_id"`
				Data    payments.PayoutResult `json:"data"`
			}
			if err := json.Unmarshal(rec.Value, &ev); err != nil || ev.EventID == "" || ev.Data.TransferID == "" {
				log.Error("skipping malformed payout result", "offset", rec.Offset, "err", err)
				return
			}
			pctx := otel.GetTextMapPropagator().Extract(ctx, telemetry.HeaderCarrier{R: rec})
			for backoff := 100 * time.Millisecond; ctx.Err() == nil; backoff = min(backoff*2, 5*time.Second) {
				sctx, span := tracer.Start(pctx, "process "+rec.Topic, trace.WithSpanKind(trace.SpanKindConsumer))
				err := svc.HandlePayoutResult(sctx, ev.EventID, ev.Data)
				span.End()
				if err == nil {
					break
				}
				log.Warn("payout result not applied, retrying", "transfer", ev.Data.TransferID, "err", err)
				sleep(ctx, backoff)
			}
		})
		if err := cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			log.Warn("offset commit failed (records will be redelivered and deduped)", "err", err)
		}
	}
}

func retry(ctx context.Context, log *slog.Logger, what string, f func() error) error {
	var err error
	for i := 0; i < 60; i++ {
		if err = f(); err == nil {
			return nil
		}
		log.Warn("retrying", "what", what, "err", err)
		sleep(ctx, time.Second)
	}
	return err
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
