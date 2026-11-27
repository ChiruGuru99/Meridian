// Package api is the HTTP layer: authentication, rate limiting, request decoding and routing.
// Business rules live in package payments.
package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"meridian/gateway/internal/ledger"
	"meridian/gateway/internal/payments"
)

var (
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_http_request_duration_seconds",
		Help:    "Public API latency by route and status code.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"route", "method", "code"})
	rateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_rate_limited_total", Help: "Requests rejected with 429.",
	}, []string{"client"})
	rateLimiterErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_rate_limiter_errors_total", Help: "Rate limiter failures (requests were allowed: fail-open).",
	})
)

// Client is an authenticated API consumer with its token-bucket parameters.
type Client struct {
	ID    string
	Key   string
	Rate  float64 // tokens per second
	Burst int
}

// ParseClients parses "key:client:rate:burst,key2:client2:rate:burst".
func ParseClients(spec string) ([]Client, error) {
	var out []Client
	for _, item := range strings.Split(spec, ",") {
		parts := strings.Split(strings.TrimSpace(item), ":")
		if len(parts) != 4 {
			return nil, fmt.Errorf("API_KEYS entry %q: want key:client:rate:burst", item)
		}
		rate, err1 := strconv.ParseFloat(parts[2], 64)
		burst, err2 := strconv.Atoi(parts[3])
		if err1 != nil || err2 != nil || rate <= 0 || burst <= 0 || len(parts[0]) < 16 {
			return nil, fmt.Errorf("API_KEYS entry for client %q is invalid (key must be >= 16 chars)", parts[1])
		}
		out = append(out, Client{Key: parts[0], ID: parts[1], Rate: rate, Burst: burst})
	}
	return out, nil
}

type Server struct {
	Svc      *payments.Service
	Ledger   *ledger.Client
	Redis    *redis.Client
	Clients  []Client
	AdminKey string
	Log      *slog.Logger
}

type ctxKey struct{}

func clientFrom(r *http.Request) Client { return r.Context().Value(ctxKey{}).(Client) }

// Public returns the public API handler (:8080).
func (s *Server) Public() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/accounts", s.mutate(func(r *http.Request, c Client, body []byte) (payments.BeginFunc, *payments.Problem) {
		var req payments.CreateAccountRequest
		if p := decode(body, &req); p != nil {
			return nil, p
		}
		return s.Svc.BeginCreateAccount(c.ID, req), nil
	}))
	mux.HandleFunc("GET /v1/accounts/{id}", s.getAccount)
	mux.HandleFunc("POST /v1/payments", s.mutate(func(r *http.Request, c Client, body []byte) (payments.BeginFunc, *payments.Problem) {
		var req payments.CreatePaymentRequest
		if p := decode(body, &req); p != nil {
			return nil, p
		}
		return s.Svc.BeginCreatePayment(c.ID, req), nil
	}))
	for path, op := range map[string]payments.Op{
		"authorise": payments.OpAuthorise, "capture": payments.OpCapture, "cancel": payments.OpCancel, "refunds": payments.OpRefund,
	} {
		mux.HandleFunc("POST /v1/payments/{id}/"+path, s.mutate(func(r *http.Request, c Client, body []byte) (payments.BeginFunc, *payments.Problem) {
			var req payments.AmountRequest
			if len(bytes.TrimSpace(body)) > 0 {
				if p := decode(body, &req); p != nil {
					return nil, p
				}
			}
			if req.AmountMinor < 0 {
				return nil, &payments.Problem{Status: 422, Code: "invalid_amount", Detail: "amount_minor must be positive"}
			}
			return s.Svc.BeginPaymentOp(c.ID, r.PathValue("id"), op, req.AmountMinor), nil
		}))
	}
	mux.HandleFunc("GET /v1/payments/{id}", s.getPayment)
	mux.HandleFunc("POST /v1/transfers", s.mutate(func(r *http.Request, c Client, body []byte) (payments.BeginFunc, *payments.Problem) {
		var req payments.TransferRequest
		if p := decode(body, &req); p != nil {
			return nil, p
		}
		return s.Svc.BeginTransfer(c.ID, req), nil
	}))
	mux.HandleFunc("GET /v1/transfers/{id}", s.getTransfer)

	return otelhttp.NewHandler(instrument(s.authenticate(s.rateLimit(mux))), "gateway",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + r.URL.Path }))
}

type builder func(r *http.Request, c Client, body []byte) (payments.BeginFunc, *payments.Problem)

func (s *Server) mutate(build builder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mutateAs(clientFrom(r).ID, w, r, build)
	}
}

func (s *Server) mutateAs(clientID string, w http.ResponseWriter, r *http.Request, build builder) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		writeProblem(w, &payments.Problem{Status: 413, Code: "body_too_large", Detail: "request body must be at most 64 KiB"})
		return
	}
	begin, p := build(r, Client{ID: clientID}, body)
	if p != nil {
		writeProblem(w, p)
		return
	}
	resp, err := s.Svc.Execute(r.Context(), clientID, r.Header.Get("Idempotency-Key"), r.Method, r.URL.Path, body, begin)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if resp.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeRaw(w, resp.Code, resp.Body)
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var owned bool
	err := s.Svc.DB.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM client_accounts WHERE account_id = $1 AND client_id = $2 AND status = 'ACTIVE')`,
		id, clientFrom(r).ID).Scan(&owned)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if !owned {
		writeProblem(w, &payments.Problem{Status: 404, Code: "account_not_found", Detail: "account not found"})
		return
	}
	acct, err := s.Ledger.GetAccount(r.Context(), id)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, 200, payments.AccountView(acct))
}

func (s *Server) getPayment(w http.ResponseWriter, r *http.Request) {
	p, err := s.Svc.GetPayment(r.Context(), s.Svc.DB, r.PathValue("id"))
	allowed := false
	if err == nil {
		allowed, err = s.Svc.CanAccessPayment(r.Context(), s.Svc.DB, clientFrom(r).ID, p)
	}
	if errors.Is(err, payments.ErrNotFound) || (err == nil && !allowed) {
		writeProblem(w, &payments.Problem{Status: 404, Code: "payment_not_found", Detail: "payment not found"})
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, 200, p)
}

func (s *Server) getTransfer(w http.ResponseWriter, r *http.Request) {
	t, err := s.Svc.GetTransfer(r.Context(), s.Svc.DB, r.PathValue("id"))
	if errors.Is(err, payments.ErrNotFound) || (err == nil && t.ClientID != clientFrom(r).ID) {
		writeProblem(w, &payments.Problem{Status: 404, Code: "transfer_not_found", Detail: "transfer not found"})
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, 200, t)
}

// ---------------------------------------------------------------- middleware

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if ok {
			for _, c := range s.Clients {
				if subtle.ConstantTimeCompare([]byte(token), []byte(c.Key)) == 1 {
					next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, c)))
					return
				}
			}
		}
		w.Header().Set("WWW-Authenticate", `Bearer realm="meridian"`)
		writeProblem(w, &payments.Problem{Status: 401, Code: "unauthenticated", Detail: "a valid API key is required"})
	})
}

// tokenBucket refills at rate/s up to burst, using Redis server time so all gateway replicas agree.
var tokenBucket = redis.NewScript(`
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local s = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(s[1]) or burst
local ts = tonumber(s[2]) or now
tokens = math.min(burst, tokens + (now - ts) / 1000 * rate)
local allowed, retry = 0, 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) / rate * 1000)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, retry}`)

func (s *Server) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := clientFrom(r)
		ctx, cancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
		res, err := tokenBucket.Run(ctx, s.Redis, []string{"ratelimit:" + c.ID}, c.Rate, c.Burst).Int64Slice()
		cancel()
		if err != nil {
			// Fail open: the limiter protects capacity, it is not a correctness control.
			rateLimiterErrors.Inc()
			next.ServeHTTP(w, r)
			return
		}
		if res[0] == 0 {
			rateLimited.WithLabelValues(c.ID).Inc()
			w.Header().Set("Retry-After", strconv.FormatInt((res[1]+999)/1000, 10))
			writeProblem(w, &payments.Problem{Status: 429, Code: "rate_limited", Detail: "too many requests"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: 200}
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		httpDuration.WithLabelValues(route, r.Method, strconv.Itoa(rec.code)).Observe(time.Since(start).Seconds())
	})
}

// ---------------------------------------------------------------- encoding

// decode is strict: unknown fields and non-integer amounts are rejected (money is never a float).
func decode(body []byte, v any) *payments.Problem {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return &payments.Problem{Status: 400, Code: "invalid_json", Detail: err.Error()}
	}
	if dec.More() {
		return &payments.Problem{Status: 400, Code: "invalid_json", Detail: "trailing data after JSON body"}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	b, _ := json.Marshal(v)
	writeRaw(w, code, b)
}

func writeRaw(w http.ResponseWriter, code int, body []byte) {
	ct := "application/json"
	if code >= 400 && bytes.Contains(body, []byte(`"type":"https://meridian.example/problems/`)) {
		ct = "application/problem+json"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func writeProblem(w http.ResponseWriter, p *payments.Problem) {
	r := p.Response()
	writeRaw(w, r.Code, r.Body)
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	var pgErr *pgconn.PgError
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &pgErr) || isConnErr(err) {
		s.Log.Warn("dependency unavailable", "path", r.URL.Path, "err", err)
		w.Header().Set("Retry-After", "1")
		writeProblem(w, &payments.Problem{Status: 503, Code: "unavailable", Detail: "a dependency is unavailable; retry with the same Idempotency-Key"})
		return
	}
	s.Log.Error("internal error", "path", r.URL.Path, "err", err)
	writeProblem(w, &payments.Problem{Status: 500, Code: "internal", Detail: "internal error"})
}

func isConnErr(err error) bool {
	return pgconn.SafeToRetry(err) || strings.Contains(err.Error(), "connect") || strings.Contains(err.Error(), "Unavailable")
}
