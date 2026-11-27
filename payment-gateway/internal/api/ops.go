package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/health/grpc_health_v1"

	"meridian/gateway/internal/payments"
)

// Admin returns the internal handler (:8082): ops queries, bank deposits, metrics and health.
// Money-moving and data routes require the admin credential.
func (s *Server) Admin() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "UP"}) })
	mux.HandleFunc("GET /readyz", s.ready)
	ops := http.NewServeMux()
	ops.HandleFunc("POST /ops/deposits", func(w http.ResponseWriter, r *http.Request) {
		s.mutateAs("bank-notifications", w, r, func(r *http.Request, _ Client, body []byte) (payments.BeginFunc, *payments.Problem) {
			var req payments.DepositRequest
			if p := decode(body, &req); p != nil {
				return nil, p
			}
			return s.Svc.BeginDeposit(req), nil
		})
	})
	ops.HandleFunc("GET /ops/payments", func(w http.ResponseWriter, r *http.Request) {
		ps, err := s.Svc.ListPayments(r.Context(), r.URL.Query().Get("status"), limit(r))
		s.reply(w, r, ps, err)
	})
	ops.HandleFunc("GET /ops/payments/{id}", func(w http.ResponseWriter, r *http.Request) {
		p, err := s.Svc.GetPayment(r.Context(), s.Svc.DB, r.PathValue("id"))
		if err != nil {
			s.reply(w, r, nil, err)
			return
		}
		tl, err := s.Svc.Timeline(r.Context(), p.ID)
		s.reply(w, r, map[string]any{"payment": p, "timeline": tl}, err)
	})
	ops.HandleFunc("GET /ops/transfers", func(w http.ResponseWriter, r *http.Request) {
		ts, err := s.Svc.ListTransfers(r.Context(), r.URL.Query().Get("status"), limit(r))
		s.reply(w, r, ts, err)
	})
	ops.HandleFunc("GET /ops/transfers/{id}", func(w http.ResponseWriter, r *http.Request) {
		t, err := s.Svc.GetTransfer(r.Context(), s.Svc.DB, r.PathValue("id"))
		if err != nil {
			s.reply(w, r, nil, err)
			return
		}
		tl, err := s.Svc.Timeline(r.Context(), t.ID)
		s.reply(w, r, map[string]any{"transfer": t, "timeline": tl}, err)
	})
	ops.HandleFunc("GET /ops/summary", s.summary)
	mux.Handle("/ops/", s.authenticateAdmin(ops))
	return mux
}

func (s *Server) authenticateAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || s.AdminKey == "" || subtle.ConstantTimeCompare([]byte(token), []byte(s.AdminKey)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="meridian-admin"`)
			writeProblem(w, &payments.Problem{Status: 401, Code: "unauthenticated", Detail: "a valid admin credential is required"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) reply(w http.ResponseWriter, r *http.Request, v any, err error) {
	if errors.Is(err, payments.ErrNotFound) {
		writeProblem(w, &payments.Problem{Status: 404, Code: "not_found", Detail: "not found"})
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, 200, v)
}

func limit(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return 100
	}
	return min(n, 1000)
}

// summary feeds the console's overview: counts by status and the outbox backlog.
func (s *Server) summary(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{}
	for _, q := range []struct{ name, sql string }{
		{"payments", `SELECT status, count(*) FROM payments GROUP BY status`},
		{"transfers", `SELECT status, count(*) FROM transfers GROUP BY status`},
		{"deposits", `SELECT status, count(*) FROM deposits GROUP BY status`},
		{"outbox", `SELECT CASE WHEN published_at IS NULL THEN 'pending' ELSE 'published' END, count(*) FROM outbox GROUP BY 1`},
	} {
		rows, err := s.Svc.DB.Query(r.Context(), q.sql)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		counts := map[string]int64{}
		for rows.Next() {
			var k string
			var n int64
			if err := rows.Scan(&k, &n); err == nil {
				counts[k] = n
			}
		}
		rows.Close()
		out[q.name] = counts
	}
	writeJSON(w, 200, out)
}

// ready reports each dependency. Only the database gates readiness: without Redis the limiter
// fails open, and without the ledger requests are accepted and completed by recovery.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{}
	status := func(err error) string {
		if err != nil {
			return "DOWN: " + err.Error()
		}
		return "UP"
	}
	dbErr := s.Svc.DB.Ping(ctx)
	checks["database"] = status(dbErr)
	checks["redis"] = status(s.Redis.Ping(ctx).Err())
	hc, err := grpc_health_v1.NewHealthClient(s.Ledger.Conn()).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err == nil && hc.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		err = errors.New(hc.Status.String())
	}
	checks["ledger"] = status(err)
	code := 200
	if dbErr != nil {
		code = 503
	}
	writeJSON(w, code, map[string]any{"status": map[bool]string{true: "UP", false: "DOWN"}[code == 200], "checks": checks})
}
