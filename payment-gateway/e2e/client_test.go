//go:build e2e || chaos

// Package e2e drives the running docker-compose stack over HTTP. Run with `make e2e`.
package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	gatewayURL    = env("GATEWAY_URL", "http://localhost:8080")
	adminURL      = env("GATEWAY_ADMIN_URL", "http://localhost:8082")
	ledgerURL     = env("LEDGER_URL", "http://localhost:8081")
	apiKey        = env("API_KEY", "load-key-93ab1c7d5e2f4a60")
	adminKey      = env("ADMIN_API_KEY", "dev-admin-key-0123456789abcdef")
	ledgerReadKey = env("LEDGER_HTTP_API_KEY", "dev-ledger-read-key-0123456789abcdef")
	reconReadKey  = env("RECON_API_KEY", "dev-recon-read-key-0123456789abcdef")
	httpc         = &http.Client{Timeout: 15 * time.Second}
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func newKey() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "e2e-" + hex.EncodeToString(b)
}

type resp struct {
	Code     int
	Body     map[string]any
	Raw      []byte
	Replayed bool
}

// call sends a request; key == "" sends no Idempotency-Key.
func call(t testing.TB, method, url, key string, body any) resp {
	return callAs(t, apiKey, method, url, key, body)
}

func callAs(t testing.TB, bearer, method, url, key string, body any) resp {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, rd)
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := httpc.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	out := resp{Code: res.StatusCode, Raw: raw, Replayed: res.Header.Get("Idempotent-Replayed") == "true"}
	_ = json.Unmarshal(raw, &out.Body)
	return out
}

// post behaves like a well-written client: on 503 (dependency down) or 409 (still in progress) it
// retries with the same Idempotency-Key until it gets a final answer.
func post(t testing.TB, path string, body any) resp {
	key := newKey()
	for i := 0; ; i++ {
		r := call(t, "POST", gatewayURL+path, key, body)
		if (r.Code != 503 && r.Code != 409) || i == 40 {
			return r
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func expect(t testing.TB, r resp, code int) resp {
	t.Helper()
	if r.Code != code {
		t.Fatalf("expected HTTP %d, got %d: %s", code, r.Code, r.Raw)
	}
	return r
}

func money(amount int64, cur string) map[string]any {
	return map[string]any{"amount_minor": amount, "currency": cur}
}

// openAccount creates a EUR/USD/GBP account and optionally funds it in EUR.
func openAccount(t testing.TB, name string, fundEUR int64) string {
	t.Helper()
	r := expect(t, post(t, "/v1/accounts", map[string]any{
		"name": name,
		"currencies": []map[string]any{
			{"currency": "EUR", "overdraft_limit_minor": 0},
			{"currency": "USD", "overdraft_limit_minor": 0},
			{"currency": "GBP", "overdraft_limit_minor": 0},
		},
	}), 201)
	id := r.Body["id"].(string)
	if fundEUR > 0 {
		expect(t, callAs(t, adminKey, "POST", adminURL+"/ops/deposits", newKey(), map[string]any{
			"account_id": id, "amount": money(fundEUR, "EUR"), "reference": "funding " + name,
		}), 201)
	}
	return id
}

func balance(t testing.TB, accountID, currency string) int64 {
	t.Helper()
	r := expect(t, call(t, "GET", gatewayURL+"/v1/accounts/"+accountID, "", nil), 200)
	for _, b := range r.Body["balances"].([]any) {
		m := b.(map[string]any)
		if m["currency"] == currency {
			return int64(m["balance_minor"].(float64))
		}
	}
	t.Fatalf("account %s has no %s balance", accountID, currency)
	return 0
}

// assertInvariants checks the ledger's own recomputation of I1, I2, I4 and I7.
func assertInvariants(t testing.TB) map[string]any {
	t.Helper()
	res, err := get(t, ledgerURL+"/api/invariants", ledgerReadKey)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var inv map[string]any
	_ = json.NewDecoder(res.Body).Decode(&inv)
	if inv["ok"] != true {
		t.Fatalf("ledger invariants violated: %v", inv)
	}
	return inv
}

// waitFor polls until f returns true or the deadline passes.
func waitFor(t testing.TB, timeout time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func get(t testing.TB, url, key string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	return httpc.Do(req)
}

func getJSON(t testing.TB, url, key string) map[string]any {
	t.Helper()
	res, err := get(t, url, key)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(res.Body).Decode(&m)
	return m
}

func transferStatus(t testing.TB, id string) string {
	r := expect(t, call(t, "GET", gatewayURL+"/v1/transfers/"+id, "", nil), 200)
	return fmt.Sprint(r.Body["status"])
}

// makeIBAN computes ISO 13616 check digits for a country code and BBAN.
func makeIBAN(country, bban string) string {
	var digits strings.Builder
	for _, r := range bban + country + "00" {
		if r >= 'A' && r <= 'Z' {
			digits.WriteString(fmt.Sprint(int(r-'A') + 10))
		} else {
			digits.WriteRune(r)
		}
	}
	n, _ := new(big.Int).SetString(digits.String(), 10)
	return fmt.Sprintf("%s%02d%s", country, 98-new(big.Int).Mod(n, big.NewInt(97)).Int64(), bban)
}
