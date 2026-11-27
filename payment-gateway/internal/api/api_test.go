package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"pgregory.net/rapid"

	"meridian/gateway/internal/db"
	"meridian/gateway/internal/ledger"
	"meridian/gateway/internal/ledgerfake"
	"meridian/gateway/internal/payments"
)

var (
	pool        *pgxpool.Pool
	rdb         *redis.Client
	fake        *ledgerfake.Ledger
	svc         *payments.Service
	server      *httptest.Server
	adminServer *httptest.Server
	mu          sync.Mutex // tests that inject ledger failures must not overlap with others
	clients     = []Client{
		{Key: "test-key-aaaaaaaaaaaaaaaa", ID: "c1", Rate: 1e6, Burst: 1e6},
		{Key: "test-key-bbbbbbbbbbbbbbbb", ID: "c2", Rate: 1e6, Burst: 1e6},
		{Key: "test-key-limited-cccccccc", ID: "limited", Rate: 0.01, Burst: 3},
	}
)

const adminKey = "test-admin-key-aaaaaaaaaaaaaaaa"

func TestMain(m *testing.M) {
	ctx := context.Background()
	pg, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithDatabase("gateway"), postgres.WithUsername("gateway"),
		postgres.WithPassword("gateway"), postgres.BasicWaitStrategies())
	must(err)
	url, err := pg.ConnectionString(ctx, "sslmode=disable")
	must(err)
	pool, err = db.Open(ctx, url)
	must(err)
	must(db.Migrate(ctx, pool))

	rc, err := tcredis.Run(ctx, "redis:8-alpine")
	must(err)
	rurl, err := rc.ConnectionString(ctx)
	must(err)
	opt, err := redis.ParseURL(rurl)
	must(err)
	rdb = redis.NewClient(opt)

	fake = ledgerfake.New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc = &payments.Service{DB: pool, Ledger: fake, Log: log}
	apiServer := &Server{Svc: svc, Redis: rdb, Clients: clients, AdminKey: adminKey, Log: log}
	server = httptest.NewServer(apiServer.Public())
	adminServer = httptest.NewServer(apiServer.Admin())
	code := m.Run()
	server.Close()
	adminServer.Close()
	_ = pg.Terminate(ctx)
	_ = rc.Terminate(ctx)
	os.Exit(code)
}

// tb is the subset of testing.TB that *rapid.T also implements, so helpers work in property tests.
type tb interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type response struct {
	Code     int
	Body     map[string]any
	Raw      string
	Replayed bool
	Header   http.Header
}

var keyN int64
var keyMu sync.Mutex

func newKey() string {
	keyMu.Lock()
	defer keyMu.Unlock()
	keyN++
	return fmt.Sprintf("k-%d-%d", time.Now().UnixNano(), keyN)
}

func do(t tb, apiKey, method, path, idemKey string, body any) response {
	return doAt(t, server.URL, apiKey, method, path, idemKey, body)
}

func doAt(t tb, baseURL, apiKey, method, path, idemKey string, body any) response {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = bytes.NewBufferString(b)
	default:
		j, _ := json.Marshal(b)
		rd = bytes.NewReader(j)
	}
	req, _ := http.NewRequest(method, baseURL+path, rd)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	r := response{Code: res.StatusCode, Raw: string(raw), Replayed: res.Header.Get("Idempotent-Replayed") == "true", Header: res.Header}
	_ = json.Unmarshal(raw, &r.Body)
	return r
}

const c1, c2 = "test-key-aaaaaaaaaaaaaaaa", "test-key-bbbbbbbbbbbbbbbb"

func post(t tb, path string, body any) response { return do(t, c1, "POST", path, newKey(), body) }

func want(t tb, r response, code int, status ...string) response {
	t.Helper()
	if r.Code != code {
		t.Fatalf("want %d, got %d: %s", code, r.Code, r.Raw)
	}
	if len(status) > 0 && r.Body["status"] != status[0] {
		t.Fatalf("want status %s, got %v: %s", status[0], r.Body["status"], r.Raw)
	}
	return r
}

func money(a int64) map[string]any { return map[string]any{"amount_minor": a, "currency": "EUR"} }

func account(t tb, apiKey string, fund int64) string {
	return accountWith(t, apiKey, fund, []map[string]any{{"currency": "EUR", "overdraft_limit_minor": 0}})
}

func accountWith(t tb, apiKey string, fund int64, currencies []map[string]any) string {
	t.Helper()
	r := want(t, do(t, apiKey, "POST", "/v1/accounts", newKey(), map[string]any{
		"name": "acct", "currencies": currencies,
	}), 201)
	id := r.Body["id"].(string)
	if fund > 0 {
		want(t, doAt(t, adminServer.URL, adminKey, "POST", "/ops/deposits", newKey(), map[string]any{"account_id": id, "amount": money(fund), "reference": "fund"}), 201)
	}
	return id
}

func payment(t tb, payer, payee string, amount int64) string {
	t.Helper()
	r := want(t, post(t, "/v1/payments", map[string]any{"payer_account_id": payer, "payee_account_id": payee, "amount": money(amount)}), 201, "CREATED")
	return r.Body["id"].(string)
}

func eur(acct string) int64 { return fake.Balance(acct, "EUR") }

// ---------------------------------------------------------------- payment lifecycle

func TestPaymentLifecycle(t *testing.T) {
	payer, payee := account(t, c1, 100_00), account(t, c1, 0)
	holds := eur(ledger.AuthHolds)
	id := payment(t, payer, payee, 60_00)

	want(t, post(t, "/v1/payments/"+id+"/authorise", nil), 200, "AUTHORISED")
	if eur(payer) != 40_00 || eur(ledger.AuthHolds)-holds != 60_00 {
		t.Fatalf("after authorise payer=%d holds+=%d", eur(payer), eur(ledger.AuthHolds)-holds)
	}
	want(t, post(t, "/v1/payments/"+id+"/capture", map[string]any{"amount_minor": 50_00}), 200, "CAPTURED")
	if eur(payer) != 50_00 || eur(payee) != 50_00 || eur(ledger.AuthHolds) != holds {
		t.Fatalf("after partial capture payer=%d payee=%d (remainder must be released)", eur(payer), eur(payee))
	}
	want(t, post(t, "/v1/payments/"+id+"/refunds", map[string]any{"amount_minor": 20_00}), 200, "PARTIALLY_REFUNDED")
	r := want(t, post(t, "/v1/payments/"+id+"/refunds", nil), 200, "REFUNDED")
	if eur(payer) != 100_00 || eur(payee) != 0 {
		t.Fatalf("after full refund payer=%d payee=%d", eur(payer), eur(payee))
	}
	if refunds := r.Body["refunds"].([]any); len(refunds) != 2 {
		t.Fatalf("want 2 refunds, got %v", refunds)
	}
	want(t, post(t, "/v1/payments/"+id+"/refunds", nil), 409)

	var events []string
	rows, _ := pool.Query(context.Background(), `SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id`, id)
	for rows.Next() {
		var e string
		_ = rows.Scan(&e)
		events = append(events, e)
	}
	rows.Close()
	wantEvents := []string{"payment.created", "payment.authorised", "payment.captured", "payment.partially_refunded", "payment.refunded"}
	if fmt.Sprint(events) != fmt.Sprint(wantEvents) {
		t.Fatalf("timeline %v, want %v", events, wantEvents)
	}
}

func TestDeclineAndCancel(t *testing.T) {
	payer, payee := account(t, c1, 10_00), account(t, c1, 0)
	declined := payment(t, payer, payee, 10_01)
	r := want(t, post(t, "/v1/payments/"+declined+"/authorise", nil), 402, "DECLINED")
	if r.Body["decline_reason"] != "insufficient_funds" {
		t.Fatalf("decline reason: %s", r.Raw)
	}
	want(t, post(t, "/v1/payments/"+declined+"/capture", nil), 409)

	voided := payment(t, payer, payee, 10_00)
	want(t, post(t, "/v1/payments/"+voided+"/authorise", nil), 200, "AUTHORISED")
	want(t, post(t, "/v1/payments/"+voided+"/cancel", nil), 200, "CANCELLED")
	if eur(payer) != 10_00 {
		t.Fatalf("void must release the hold, payer=%d", eur(payer))
	}
	never := payment(t, payer, payee, 1)
	want(t, post(t, "/v1/payments/"+never+"/cancel", nil), 200, "CANCELLED")
}

// ---------------------------------------------------------------- idempotency

func TestIdempotentReplayAndKeyReuse(t *testing.T) {
	payer, payee := account(t, c1, 10_00), account(t, c1, 0)
	body := map[string]any{"payer_account_id": payer, "payee_account_id": payee, "amount": money(5_00)}
	k := newKey()
	first := want(t, do(t, c1, "POST", "/v1/payments", k, body), 201)
	second := want(t, do(t, c1, "POST", "/v1/payments", k, body), 201)
	if !second.Replayed || first.Raw != second.Raw {
		t.Fatalf("replay must return the identical response:\n%s\n%s", first.Raw, second.Raw)
	}
	body["amount"] = money(5_01)
	want(t, do(t, c1, "POST", "/v1/payments", k, body), 422)
	// Keys are scoped per client: c2 may use the same key independently.
	other := account(t, c2, 0)
	want(t, do(t, c2, "POST", "/v1/payments", k, map[string]any{"payer_account_id": other, "payee_account_id": payee, "amount": money(1)}), 201)

	id := first.Body["id"].(string)
	ak := newKey()
	before := fake.Effects
	a1 := want(t, do(t, c1, "POST", "/v1/payments/"+id+"/authorise", ak, nil), 200)
	a2 := want(t, do(t, c1, "POST", "/v1/payments/"+id+"/authorise", ak, nil), 200)
	if !a2.Replayed || a1.Raw != a2.Raw || fake.Effects != before+1 {
		t.Fatalf("authorise replay: effects %d -> %d", before, fake.Effects)
	}
}

func TestConcurrentRequestsOneEffect(t *testing.T) {
	mu.Lock()
	defer mu.Unlock()
	payer, payee := account(t, c1, 10_00), account(t, c1, 0)
	id := payment(t, payer, payee, 7_00)
	before := fake.Effects

	// 20 identical retries racing (same key) plus 10 different keys racing for the same payment.
	sameKey := newKey()
	var wg sync.WaitGroup
	results := make(chan response, 30)
	for i := 0; i < 30; i++ {
		k := sameKey
		if i >= 20 {
			k = newKey()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- do(t, c1, "POST", "/v1/payments/"+id+"/authorise", k, nil)
		}()
	}
	wg.Wait()
	close(results)
	ok := 0
	for r := range results {
		switch r.Code {
		case 200:
			ok++
		case 409:
		default:
			t.Fatalf("unexpected %d: %s", r.Code, r.Raw)
		}
	}
	if ok == 0 || fake.Effects != before+1 || eur(payer) != 3_00 {
		t.Fatalf("ok=%d effects +%d payer=%d: want exactly one authorisation", ok, fake.Effects-before, eur(payer))
	}
}

// ---------------------------------------------------------------- failures and recovery

func age(t tb, table, id string) {
	t.Helper()
	col := "id"
	if table == "client_accounts" {
		col = "account_id"
	}
	_, err := pool.Exec(context.Background(), `UPDATE `+table+` SET updated_at = now() - interval '1 minute' WHERE `+col+` = $1`, id)
	must(err)
}

func TestLedgerOutageIsRecoveredExactlyOnce(t *testing.T) {
	mu.Lock()
	defer mu.Unlock()
	payer, payee := account(t, c1, 10_00), account(t, c1, 0)
	id := payment(t, payer, payee, 4_00)
	k := newKey()
	before := fake.Effects

	fake.FailNext(1)
	want(t, do(t, c1, "POST", "/v1/payments/"+id+"/authorise", k, nil), 503)
	want(t, do(t, c1, "GET", "/v1/payments/"+id, "", nil), 200, "AUTHORISING")
	// While the original request's lock is fresh, a retry is told to wait.
	want(t, do(t, c1, "POST", "/v1/payments/"+id+"/authorise", k, nil), 409)

	age(t, "payments", id)
	if n := svc.RecoverOnce(context.Background()); n == 0 {
		t.Fatal("recovery found nothing")
	}
	want(t, do(t, c1, "GET", "/v1/payments/"+id, "", nil), 200, "AUTHORISED")
	r := want(t, do(t, c1, "POST", "/v1/payments/"+id+"/authorise", k, nil), 200, "AUTHORISED")
	if !r.Replayed || fake.Effects != before+1 {
		t.Fatalf("replayed=%v effects +%d", r.Replayed, fake.Effects-before)
	}
}

func TestLostLedgerResponseDoesNotDoublePost(t *testing.T) {
	mu.Lock()
	defer mu.Unlock()
	payer, payee := account(t, c1, 10_00), account(t, c1, 0)
	id := payment(t, payer, payee, 4_00)
	before := fake.Effects

	fake.LoseNext(1) // ledger applies the posting but the response never arrives
	want(t, post(t, "/v1/payments/"+id+"/authorise", nil), 503)
	age(t, "payments", id)
	svc.RecoverOnce(context.Background())
	want(t, do(t, c1, "GET", "/v1/payments/"+id, "", nil), 200, "AUTHORISED")
	if fake.Effects != before+1 || eur(payer) != 6_00 {
		t.Fatalf("effects +%d payer=%d: the re-drive must reuse the ledger key", fake.Effects-before, eur(payer))
	}
}

func TestRetryTakesOverAfterLockExpiry(t *testing.T) {
	mu.Lock()
	defer mu.Unlock()
	payer, payee := account(t, c1, 10_00), account(t, c1, 0)
	id := payment(t, payer, payee, 4_00)
	k := newKey()
	fake.FailNext(1)
	want(t, do(t, c1, "POST", "/v1/payments/"+id+"/authorise", k, nil), 503)
	_, err := pool.Exec(context.Background(), `UPDATE idempotency_keys SET locked_at = now() - interval '1 minute' WHERE key = $1`, k)
	must(err)
	want(t, do(t, c1, "POST", "/v1/payments/"+id+"/authorise", k, nil), 200, "AUTHORISED")
}

// ---------------------------------------------------------------- transfers and the payout saga

func TestInternalTransfer(t *testing.T) {
	a, b := account(t, c1, 10_00), account(t, c1, 0)
	want(t, post(t, "/v1/transfers", map[string]any{"source_account_id": a, "destination": map[string]any{"type": "internal", "account_id": b},
		"amount": money(3_00)}), 201, "COMPLETED")
	want(t, post(t, "/v1/transfers", map[string]any{"source_account_id": a, "destination": map[string]any{"type": "internal", "account_id": b},
		"amount": money(7_01)}), 402, "FAILED")
	if eur(a) != 7_00 || eur(b) != 3_00 {
		t.Fatalf("a=%d b=%d", eur(a), eur(b))
	}
	want(t, post(t, "/v1/transfers", map[string]any{"source_account_id": a, "destination": map[string]any{"type": "internal", "account_id": "SYS-NOSTRO"},
		"amount": money(1)}), 400)
}

func payout(t tb, src, iban string, amount int64) string {
	r := want(t, post(t, "/v1/transfers", map[string]any{"source_account_id": src,
		"destination": map[string]any{"type": "external", "iban": iban, "name": "Beneficiary"}, "amount": money(amount)}), 202, "SUBMITTED")
	return r.Body["id"].(string)
}

func TestPayoutSagaSettlesOrCompensates(t *testing.T) {
	ctx := context.Background()
	src := account(t, c1, 10_00)
	clearing, nostro := eur(ledger.PayoutClearing), eur(ledger.Nostro)

	settled := payout(t, src, "DE89370400440532013000", 4_00)
	var instr int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE aggregate_id = $1 AND event_type = 'payout.instruction'`, settled).Scan(&instr)
	if instr != 1 || eur(src) != 6_00 || eur(ledger.PayoutClearing) != clearing+4_00 {
		t.Fatalf("reserve: instructions=%d src=%d", instr, eur(src))
	}
	must(svc.HandlePayoutResult(ctx, "msg-"+settled, payments.PayoutResult{TransferID: settled, Status: "SETTLED", RailReference: "R1"}))
	want(t, do(t, c1, "GET", "/v1/transfers/"+settled, "", nil), 200, "COMPLETED")
	if eur(ledger.PayoutClearing) != clearing || eur(ledger.Nostro) != nostro-4_00 {
		t.Fatalf("settle: clearing=%d nostro delta=%d", eur(ledger.PayoutClearing)-clearing, eur(ledger.Nostro)-nostro)
	}

	rejected := payout(t, src, "GB82WEST12345698765432", 5_00)
	must(svc.HandlePayoutResult(ctx, "msg-"+rejected, payments.PayoutResult{TransferID: rejected, Status: "REJECTED", ReasonCode: "AC04", Reason: "ClosedAccountNumber"}))
	r := want(t, do(t, c1, "GET", "/v1/transfers/"+rejected, "", nil), 200, "COMPENSATED")
	if r.Body["compensation_transaction_id"] == nil || eur(src) != 6_00 || eur(ledger.PayoutClearing) != clearing {
		t.Fatalf("compensation: %s src=%d", r.Raw, eur(src))
	}

	// Redelivery of the same message, and a conflicting late message, are both no-ops.
	before := fake.Effects
	must(svc.HandlePayoutResult(ctx, "msg-"+rejected, payments.PayoutResult{TransferID: rejected, Status: "REJECTED"}))
	must(svc.HandlePayoutResult(ctx, "other-msg", payments.PayoutResult{TransferID: rejected, Status: "SETTLED"}))
	if fake.Effects != before {
		t.Fatal("duplicate or late rail results must not move money")
	}
	want(t, do(t, c1, "GET", "/v1/transfers/"+rejected, "", nil), 200, "COMPENSATED")
}

func TestSagaStepRecoversAfterLedgerOutage(t *testing.T) {
	mu.Lock()
	defer mu.Unlock()
	ctx := context.Background()
	src := account(t, c1, 10_00)
	id := payout(t, src, "NL91ABNA0417164300", 2_00)
	fake.FailNext(1)
	must(svc.HandlePayoutResult(ctx, "msg-"+id, payments.PayoutResult{TransferID: id, Status: "REJECTED", ReasonCode: "MS03"}))
	want(t, do(t, c1, "GET", "/v1/transfers/"+id, "", nil), 200, "COMPENSATING")
	age(t, "transfers", id)
	svc.RecoverOnce(ctx)
	want(t, do(t, c1, "GET", "/v1/transfers/"+id, "", nil), 200, "COMPENSATED")
	if eur(src) != 10_00 {
		t.Fatalf("src=%d", eur(src))
	}
}

// ---------------------------------------------------------------- trust boundary

func TestOwnershipAndValidation(t *testing.T) {
	mine, theirs := account(t, c1, 10_00), account(t, c2, 10_00)
	want(t, post(t, "/v1/payments", map[string]any{"payer_account_id": theirs, "payee_account_id": mine, "amount": money(1)}), 404)
	want(t, post(t, "/v1/transfers", map[string]any{"source_account_id": theirs, "destination": map[string]any{"type": "internal", "account_id": mine}, "amount": money(1)}), 404)
	id := payment(t, mine, theirs, 1)
	want(t, do(t, c2, "GET", "/v1/payments/"+id, "", nil), 200)
	want(t, do(t, c2, "POST", "/v1/payments/"+id+"/authorise", newKey(), nil), 404)
	want(t, post(t, "/v1/payments/"+id+"/authorise", nil), 200)
	want(t, post(t, "/v1/payments/"+id+"/capture", nil), 404)
	want(t, do(t, c2, "POST", "/v1/payments/"+id+"/capture", newKey(), nil), 200)
	want(t, post(t, "/v1/payments/"+id+"/refunds", nil), 404)
	want(t, do(t, c2, "POST", "/v1/payments/"+id+"/refunds", newKey(), nil), 200)
	want(t, do(t, c2, "GET", "/v1/accounts/"+mine, "", nil), 404)
	want(t, post(t, "/v1/payments", map[string]any{"payer_account_id": mine, "payee_account_id": "acc_missing", "amount": money(1)}), 404)
	want(t, post(t, "/v1/payments", map[string]any{"payer_account_id": mine, "payee_account_id": "SYS-NOSTRO", "amount": money(1)}), 404)
	multi := accountWith(t, c1, 0, []map[string]any{
		{"currency": "EUR", "overdraft_limit_minor": 0},
		{"currency": "USD", "overdraft_limit_minor": 0},
	})
	want(t, post(t, "/v1/payments", map[string]any{
		"payer_account_id": multi, "payee_account_id": theirs,
		"amount": map[string]any{"amount_minor": 1, "currency": "USD"},
	}), 422)

	want(t, post(t, "/v1/payments", `{"payer_account_id":"`+mine+`","payee_account_id":"x","amount":{"amount_minor":10.5,"currency":"EUR"}}`), 400)
	want(t, post(t, "/v1/payments", `{"payer_account_id":"`+mine+`","payee_account_id":"x","amount":{"amount_minor":10,"currency":"EUR"},"extra":1}`), 400)
	want(t, post(t, "/v1/payments", map[string]any{"payer_account_id": mine, "payee_account_id": "x", "amount": map[string]any{"amount_minor": 1, "currency": "JPY"}}), 422)
	want(t, post(t, "/v1/payments", map[string]any{"payer_account_id": mine, "payee_account_id": "x", "amount": money(0)}), 422)
	want(t, do(t, c1, "POST", "/v1/payments", "", map[string]any{"payer_account_id": mine, "payee_account_id": "x", "amount": money(1)}), 400)
	want(t, do(t, "wrong-key-xxxxxxxxxxxx", "GET", "/v1/payments/"+id, "", nil), 401)
	want(t, post(t, "/v1/accounts", map[string]any{
		"name": "credit", "currencies": []map[string]any{{"currency": "EUR", "overdraft_limit_minor": 1}},
	}), 422)
	want(t, post(t, "/v1/transfers", map[string]any{"source_account_id": mine, "destination": map[string]any{"type": "external", "iban": "DE00000000000000000000", "name": "x"}, "amount": money(1)}), 400)
}

func TestDepositsAndOpsRequireAdminCredential(t *testing.T) {
	id := account(t, c1, 0)
	body := map[string]any{"account_id": id, "amount": money(100), "reference": "bank notification"}
	want(t, post(t, "/v1/deposits", body), 404)
	want(t, doAt(t, adminServer.URL, c1, "POST", "/ops/deposits", newKey(), body), 401)
	want(t, doAt(t, adminServer.URL, adminKey, "POST", "/ops/deposits", newKey(), body), 201)
	if eur(id) != 100 {
		t.Fatalf("admin deposit balance = %d", eur(id))
	}
	want(t, doAt(t, adminServer.URL, "", "GET", "/ops/payments", "", nil), 401)
	want(t, doAt(t, adminServer.URL, adminKey, "GET", "/ops/payments", "", nil), 200)
}

func TestRateLimitAndFailOpen(t *testing.T) {
	limited := "test-key-limited-cccccccc"
	codes := []int{}
	for i := 0; i < 5; i++ {
		codes = append(codes, do(t, limited, "GET", "/v1/payments/nope", "", nil).Code)
	}
	if fmt.Sprint(codes) != "[404 404 404 429 429]" {
		t.Fatalf("burst of 3 then 429, got %v", codes)
	}
	r := do(t, limited, "GET", "/v1/payments/nope", "", nil)
	if r.Header.Get("Retry-After") == "" {
		t.Fatal("429 must carry Retry-After")
	}

	// Redis unreachable: requests are still served (fail-open).
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond})
	h := (&Server{Svc: svc, Redis: dead, Clients: clients, Log: svc.Log}).Public()
	req := httptest.NewRequest("GET", "/v1/payments/nope", nil)
	req.Header.Set("Authorization", "Bearer "+limited)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("fail-open: got %d", rec.Code)
	}
}

// ---------------------------------------------------------------- property: random operations with retries

// TestRandomOperationsWithRetries drives random payment operations, retrying a random subset with the
// same key, while the ledger randomly fails or loses responses. Afterwards every retried key returned
// exactly its first response, and the ledger balances equal what the final payment states imply.
func TestRandomOperationsWithRetries(t *testing.T) {
	mu.Lock()
	defer mu.Unlock()
	rapid.Check(t, func(rt *rapid.T) {
		const funding = 1_000_00
		payer, payee := account(rt, c1, funding), account(rt, c1, 0)
		ids := []string{}
		for i := rapid.IntRange(1, 3).Draw(rt, "payments"); i > 0; i-- {
			ids = append(ids, payment(rt, payer, payee, rapid.Int64Range(1, 400_00).Draw(rt, "amount")))
		}
		type sent struct {
			path, key string
			body      any
			first     response
		}
		var history []sent
		ops := []string{"authorise", "capture", "cancel", "refunds"}
		for i := rapid.IntRange(1, 20).Draw(rt, "steps"); i > 0; i-- {
			if len(history) > 0 && rapid.Bool().Draw(rt, "retry") {
				h := history[rapid.IntRange(0, len(history)-1).Draw(rt, "which")]
				r := do(rt, c1, "POST", h.path, h.key, h.body)
				if h.first.Code < 500 && h.first.Code != 409 && (r.Code != h.first.Code || r.Raw != h.first.Raw) {
					rt.Fatalf("retry of %s %s changed the answer:\n%d %s\n%d %s", h.path, h.key, h.first.Code, h.first.Raw, r.Code, r.Raw)
				}
				continue
			}
			switch rapid.IntRange(0, 9).Draw(rt, "fault") {
			case 0:
				fake.FailNext(1)
			case 1:
				fake.LoseNext(1)
			}
			id := ids[rapid.IntRange(0, len(ids)-1).Draw(rt, "payment")]
			op := rapid.SampledFrom(ops).Draw(rt, "op")
			var body any
			if op == "capture" || op == "refunds" {
				if rapid.Bool().Draw(rt, "partial") {
					body = map[string]any{"amount_minor": rapid.Int64Range(1, 100_00).Draw(rt, "partialAmount")}
				}
			}
			k := newKey()
			path := "/v1/payments/" + id + "/" + op
			history = append(history, sent{path, k, body, do(rt, c1, "POST", path, k, body)})
		}
		fake.FailNext(0)
		fake.LoseNext(0)
		for _, id := range ids {
			age(rt, "payments", id)
		}
		svc.RecoverOnce(context.Background())

		// Balances implied by the final states.
		var held, captured, refunded int64
		for _, id := range ids {
			p := want(rt, do(rt, c1, "GET", "/v1/payments/"+id, "", nil), 200).Body
			if s := p["status"].(string); payments.Status(s).InFlight() {
				rt.Fatalf("payment %s still in flight after recovery: %v", id, p)
			}
			amount := int64(p["amount"].(map[string]any)["amount_minor"].(float64))
			if p["status"] == "AUTHORISED" {
				held += amount
			}
			captured += int64(p["captured_amount_minor"].(float64))
			refunded += int64(p["refunded_amount_minor"].(float64))
		}
		if got, wantPayer := eur(payer), funding-held-captured+refunded; got != wantPayer {
			rt.Fatalf("payer balance %d, states imply %d (held %d captured %d refunded %d)", got, wantPayer, held, captured, refunded)
		}
		if got := eur(payee); got != captured-refunded {
			rt.Fatalf("payee balance %d, states imply %d", got, captured-refunded)
		}
	})
}
