package io.meridian.ledger;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Read-only HTTP API for the ops console, the statement generator and invariant checks. */
@RestController
@RequestMapping("/api")
public class QueryController {
    private final JdbcClient jdbc;
    private final LedgerService ledger;

    public QueryController(JdbcClient jdbc, LedgerService ledger) {
        this.jdbc = jdbc;
        this.ledger = ledger;
    }

    public record Entry(long postingId, UUID transactionId, String type, String description, Instant bookedAt,
                        String currency, long amount, long balanceChange, long runningBalance, String idempotencyKey) {}

    @GetMapping("/accounts")
    public List<Model.Account> accounts(@RequestParam(defaultValue = "200") int limit) {
        return jdbc.sql("SELECT id FROM accounts ORDER BY id LIMIT :limit")
                .param("limit", clamp(limit, 1000))
                .query(String.class).list().stream()
                .map(ledger::getAccount).toList();
    }

    @GetMapping("/accounts/{id}")
    public Model.Account account(@PathVariable String id) {
        return ledger.getAccount(id);
    }

    /**
     * Statement lines with a running balance in the account's normal sign.
     * ponytail: the running balance is a window over the account's full history; fine for this
     * scale, needs periodic balance snapshots once accounts carry millions of postings.
     */
    @GetMapping("/accounts/{id}/entries")
    public List<Entry> entries(@PathVariable String id, @RequestParam String currency,
                               @RequestParam(required = false) Instant from, @RequestParam(required = false) Instant to,
                               @RequestParam(defaultValue = "500") int limit) {
        ledger.getAccount(id); // 404 for unknown accounts
        return jdbc.sql("""
                        SELECT * FROM (
                          SELECT p.id, p.transaction_id, t.type, t.description, t.booked_at, p.currency, p.amount,
                                 CASE WHEN a.type IN ('ASSET','EXPENSE') THEN p.amount ELSE -p.amount END AS change,
                                 sum(CASE WHEN a.type IN ('ASSET','EXPENSE') THEN p.amount ELSE -p.amount END)
                                     OVER (ORDER BY p.id) AS running,
                                 t.idempotency_key
                          FROM postings p
                          JOIN ledger_transactions t ON t.id = p.transaction_id
                          JOIN accounts a ON a.id = p.account_id
                          WHERE p.account_id = :id AND p.currency = :cur
                        ) s
                        WHERE (CAST(:from AS timestamptz) IS NULL OR booked_at >= :from)
                          AND (CAST(:to AS timestamptz) IS NULL OR booked_at < :to)
                        ORDER BY id LIMIT :limit""")
                .param("id", id).param("cur", currency)
                .param("from", from == null ? null : Timestamp.from(from))
                .param("to", to == null ? null : Timestamp.from(to))
                .param("limit", clamp(limit, 10_000))
                .query((rs, n) -> new Entry(rs.getLong(1), rs.getObject(2, UUID.class), rs.getString(3), rs.getString(4),
                        rs.getTimestamp(5).toInstant(), rs.getString(6), rs.getLong(7), rs.getLong(8), rs.getLong(9),
                        rs.getString(10)))
                .list();
    }

    @GetMapping("/transactions")
    public List<Model.Transaction> transactions(@RequestParam(defaultValue = "50") int limit) {
        return jdbc.sql("SELECT id FROM ledger_transactions ORDER BY booked_at DESC, id LIMIT :limit")
                .param("limit", clamp(limit, 500))
                .query(UUID.class).list().stream()
                .map(ledger::getTransaction).toList();
    }

    @GetMapping("/transactions/{id}")
    public Model.Transaction transaction(@PathVariable UUID id) {
        return ledger.getTransaction(id);
    }

    /** Recomputes I1, I2 and I7 from the raw postings. All counts must be zero. */
    @GetMapping("/invariants")
    public Map<String, Object> invariants() {
        long unbalanced = count("""
                SELECT count(*) FROM (SELECT 1 FROM postings GROUP BY transaction_id, currency HAVING sum(amount) <> 0) x""");
        long balanceMismatches = count("""
                SELECT count(*) FROM account_balances b JOIN accounts a ON a.id = b.account_id
                LEFT JOIN (SELECT account_id, currency, sum(amount) AS s FROM postings GROUP BY 1, 2) p
                       ON p.account_id = b.account_id AND p.currency = b.currency
                WHERE b.balance <> (CASE WHEN a.type IN ('ASSET','EXPENSE') THEN 1 ELSE -1 END) * coalesce(p.s, 0)""");
        long limitBreaches = count("""
                SELECT count(*) FROM account_balances WHERE overdraft_limit IS NOT NULL AND balance < -overdraft_limit""");
        long doubleReversals = count("""
                SELECT count(*) FROM (SELECT reverses_id FROM ledger_transactions WHERE reverses_id IS NOT NULL
                                      GROUP BY 1 HAVING count(*) > 1) x""");
        var result = new LinkedHashMap<String, Object>();
        result.put("ok", unbalanced + balanceMismatches + limitBreaches + doubleReversals == 0);
        result.put("unbalanced_transactions", unbalanced);
        result.put("balance_mismatches", balanceMismatches);
        result.put("limit_breaches", limitBreaches);
        result.put("double_reversals", doubleReversals);
        result.put("transactions", count("SELECT count(*) FROM ledger_transactions"));
        result.put("postings", count("SELECT count(*) FROM postings"));
        result.put("outbox_pending", count("SELECT count(*) FROM outbox WHERE published_at IS NULL"));
        result.put("checked_at", Instant.now().toString());
        return result;
    }

    @ExceptionHandler(LedgerException.class)
    public ProblemDetail handle(LedgerException e) {
        HttpStatus status = switch (e.reason) {
            case ACCOUNT_NOT_FOUND, TRANSACTION_NOT_FOUND -> HttpStatus.NOT_FOUND;
            default -> HttpStatus.BAD_REQUEST;
        };
        var pd = ProblemDetail.forStatusAndDetail(status, e.getMessage());
        pd.setProperty("code", e.reason.name());
        return pd;
    }

    private long count(String sql) {
        return jdbc.sql(sql).query(Long.class).single();
    }

    private static int clamp(int limit, int max) {
        return Math.max(1, Math.min(limit, max));
    }
}
