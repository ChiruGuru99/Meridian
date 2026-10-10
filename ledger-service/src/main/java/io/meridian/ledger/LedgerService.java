package io.meridian.ledger;

import io.meridian.ledger.LedgerException.Reason;
import io.meridian.ledger.Model.Account;
import io.meridian.ledger.Model.AccountType;
import io.meridian.ledger.Model.Balance;
import io.meridian.ledger.Model.CurrencyLimit;
import io.meridian.ledger.Model.PostCommand;
import io.meridian.ledger.Model.PostingLine;
import io.meridian.ledger.Model.Transaction;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * The write side of the ledger. Every money movement goes through {@link #post} or {@link #reverse}.
 *
 * <p>Concurrency: balance rows are locked with SELECT ... FOR UPDATE in a fixed (account, currency)
 * order, so two transactions touching the same accounts serialise without deadlocking (I2).
 * Idempotency: the transaction row is inserted first with ON CONFLICT DO NOTHING on the
 * idempotency key; a concurrent duplicate blocks on the unique index and then sees the winner (I3).
 */
@Service
public class LedgerService {
    public static final String TOPIC = "ledger.transactions.v1";
    private static final Pattern CURRENCY = Pattern.compile("^[A-Z]{3}$");
    private static final Pattern ACCOUNT_ID = Pattern.compile("^[A-Za-z0-9_-]{1,64}$");

    private final JdbcClient jdbc;
    private final JsonMapper json;
    private final MeterRegistry metrics;

    public LedgerService(JdbcClient jdbc, JsonMapper json, MeterRegistry metrics) {
        this.jdbc = jdbc;
        this.json = json;
        this.metrics = metrics;
    }

    public record PostResult(Transaction transaction, boolean replayed) {}

    private record BalanceKey(String accountId, String currency) implements Comparable<BalanceKey> {
        public int compareTo(BalanceKey o) {
            int c = accountId.compareTo(o.accountId);
            return c != 0 ? c : currency.compareTo(o.currency);
        }
    }

    // ---------------------------------------------------------------- accounts

    /** Idempotent on id: creating an existing id returns the existing account unchanged. */
    @Transactional
    public Account createAccount(String id, String name, AccountType type, List<CurrencyLimit> currencies) {
        if (id == null || id.isBlank()) {
            id = "acc_" + UUID.randomUUID().toString().replace("-", "");
        }
        if (!ACCOUNT_ID.matcher(id).matches()) throw invalid("invalid account id");
        if (name == null || name.isBlank() || name.length() > 200) throw invalid("name must be 1..200 chars");
        if (type == null) throw invalid("account type is required");
        if (currencies == null || currencies.isEmpty()) throw invalid("at least one currency is required");
        var seen = new TreeSet<String>();
        for (CurrencyLimit c : currencies) {
            if (c.currency() == null || !CURRENCY.matcher(c.currency()).matches()) throw invalid("invalid currency " + c.currency());
            if (!seen.add(c.currency())) throw invalid("duplicate currency " + c.currency());
            if (c.overdraftLimit() != null && c.overdraftLimit() < 0) throw invalid("overdraft limit must be >= 0");
        }

        int inserted = jdbc.sql("INSERT INTO accounts (id, name, type) VALUES (:id, :name, :type) ON CONFLICT (id) DO NOTHING")
                .param("id", id).param("name", name).param("type", type.name())
                .update();
        if (inserted == 1) {
            for (CurrencyLimit c : currencies) {
                jdbc.sql("INSERT INTO account_balances (account_id, currency, overdraft_limit) VALUES (:id, :cur, :lim)")
                        .param("id", id).param("cur", c.currency()).param("lim", c.overdraftLimit())
                        .update();
            }
        }
        return getAccount(id);
    }

    public Account getAccount(String id) {
        var type = jdbc.sql("SELECT name, type FROM accounts WHERE id = :id").param("id", id)
                .query((rs, n) -> new String[]{rs.getString(1), rs.getString(2)}).optional()
                .orElseThrow(() -> new LedgerException(Reason.ACCOUNT_NOT_FOUND, "account " + id + " not found"));
        List<Balance> balances = jdbc.sql("""
                        SELECT currency, balance, overdraft_limit, version FROM account_balances
                        WHERE account_id = :id ORDER BY currency""")
                .param("id", id)
                .query((rs, n) -> new Balance(rs.getString(1), rs.getLong(2), (Long) rs.getObject(3), rs.getLong(4)))
                .list();
        return new Account(id, type[0], AccountType.valueOf(type[1]), balances);
    }

    // ---------------------------------------------------------------- postings

    @Transactional
    public PostResult post(PostCommand cmd) {
        return postInternal(cmd, null);
    }

    /** Corrections are new transactions with negated postings; the original is never touched (I4). */
    @Transactional
    public PostResult reverse(String idempotencyKey, UUID transactionId, String reason) {
        Transaction original = getTransaction(transactionId);
        if (original.reversesId() != null) throw invalid("a reversal cannot itself be reversed");
        List<PostingLine> negated = original.postings().stream()
                .map(p -> new PostingLine(p.accountId(), p.currency(), -p.amount()))
                .toList();
        var cmd = new PostCommand(idempotencyKey, "REVERSAL", reason == null ? "" : reason, negated,
                Map.of("reverses", transactionId.toString()));
        return postInternal(cmd, transactionId);
    }

    private PostResult postInternal(PostCommand cmd, UUID reversesId) {
        String key = cmd.idempotencyKey();
        if (key == null || key.isBlank() || key.length() > 255) throw invalid("idempotency key must be 1..255 chars");
        if (cmd.type() == null || cmd.type().isBlank() || cmd.type().length() > 64) throw invalid("type must be 1..64 chars");
        PostingRules.validate(cmd.postings());
        Map<String, String> metadata = cmd.metadata() == null ? Map.of() : cmd.metadata();
        String hash = requestHash(cmd, metadata, reversesId);

        UUID id = UUID.randomUUID();
        Optional<Timestamp> booked;
        try {
            booked = jdbc.sql("""
                            INSERT INTO ledger_transactions (id, idempotency_key, request_hash, type, description, reverses_id, metadata)
                            VALUES (:id, :key, :hash, :type, :desc, :rev, CAST(:meta AS jsonb))
                            ON CONFLICT (idempotency_key) DO NOTHING
                            RETURNING booked_at""")
                    .param("id", id).param("key", key).param("hash", hash).param("type", cmd.type())
                    .param("desc", cmd.description() == null ? "" : cmd.description())
                    .param("rev", reversesId).param("meta", json.writeValueAsString(metadata))
                    .query(Timestamp.class).optional();
        } catch (DuplicateKeyException e) {
            // UNIQUE(reverses_id): someone already reversed this transaction under a different key.
            throw reject(Reason.ALREADY_REVERSED, "transaction " + reversesId + " is already reversed");
        }

        if (booked.isEmpty()) {
            var existing = jdbc.sql("SELECT id, request_hash FROM ledger_transactions WHERE idempotency_key = :key")
                    .param("key", key)
                    .query((rs, n) -> Map.entry(rs.getObject(1, UUID.class), rs.getString(2)))
                    .single();
            if (!existing.getValue().equals(hash)) {
                throw reject(Reason.IDEMPOTENCY_KEY_REUSED, "idempotency key " + key + " was used with a different request");
            }
            metrics.counter("ledger.transactions.replayed").increment();
            return new PostResult(getTransaction(existing.getKey()), true);
        }

        applyPostings(id, cmd.postings());
        var txn = new Transaction(id, key, cmd.type(), cmd.description() == null ? "" : cmd.description(),
                cmd.postings(), metadata, reversesId, booked.get().toInstant());
        writeOutbox(txn);
        metrics.counter("ledger.transactions.posted", "type", cmd.type()).increment();
        return new PostResult(txn, false);
    }

    private void applyPostings(UUID txnId, List<PostingLine> postings) {
        var deltas = new TreeMap<BalanceKey, Long>();
        for (PostingLine p : postings) {
            deltas.merge(new BalanceKey(p.accountId(), p.currency()), p.amount(), Math::addExact);
        }
        // Lock in sorted key order: every writer acquires locks in the same order, so no deadlock cycle can form.
        for (var e : deltas.entrySet()) {
            BalanceKey k = e.getKey();
            long delta = e.getValue();
            var row = jdbc.sql("""
                            SELECT b.balance, b.overdraft_limit, a.type
                            FROM account_balances b JOIN accounts a ON a.id = b.account_id
                            WHERE b.account_id = :acc AND b.currency = :cur
                            FOR UPDATE OF b""")
                    .param("acc", k.accountId()).param("cur", k.currency())
                    .query((rs, n) -> new Object[]{rs.getLong(1), rs.getObject(2), AccountType.valueOf(rs.getString(3))})
                    .optional()
                    .orElseThrow(() -> missingBalance(k));
            long balance = (long) row[0];
            Long limit = (Long) row[1];
            long change = Math.multiplyExact((long) ((AccountType) row[2]).sign, delta);
            long next = Math.addExact(balance, change);
            if (change < 0 && limit != null && next < -limit) {
                throw reject(Reason.INSUFFICIENT_FUNDS, "insufficient funds on " + k.accountId() + " " + k.currency()
                        + ": balance " + balance + ", limit " + limit + ", requested " + (-change));
            }
            jdbc.sql("""
                            UPDATE account_balances SET balance = :next, version = version + 1, updated_at = now()
                            WHERE account_id = :acc AND currency = :cur""")
                    .param("next", next).param("acc", k.accountId()).param("cur", k.currency())
                    .update();
        }
        int seq = 0;
        for (PostingLine p : postings) {
            jdbc.sql("INSERT INTO postings (transaction_id, seq, account_id, currency, amount) VALUES (:t, :s, :a, :c, :m)")
                    .param("t", txnId).param("s", seq++).param("a", p.accountId()).param("c", p.currency()).param("m", p.amount())
                    .update();
        }
    }

    private LedgerException missingBalance(BalanceKey k) {
        boolean accountExists = jdbc.sql("SELECT count(*) FROM accounts WHERE id = :id").param("id", k.accountId())
                .query(Long.class).single() > 0;
        return accountExists
                ? reject(Reason.CURRENCY_NOT_ENABLED, "account " + k.accountId() + " has no " + k.currency() + " balance")
                : reject(Reason.ACCOUNT_NOT_FOUND, "account " + k.accountId() + " not found");
    }

    private void writeOutbox(Transaction txn) {
        var postings = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < txn.postings().size(); i++) {
            PostingLine p = txn.postings().get(i);
            postings.add(Map.of("seq", i, "account_id", p.accountId(), "currency", p.currency(), "amount", p.amount()));
        }
        var t = new LinkedHashMap<String, Object>();
        t.put("id", txn.id().toString());
        t.put("idempotency_key", txn.idempotencyKey());
        t.put("type", txn.type());
        t.put("description", txn.description());
        t.put("reverses_id", txn.reversesId() == null ? null : txn.reversesId().toString());
        t.put("booked_at", txn.bookedAt().toString());
        t.put("metadata", txn.metadata());
        t.put("postings", postings);
        var event = new LinkedHashMap<String, Object>();
        event.put("event_id", UUID.randomUUID().toString());
        event.put("event_type", "ledger.transaction.posted");
        event.put("occurred_at", txn.bookedAt().toString());
        event.put("transaction", t);

        // Carry the caller's trace context so the consumer's span joins the original request's trace.
        var headers = new HashMap<String, String>();
        W3CTraceContextPropagator.getInstance().inject(Context.current(), headers, Map::put);

        jdbc.sql("""
                        INSERT INTO outbox (topic, message_key, event_type, payload, headers)
                        VALUES (:topic, :key, :type, CAST(:payload AS jsonb), CAST(:headers AS jsonb))""")
                .param("topic", TOPIC).param("key", txn.id().toString()).param("type", "ledger.transaction.posted")
                .param("payload", json.writeValueAsString(event)).param("headers", json.writeValueAsString(headers))
                .update();
    }

    // ---------------------------------------------------------------- reads

    public Transaction getTransaction(UUID id) {
        var header = jdbc.sql("""
                        SELECT idempotency_key, type, description, metadata::text, reverses_id, booked_at
                        FROM ledger_transactions WHERE id = :id""")
                .param("id", id)
                .query((rs, n) -> new Object[]{rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getObject(5, UUID.class), rs.getTimestamp(6).toInstant()})
                .optional()
                .orElseThrow(() -> new LedgerException(Reason.TRANSACTION_NOT_FOUND, "transaction " + id + " not found"));
        List<PostingLine> postings = jdbc.sql("SELECT account_id, currency, amount FROM postings WHERE transaction_id = :id ORDER BY seq")
                .param("id", id)
                .query((rs, n) -> new PostingLine(rs.getString(1), rs.getString(2), rs.getLong(3)))
                .list();
        Map<String, String> metadata = json.readValue((String) header[3], new TypeReference<Map<String, String>>() {});
        return new Transaction(id, (String) header[0], (String) header[1], (String) header[2], postings, metadata,
                (UUID) header[4], (Instant) header[5]);
    }

    // ---------------------------------------------------------------- helpers

    static String requestHash(PostCommand cmd, Map<String, String> metadata, UUID reversesId) {
        var sb = new StringBuilder();
        sb.append(cmd.type()).append('\n').append(cmd.description() == null ? "" : cmd.description()).append('\n')
                .append(reversesId).append('\n');
        for (PostingLine p : cmd.postings()) {
            sb.append(p.accountId()).append('|').append(p.currency()).append('|').append(p.amount()).append('\n');
        }
        new TreeMap<>(metadata).forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private LedgerException invalid(String message) {
        return reject(Reason.INVALID_ARGUMENT, message);
    }

    private LedgerException reject(Reason reason, String message) {
        metrics.counter("ledger.transactions.rejected", "reason", reason.name()).increment();
        return new LedgerException(reason, message);
    }
}
