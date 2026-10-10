package io.meridian.ledger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.protobuf.StatusProto;
import io.meridian.ledger.LedgerException.Reason;
import io.meridian.ledger.Model.PostCommand;
import io.meridian.ledger.Model.PostingLine;
import io.meridian.ledger.grpc.v1.LedgerServiceGrpc;
import io.meridian.ledger.grpc.v1.PostTransactionRequest;
import io.meridian.ledger.grpc.v1.Posting;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.meridian.ledger.TestEnv.balance;
import static io.meridian.ledger.TestEnv.fund;
import static io.meridian.ledger.TestEnv.key;
import static io.meridian.ledger.TestEnv.ledger;
import static io.meridian.ledger.TestEnv.newAccount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Ledger behaviour against a real Postgres, including attacks that bypass the application. */
class LedgerIntegrationTest {

    private static PostCommand transfer(String key, String from, String to, String currency, long amount) {
        return new PostCommand(key, "TRANSFER", "test", List.of(
                new PostingLine(from, currency, amount), new PostingLine(to, currency, -amount)), Map.of());
    }

    private static Reason reasonOf(Runnable r) {
        try {
            r.run();
        } catch (LedgerException e) {
            return e.reason;
        }
        throw new AssertionError("expected a LedgerException");
    }

    @Test
    void transferMovesMoneyAndRecordsBalancedPostings() {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR");
        fund(a, "EUR", 10_000);
        var result = ledger().post(transfer(key(), a, b, "EUR", 2_500));
        assertThat(result.replayed()).isFalse();
        assertThat(balance(a, "EUR")).isEqualTo(7_500);
        assertThat(balance(b, "EUR")).isEqualTo(2_500);
        var stored = ledger().getTransaction(result.transaction().id());
        assertThat(stored.postings()).extracting(PostingLine::amount).containsExactly(2_500L, -2_500L);
    }

    @Test
    void sameKeySamePayloadReplaysWithoutSecondEffect() {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR");
        fund(a, "EUR", 1_000);
        String k = key();
        var first = ledger().post(transfer(k, a, b, "EUR", 400));
        var second = ledger().post(transfer(k, a, b, "EUR", 400));
        assertThat(second.replayed()).isTrue();
        assertThat(second.transaction().id()).isEqualTo(first.transaction().id());
        assertThat(balance(a, "EUR")).isEqualTo(600);
        long postings = TestEnv.app().sql("SELECT count(*) FROM postings WHERE transaction_id = :id")
                .param("id", first.transaction().id()).query(Long.class).single();
        assertThat(postings).isEqualTo(2);
    }

    @Test
    void sameKeyDifferentPayloadIsRejected() {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR");
        fund(a, "EUR", 1_000);
        String k = key();
        ledger().post(transfer(k, a, b, "EUR", 400));
        assertThat(reasonOf(() -> ledger().post(transfer(k, a, b, "EUR", 401)))).isEqualTo(Reason.IDEMPOTENCY_KEY_REUSED);
        assertThat(balance(a, "EUR")).isEqualTo(600);
    }

    @Test
    void concurrentDuplicatesOfOneKeyProduceExactlyOneTransaction() throws Exception {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR");
        fund(a, "EUR", 1_000);
        String k = key();
        Set<UUID> ids = ConcurrentHashMap.newKeySet();
        var start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(16)) {
            for (int i = 0; i < 16; i++) {
                pool.submit(() -> {
                    start.await();
                    ids.add(ledger().post(transfer(k, a, b, "EUR", 100)).transaction().id());
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(ids).hasSize(1);
        assertThat(balance(a, "EUR")).isEqualTo(900);
        assertThat(balance(b, "EUR")).isEqualTo(100);
    }

    @Test
    void insufficientFundsRejectsAtomically() {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR");
        fund(a, "EUR", 100);
        String k = key();
        assertThat(reasonOf(() -> ledger().post(transfer(k, a, b, "EUR", 101)))).isEqualTo(Reason.INSUFFICIENT_FUNDS);
        assertThat(balance(a, "EUR")).isEqualTo(100);
        assertThat(balance(b, "EUR")).isZero();
        // Nothing of the rejected attempt survives, not even the idempotency key.
        long rows = TestEnv.app().sql("SELECT count(*) FROM ledger_transactions WHERE idempotency_key = :k").param("k", k).query(Long.class).single();
        assertThat(rows).isZero();
    }

    @Test
    void overdraftLimitIsHonouredExactly() {
        String a = newAccount("A", 5_000L, "EUR"), b = newAccount("B", 0L, "EUR");
        ledger().post(transfer(key(), a, b, "EUR", 5_000));
        assertThat(balance(a, "EUR")).isEqualTo(-5_000);
        assertThat(reasonOf(() -> ledger().post(transfer(key(), a, b, "EUR", 1)))).isEqualTo(Reason.INSUFFICIENT_FUNDS);
    }

    @Test
    void unknownAccountAndDisabledCurrencyAreRejected() {
        String a = newAccount("A", 0L, "EUR");
        fund(a, "EUR", 100);
        assertThat(reasonOf(() -> ledger().post(transfer(key(), a, "nope_" + UUID.randomUUID().toString().substring(0, 8), "EUR", 1))))
                .isEqualTo(Reason.ACCOUNT_NOT_FOUND);
        String usdOnly = newAccount("U", 0L, "USD");
        assertThat(reasonOf(() -> ledger().post(transfer(key(), a, usdOnly, "EUR", 1)))).isEqualTo(Reason.CURRENCY_NOT_ENABLED);
    }

    @Test
    void reversalRestoresBalancesAndCanHappenOnlyOnce() {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR");
        fund(a, "EUR", 1_000);
        var original = ledger().post(transfer(key(), a, b, "EUR", 300)).transaction();
        String rk = key();
        var reversal = ledger().reverse(rk, original.id(), "customer dispute").transaction();
        assertThat(reversal.reversesId()).isEqualTo(original.id());
        assertThat(reversal.postings()).extracting(PostingLine::amount).containsExactly(-300L, 300L);
        assertThat(balance(a, "EUR")).isEqualTo(1_000);
        assertThat(balance(b, "EUR")).isZero();
        // Retrying the same reversal is a replay; a second, different reversal is refused.
        assertThat(ledger().reverse(rk, original.id(), "customer dispute").replayed()).isTrue();
        assertThat(reasonOf(() -> ledger().reverse(key(), original.id(), "again"))).isEqualTo(Reason.ALREADY_REVERSED);
        assertThat(reasonOf(() -> ledger().reverse(key(), reversal.id(), "reverse the reversal"))).isEqualTo(Reason.INVALID_ARGUMENT);
        // The original is untouched (I4).
        assertThat(ledger().getTransaction(original.id()).postings()).extracting(PostingLine::amount).containsExactly(300L, -300L);
    }

    @Test
    void reversalIsSubjectToFundsCheck() {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR"), c = newAccount("C", 0L, "EUR");
        fund(a, "EUR", 500);
        var t = ledger().post(transfer(key(), a, b, "EUR", 500)).transaction();
        ledger().post(transfer(key(), b, c, "EUR", 500)); // B spent the money
        assertThat(reasonOf(() -> ledger().reverse(key(), t.id(), "too late"))).isEqualTo(Reason.INSUFFICIENT_FUNDS);
    }

    @Test
    void everyTransactionWritesExactlyOneOutboxEvent() {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR");
        fund(a, "EUR", 100);
        var t = ledger().post(transfer(key(), a, b, "EUR", 10)).transaction();
        String payload = TestEnv.app().sql("SELECT payload::text FROM outbox WHERE message_key = :k")
                .param("k", t.id().toString()).query(String.class).single();
        assertThat(payload).contains("\"event_type\": \"ledger.transaction.posted\"").contains(t.id().toString());
    }

    // ---------------------------------------------------------------- the database defends itself

    @Test
    void databaseRejectsUnbalancedTransactionAtCommitEvenForTheOwner() {
        String a = newAccount("A", null, "EUR");
        UUID id = UUID.randomUUID();
        // One statement = one transaction: the deferred trigger fires at its commit and rolls everything back.
        assertThatThrownBy(() -> TestEnv.owner().sql("""
                        WITH t AS (INSERT INTO ledger_transactions (id, idempotency_key, request_hash, type)
                                   VALUES (:id, :k, 'x', 'RAW') RETURNING id)
                        INSERT INTO postings (transaction_id, seq, account_id, currency, amount)
                        SELECT id, 0, :a, 'EUR', 100 FROM t""")
                .param("id", id).param("k", key()).param("a", a).update())
                .isInstanceOf(DataAccessException.class).hasStackTraceContaining("unbalanced");
        long rows = TestEnv.app().sql("SELECT count(*) FROM ledger_transactions WHERE id = :id").param("id", id).query(Long.class).single();
        assertThat(rows).isZero();
    }

    @Test
    void postingsAndTransactionsCannotBeUpdatedDeletedOrTruncated() {
        String a = newAccount("A", 0L, "EUR");
        fund(a, "EUR", 100);
        var owner = TestEnv.owner();
        assertThatThrownBy(() -> owner.sql("UPDATE postings SET amount = amount * 2 WHERE account_id = :a").param("a", a).update())
                .hasStackTraceContaining("append-only");
        assertThatThrownBy(() -> owner.sql("DELETE FROM postings WHERE account_id = :a").param("a", a).update())
                .hasStackTraceContaining("append-only");
        assertThatThrownBy(() -> owner.sql("TRUNCATE postings CASCADE").update()).hasStackTraceContaining("append-only");
        assertThatThrownBy(() -> owner.sql("UPDATE ledger_transactions SET description = 'edited'").update()).hasStackTraceContaining("append-only");
        assertThatThrownBy(() -> owner.sql("DELETE FROM ledger_transactions").update()).hasStackTraceContaining("append-only");
        // The application role does not even hold the privilege.
        assertThatThrownBy(() -> TestEnv.app().sql("UPDATE postings SET amount = 1 WHERE account_id = :a").param("a", a).update())
                .hasStackTraceContaining("permission denied");
        assertThatThrownBy(() -> TestEnv.app().sql("DELETE FROM ledger_transactions WHERE false").update())
                .hasStackTraceContaining("permission denied");
    }

    @Test
    void balanceCheckConstraintBlocksOverdraftWritesThatBypassTheService() {
        String a = newAccount("A", 0L, "EUR");
        assertThatThrownBy(() -> TestEnv.app().sql("UPDATE account_balances SET balance = -1 WHERE account_id = :a").param("a", a).update())
                .hasStackTraceContaining("balance_within_limit");
    }

    // ---------------------------------------------------------------- read side and gRPC adapter

    @Test
    void statementShowsRunningBalance() {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR");
        Instant before = Instant.now().minusSeconds(1);
        fund(a, "EUR", 1_000);
        ledger().post(transfer(key(), a, b, "EUR", 300));
        var entries = TestEnv.bean(QueryController.class).entries(a, "EUR", before, null, 100);
        assertThat(entries).extracting(QueryController.Entry::balanceChange).containsExactly(1_000L, -300L);
        assertThat(entries).extracting(QueryController.Entry::runningBalance).containsExactly(1_000L, 700L);
    }

    @Test
    void invariantsHoldAcrossEverythingWritten() {
        var inv = TestEnv.bean(QueryController.class).invariants();
        assertThat(inv).containsEntry("ok", true);
    }

    @Test
    void grpcSurfacesBusinessRejectionsAsStatusWithErrorInfo() throws InvalidProtocolBufferException {
        String a = newAccount("A", 0L, "EUR"), b = newAccount("B", 0L, "EUR");
        int port = TestEnv.bean(GrpcServer.class).port();
        ManagedChannel ch = NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        try {
            var stub = LedgerServiceGrpc.newBlockingStub(ch);
            var req = PostTransactionRequest.newBuilder().setIdempotencyKey(key()).setType("TRANSFER")
                    .addPostings(Posting.newBuilder().setAccountId(a).setCurrency("EUR").setAmount(1))
                    .addPostings(Posting.newBuilder().setAccountId(b).setCurrency("EUR").setAmount(-1))
                    .build();
            StatusRuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(StatusRuntimeException.class, () -> stub.postTransaction(req));
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            var info = StatusProto.fromThrowable(e).getDetails(0).unpack(ErrorInfo.class);
            assertThat(info.getReason()).isEqualTo("INSUFFICIENT_FUNDS");

            fund(a, "EUR", 5);
            var ok = stub.postTransaction(req);
            assertThat(ok.getPostingsCount()).isEqualTo(2);
            assertThat(stub.postTransaction(req).getId()).isEqualTo(ok.getId()); // replay over the wire
        } finally {
            ch.shutdownNow();
        }
    }
}
