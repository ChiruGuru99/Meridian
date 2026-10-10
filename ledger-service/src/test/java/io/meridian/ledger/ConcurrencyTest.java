package io.meridian.ledger;

import io.meridian.ledger.LedgerException.Reason;
import io.meridian.ledger.Model.PostCommand;
import io.meridian.ledger.Model.PostingLine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Thousands of parallel transfers across a handful of hot accounts. Many must be refused for
 * insufficient funds; none may fail any other way (no deadlocks, no lock timeouts, no constraint
 * violations), money is conserved, and no balance ever ends below its limit.
 */
class ConcurrencyTest {
    static final int ACCOUNTS = 5, TRANSFERS = 4_000, THREADS = 32;
    static final long FUNDING = 100_000;

    @Test
    void parallelTransfersPreserveEveryInvariant() throws Exception {
        LedgerService ledger = TestEnv.ledger();
        List<String> accounts = new ArrayList<>();
        long[] limits = {0, 0, 0, 0, 25_000}; // one account may go overdrawn up to 250.00
        for (int i = 0; i < ACCOUNTS; i++) {
            String id = TestEnv.newAccount("C" + i, limits[i], "EUR");
            TestEnv.fund(id, "EUR", FUNDING);
            accounts.add(id);
        }

        AtomicInteger ok = new AtomicInteger(), declined = new AtomicInteger();
        var unexpected = new ConcurrentLinkedQueue<Throwable>();
        var start = new CountDownLatch(1);
        long t0;
        try (var pool = Executors.newFixedThreadPool(THREADS)) {
            for (int i = 0; i < TRANSFERS; i++) {
                pool.submit(() -> {
                    var rnd = ThreadLocalRandom.current();
                    int from = rnd.nextInt(ACCOUNTS), to = (from + 1 + rnd.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
                    long amount = rnd.nextLong(1, 20_000);
                    try {
                        start.await();
                        ledger.post(new PostCommand(TestEnv.key(), "TRANSFER", "concurrency", List.of(
                                new PostingLine(accounts.get(from), "EUR", amount),
                                new PostingLine(accounts.get(to), "EUR", -amount)), Map.of()));
                        ok.incrementAndGet();
                    } catch (LedgerException e) {
                        if (e.reason == Reason.INSUFFICIENT_FUNDS) declined.incrementAndGet();
                        else unexpected.add(e);
                    } catch (Throwable e) {
                        unexpected.add(e);
                    }
                });
            }
            t0 = System.nanoTime();
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(5, TimeUnit.MINUTES)).isTrue();
        }
        double seconds = (System.nanoTime() - t0) / 1e9;
        System.out.printf("ConcurrencyTest: %d transfers on %d threads in %.2fs (%.0f/s): %d posted, %d declined%n",
                TRANSFERS, THREADS, seconds, TRANSFERS / seconds, ok.get(), declined.get());

        assertThat(unexpected).as("only INSUFFICIENT_FUNDS may fail").isEmpty();
        assertThat(ok.get() + declined.get()).isEqualTo(TRANSFERS);
        assertThat(ok.get()).isPositive();
        assertThat(declined.get()).as("the load must actually hit the limits").isPositive();

        long total = 0;
        for (int i = 0; i < ACCOUNTS; i++) {
            long b = TestEnv.balance(accounts.get(i), "EUR");
            assertThat(b).isGreaterThanOrEqualTo(-limits[i]);
            total += b;
        }
        assertThat(total).as("money is conserved inside the closed group").isEqualTo(ACCOUNTS * FUNDING);

        long postings = TestEnv.app().sql("SELECT count(*) FROM postings WHERE account_id IN (:ids)")
                .param("ids", accounts).query(Long.class).single();
        assertThat(postings).isEqualTo(2L * ok.get() + ACCOUNTS);
        assertThat(TestEnv.bean(QueryController.class).invariants()).containsEntry("ok", true);
    }
}
