package io.meridian.ledger;

import io.meridian.ledger.LedgerException.Reason;
import io.meridian.ledger.Model.PostCommand;
import io.meridian.ledger.Model.PostingLine;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Property-based tests for the ledger invariants (I1, I2, I4, I7). */
class LedgerPropertiesTest {

    // ---------------------------------------------------------------- I1, pure

    record Leg(int account, String currency, long amount) {}

    @Provide
    Arbitrary<List<Leg>> legs() {
        return Combinators.combine(Arbitraries.integers().between(0, 5), Arbitraries.of("EUR", "USD", "GBP"),
                        Arbitraries.longs().between(-1_000_000, 1_000_000).filter(x -> x != 0))
                .as(Leg::new).list().ofMinSize(1).ofMaxSize(10);
    }

    /** Appends one balancing leg per currency so the whole set nets to zero per currency. */
    private static List<PostingLine> balance(List<Leg> legs) {
        var lines = new ArrayList<PostingLine>();
        var totals = new TreeMap<String, Long>();
        for (Leg l : legs) {
            lines.add(new PostingLine("acc" + l.account(), l.currency(), l.amount()));
            totals.merge(l.currency(), l.amount(), Long::sum);
        }
        totals.forEach((cur, sum) -> {
            if (sum != 0) lines.add(new PostingLine("balancer", cur, -sum));
        });
        return lines;
    }

    @Property(tries = 2000)
    void anyPerCurrencyBalancedSetIsAccepted(@ForAll("legs") List<Leg> legs) {
        PostingRules.validate(balance(legs)); // at most 10 legs + 3 balancers, always >= 2 legs
    }

    @Property(tries = 2000)
    void anyPerturbationOfABalancedSetIsRejected(@ForAll("legs") List<Leg> legs,
                                                 @ForAll @IntRange(min = 0, max = 100) int which,
                                                 @ForAll("nonZero") long delta) {
        List<PostingLine> lines = new ArrayList<>(balance(legs));
        int i = which % lines.size();
        PostingLine p = lines.get(i);
        lines.set(i, new PostingLine(p.accountId(), p.currency(), p.amount() + delta));
        assertThatThrownBy(() -> PostingRules.validate(lines)).isInstanceOf(LedgerException.class);
    }

    @Provide
    Arbitrary<Long> nonZero() {
        return Arbitraries.longs().between(-1_000, 1_000).filter(x -> x != 0);
    }

    // ---------------------------------------------------------------- I2, I4, I7 against Postgres

    /** A transfer (from -> to) or a reversal of the n-th successful transfer so far. */
    record Op(int from, int to, String currency, long amount, boolean reversal, int target) {}

    @Provide
    Arbitrary<List<Op>> ops() {
        Arbitrary<Op> transfer = Combinators.combine(
                        Arbitraries.integers().between(0, 2), Arbitraries.integers().between(1, 2),
                        Arbitraries.of("EUR", "USD"), Arbitraries.longs().between(1, 3_000))
                .as((from, offset, cur, amount) -> new Op(from, (from + offset) % 3, cur, amount, false, 0));
        Arbitrary<Op> reversal = Arbitraries.integers().between(0, 30).map(t -> new Op(0, 0, null, 0, true, t));
        return Arbitraries.frequencyOf(Tuple.of(5, transfer), Tuple.of(1, reversal)).list().ofMinSize(1).ofMaxSize(40);
    }

    /**
     * Model-based test: a tiny in-memory model predicts whether each operation must be accepted or
     * refused for insufficient funds. The real ledger must agree on every step, and afterwards every
     * stored balance must equal the model and the database-wide invariants must hold.
     */
    @Property(tries = 40)
    void ledgerMatchesModelAndNeverBreachesLimits(@ForAll("ops") List<Op> ops,
                                                  @ForAll @Size(3) List<@net.jqwik.api.constraints.LongRange(min = 0, max = 1_500) Long> limits) {
        LedgerService ledger = TestEnv.ledger();
        String[] acc = new String[3];
        Map<String, Long> model = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            acc[i] = TestEnv.newAccount("P" + i, limits.get(i), "EUR", "USD");
            for (String cur : List.of("EUR", "USD")) {
                TestEnv.fund(acc[i], cur, 2_000);
                model.put(acc[i] + cur, 2_000L);
            }
        }
        record Done(UUID id, Op op) {}
        List<Done> done = new ArrayList<>();
        Set<UUID> reversed = new HashSet<>();

        for (Op op : ops) {
            if (op.reversal()) {
                if (done.isEmpty()) continue;
                Done d = done.get(op.target() % done.size());
                Op o = d.op();
                // Reversing moves the money back: the original destination is debited.
                boolean already = reversed.contains(d.id());
                boolean funds = model.get(acc[o.to()] + o.currency()) - o.amount() >= -limits.get(o.to());
                Reason expected = already ? Reason.ALREADY_REVERSED : funds ? null : Reason.INSUFFICIENT_FUNDS;
                Reason actual = outcome(() -> ledger.reverse(TestEnv.key(), d.id(), "property"));
                assertThat(actual).as("reversal of %s", o).isEqualTo(expected);
                if (expected == null) {
                    reversed.add(d.id());
                    move(model, acc[o.to()] + o.currency(), acc[o.from()] + o.currency(), o.amount());
                }
            } else {
                boolean funds = model.get(acc[op.from()] + op.currency()) - op.amount() >= -limits.get(op.from());
                UUID[] id = new UUID[1];
                Reason actual = outcome(() -> id[0] = ledger.post(new PostCommand(TestEnv.key(), "TRANSFER", "property", List.of(
                        new PostingLine(acc[op.from()], op.currency(), op.amount()),
                        new PostingLine(acc[op.to()], op.currency(), -op.amount())), Map.of())).transaction().id());
                assertThat(actual).as("transfer %s", op).isEqualTo(funds ? null : Reason.INSUFFICIENT_FUNDS);
                if (funds) {
                    done.add(new Done(id[0], op));
                    move(model, acc[op.from()] + op.currency(), acc[op.to()] + op.currency(), op.amount());
                }
            }
        }

        for (int i = 0; i < 3; i++) {
            for (String cur : List.of("EUR", "USD")) {
                long actual = TestEnv.balance(acc[i], cur);
                assertThat(actual).isEqualTo(model.get(acc[i] + cur));
                assertThat(actual).isGreaterThanOrEqualTo(-limits.get(i));
            }
        }
        assertThat(TestEnv.bean(QueryController.class).invariants()).containsEntry("ok", true);
    }

    private static void move(Map<String, Long> model, String from, String to, long amount) {
        model.merge(from, -amount, Long::sum);
        model.merge(to, amount, Long::sum);
    }

    private static Reason outcome(Runnable r) {
        try {
            r.run();
            return null;
        } catch (LedgerException e) {
            return e.reason;
        }
    }
}
