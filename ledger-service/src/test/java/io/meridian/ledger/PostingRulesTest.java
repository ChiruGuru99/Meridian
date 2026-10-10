package io.meridian.ledger;

import io.meridian.ledger.LedgerException.Reason;
import io.meridian.ledger.Model.PostingLine;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostingRulesTest {

    private static PostingLine p(String acc, String cur, long amount) {
        return new PostingLine(acc, cur, amount);
    }

    @Test
    void acceptsBalancedTwoLegTransaction() {
        assertThatCode(() -> PostingRules.validate(List.of(p("A", "EUR", 100), p("B", "EUR", -100)))).doesNotThrowAnyException();
    }

    @Test
    void acceptsMultiLegMultiCurrencyWhenEachCurrencyNetsToZero() {
        assertThatCode(() -> PostingRules.validate(List.of(
                p("A", "EUR", 100), p("B", "EUR", -60), p("C", "EUR", -40),
                p("A", "USD", -7), p("D", "USD", 7)))).doesNotThrowAnyException();
    }

    @Test
    void rejectsUnbalanced() {
        assertThatThrownBy(() -> PostingRules.validate(List.of(p("A", "EUR", 100), p("B", "EUR", -99))))
                .isInstanceOfSatisfying(LedgerException.class, e -> org.assertj.core.api.Assertions.assertThat(e.reason).isEqualTo(Reason.UNBALANCED));
    }

    @Test
    void rejectsCrossCurrencyNettingThatOnlyBalancesInTotal() {
        // +5 EUR and -5 USD sum to zero numerically but are two unbalanced currencies.
        assertThatThrownBy(() -> PostingRules.validate(List.of(p("A", "EUR", 5), p("B", "USD", -5))))
                .isInstanceOf(LedgerException.class).hasMessageContaining("EUR");
    }

    @Test
    void rejectsSingleLegZeroAmountBadCurrencyAndOversizedAmounts() {
        assertThatThrownBy(() -> PostingRules.validate(List.of(p("A", "EUR", 0), p("B", "EUR", 0)))).isInstanceOf(LedgerException.class);
        assertThatThrownBy(() -> PostingRules.validate(List.of(p("A", "EUR", 1)))).isInstanceOf(LedgerException.class);
        assertThatThrownBy(() -> PostingRules.validate(List.of(p("A", "eur", 1), p("B", "eur", -1)))).isInstanceOf(LedgerException.class);
        assertThatThrownBy(() -> PostingRules.validate(List.of(p("A", "EURO", 1), p("B", "EURO", -1)))).isInstanceOf(LedgerException.class);
        long tooBig = PostingRules.MAX_AMOUNT + 1;
        assertThatThrownBy(() -> PostingRules.validate(List.of(p("A", "EUR", tooBig), p("B", "EUR", -tooBig)))).isInstanceOf(LedgerException.class);
        assertThatThrownBy(() -> PostingRules.validate(List.of(p("bad id!", "EUR", 1), p("B", "EUR", -1)))).isInstanceOf(LedgerException.class);
    }

    @Test
    void sumCannotOverflowIntoBalance() {
        // Many maximal legs: validation must fail on the bound, never wrap around to zero.
        long max = PostingRules.MAX_AMOUNT;
        assertThatCode(() -> PostingRules.validate(List.of(p("A", "EUR", max), p("B", "EUR", max), p("C", "EUR", -max), p("D", "EUR", -max))))
                .doesNotThrowAnyException();
    }
}
