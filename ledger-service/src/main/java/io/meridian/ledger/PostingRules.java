package io.meridian.ledger;

import io.meridian.ledger.LedgerException.Reason;
import io.meridian.ledger.Model.PostingLine;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Pure validation of a requested transaction (I1, I6). The DB trigger re-checks balance at commit. */
public final class PostingRules {
    private static final Pattern CURRENCY = Pattern.compile("^[A-Z]{3}$");
    private static final Pattern ACCOUNT_ID = Pattern.compile("^[A-Za-z0-9_-]{1,64}$");
    /** 10^15 minor units: far above any real payment, far below overflow when summing legs. */
    public static final long MAX_AMOUNT = 1_000_000_000_000_000L;
    public static final int MAX_LEGS = 64;

    private PostingRules() {}

    public static void validate(List<PostingLine> postings) {
        if (postings == null || postings.size() < 2 || postings.size() > MAX_LEGS) {
            throw new LedgerException(Reason.INVALID_ARGUMENT, "a transaction needs 2.." + MAX_LEGS + " postings");
        }
        Map<String, Long> totals = new TreeMap<>();
        for (PostingLine p : postings) {
            if (p.accountId() == null || !ACCOUNT_ID.matcher(p.accountId()).matches()) {
                throw new LedgerException(Reason.INVALID_ARGUMENT, "invalid account id: " + p.accountId());
            }
            if (p.currency() == null || !CURRENCY.matcher(p.currency()).matches()) {
                throw new LedgerException(Reason.INVALID_ARGUMENT, "currency must be ISO-4217 alpha-3: " + p.currency());
            }
            if (p.amount() == 0 || Math.abs(p.amount()) > MAX_AMOUNT) {
                throw new LedgerException(Reason.INVALID_ARGUMENT, "amount must be non-zero and |amount| <= " + MAX_AMOUNT);
            }
            totals.merge(p.currency(), p.amount(), Math::addExact);
        }
        totals.forEach((currency, sum) -> {
            if (sum != 0) {
                throw new LedgerException(Reason.UNBALANCED, "postings in " + currency + " sum to " + sum + ", expected 0");
            }
        });
    }
}
