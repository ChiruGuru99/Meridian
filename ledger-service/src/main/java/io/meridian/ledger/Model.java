package io.meridian.ledger;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Value types shared by the gRPC and HTTP adapters. Money is always (long minor units, ISO currency). */
public final class Model {
    private Model() {}

    public enum AccountType {
        ASSET(1), LIABILITY(-1), EQUITY(-1), INCOME(-1), EXPENSE(1);

        /** +1 for debit-normal accounts, -1 for credit-normal ones: balance += sign * postingAmount. */
        public final int sign;

        AccountType(int sign) {
            this.sign = sign;
        }
    }

    /** A posting leg as requested: debit > 0, credit < 0. */
    public record PostingLine(String accountId, String currency, long amount) {}

    public record PostCommand(String idempotencyKey, String type, String description,
                              List<PostingLine> postings, Map<String, String> metadata) {}

    /** overdraftLimit null = unlimited. */
    public record CurrencyLimit(String currency, Long overdraftLimit) {}

    public record Balance(String currency, long balance, Long overdraftLimit, long version) {}

    public record Account(String id, String name, AccountType type, List<Balance> balances) {}

    public record Transaction(UUID id, String idempotencyKey, String type, String description,
                              List<PostingLine> postings, Map<String, String> metadata,
                              UUID reversesId, Instant bookedAt) {}
}
