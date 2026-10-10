package io.meridian.ledger;

/** A business rejection. The reason is stable and is surfaced to gRPC clients as ErrorInfo.reason. */
public class LedgerException extends RuntimeException {

    public enum Reason {
        INVALID_ARGUMENT,
        UNBALANCED,
        ACCOUNT_NOT_FOUND,
        TRANSACTION_NOT_FOUND,
        CURRENCY_NOT_ENABLED,
        INSUFFICIENT_FUNDS,
        IDEMPOTENCY_KEY_REUSED,
        ALREADY_REVERSED
    }

    public final Reason reason;

    public LedgerException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }
}
