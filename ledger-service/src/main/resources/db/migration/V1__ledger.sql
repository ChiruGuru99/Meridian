-- Double-entry ledger. Amounts are integer minor units; debit > 0, credit < 0.

CREATE TABLE accounts (
    id          text PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_-]{1,64}$'),
    name        text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    type        text NOT NULL CHECK (type IN ('ASSET', 'LIABILITY', 'EQUITY', 'INCOME', 'EXPENSE')),
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- One row per (account, currency). balance is in the account's normal-balance sign,
-- so customer money is positive. overdraft_limit NULL = unlimited (system accounts).
CREATE TABLE account_balances (
    account_id      text   NOT NULL REFERENCES accounts (id),
    currency        char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance         bigint NOT NULL DEFAULT 0,
    overdraft_limit bigint CHECK (overdraft_limit >= 0),
    version         bigint NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, currency),
    -- I2 backstop: even a buggy code path cannot push a balance past its limit.
    CONSTRAINT balance_within_limit CHECK (overdraft_limit IS NULL OR balance >= -overdraft_limit)
);

CREATE TABLE ledger_transactions (
    id              uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    request_hash    text NOT NULL,
    type            text NOT NULL,
    description     text NOT NULL DEFAULT '',
    reverses_id     uuid UNIQUE REFERENCES ledger_transactions (id),
    metadata        jsonb NOT NULL DEFAULT '{}',
    booked_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE postings (
    id             bigserial PRIMARY KEY,
    transaction_id uuid    NOT NULL REFERENCES ledger_transactions (id),
    seq            int     NOT NULL,
    account_id     text    NOT NULL,
    currency       char(3) NOT NULL,
    amount         bigint  NOT NULL CHECK (amount <> 0),
    UNIQUE (transaction_id, seq),
    FOREIGN KEY (account_id, currency) REFERENCES account_balances (account_id, currency)
);
CREATE INDEX postings_account_idx ON postings (account_id, currency, id);

-- I1: every transaction's postings sum to zero per currency. Checked at COMMIT
-- (deferred) so that all legs of a transaction are visible together.
CREATE FUNCTION check_postings_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    bad record;
BEGIN
    SELECT currency, sum(amount) AS total INTO bad
    FROM postings
    WHERE transaction_id = NEW.transaction_id
    GROUP BY currency
    HAVING sum(amount) <> 0
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'unbalanced transaction %: % postings sum to %', NEW.transaction_id, bad.currency, bad.total
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER postings_balanced
    AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_postings_balanced();

-- I4: the ledger is append-only. Corrections are reversals.
CREATE FUNCTION forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % rejected', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'insufficient_privilege';
END $$;

CREATE TRIGGER postings_append_only BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER postings_no_truncate BEFORE TRUNCATE ON postings
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER transactions_append_only BEFORE UPDATE OR DELETE ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER transactions_no_truncate BEFORE TRUNCATE ON ledger_transactions
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();

-- Transactional outbox, written in the same DB transaction as the postings.
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    topic        text NOT NULL,
    message_key  text NOT NULL,
    event_type   text NOT NULL,
    payload      jsonb NOT NULL,
    headers      jsonb NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- System accounts in every supported currency.
INSERT INTO accounts (id, name, type) VALUES
    ('SYS-NOSTRO', 'Nostro at partner bank', 'ASSET'),
    ('SYS-AUTH-HOLDS', 'Authorisation holds', 'LIABILITY'),
    ('SYS-PAYOUT-CLEARING', 'Outbound payout clearing', 'LIABILITY');

INSERT INTO account_balances (account_id, currency, overdraft_limit)
SELECT a.id, c.code, CASE WHEN a.id = 'SYS-NOSTRO' THEN NULL ELSE 0 END
FROM accounts a CROSS JOIN (VALUES ('EUR'), ('USD'), ('GBP')) AS c (code);

-- The application role can append to the ledger but never rewrite it.
-- (Flyway runs as the schema owner; the service connects as ledger_app.)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ledger_app') THEN
        GRANT USAGE ON SCHEMA public TO ledger_app;
        GRANT SELECT, INSERT ON accounts, ledger_transactions, postings TO ledger_app;
        GRANT SELECT, INSERT, UPDATE ON account_balances, outbox TO ledger_app;
        GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO ledger_app;
    END IF;
END $$;
