-- payment-gateway schema. Amounts are integer minor units.

-- Accounts opened through this gateway, and which API client owns them (only owners may debit).
CREATE TABLE client_accounts (
    account_id text PRIMARY KEY,
    client_id  text  NOT NULL,
    name       text  NOT NULL,
    currencies jsonb NOT NULL,
    status     text  NOT NULL CHECK (status IN ('PENDING', 'ACTIVE', 'FAILED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX client_accounts_status_idx ON client_accounts (status, updated_at);

CREATE TABLE payments (
    id                text PRIMARY KEY,
    client_id         text    NOT NULL,
    payer_account_id  text    NOT NULL,
    payee_account_id  text    NOT NULL,
    currency          char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    amount            bigint  NOT NULL CHECK (amount > 0),
    captured_amount   bigint  NOT NULL DEFAULT 0 CHECK (captured_amount BETWEEN 0 AND amount),
    refunded_amount   bigint  NOT NULL DEFAULT 0 CHECK (refunded_amount BETWEEN 0 AND captured_amount),
    status            text    NOT NULL,
    -- amount of the in-flight capture or refund; NULL when nothing is in flight
    pending_amount    bigint CHECK (pending_amount > 0),
    pending_refund_id text,
    description       text    NOT NULL DEFAULT '',
    decline_reason    text,
    version           bigint  NOT NULL DEFAULT 0,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX payments_status_idx ON payments (status, updated_at);
CREATE INDEX payments_created_idx ON payments (created_at DESC);

CREATE TABLE refunds (
    id            text PRIMARY KEY,
    payment_id    text   NOT NULL REFERENCES payments (id),
    amount        bigint NOT NULL CHECK (amount > 0),
    status        text   NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
    ledger_txn_id text,
    failure_reason text,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX refunds_payment_idx ON refunds (payment_id);

CREATE TABLE transfers (
    id                     text PRIMARY KEY,
    client_id              text    NOT NULL,
    kind                   text    NOT NULL CHECK (kind IN ('internal', 'external')),
    source_account_id      text    NOT NULL,
    destination_account_id text,
    beneficiary_iban       text,
    beneficiary_name       text,
    currency               char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    amount                 bigint  NOT NULL CHECK (amount > 0),
    reference              text    NOT NULL DEFAULT '',
    status                 text    NOT NULL,
    reserve_txn_id         text,
    settle_txn_id          text,
    compensation_txn_id    text,
    rail_reference         text,
    failure_reason         text,
    version                bigint  NOT NULL DEFAULT 0,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now(),
    CHECK ((kind = 'internal') = (destination_account_id IS NOT NULL)),
    CHECK ((kind = 'external') = (beneficiary_iban IS NOT NULL))
);
CREATE INDEX transfers_status_idx ON transfers (status, updated_at);
CREATE INDEX transfers_created_idx ON transfers (created_at DESC);

CREATE TABLE deposits (
    id            text PRIMARY KEY,
    client_id     text    NOT NULL,
    account_id    text    NOT NULL,
    currency      char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    amount        bigint  NOT NULL CHECK (amount > 0),
    reference     text    NOT NULL DEFAULT '',
    status        text    NOT NULL CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED')),
    ledger_txn_id text,
    failure_reason text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX deposits_status_idx ON deposits (status, updated_at);

-- I3: one row per (client, key). The stored response is replayed for retries.
CREATE TABLE idempotency_keys (
    client_id      text NOT NULL,
    key            text NOT NULL CHECK (length(key) BETWEEN 1 AND 255),
    request_method text NOT NULL,
    request_path   text NOT NULL,
    request_hash   text NOT NULL,
    status         text NOT NULL CHECK (status IN ('in_progress', 'completed')),
    resource_type  text,
    resource_id    text,
    response_code  int,
    response_body  bytea,
    locked_at      timestamptz NOT NULL DEFAULT now(),
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id, key)
);
CREATE INDEX idempotency_resource_idx ON idempotency_keys (resource_type, resource_id) WHERE status = 'in_progress';

-- Transactional outbox; rows are kept after publishing and double as the audit timeline.
CREATE TABLE outbox (
    id             bigserial PRIMARY KEY,
    topic          text  NOT NULL,
    message_key    text  NOT NULL,
    event_type     text  NOT NULL,
    aggregate_type text  NOT NULL,
    aggregate_id   text  NOT NULL,
    payload        jsonb NOT NULL,
    headers        jsonb NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX outbox_aggregate_idx ON outbox (aggregate_id, id);

-- Idempotent consumers record each processed message id in the same transaction as the effect.
CREATE TABLE inbox (
    consumer     text NOT NULL,
    message_id   text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
