"""Recon schema and idempotent storage of ledger events and reconciliation runs."""
from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

SCHEMA = """
CREATE TABLE IF NOT EXISTS internal_entries (
    transaction_id  text        NOT NULL,
    seq             int         NOT NULL,
    account_id      text        NOT NULL,
    currency        char(3)     NOT NULL,
    amount          bigint      NOT NULL,
    booked_at       timestamptz NOT NULL,
    idempotency_key text        NOT NULL,
    event_id        text        NOT NULL,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, seq)
);
CREATE INDEX IF NOT EXISTS internal_entries_day_idx
    ON internal_entries (account_id, currency, booked_at);

CREATE TABLE IF NOT EXISTS recon_runs (
    id             bigserial PRIMARY KEY,
    business_date  date        NOT NULL,
    account_id     text        NOT NULL,
    statement_file text        NOT NULL,
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    matched_count  int         NOT NULL DEFAULT 0,
    break_count    int         NOT NULL DEFAULT 0,
    totals         jsonb       NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS recon_breaks (
    id            bigserial PRIMARY KEY,
    run_id        bigint  NOT NULL REFERENCES recon_runs (id),
    break_type    text    NOT NULL,
    currency      char(3) NOT NULL,
    reference     text    NOT NULL,
    ledger_amount bigint,
    bank_amount   bigint,
    detail        text    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS recon_breaks_run_idx ON recon_breaks (run_id);
"""

ACCOUNT_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")
MAX_AMOUNT = 10**15
MAX_LEGS = 64


class InvalidEvent(ValueError):
    """The Kafka message is not a valid ledger transaction event."""


def connect(url: str | None = None) -> psycopg.Connection:
    url = url or os.environ.get(
        "RECON_DB_URL", "postgresql://recon:recon@localhost:5432/recon"
    )
    return psycopg.connect(
        url,
        autocommit=True,
        row_factory=dict_row,
        options="-c statement_timeout=10000",
    )


def migrate(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA)


def _text(value, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidEvent(f"{name} must be a non-empty string")
    return value


def prepare_event(event: object, accounts: set[str]) -> list[tuple]:
    """Validate an event and return only the postings owned by this consumer."""
    if not isinstance(event, dict):
        raise InvalidEvent("event must be an object")
    if event.get("event_type") != "ledger.transaction.posted":
        raise InvalidEvent("event_type must be ledger.transaction.posted")
    event_id = _text(event.get("event_id"), "event_id")

    transaction = event.get("transaction")
    if not isinstance(transaction, dict):
        raise InvalidEvent("transaction must be an object")
    transaction_id = _text(transaction.get("id"), "transaction.id")
    idempotency_key = _text(transaction.get("idempotency_key"), "transaction.idempotency_key")
    booked_at = _text(transaction.get("booked_at"), "transaction.booked_at")
    try:
        parsed_booked_at = datetime.fromisoformat(booked_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidEvent("transaction.booked_at must be an ISO timestamp") from exc
    if parsed_booked_at.tzinfo is None:
        raise InvalidEvent("transaction.booked_at must include a timezone")

    postings = transaction.get("postings")
    if not isinstance(postings, list) or not 2 <= len(postings) <= MAX_LEGS:
        raise InvalidEvent(f"transaction.postings must contain 2..{MAX_LEGS} items")

    totals: dict[str, int] = defaultdict(int)
    sequences: set[int] = set()
    rows = []
    for posting in postings:
        if not isinstance(posting, dict):
            raise InvalidEvent("each posting must be an object")
        sequence = posting.get("seq")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise InvalidEvent("posting.seq must be a non-negative integer")
        if sequence in sequences:
            raise InvalidEvent(f"duplicate posting sequence {sequence}")
        sequences.add(sequence)

        account_id = posting.get("account_id")
        if not isinstance(account_id, str) or not ACCOUNT_PATTERN.fullmatch(account_id):
            raise InvalidEvent(f"invalid posting account_id {account_id!r}")
        currency = posting.get("currency")
        if not isinstance(currency, str) or not CURRENCY_PATTERN.fullmatch(currency):
            raise InvalidEvent(f"invalid posting currency {currency!r}")
        amount = posting.get("amount")
        if (
            isinstance(amount, bool)
            or not isinstance(amount, int)
            or amount == 0
            or abs(amount) > MAX_AMOUNT
        ):
            raise InvalidEvent(
                f"posting amount must be non-zero and at most {MAX_AMOUNT} in absolute value"
            )
        totals[currency] += amount
        if account_id in accounts:
            rows.append((
                transaction_id,
                sequence,
                account_id,
                currency,
                amount,
                booked_at,
                idempotency_key,
                event_id,
            ))

    unbalanced = [currency for currency, amount in totals.items() if amount != 0]
    if unbalanced:
        raise InvalidEvent(f"postings do not balance for {', '.join(sorted(unbalanced))}")
    return rows


def apply_prepared_event(conn: psycopg.Connection, rows: list[tuple]) -> int:
    inserted = 0
    with conn.cursor() as cursor:
        for row in rows:
            cursor.execute(
                """
                INSERT INTO internal_entries (
                    transaction_id, seq, account_id, currency, amount,
                    booked_at, idempotency_key, event_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id, seq) DO NOTHING
                """,
                row,
            )
            inserted += cursor.rowcount
    return inserted


def apply_event(conn: psycopg.Connection, event: dict, accounts: set[str]) -> int:
    """Idempotently store an event's postings on reconciled accounts."""
    return apply_prepared_event(conn, prepare_event(event, accounts))


def save_run(
    conn: psycopg.Connection,
    business_date: str,
    account: str,
    statement_file: str,
    matched: int,
    breaks: list[dict],
    totals: dict,
) -> int:
    with conn.transaction():
        run_id = conn.execute(
            """
            INSERT INTO recon_runs (
                business_date, account_id, statement_file, matched_count,
                break_count, totals, finished_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, now())
            RETURNING id
            """,
            (business_date, account, statement_file, matched, len(breaks), json.dumps(totals)),
        ).fetchone()["id"]
        with conn.cursor() as cursor:
            for item in breaks:
                cursor.execute(
                    """
                    INSERT INTO recon_breaks (
                        run_id, break_type, currency, reference,
                        ledger_amount, bank_amount, detail
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        item["type"],
                        item["currency"],
                        item["reference"],
                        item["ledger_amount"],
                        item["bank_amount"],
                        item["detail"],
                    ),
                )
    return run_id
