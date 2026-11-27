"""Integration tests for the recon store against real Postgres."""
import csv

import pytest
from testcontainers.community.postgres import PostgresContainer

from recon import job, store


@pytest.fixture(scope="module")
def conn():
    with PostgresContainer("postgres:17-alpine", driver=None) as postgres:
        connection = store.connect(postgres.get_connection_url())
        store.migrate(connection)
        yield connection
        connection.close()


def event(
    transaction_id,
    postings,
    booked="2026-10-10T10:00:00Z",
    event_id="e1",
    currency="EUR",
):
    return {
        "event_id": event_id,
        "event_type": "ledger.transaction.posted",
        "transaction": {
            "id": transaction_id,
            "idempotency_key": f"k-{transaction_id}",
            "booked_at": booked,
            "postings": [
                {
                    "seq": index,
                    "account_id": account,
                    "currency": currency,
                    "amount": amount,
                }
                for index, (account, amount) in enumerate(postings)
            ],
        },
    }


def test_redelivered_events_are_applied_once(conn):
    message = event("dup-1", [("SYS-NOSTRO", 700), ("acc_x", -700)])
    with conn.transaction():
        assert store.apply_event(conn, message, {"SYS-NOSTRO"}) == 1
    for _ in range(3):
        with conn.transaction():
            assert store.apply_event(conn, message, {"SYS-NOSTRO"}) == 0
            assert store.apply_event(
                conn, dict(message, event_id="e-other"), {"SYS-NOSTRO"}
            ) == 0
    count = conn.execute(
        "SELECT count(*) AS n FROM internal_entries WHERE transaction_id = 'dup-1'"
    ).fetchone()["n"]
    assert count == 1


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.pop("event_id"),
        lambda value: value["transaction"].pop("postings"),
        lambda value: value["transaction"]["postings"][0].update(amount=True),
        lambda value: value["transaction"]["postings"][0].update(amount=99),
    ],
)
def test_malformed_events_are_rejected_before_storage(conn, mutate):
    message = event("bad-event", [("SYS-NOSTRO", 10), ("acc_x", -10)])
    mutate(message)
    with pytest.raises(store.InvalidEvent):
        store.apply_event(conn, message, {"SYS-NOSTRO"})


def test_run_persists_breaks_and_balances(conn, tmp_path):
    with conn.transaction():
        store.apply_event(
            conn,
            event(
                "old",
                [("SYS-NOSTRO", 1_000), ("a", -1_000)],
                booked="2026-09-30T10:00:00Z",
            ),
            {"SYS-NOSTRO"},
        )
        store.apply_event(
            conn,
            event(
                "r1",
                [("SYS-NOSTRO", 500), ("a", -500)],
                booked="2026-10-01T10:00:00Z",
            ),
            {"SYS-NOSTRO"},
        )
        store.apply_event(
            conn,
            event(
                "r2",
                [("SYS-NOSTRO", -200), ("a", 200)],
                booked="2026-10-01T11:00:00Z",
            ),
            {"SYS-NOSTRO"},
        )
    path = tmp_path / "statement.csv"
    with open(path, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(statement_fields())
        writer.writerow(["OPEN", "EUR", "2026-10-01", 1_000, "", "", ""])
        writer.writerow(["ENTRY", "EUR", "2026-10-01", 500, "r1", "B1", ""])
        writer.writerow(["ENTRY", "EUR", "2026-10-01", -2_500, "", "B9", "FEE"])
        writer.writerow(["CLOSE", "EUR", "2026-10-01", -1_000, "", "", ""])
    summary = job.run(conn, str(path), "SYS-NOSTRO")
    assert summary["matched"] == 1
    assert sorted(item["type"] for item in summary["breaks"]) == [
        "CLOSING_BALANCE_MISMATCH",
        "MISSING_IN_BANK",
        "MISSING_IN_LEDGER",
    ]
    assert summary["totals"]["EUR"]["ledger_closing"] == 1_300
    saved = conn.execute(
        "SELECT count(*) AS n FROM recon_breaks WHERE run_id = %s",
        (summary["run_id"],),
    ).fetchone()["n"]
    assert saved == 3


def test_run_includes_currency_with_only_historical_ledger_movements(conn, tmp_path):
    store.apply_event(
        conn,
        event(
            "usd-old",
            [("SYS-NOSTRO", 4_000), ("a", -4_000)],
            booked="2026-09-29T10:00:00Z",
            event_id="usd-old-event",
            currency="USD",
        ),
        {"SYS-NOSTRO"},
    )
    path = tmp_path / "eur-only.csv"
    with path.open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(statement_fields())
        writer.writerow(["OPEN", "EUR", "2026-10-02", 1_300, "", "", ""])
        writer.writerow(["CLOSE", "EUR", "2026-10-02", 1_300, "", "", ""])
    summary = job.run(conn, str(path), "SYS-NOSTRO")
    assert summary["totals"]["USD"] == {
        "ledger_opening": 4_000,
        "ledger_movements": 0,
        "ledger_closing": 4_000,
        "bank_opening": 0,
        "bank_movements": 0,
        "bank_closing": 0,
    }
    assert {
        item["type"] for item in summary["breaks"] if item["currency"] == "USD"
    } == {"OPENING_BALANCE_MISMATCH", "CLOSING_BALANCE_MISMATCH"}


def statement_fields():
    return [
        "record_type",
        "currency",
        "booking_date",
        "amount_minor",
        "reference",
        "bank_reference",
        "description",
    ]
