"""Reconcile one partner-bank statement against ingested ledger postings."""
from __future__ import annotations

import time
from datetime import datetime, date, time as clock, timedelta, timezone

import psycopg

from . import metrics, store
from .matching import Entry, balance_breaks, reconcile
from .statement import parse


def wait_for_ingestion(
    brokers: str,
    topic: str,
    group: str,
    timeout_s: float = 60,
) -> None:
    """Wait until the ingester has committed through each topic partition's end."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": group,
        "enable.auto.commit": False,
    })
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        deadline = time.monotonic() + timeout_s
        while True:
            topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
            committed = {
                item.partition: item.offset
                for item in consumer.committed(topic_partitions, timeout=10)
            }
            lag = 0
            for partition in partitions:
                _, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=10
                )
                committed_offset = committed.get(partition, -1)
                lag += high - max(committed_offset, 0) if high > 0 else 0
            if lag == 0:
                return
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"ingestion still {lag} events behind after {timeout_s}s"
                )
            time.sleep(0.5)
    finally:
        consumer.close()


def run(conn: psycopg.Connection, statement_path: str, account: str) -> dict:
    statement = parse(statement_path)
    day = date.fromisoformat(statement.business_date)
    start = datetime.combine(day, clock.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    rows = conn.execute(
        """
        SELECT transaction_id, currency, amount, booked_at
        FROM internal_entries
        WHERE account_id = %s AND booked_at >= %s AND booked_at < %s
        ORDER BY booked_at, transaction_id, seq
        """,
        (account, start, end),
    ).fetchall()
    entries = [
        Entry(
            row["transaction_id"],
            row["currency"].strip(),
            row["amount"],
            row["booked_at"].isoformat(),
        )
        for row in rows
    ]
    openings = {
        row["currency"].strip(): row["amount"]
        for row in conn.execute(
            """
            SELECT currency, sum(amount)::bigint AS amount
            FROM internal_entries
            WHERE account_id = %s AND booked_at < %s
            GROUP BY currency
            """,
            (account, start),
        ).fetchall()
    }

    result = reconcile(entries, statement.lines)
    breaks = [item.as_dict() for item in result.breaks]
    currencies = (
        set(openings)
        | set(statement.opening)
        | set(statement.closing)
        | set(result.ledger_total)
        | set(result.bank_total)
    )
    totals = {}
    for currency in sorted(currencies):
        ledger_opening = openings.get(currency, 0)
        ledger_closing = ledger_opening + result.ledger_total.get(currency, 0)
        bank_opening = statement.opening.get(currency, 0)
        bank_closing = statement.closing.get(currency, 0)
        breaks.extend(
            item.as_dict()
            for item in balance_breaks(
                currency,
                ledger_opening,
                ledger_closing,
                bank_opening,
                bank_closing,
            )
        )
        totals[currency] = {
            "ledger_opening": ledger_opening,
            "ledger_movements": result.ledger_total.get(currency, 0),
            "ledger_closing": ledger_closing,
            "bank_opening": bank_opening,
            "bank_movements": result.bank_total.get(currency, 0),
            "bank_closing": bank_closing,
        }

    run_id = store.save_run(
        conn,
        statement.business_date,
        account,
        statement_path,
        result.matched,
        breaks,
        totals,
    )
    metrics.record_run(breaks)
    return {
        "run_id": run_id,
        "business_date": statement.business_date,
        "matched": result.matched,
        "breaks": breaks,
        "totals": totals,
    }


def report(summary: dict) -> str:
    out = [
        f"Reconciliation run {summary['run_id']} for {summary['business_date']}: "
        f"{summary['matched']} matched, {len(summary['breaks'])} breaks"
    ]
    for currency, totals in summary["totals"].items():
        out.append(
            f"  {currency}: ledger {totals['ledger_opening']} -> "
            f"{totals['ledger_closing']} (movements {totals['ledger_movements']}), "
            f"bank {totals['bank_opening']} -> {totals['bank_closing']} "
            f"(movements {totals['bank_movements']})"
        )
    for item in summary["breaks"]:
        reference = item["reference"] or "-"
        out.append(
            f"  BREAK {item['type']:<26} {item['currency']} "
            f"ref={reference:<38} ledger={item['ledger_amount']} "
            f"bank={item['bank_amount']}  {item['detail']}"
        )
    return "\n".join(out)
