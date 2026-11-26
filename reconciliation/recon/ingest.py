"""Idempotent consumer of ledger.transactions.v1."""
from __future__ import annotations

import json
import logging
import threading
import time

from . import metrics, store

log = logging.getLogger("recon.ingest")
TOPIC = "ledger.transactions.v1"
GROUP = "reconciliation"


def process_message(conn, raw: bytes, accounts: set[str]) -> int | None:
    """Apply one message. Poison messages are counted and skipped; DB errors propagate."""
    try:
        event = json.loads(raw)
    except (UnicodeDecodeError, TypeError, ValueError):
        metrics.malformed.inc()
        log.error("skipping event with invalid JSON")
        return None
    try:
        rows = store.prepare_event(event, accounts)
    except store.InvalidEvent as exc:
        metrics.malformed.inc()
        log.error("skipping malformed event: %s", exc)
        return None

    inserted = store.apply_prepared_event(conn, rows)
    if inserted:
        metrics.ingested.inc(inserted)
    elif rows:
        metrics.duplicates.inc()
    return inserted


def consume_forever(
    db_url: str | None,
    brokers: str,
    accounts: set[str],
    stop: threading.Event,
) -> None:
    from confluent_kafka import Consumer

    while not stop.is_set():
        consumer = None
        try:
            conn = store.connect(db_url)
            consumer = Consumer({
                "bootstrap.servers": brokers,
                "group.id": GROUP,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            })
            consumer.subscribe([TOPIC])
            while not stop.is_set():
                messages = consumer.consume(num_messages=500, timeout=1.0)
                if not messages:
                    continue
                with conn.transaction():
                    for message in messages:
                        if message.error():
                            log.warning("kafka error: %s", message.error())
                            continue
                        process_message(conn, message.value(), accounts)
                consumer.commit(asynchronous=False)
        except Exception:
            log.exception("ingester failed; restarting from the last committed offset")
            time.sleep(2)
        finally:
            if consumer is not None:
                consumer.close()
            if "conn" in locals():
                conn.close()
                del conn
