from prometheus_client import Counter, Gauge

ingested = Counter(
    "recon_entries_ingested",
    "Ledger postings stored by the ingester (new rows).",
)
duplicates = Counter(
    "recon_events_duplicate",
    "Ledger events whose postings were already stored (redelivery).",
)
malformed = Counter(
    "recon_events_malformed",
    "Ledger events that could not be parsed or validated and were skipped.",
)
breaks = Gauge(
    "recon_breaks",
    "Breaks found by the most recent run, by type.",
    ["break_type"],
)
last_run = Gauge(
    "recon_last_run_timestamp_seconds",
    "When the most recent run finished.",
)

BREAK_TYPES = [
    "MISSING_IN_BANK",
    "MISSING_IN_LEDGER",
    "AMOUNT_MISMATCH",
    "DUPLICATE_IN_BANK",
    "OPENING_BALANCE_MISMATCH",
    "CLOSING_BALANCE_MISMATCH",
]


def record_run(found: list[dict]) -> None:
    for break_type in BREAK_TYPES:
        breaks.labels(break_type).set(
            sum(1 for item in found if item["type"] == break_type)
        )
    last_run.set_to_current_time()
