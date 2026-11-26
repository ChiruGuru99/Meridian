import json

import pytest

from recon import ingest, store


def test_poison_message_is_skipped_without_touching_the_database():
    assert ingest.process_message(None, b"{not-json", {"SYS-NOSTRO"}) is None
    assert ingest.process_message(
        None,
        json.dumps({"event_type": "ledger.transaction.posted"}).encode(),
        {"SYS-NOSTRO"},
    ) is None


def test_valid_message_and_duplicate_are_classified():
    event = {
        "event_id": "event-1",
        "event_type": "ledger.transaction.posted",
        "transaction": {
            "id": "transaction-1",
            "idempotency_key": "key-1",
            "booked_at": "2026-10-10T10:00:00Z",
            "postings": [
                {
                    "seq": 0,
                    "account_id": "SYS-NOSTRO",
                    "currency": "EUR",
                    "amount": 100,
                },
                {
                    "seq": 1,
                    "account_id": "other",
                    "currency": "EUR",
                    "amount": -100,
                },
            ],
        },
    }
    raw = json.dumps(event).encode()
    applied = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, row):
            self.rowcount = int(row[0] not in applied)
            if self.rowcount:
                applied.append(row[0])

    class Connection:
        def cursor(self):
            return Cursor()

    connection = Connection()
    assert ingest.process_message(connection, raw, {"SYS-NOSTRO"}) == 1
    assert ingest.process_message(connection, raw, {"SYS-NOSTRO"}) == 0
    assert store.prepare_event(event, {"unrelated"}) == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda event: event["transaction"].pop("booked_at"),
        lambda event: event["transaction"]["postings"][0].update(amount=True),
        lambda event: event["transaction"]["postings"][0].update(amount=99),
        lambda event: event["transaction"]["postings"][1].update(seq=0),
    ],
)
def test_valid_json_with_invalid_event_shape_is_rejected(mutate):
    event = {
        "event_id": "event-2",
        "event_type": "ledger.transaction.posted",
        "transaction": {
            "id": "transaction-2",
            "idempotency_key": "key-2",
            "booked_at": "2026-10-10T10:00:00Z",
            "postings": [
                {
                    "seq": 0,
                    "account_id": "SYS-NOSTRO",
                    "currency": "EUR",
                    "amount": 10,
                },
                {
                    "seq": 1,
                    "account_id": "other",
                    "currency": "EUR",
                    "amount": -10,
                },
            ],
        },
    }
    mutate(event)
    assert ingest.process_message(
        None, json.dumps(event).encode(), {"SYS-NOSTRO"}
    ) is None
