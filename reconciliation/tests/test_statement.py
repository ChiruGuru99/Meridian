import io
import json
import random
from datetime import date
from urllib.parse import parse_qs, urlparse

import pytest

from recon import statement
from recon.matching import Entry, reconcile


def random_day(seed: int) -> list[Entry]:
    rng = random.Random(seed)
    return [
        Entry(
            f"txn-{seed}-{index}",
            "EUR",
            rng.choice([-1, 1]) * rng.randrange(1, 10**7),
            "2026-10-10T09:00:00Z",
        )
        for index in range(rng.randrange(0, 25))
    ]


def test_reconciliation_finds_exactly_the_injected_discrepancies():
    for seed in range(500):
        entries = random_day(seed)
        lines, truth = statement.simulate_bank(entries, "2026-10-10", seed)
        result = reconcile(entries, lines)
        found = sorted((item.type, item.reference) for item in result.breaks)
        expected = sorted((item["type"], item["reference"]) for item in truth)
        assert found == expected, f"seed {seed}"
        changed = sum(
            1
            for item in truth
            if item["type"] in ("MISSING_IN_BANK", "AMOUNT_MISMATCH")
        )
        assert result.matched == len(entries) - changed


def test_without_injections_the_statement_matches_perfectly():
    entries = random_day(7)
    lines, truth = statement.simulate_bank(
        entries, "2026-10-10", 7, inject=False
    )
    assert truth == []
    result = reconcile(entries, lines)
    assert result.breaks == []
    assert result.matched == len(entries)


def test_generate_and_parse_round_trip(tmp_path, monkeypatch):
    entries = [
        Entry("t1", "EUR", 500, "2026-10-10T10:00:00Z"),
        Entry("t2", "EUR", -200, "2026-10-10T11:00:00Z"),
        Entry("t3", "EUR", 50, "2026-10-10T12:00:00Z"),
    ]
    fetched = {"EUR": (1_000, entries), "USD": (0, []), "GBP": (300, [])}
    monkeypatch.setattr(
        statement,
        "fetch_nostro",
        lambda url, account, currency, day: fetched[currency],
    )
    output = tmp_path / "statement.csv"
    summary = statement.generate(
        "http://ledger",
        "SYS-NOSTRO",
        date(2026, 10, 10),
        str(output),
        seed=1,
        inject=True,
    )
    parsed = statement.parse(str(output))
    assert parsed.business_date == "2026-10-10"
    assert parsed.opening == {"EUR": 1_000, "GBP": 300}
    assert parsed.closing["EUR"] == 1_000 + sum(
        line.amount for line in parsed.lines if line.currency == "EUR"
    )
    assert parsed.closing["GBP"] == 300
    assert {item["type"] for item in summary["injected"]} == {
        "MISSING_IN_BANK",
        "AMOUNT_MISMATCH",
        "DUPLICATE_IN_BANK",
        "MISSING_IN_LEDGER",
    }
    assert (tmp_path / "statement.csv.truth.json").exists()


def test_fetch_nostro_pages_past_ten_thousand_entries(monkeypatch):
    monkeypatch.setattr(statement, "PAGE_SIZE", 2)
    pages = [
        [
            ledger_row(1, "2026-10-09T10:00:00Z", 100, 100),
            ledger_row(2, "2026-10-10T09:00:00Z", -10, 90),
        ],
        [
            ledger_row(3, "2026-10-10T10:00:00Z", 20, 110),
            ledger_row(4, "2026-10-10T11:00:00Z", -5, 105),
        ],
        [],
    ]
    queries = []

    class Response(io.StringIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    def urlopen(url, timeout):
        queries.append(parse_qs(urlparse(url).query))
        return Response(json.dumps(pages[len(queries) - 1]))

    monkeypatch.setattr(statement.urllib.request, "urlopen", urlopen)
    opening, entries = statement.fetch_nostro(
        "http://ledger", "SYS-NOSTRO", "EUR", date(2026, 10, 10)
    )
    assert opening == 100
    assert [entry.reference for entry in entries] == ["txn-2", "txn-3", "txn-4"]
    assert "after" not in queries[0]
    assert queries[1]["after"] == ["2"]
    assert queries[2]["after"] == ["4"]


@pytest.mark.parametrize(
    "rows,message",
    [
        (
            [
                ["OPEN", "EUR", "2026-10-10", "100", "", "", ""],
                ["ENTRY", "EUR", "2026-10-10", "1.5", "t1", "B1", ""],
                ["CLOSE", "EUR", "2026-10-10", "100", "", "", ""],
            ],
            "amount_minor must be an integer",
        ),
        (
            [
                ["OPEN", "EUR", "2026-10-10", "100", "", "", ""],
                ["CLOSE", "EUR", "2026-10-11", "100", "", "", ""],
            ],
            "booking_date must be 2026-10-10",
        ),
        (
            [
                ["OPEN", "EUR", "2026-10-10", "100", "", "", ""],
                ["ENTRY", "USD", "2026-10-10", "10", "t1", "B1", ""],
                ["CLOSE", "EUR", "2026-10-10", "100", "", "", ""],
            ],
            "missing OPEN for USD",
        ),
    ],
)
def test_parse_rejects_malformed_statement(tmp_path, rows, message):
    path = tmp_path / "bad.csv"
    with path.open("w", newline="") as file:
        file.write(",".join(statement.FIELDS) + "\n")
        for row in rows:
            file.write(",".join(row) + "\n")
    with pytest.raises(ValueError, match=message):
        statement.parse(str(path))


def ledger_row(posting_id, booked_at, change, running):
    return {
        "postingId": posting_id,
        "transactionId": f"txn-{posting_id}",
        "bookedAt": booked_at,
        "balanceChange": change,
        "runningBalance": running,
    }
