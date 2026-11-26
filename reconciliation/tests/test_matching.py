from recon.matching import (
    AMOUNT_MISMATCH,
    CLOSING_BALANCE_MISMATCH,
    DUPLICATE_IN_BANK,
    MISSING_IN_BANK,
    MISSING_IN_LEDGER,
    Entry,
    Line,
    balance_breaks,
    reconcile,
)


def entry(reference, amount, currency="EUR"):
    return Entry(reference, currency, amount, "2026-10-10T10:00:00Z")


def line(reference, amount, currency="EUR", bank_reference="B1"):
    return Line(reference, currency, amount, "2026-10-10", bank_reference, "")


def types(result):
    return sorted((item.type, item.reference) for item in result.breaks)


def test_clean_day_matches_everything():
    result = reconcile(
        [entry("t1", 100), entry("t2", -40)],
        [line("t2", -40), line("t1", 100)],
    )
    assert result.matched == 2
    assert result.breaks == []
    assert result.ledger_total == result.bank_total == {"EUR": 60}


def test_each_break_type_is_classified():
    entries = [
        entry("ok", 10),
        entry("gone", 20),
        entry("short", 30),
        entry("twice", 40),
        entry("usd", 5, "USD"),
    ]
    lines = [
        line("ok", 10),
        line("short", 29),
        line("twice", 40),
        line("twice", 40, bank_reference="B2"),
        line("usd", 5, "USD"),
        line("stranger", 7),
        line("", -25, bank_reference="FEE1"),
    ]
    result = reconcile(entries, lines)
    assert result.matched == 3
    assert types(result) == sorted([
        (MISSING_IN_BANK, "gone"),
        (AMOUNT_MISMATCH, "short"),
        (DUPLICATE_IN_BANK, "twice"),
        (MISSING_IN_LEDGER, "stranger"),
        (MISSING_IN_LEDGER, "FEE1"),
    ])
    mismatch = next(item for item in result.breaks if item.type == AMOUNT_MISMATCH)
    assert (mismatch.ledger_amount, mismatch.bank_amount) == (30, 29)


def test_same_reference_in_two_currencies_is_two_items():
    result = reconcile(
        [entry("t", 10, "EUR"), entry("t", 10, "USD")],
        [line("t", 10, "EUR")],
    )
    assert result.matched == 1
    assert types(result) == [(MISSING_IN_BANK, "t")]


def test_balance_breaks():
    assert balance_breaks("EUR", 100, 200, 100, 200) == []
    [item] = balance_breaks("EUR", 100, 200, 100, 175)
    assert item.type == CLOSING_BALANCE_MISMATCH
    assert item.detail == "difference -25"
