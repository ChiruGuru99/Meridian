"""Pure reconciliation logic: no I/O, so every rule is unit-testable."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Entry:
    """A ledger posting on the reconciled account."""

    reference: str
    currency: str
    amount: int
    booked_at: str


@dataclass(frozen=True)
class Line:
    """A bank statement line. Reference is empty for a bank-initiated booking."""

    reference: str
    currency: str
    amount: int
    booking_date: str
    bank_reference: str
    description: str = ""


MISSING_IN_BANK = "MISSING_IN_BANK"
MISSING_IN_LEDGER = "MISSING_IN_LEDGER"
AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
DUPLICATE_IN_BANK = "DUPLICATE_IN_BANK"
OPENING_BALANCE_MISMATCH = "OPENING_BALANCE_MISMATCH"
CLOSING_BALANCE_MISMATCH = "CLOSING_BALANCE_MISMATCH"


@dataclass(frozen=True)
class Break:
    type: str
    currency: str
    reference: str
    ledger_amount: int | None
    bank_amount: int | None
    detail: str = ""

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class Result:
    matched: int
    breaks: list[Break]
    ledger_total: dict[str, int]
    bank_total: dict[str, int]


def reconcile(entries: list[Entry], lines: list[Line]) -> Result:
    """Match postings to statement lines by currency and end-to-end reference."""
    ledger: dict[tuple[str, str], int] = defaultdict(int)
    for entry in entries:
        ledger[(entry.currency, entry.reference)] += entry.amount

    bank: dict[tuple[str, str], list[Line]] = defaultdict(list)
    breaks: list[Break] = []
    for line in lines:
        if not line.reference:
            breaks.append(Break(
                MISSING_IN_LEDGER,
                line.currency,
                line.bank_reference,
                None,
                line.amount,
                f"bank-initiated booking: {line.description}",
            ))
        else:
            bank[(line.currency, line.reference)].append(line)

    matched = 0
    for key in sorted(set(ledger) | set(bank)):
        currency, reference = key
        expected = ledger.get(key)
        booked = list(bank.get(key, []))
        if expected is None:
            breaks.extend(
                Break(MISSING_IN_LEDGER, currency, reference, None, item.amount,
                      "reference unknown to the ledger")
                for item in booked
            )
            continue
        if not booked:
            breaks.append(Break(MISSING_IN_BANK, currency, reference, expected, None,
                                "not on the bank statement"))
            continue

        exact = next((item for item in booked if item.amount == expected), None)
        if exact is not None:
            matched += 1
            booked.remove(exact)
        else:
            first = booked.pop(0)
            breaks.append(Break(
                AMOUNT_MISMATCH,
                currency,
                reference,
                expected,
                first.amount,
                f"difference {first.amount - expected}",
            ))
        breaks.extend(
            Break(DUPLICATE_IN_BANK, currency, reference, expected, item.amount,
                  "booked more than once by the bank")
            for item in booked
        )

    ledger_total: dict[str, int] = defaultdict(int)
    for entry in entries:
        ledger_total[entry.currency] += entry.amount
    bank_total: dict[str, int] = defaultdict(int)
    for line in lines:
        bank_total[line.currency] += line.amount
    return Result(matched, breaks, dict(ledger_total), dict(bank_total))


def balance_breaks(currency: str, ledger_opening: int, ledger_closing: int,
                   bank_opening: int, bank_closing: int) -> list[Break]:
    """Check that the statement and ledger start and finish on the same balances."""
    out = []
    if ledger_opening != bank_opening:
        out.append(Break(
            OPENING_BALANCE_MISMATCH,
            currency,
            "",
            ledger_opening,
            bank_opening,
            f"difference {bank_opening - ledger_opening}",
        ))
    if ledger_closing != bank_closing:
        out.append(Break(
            CLOSING_BALANCE_MISMATCH,
            currency,
            "",
            ledger_closing,
            bank_closing,
            f"difference {bank_closing - ledger_closing}",
        ))
    return out
