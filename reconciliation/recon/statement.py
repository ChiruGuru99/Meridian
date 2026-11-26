"""Simulated partner-bank statements backed by the ledger read API."""
from __future__ import annotations

import csv
import json
import random
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from .matching import (
    AMOUNT_MISMATCH,
    DUPLICATE_IN_BANK,
    MISSING_IN_BANK,
    MISSING_IN_LEDGER,
    Entry,
    Line,
)

FIELDS = [
    "record_type",
    "currency",
    "booking_date",
    "amount_minor",
    "reference",
    "bank_reference",
    "description",
]
PAGE_SIZE = 10_000
MIN_AMOUNT = -(2**63)
MAX_AMOUNT = 2**63 - 1
CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")
INTEGER_PATTERN = re.compile(r"^[+-]?\d+$")


@dataclass
class Statement:
    business_date: str
    opening: dict[str, int]
    closing: dict[str, int]
    lines: list[Line]


def simulate_bank(
    entries: list[Entry],
    business_date: str,
    seed: int,
    inject: bool = True,
) -> tuple[list[Line], list[dict]]:
    """Return statement lines and the discrepancies deliberately injected into them."""
    rng = random.Random(seed)

    def bank_ref() -> str:
        return f"BNK{rng.randrange(10**10):010d}"

    lines = [
        Line(
            entry.reference,
            entry.currency,
            entry.amount,
            business_date,
            bank_ref(),
            "SEPA" if entry.amount < 0 else "CREDIT",
        )
        for entry in entries
    ]
    truth: list[dict] = []
    if not inject:
        return lines, truth

    order = list(range(len(lines)))
    rng.shuffle(order)
    drop, alter, duplicate = (order + [None, None, None])[:3]
    out: list[Line] = []
    for index, line in enumerate(lines):
        if index == drop:
            truth.append({
                "type": MISSING_IN_BANK,
                "currency": line.currency,
                "reference": line.reference,
            })
            continue
        if index == alter:
            fee = rng.choice([150, 250, 1000])
            if line.amount - fee == 0:
                fee += 1
            line = Line(
                line.reference,
                line.currency,
                line.amount - fee,
                line.booking_date,
                line.bank_reference,
                line.description + " LESS CHARGES",
            )
            truth.append({
                "type": AMOUNT_MISMATCH,
                "currency": line.currency,
                "reference": line.reference,
            })
        out.append(line)
        if index == duplicate:
            out.append(Line(
                line.reference,
                line.currency,
                line.amount,
                line.booking_date,
                bank_ref(),
                line.description,
            ))
            truth.append({
                "type": DUPLICATE_IN_BANK,
                "currency": line.currency,
                "reference": line.reference,
            })

    currency = entries[0].currency if entries else "EUR"
    fee = Line("", currency, -2500, business_date, bank_ref(), "ACCOUNT MAINTENANCE FEE")
    out.append(fee)
    truth.append({
        "type": MISSING_IN_LEDGER,
        "currency": currency,
        "reference": fee.bank_reference,
    })
    return out, truth


def fetch_nostro(
    ledger_url: str,
    account: str,
    currency: str,
    business_date: date,
) -> tuple[int, list[Entry]]:
    """Read all history through the business day using posting-id keyset pagination."""
    start = datetime.combine(business_date, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    rows: list[dict] = []
    cursor = 0
    while True:
        params: dict[str, object] = {
            "currency": currency,
            "to": end.isoformat().replace("+00:00", "Z"),
            "limit": PAGE_SIZE,
        }
        if cursor:
            params["after"] = cursor
        query = urllib.parse.urlencode(params)
        with urllib.request.urlopen(
            f"{ledger_url}/api/accounts/{account}/entries?{query}",
            timeout=30,
        ) as response:
            page = json.load(response)
        if not isinstance(page, list):
            raise ValueError("ledger entries response must be an array")
        if page:
            ids = [_ledger_row_id(row) for row in page]
            if ids != sorted(ids) or len(ids) != len(set(ids)):
                raise ValueError("ledger entries are not uniquely ordered by postingId")
            if ids[0] <= cursor:
                raise ValueError("ledger entries pagination did not advance")
            rows.extend(page)
            cursor = ids[-1]
        if len(page) < PAGE_SIZE:
            break

    before = []
    during = []
    for row in rows:
        booked_at = _ledger_timestamp(row)
        amount = _ledger_integer(row, "balanceChange")
        running = _ledger_integer(row, "runningBalance")
        transaction_id = row.get("transactionId")
        if not isinstance(transaction_id, str) or not transaction_id:
            raise ValueError("ledger entry transactionId must be a non-empty string")
        if booked_at < start:
            before.append(running)
        else:
            during.append(Entry(transaction_id, currency, amount, booked_at.isoformat()))
    opening = before[-1] if before else 0
    return opening, during


def _ledger_row_id(row: object) -> int:
    if not isinstance(row, dict):
        raise ValueError("ledger entry must be an object")
    value = row.get("postingId")
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("ledger entry postingId must be a positive integer")
    return value


def _ledger_integer(row: dict, name: str) -> int:
    value = row.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"ledger entry {name} must be an integer")
    return value


def _ledger_timestamp(row: dict) -> datetime:
    value = row.get("bookedAt")
    if not isinstance(value, str):
        raise ValueError("ledger entry bookedAt must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("ledger entry bookedAt must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("ledger entry bookedAt must include a timezone")
    return parsed


def generate(
    ledger_url: str,
    account: str,
    business_date: date,
    out_path: str,
    seed: int,
    inject: bool,
    currencies: tuple[str, ...] = ("EUR", "USD", "GBP"),
) -> dict:
    day = business_date.isoformat()
    rows: list[dict] = []
    truth: list[dict] = []
    fetched = [
        (currency, *fetch_nostro(ledger_url, account, currency, business_date))
        for currency in currencies
    ]
    active = [item for item in fetched if item[1] != 0 or item[2]]
    if not active:
        currency = currencies[0] if currencies else "EUR"
        active = [(currency, 0, [])]
    for index, (currency, opening, entries) in enumerate(active):
        lines, injected = simulate_bank(
            entries,
            day,
            seed + index,
            inject=inject and not truth,
        )
        truth.extend(injected)
        rows.append({
            "record_type": "OPEN",
            "currency": currency,
            "booking_date": day,
            "amount_minor": opening,
        })
        for line in lines:
            rows.append({
                "record_type": "ENTRY",
                "currency": line.currency,
                "booking_date": line.booking_date,
                "amount_minor": line.amount,
                "reference": line.reference,
                "bank_reference": line.bank_reference,
                "description": line.description,
            })
        rows.append({
            "record_type": "CLOSE",
            "currency": currency,
            "booking_date": day,
            "amount_minor": opening + sum(line.amount for line in lines),
        })

    with open(out_path, "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "business_date": day,
        "account": account,
        "statement": out_path,
        "injected": truth,
    }
    with open(out_path + ".truth.json", "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2)
    return summary


def parse(path: str) -> Statement:
    opening: dict[str, int] = {}
    closing: dict[str, int] = {}
    lines: list[Line] = []
    business_date: str | None = None
    with open(path, encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError("statement is missing a CSV header")
        if reader.fieldnames != FIELDS:
            raise ValueError(f"statement header must be {','.join(FIELDS)}")

        for row in reader:
            line_number = reader.line_num
            if None in row:
                raise ValueError(f"line {line_number}: unexpected extra CSV fields")
            if any(value is None for value in row.values()):
                raise ValueError(f"line {line_number}: missing CSV fields")
            row_date = _statement_date(row["booking_date"], line_number)
            if business_date is None:
                business_date = row_date
            elif row_date != business_date:
                raise ValueError(
                    f"line {line_number}: booking_date must be {business_date}"
                )
            currency = row["currency"]
            if not isinstance(currency, str) or not CURRENCY_PATTERN.fullmatch(currency):
                raise ValueError(f"line {line_number}: invalid currency {currency!r}")
            amount = _statement_amount(row["amount_minor"], line_number)
            record_type = row["record_type"]
            if record_type == "OPEN":
                if currency in opening:
                    raise ValueError(f"line {line_number}: duplicate OPEN for {currency}")
                opening[currency] = amount
            elif record_type == "CLOSE":
                if currency in closing:
                    raise ValueError(f"line {line_number}: duplicate CLOSE for {currency}")
                closing[currency] = amount
            elif record_type == "ENTRY":
                if not row["bank_reference"]:
                    raise ValueError(f"line {line_number}: ENTRY bank_reference is required")
                lines.append(Line(
                    row["reference"],
                    currency,
                    amount,
                    row_date,
                    row["bank_reference"],
                    row["description"],
                ))
            else:
                raise ValueError(
                    f"line {line_number}: unknown record type {record_type!r}"
                )

    if business_date is None:
        raise ValueError("statement contains no records")
    currencies = set(opening) | set(closing) | {line.currency for line in lines}
    for currency in sorted(currencies):
        if currency not in opening:
            raise ValueError(f"statement is missing OPEN for {currency}")
        if currency not in closing:
            raise ValueError(f"statement is missing CLOSE for {currency}")
        movements = sum(
            line.amount for line in lines if line.currency == currency
        )
        if closing[currency] != opening[currency] + movements:
            raise ValueError(
                f"{currency} closing balance does not equal opening plus statement entries"
            )
    if not currencies:
        raise ValueError("statement contains no currencies")
    return Statement(business_date, opening, closing, lines)


def _statement_date(value: str, line_number: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"line {line_number}: booking_date must be YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"line {line_number}: booking_date must be YYYY-MM-DD"
        ) from exc
    if parsed.isoformat() != value:
        raise ValueError(f"line {line_number}: booking_date must be YYYY-MM-DD")
    return value


def _statement_amount(value: str, line_number: int) -> int:
    if not isinstance(value, str) or not INTEGER_PATTERN.fullmatch(value):
        raise ValueError(f"line {line_number}: amount_minor must be an integer")
    amount = int(value)
    if not MIN_AMOUNT <= amount <= MAX_AMOUNT:
        raise ValueError(f"line {line_number}: amount_minor is outside signed 64-bit range")
    return amount
