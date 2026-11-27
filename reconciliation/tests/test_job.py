import csv

from recon import job


class Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class Connection:
    def execute(self, sql, params):
        if "sum(amount)" in sql:
            return Result([{"currency": "USD", "amount": 4_000}])
        return Result([])


def test_job_checks_currency_with_no_day_movements(tmp_path, monkeypatch):
    path = tmp_path / "eur.csv"
    with path.open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "record_type",
            "currency",
            "booking_date",
            "amount_minor",
            "reference",
            "bank_reference",
            "description",
        ])
        writer.writerow(["OPEN", "EUR", "2026-10-10", 0, "", "", ""])
        writer.writerow(["CLOSE", "EUR", "2026-10-10", 0, "", "", ""])
    monkeypatch.setattr(job.store, "save_run", lambda *args: 7)
    monkeypatch.setattr(job.metrics, "record_run", lambda found: None)

    result = job.run(Connection(), str(path), "SYS-NOSTRO")

    assert result["totals"]["USD"] == {
        "ledger_opening": 4_000,
        "ledger_movements": 0,
        "ledger_closing": 4_000,
        "bank_opening": 0,
        "bank_movements": 0,
        "bank_closing": 0,
    }
    assert {
        item["type"] for item in result["breaks"] if item["currency"] == "USD"
    } == {"OPENING_BALANCE_MISMATCH", "CLOSING_BALANCE_MISMATCH"}
