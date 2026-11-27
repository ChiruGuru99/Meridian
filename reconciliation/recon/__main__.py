"""CLI: serve | generate-statement | run."""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
from datetime import date, datetime, timezone

from . import api, ingest, job, statement, store

log = logging.getLogger("recon")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(prog="recon")
    subcommands = parser.add_subparsers(dest="cmd", required=True)
    subcommands.add_parser(
        "serve", help="ingest ledger events and serve the read API"
    )
    generate = subcommands.add_parser(
        "generate-statement",
        help="simulate the partner bank's statement for a business day",
    )
    generate.add_argument(
        "--date",
        default=None,
        help="business date YYYY-MM-DD (default: today UTC)",
    )
    generate.add_argument("--out", required=True)
    generate.add_argument("--seed", type=int, default=42)
    generate.add_argument("--no-injections", action="store_true")
    run = subcommands.add_parser(
        "run", help="reconcile a statement against ingested ledger entries"
    )
    run.add_argument("--statement", required=True)
    args = parser.parse_args(argv)

    db_url = os.environ.get("RECON_DB_URL")
    brokers = os.environ.get("KAFKA_BROKERS", "localhost:19092")
    account = os.environ.get("RECON_ACCOUNT", "SYS-NOSTRO")

    if args.cmd == "serve":
        with store.connect(db_url) as conn:
            store.migrate(conn)
        stop = threading.Event()
        threading.Thread(
            target=ingest.consume_forever,
            args=(db_url, brokers, {account}, stop),
            daemon=True,
        ).start()
        port = int(os.environ.get("PORT", "8083"))
        log.info("recon API on :%d, ingesting %s for %s", port, ingest.TOPIC, account)
        try:
            api.serve(db_url, port).serve_forever()
        finally:
            stop.set()
        return 0

    if args.cmd == "generate-statement":
        try:
            day = (
                date.fromisoformat(args.date)
                if args.date
                else datetime.now(timezone.utc).date()
            )
        except ValueError:
            log.error("--date must be YYYY-MM-DD")
            return 2
        summary = statement.generate(
            os.environ.get("LEDGER_URL", "http://localhost:8081"),
            account,
            day,
            args.out,
            args.seed,
            inject=not args.no_injections,
        )
        print(
            f"statement for {summary['business_date']} written to "
            f"{args.out}; injected discrepancies:"
        )
        for item in summary["injected"]:
            print(
                f"  {item['type']:<20} "
                f"{item['currency']} {item['reference']}"
            )
        return 0

    if args.cmd == "run":
        job.wait_for_ingestion(brokers, ingest.TOPIC, ingest.GROUP)
        try:
            with store.connect(db_url) as conn:
                store.migrate(conn)
                summary = job.run(conn, args.statement, account)
        except (OSError, ValueError) as exc:
            log.error("cannot reconcile %s: %s", args.statement, exc)
            return 2
        print(job.report(summary))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
