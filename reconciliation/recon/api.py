"""Read-only reconciliation HTTP API, metrics, and health endpoint."""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import store

log = logging.getLogger("recon.api")


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value)
    raise TypeError(type(value))


def make_handler(db_url: str | None):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def send(self, code: int, body: bytes, content_type: str = "application/json") -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, code: int, body) -> None:
            self.send(
                code,
                json.dumps(body, default=_json_default).encode(),
            )

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            try:
                if path == "/metrics":
                    return self.send(200, generate_latest(), CONTENT_TYPE_LATEST)
                with store.connect(db_url) as conn:
                    if path == "/healthz":
                        conn.execute("SELECT 1")
                        return self.send_json(200, {"status": "UP"})
                    if path == "/api/recon/runs":
                        rows = conn.execute(
                            "SELECT * FROM recon_runs ORDER BY id DESC LIMIT 50"
                        ).fetchall()
                        return self.send_json(200, rows)
                    match = re.fullmatch(r"/api/recon/runs/(\d+)", path)
                    if match:
                        run = conn.execute(
                            "SELECT * FROM recon_runs WHERE id = %s",
                            (int(match.group(1)),),
                        ).fetchone()
                        if run is None:
                            return self.send_json(404, {"code": "not_found"})
                        run["breaks"] = conn.execute(
                            "SELECT * FROM recon_breaks WHERE run_id = %s ORDER BY id",
                            (run["id"],),
                        ).fetchall()
                        return self.send_json(200, run)
                    if path == "/api/recon/ingestion":
                        row = conn.execute(
                            """
                            SELECT count(*) AS entries,
                                   max(ingested_at) AS last_ingested_at,
                                   count(DISTINCT transaction_id) AS transactions
                            FROM internal_entries
                            """
                        ).fetchone()
                        return self.send_json(200, row)
                return self.send_json(404, {"code": "not_found"})
            except Exception:
                log.exception("request failed")
                return self.send_json(503, {"code": "unavailable"})

    return Handler


def serve(db_url: str | None, port: int) -> ThreadingHTTPServer:
    return ThreadingHTTPServer(("0.0.0.0", port), make_handler(db_url))
