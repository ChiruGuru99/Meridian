import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from recon.api import make_handler


def start_server(monkeypatch=None):
    if monkeypatch is not None:
        class Result:
            def fetchall(self):
                return [{"id": 7, "status": "COMPLETED"}]

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def execute(self, sql, params=()):
                assert "FROM recon_runs" in sql
                return Result()

        monkeypatch.setattr("recon.api.store.connect", lambda db_url: Connection())

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_handler(None, "test-recon-key-0123456789abcdef"),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_reconciliation_reads_require_bearer_token():
    server, thread = start_server()
    url = f"http://127.0.0.1:{server.server_port}/api/recon/runs"
    try:
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(url)
        assert error.value.code == 401

        request = urllib.request.Request(
            url, headers={"Authorization": "Bearer wrong-key"}
        )
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(request)
        assert error.value.code == 401
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_reconciliation_reads_accept_valid_bearer_token(monkeypatch):
    server, thread = start_server(monkeypatch)
    url = f"http://127.0.0.1:{server.server_port}/api/recon/runs"
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": "Bearer test-recon-key-0123456789abcdef",
        },
    )
    try:
        with urllib.request.urlopen(request) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/json"
            assert json.load(response) == [{"id": 7, "status": "COMPLETED"}]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_metrics_remain_available_without_credentials():
    server, thread = start_server()
    url = f"http://127.0.0.1:{server.server_port}/metrics"
    try:
        with urllib.request.urlopen(url) as response:
            assert response.status == 200
            assert response.headers["Content-Type"].startswith("text/plain")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
