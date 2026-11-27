import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from recon.api import make_handler


def test_reconciliation_reads_require_bearer_token():
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_handler(None, "test-recon-key-0123456789abcdef"),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
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
