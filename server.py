#!/usr/bin/env python3
"""Ticker Board server.

Serves index.html and forwards stock data requests to Yahoo Finance
(browsers block the page from calling Yahoo directly). Uses only the Python
standard library, so there is nothing to install.

    python3 server.py              # then open http://localhost:8000
    PORT=9000 python3 server.py    # use a different port

Your folders and tickers are saved on this computer, in
ticker-board-data.json next to this file (set TICKERBOARD_DATA to put it
somewhere else). Back that file up or copy it to move your setup.
"""
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8000"))
ROOT = os.path.dirname(os.path.abspath(__file__))
UPSTREAMS = [
    u.strip().rstrip("/")
    for u in os.environ.get(
        "TICKERBOARD_UPSTREAM",
        "https://query1.finance.yahoo.com,https://query2.finance.yahoo.com",
    ).split(",")
    if u.strip()
]
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
CACHE_SECONDS = 20
DATA_FILE = os.environ.get("TICKERBOARD_DATA", os.path.join(ROOT, "ticker-board-data.json"))
MAX_STATE_BYTES = 1_000_000

SYMBOL_RE = re.compile(r"^[A-Za-z0-9.\-^=]{1,15}$")
RANGES = {"1d", "5d", "1mo", "3mo", "6mo", "ytd", "1y", "2y", "5y", "10y", "max"}
INTERVALS = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"}

_cache = {}
_lock = threading.Lock()
_state_lock = threading.Lock()


def read_state():
    """Return the saved state object, or None if nothing has been saved yet."""
    with _state_lock:
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, ValueError):
            return None
    return data if isinstance(data, dict) else None


def write_state(data):
    """Write atomically so a crash mid-save can't corrupt the file."""
    tmp = DATA_FILE + ".tmp"
    with _state_lock:
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2)
        os.replace(tmp, DATA_FILE)


def fetch_upstream(path_and_query):
    """Return (status, body bytes). Successful responses are cached briefly."""
    now = time.time()
    with _lock:
        hit = _cache.get(path_and_query)
        if hit and now - hit[0] < CACHE_SECONDS:
            return 200, hit[1]

    result = (502, json.dumps({"error": "Could not reach the data source"}).encode())
    for base in UPSTREAMS:
        request = urllib.request.Request(
            base + path_and_query,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=12) as response:
                status, body = response.status, response.read()
        except urllib.error.HTTPError as err:
            status, body = err.code, err.read()
        except (urllib.error.URLError, TimeoutError, OSError) as err:
            result = (502, json.dumps({"error": str(err)}).encode())
            continue

        if status == 200:
            with _lock:
                if len(_cache) > 400:
                    _cache.clear()
                _cache[path_and_query] = (now, body)
            return 200, body
        if status == 404:  # unknown symbol: the other host will say the same
            return 404, body
        result = (status, body)
    return result


class Handler(BaseHTTPRequestHandler):
    server_version = "TickerBoard/1.0"

    def log_message(self, fmt, *args):
        # Only log API problems, not every page and chart request.
        if args and str(args[1]).startswith(("4", "5")):
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def reply(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def bad_request(self, message):
        self.reply(400, json.dumps({"error": message}).encode())

    def same_origin(self):
        """Refuse writes from other websites: the page's Origin must match this server."""
        origin = self.headers.get("Origin")
        return origin is None or origin == "http://" + (self.headers.get("Host") or "")

    def do_PUT(self):
        if urllib.parse.urlparse(self.path).path != "/api/state":
            return self.reply(404, b"Not found", "text/plain")
        if not self.same_origin():
            return self.reply(403, json.dumps({"error": "Cross-site request refused"}).encode())
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > MAX_STATE_BYTES:
            return self.bad_request("Body missing or too large")
        try:
            data = json.loads(self.rfile.read(length))
        except ValueError:
            return self.bad_request("Body is not valid JSON")
        if not isinstance(data, dict):
            return self.bad_request("Expected a JSON object")
        try:
            write_state(data)
        except OSError as err:
            return self.reply(500, json.dumps({"error": "Could not write %s: %s" % (DATA_FILE, err)}).encode())
        self.reply(200, b'{"ok":true}')

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(url.query)

        if url.path == "/api/state":
            return self.reply(200, json.dumps({"state": read_state()}).encode())

        if url.path in ("/", "/index.html"):
            try:
                with open(os.path.join(ROOT, "index.html"), "rb") as handle:
                    return self.reply(200, handle.read(), "text/html; charset=utf-8")
            except OSError:
                return self.reply(500, b"index.html is missing next to server.py", "text/plain")

        if url.path == "/api/chart":
            symbol = params.get("symbol", [""])[0]
            rng = params.get("range", ["6mo"])[0]
            interval = params.get("interval", ["1d"])[0]
            if not SYMBOL_RE.match(symbol):
                return self.bad_request("Invalid symbol")
            if rng not in RANGES or interval not in INTERVALS:
                return self.bad_request("Invalid range or interval")
            path = "/v8/finance/chart/%s?range=%s&interval=%s&includePrePost=false" % (
                urllib.parse.quote(symbol, safe=""),
                rng,
                interval,
            )
            status, body = fetch_upstream(path)
            return self.reply(status, body)

        if url.path == "/api/search":
            query = params.get("q", [""])[0].strip()[:40]
            if not query:
                return self.reply(200, b'{"quotes":[]}')
            path = "/v1/finance/search?" + urllib.parse.urlencode(
                {"q": query, "quotesCount": 8, "newsCount": 0, "listsCount": 0}
            )
            status, body = fetch_upstream(path)
            return self.reply(status, body)

        self.reply(404, b"Not found", "text/plain")


def main():
    try:
        server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    except OSError as err:
        print("Could not start on port %d: %s" % (PORT, err))
        print("Try another port, for example:  PORT=8001 python3 server.py")
        sys.exit(1)
    print("Ticker Board is running at http://localhost:%d  (press Ctrl+C to stop)" % PORT)
    print("Folders and tickers are saved in: %s" % DATA_FILE)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
