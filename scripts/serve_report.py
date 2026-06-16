"""Serve a local report directory over HTTP so tile requests work correctly.

Opening a report HTML file directly (file://) gives it a null origin, which
causes browsers to block cross-origin requests to external tile servers
(CORS policy).  Serving over http://localhost gives the page a real origin
and the tile fetches succeed.

Usage
-----
    python scripts/serve_report.py /path/to/report/output

Optional arguments:
    --port  Port to listen on (default: 8765)
    --no-open  Don't automatically open the browser

The script starts a simple HTTP server rooted at the given directory and
opens http://localhost:<port>/report.html in your default browser.
"""

from __future__ import annotations

import argparse
import http.server
import os
import threading
import time
import webbrowser
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Serve a report directory over HTTP for local preview."
    )
    parser.add_argument(
        "report_dir",
        type=Path,
        help="Directory containing the report HTML file(s) to serve.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port to listen on (default: 8765).",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Do not automatically open the browser.",
    )
    args = parser.parse_args()

    report_dir: Path = args.report_dir.resolve()

    if not report_dir.is_dir():
        print(f"ERROR: '{report_dir}' is not a directory or does not exist.")
        raise SystemExit(1)

    # Find the HTML file to open — prefer report.html, fall back to first .html
    html_candidates = sorted(report_dir.glob("*.html"))
    preferred = report_dir / "report.html"
    html_file = preferred if preferred.exists() else (html_candidates[0] if html_candidates else None)

    url = f"http://localhost:{args.port}/{html_file.name if html_file else ''}"

    # Change into the report directory so the server roots there
    os.chdir(report_dir)

    handler = http.server.SimpleHTTPRequestHandler

    # Silence the per-request log spam
    class _QuietHandler(handler):
        def log_message(self, fmt: str, *a) -> None:  # type: ignore[override]
            pass

    server = http.server.HTTPServer(("", args.port), _QuietHandler)

    print(f"Serving '{report_dir}' at {url}")
    print("Press Ctrl+C to stop.\n")

    if not args.no_open:
        # Open the browser slightly after the server starts
        def _open() -> None:
            time.sleep(0.3)
            webbrowser.open(url)

        threading.Thread(target=_open, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
