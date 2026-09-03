"""Live-reload server for the DEMO style-guide report.

Renders the demo, serves it over HTTP, watches all shared template/CSS inputs
and rebuilds + reloads the browser whenever one of them changes::

    rs-report-demo-live [--port 8877] [--output-dir demo_output] [--no-browser]

Workflow::

    1. Run once (it opens your browser automatically).
    2. Edit base.css / macros.html / body.html / sample_data.py.
    3. Save. The page reloads itself. That is the whole loop.

Pure stdlib — no new dependencies.
"""

import argparse
import json
import os
import threading
import time
import traceback
import webbrowser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from rsxml import Logger

from util.html.demo.build_demo import build_demo

HERE = Path(__file__).parent
UTIL_TEMPLATES = HERE.parent / "templates"
DEMO_TEMPLATES = HERE / "templates"

# Anything that can change the rendered demo output.
WATCH_FILES = [
    UTIL_TEMPLATES / "base.css",
    UTIL_TEMPLATES / "macros.html",
    UTIL_TEMPLATES / "template.html",
    UTIL_TEMPLATES / "highlight_cards.css",
    DEMO_TEMPLATES / "body.html",
    DEMO_TEMPLATES / "demo.css",
    HERE / "sample_data.py",
    HERE / "build_demo.py",
]

POLL_INTERVAL_S = 0.4
MARKER = "rs-live-reload"
DEMO_PAGE = "report.html"

_reload_script = """<script id="rs-live-reload">
(function(){
  let v = null;
  async function poll(){
    try {
      const r = await fetch('/__poll', {cache: 'no-store'});
      const j = await r.json();
      if (v === null) v = j.version;
      else if (j.version !== v) location.reload();
    } catch (e) { /* server restarting; keep polling */ }
    setTimeout(poll, 600);
  }
  poll();
})();
</script>
</body>"""

_state = {"version": 0}
_lock = threading.Lock()


def _mtime_snapshot(paths: list[Path]) -> dict[Path, float | None]:
    snap = {}
    for p in paths:
        try:
            snap[p] = os.path.getmtime(p)
        except OSError:
            snap[p] = None
    return snap


def _inject_reload_script(html_path: Path) -> None:
    """Append the auto-reload script to the rendered demo page."""
    try:
        html = html_path.read_text(encoding="utf-8")
    except OSError:
        return
    if MARKER in html:
        return
    idx = html.lower().rfind("</body>")
    if idx == -1:
        html_path.write_text(html + "\n" + _reload_script, encoding="utf-8")
        return
    html_path.write_text(html[:idx] + _reload_script + html[idx + len("</body>"):], encoding="utf-8")


def _rebuild(output_dir: Path, log: Logger) -> bool:
    """Rebuild the demo (HTML-only) and inject the reload script. True on success."""
    try:
        build_demo(output_dir, html_only=True)
    except Exception:  # noqa: BLE001 - keep the server alive on template errors
        log.error("Demo rebuild failed; serving last good version:")
        traceback.print_exc()
        return False
    _inject_reload_script(output_dir / DEMO_PAGE)
    return True


def _make_handler(output_dir: Path, log: Logger):
    root = str(output_dir)
    state = _state
    lock = _lock

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=root, **kwargs)

        def do_GET(self):  # noqa: N802 - stdlib naming
            if self.path.split("?")[0] == "/__poll":
                with lock:
                    body = json.dumps({"version": state["version"]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
                return
            super().do_GET()

        def log_message(self, fmt, *args):  # quiet request spam by default
            if os.environ.get("RS_DEMO_LIVE_DEBUG"):
                super().log_message(fmt, *args)

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-dir", type=Path, default=Path("demo_output"), help="Where the demo report is written (default: ./demo_output)")
    parser.add_argument("--port", type=int, default=8877, help="Port for the live server (default: 8877)")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open the browser")
    args = parser.parse_args()

    log = Logger("demo live")
    output_dir = args.output_dir.resolve()

    log.info("Initial demo build…")
    _rebuild(output_dir, log)

    url = f"http://localhost:{args.port}/{DEMO_PAGE}"

    handler = _make_handler(output_dir, log)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    log.info(f"Live demo at {url}")
    log.info("Watching for changes in shared templates/CSS — Ctrl-C to stop.")
    if not args.no_browser:
        try:
            webbrowser.open(url)
        except Exception:  # noqa: BLE001
            log.warning("Could not open a browser automatically; open the URL above manually.")

    watched = [p for p in WATCH_FILES if p.exists()]
    previous = _mtime_snapshot(watched)
    try:
        while True:
            time.sleep(POLL_INTERVAL_S)
            current = _mtime_snapshot(watched)
            changed = [p for p in watched if current.get(p) != previous.get(p)]
            if changed:
                for p in changed:
                    log.info(f"Change detected: {p.relative_to(Path.cwd()) if p.is_relative_to(Path.cwd()) else p}")
                if _rebuild(output_dir, log):
                    with _lock:
                        _state["version"] += 1
                    log.info(f"Rebuilt (v{_state['version']}) — browser will reload.")
                previous = current
    except KeyboardInterrupt:
        log.info("Stopping live demo server.")
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
