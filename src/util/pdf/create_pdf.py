"""PDF generation from rendered report HTML.

PDFs are rendered with **Chrome/Chromium/Edge ``--headless`` print-to-PDF** —
the only supported PDF engine. This is a real browser engine, so the PDF is
laid out by the *same* CSS engine that renders the HTML on screen: CSS Grid,
Flexbox, ``calc()`` with custom properties, webfonts, ``@media print`` —
everything behaves identically. This is the only way the PDF can look exactly
like the static HTML report for every section and control.
"""

from __future__ import annotations
import shlex
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path

#: Console logger consistent with the rest of the pipeline (rsxml).
from rsxml import Logger as _RsxmlLogger  # noqa: E402

_log = _RsxmlLogger("pdf")
#: Binary names searched on PATH (in order).
_CHROME_BIN_NAMES = (
    "google-chrome-stable",
    "google-chrome",
    "chromium",
    "chromium-browser",
    "chrome",
    "chrome-headless-shell",
    "msedge",
    "microsoft-edge",
)

#: macOS application bundles (checked in ~/Applications and /Applications).
_MAC_CHROME_APPS = (
    "Google Chrome.app",
    "Chromium.app",
    "Microsoft Edge.app",
    "Brave Browser.app",
    "Google Chrome for Testing.app",
)

_LINUX_CHROME_PATHS = (
    "/usr/bin/google-chrome-stable",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/snap/bin/chromium",
    "/snap/bin/google-chrome",
    "/opt/google/chrome/chrome",
)

_WINDOWS_CHROME_PATHS = (
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
)

#: Playwright-managed Chromium installs (checked last — best effort).
_PLAYWRIGHT_CACHE_DIRS = (
    "~/.cache/ms-playwright",
    "~/Library/Caches/ms-playwright",
)


def _running_in_container() -> bool:
    """True when running inside Docker/Podman (where Chrome's user-namespace
    sandbox cannot initialize unless the container grants extra caps)."""
    return any(Path(p).exists() for p in ("/.dockerenv", "/run/.containerenv"))


def _is_executable(path: str | os.PathLike[str] | None) -> bool:
    if not path:
        return False
    p = Path(path)
    try:
        return p.is_file() and os.access(p, os.X_OK)
    except OSError:  # pragma: no cover - permissions weirdness
        return False


def find_chrome() -> str | None:
    """Locate a Chrome/Chromium/Edge executable without importing heavy deps.

    Search order:

    1. ``CHROME_PATH`` / ``BROWSER_PATH`` environment variables.
    2. Common binary names on ``PATH``.
    3. Platform-specific install locations (macOS apps, Linux/Windows paths).
    4. Playwright-managed Chromium downloads.
    5. Kaleido's ``choreographer`` browser search (when installed — it also
       knows about its own managed downloads).

    Returns the path to the executable, or ``None``.
    """
    for var in ("CHROME_PATH", "BROWSER_PATH"):
        candidate = os.environ.get(var)
        if _is_executable(candidate):
            return candidate

    if os.name != "nt":
        for exe in _CHROME_BIN_NAMES:
            found = shutil.which(exe)
            if _is_executable(found):
                return found

    if sys.platform == "darwin":
        for app_dir in (Path.home() / "Applications", Path("/Applications")):
            for app in _MAC_CHROME_APPS:
                candidate = app_dir / app / "Contents" / "MacOS" / app.removesuffix(".app")
                if _is_executable(candidate):
                    return str(candidate)
    elif sys.platform.startswith("linux"):
        for candidate in _LINUX_CHROME_PATHS:
            if _is_executable(candidate):
                return candidate
    elif os.name == "nt":
        for candidate in _WINDOWS_CHROME_PATHS:
            if _is_executable(candidate):
                return candidate

    # Playwright-managed Chromium (best effort; glob a couple of known shapes).
    for cache_dir in _PLAYWRIGHT_CACHE_DIRS:
        base = Path(cache_dir).expanduser()
        if not base.is_dir():
            continue
        patterns = (
            "chromium-*/chrome-*/**/chrome",
            "chromium-*/chrome-*/**/Chromium",
            "chromium_headless_shell-*/chrome-headless-shell-*/chrome-headless-shell",
        )
        for pattern in patterns:
            for candidate in sorted(base.glob(pattern)):
                if _is_executable(candidate):
                    return str(candidate)

    # Kaleido's choreographer search (knows PATH + its own managed downloads).
    try:  # pragma: no cover - environment-dependent
        from choreographer.utils import get_browser_path

        found = get_browser_path(executable_names=_CHROME_BIN_NAMES)
        if _is_executable(found):
            return found
    except (ImportError, OSError):
        pass

    return None


def _chrome_version(binary: str) -> str | None:
    """Best-effort version string from the binary (for logging)."""
    try:  # pragma: no cover - env dependent
        result = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return (result.stdout or result.stderr).strip() or None
    except (OSError, subprocess.TimeoutExpired):
        return None


def _pdf_state(pdf_path: Path) -> str:
    """Describe the (possibly partial) output PDF for error messages."""
    if not pdf_path.is_file():
        return "not created"
    size = pdf_path.stat().st_size
    return "created but empty" if size == 0 else f"created ({size:,} bytes)"


def _chrome_failure_message(
    headline: str,
    *,
    chrome: str,
    chrome_version: str | None,
    html_path: str | os.PathLike[str],
    input_url: str,
    pdf_path: str | os.PathLike[str],
    stderr: str,
    stdout: str,
) -> str:
    """Build a diagnosis-friendly RuntimeError message for a Chrome failure.

    Includes the binary/version actually used, the exact input/output paths,
    the partial output state and Chrome's own stderr/stdout, plus pointers to
    the knobs that usually fix a hang.
    """
    hints = [
        "• Raise the budget: RS_PDF_TIMEOUT_S=300 (or pass timeout_s=300 / --pdf-timeout 300).",
        "• Diagnose: RS_PDF_DEBUG=1 (or --pdf-debug) keeps Chrome's verbose log and the injected HTML source next to the PDF.",
        "• Restricted container/CI: CHROME_NO_SANDBOX=1 usually fixes renderer startup failures.",
        "• Unreachable CDN resources (Pico/fonts/plotly.js/map tiles) are the most common hang cause — Chrome waits on them while rendering.",
    ]
    body = (
        f"{headline}.\n"
        f"Binary: {chrome} ({chrome_version or 'version unknown'})\n"
        f"Input: {html_path} ({input_url})\n"
        f"Output: {pdf_path} ({_pdf_state(Path(pdf_path))})\n"
    )
    if stderr.strip():
        body += f"Chrome stderr (tail, 4000 chars max):\n{stderr.strip()[-4000:]}\n"
    if stdout.strip():
        body += f"Chrome stdout (tail, 4000 chars max):\n{stdout.strip()[-4000:]}\n"
    body += "Debugging pointers:\n" + "\n".join(hints)
    return body


def _make_pdf_with_chrome(
    html_path: str | os.PathLike[str],
    pdf_path: str | os.PathLike[str],
    chrome: str,
    *,
    page_margin: str = "0.1in",
    zoom: float = 1.0,
    extra_styles: Sequence[object] | None = None,
    timeout_s: int = 120,
    debug: bool = False,
) -> str:
    """Render ``html_path`` to ``pdf_path`` with headless Chrome print-to-PDF.

    The source HTML is copied next to itself with any layout overrides
    (``page_margin`` → ``@page`` rule, ``zoom`` → CSS ``zoom``, extra styles)
    injected into ``<head>``, so relative resources (``figures/…``) resolve
    exactly as they do in the browser. Returns ``pdf_path``.
    """
    _log.debug(f"Creating PDF from HTML: {html_path} -> {pdf_path}")
    html_path = Path(html_path).resolve()
    pdf_path = Path(pdf_path).resolve()
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    # A CSS page-margin override on top of what the document already defines.
    injected = [f"@page {{ margin: {page_margin}; }}"]
    if zoom != 1.0:
        injected.append(f":root {{ zoom: {float(zoom)}; }}")
    for style in extra_styles or []:
        if isinstance(style, str):
            injected.append(style)
        elif isinstance(style, os.PathLike):
            injected.append(Path(style).read_text(encoding="utf-8"))
        else:
            _log.warning(
                f"Chrome PDF engine: skipping extra style of type {type(style).__name__} (pass CSS as a string or file path)."
            )

    if injected:
        original = html_path.read_text(encoding="utf-8")
        style_block = "<style>\n" + "\n".join(injected) + "\n</style>"
        if "</head>" in original:
            patched = original.replace("</head>", f"{style_block}\n</head>", 1)
        else:  # pragma: no cover - malformed doc; append at the top instead
            patched = style_block + "\n" + original
        tmp_html = html_path.with_name(f".{html_path.stem}.pdf-src.html")
        tmp_html.write_text(patched, encoding="utf-8")
        input_url = tmp_html.as_uri()
    else:
        tmp_html = None
        input_url = html_path.as_uri()

    # Isolate the Chrome profile so a running user session never collides.
    profile_dir = tempfile.mkdtemp(prefix="rs-pdf-profile-")
    cmd = [
        chrome,
        "--headless=new",
        "--disable-gpu",
        "--disable-extensions",
        "--disable-background-networking",
        "--disable-dev-shm-usage",  # Docker defaults /dev/shm to 64MB; Chrome renderers live there
        "--no-first-run",
        "--no-default-browser-check",
        "--hide-scrollbars",
        "--no-pdf-header-footer",
        f"--print-to-pdf={pdf_path}",
        f"--user-data-dir={profile_dir}",
        # Virtual time only needs to cover fonts/layout settling, not the
        # wall-clock deadline — a runaway budget keeps the process alive far
        # past the point where the PDF is already written.
        f"--virtual-time-budget={min(timeout_s, 30) * 1000}",
    ]
    if debug:
        # Verbose Chrome logging so a hang/crash can be diagnosed; the log
        # lands next to the PDF and the temp artifacts are kept (see finally).
        cmd += [
            "--enable-logging=stderr",
            "--v=1",
            f"--log-file={pdf_path.with_suffix('.chrome-debug.log')}",
        ]
    if (
        (hasattr(os, "geteuid") and os.geteuid() == 0)  # root (CI/docker)
        or _running_in_container()  # Docker/Podman without userns sandbox
        or os.environ.get("CHROME_NO_SANDBOX") == "1"  # explicit opt-out
    ):
        cmd.append("--no-sandbox")
    cmd.append(input_url)
    _log.debug(f"Chrome print-to-PDF command: {' '.join(shlex.quote(arg) for arg in cmd)}")

    chrome_version = _chrome_version(chrome)
    _log.info(
        f"Chrome PDF render: {chrome} ({chrome_version or 'version unknown'}) -> {pdf_path} (wall-clock budget {timeout_s}s)"
    )

    failure_kwargs = {
        "chrome": chrome,
        "chrome_version": chrome_version,
        "html_path": html_path,
        "input_url": input_url,
        "pdf_path": pdf_path,
    }

    # Headless Chrome writes the PDF as soon as the page is printable and
    # then keeps running until its virtual-time budget expires — sometimes
    # far longer than the render itself took. So the success signal is the
    # PDF file appearing and stabilizing, not the process exiting. Once the
    # file looks complete we stop waiting on Chrome and terminate it.
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    timed_out = False
    try:
        deadline = time.monotonic() + timeout_s
        last_size = -1
        stable_at = 0.0
        while True:
            if pdf_path.is_file():
                size = pdf_path.stat().st_size
                if size == last_size:
                    if size > 0 and time.monotonic() - stable_at >= 0.5:
                        break  # PDF complete and stable
                else:
                    last_size = size
                    stable_at = time.monotonic()
            if proc.poll() is not None:
                break  # Chrome exited on its own
            if time.monotonic() >= deadline:
                timed_out = True
                break
            time.sleep(0.2)
    finally:
        if proc.poll() is None:  # job done (or deadlined) but Chrome still alive
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:  # pragma: no cover - stuck child
                proc.kill()
                proc.wait(timeout=5)
        if debug:
            leftovers = [str(pdf_path.with_suffix(".chrome-debug.log"))]
            if tmp_html is not None:
                leftovers.append(str(tmp_html))
            leftovers.append(profile_dir)
            _log.warning(f"RS_PDF_DEBUG: kept for inspection: {', '.join(leftovers)}")
        else:
            shutil.rmtree(profile_dir, ignore_errors=True)
            if tmp_html is not None:  # pragma: no cover - tidy
                tmp_html.unlink(missing_ok=True)

    stdout = proc.stdout.read() if proc.stdout else ""
    stderr = proc.stderr.read() if proc.stderr else ""

    if timed_out:
        detail = _chrome_failure_message(
            f"Chrome print-to-PDF timed out after {timeout_s}s (no PDF produced)",
            **failure_kwargs,
            stderr=stderr,
            stdout=stdout,
        )
        _log.error(detail)
        _log.error(f"Chrome print-to-PDF timed out after {timeout_s}s. Output {_pdf_state(pdf_path)}.")
        raise RuntimeError(detail)

    if not pdf_path.is_file() or pdf_path.stat().st_size == 0:
        detail = _chrome_failure_message(
            f"Chrome print-to-PDF failed (exit {proc.returncode})",
            **failure_kwargs,
            stderr=stderr,
            stdout=stdout,
        )
        _log.error(detail)
        _log.error(f"Chrome print-to-PDF failed (exit {proc.returncode}). Output {_pdf_state(pdf_path)}.")
        raise RuntimeError(detail)

    _log.info(f"PDF written with Chrome engine ({chrome_version or chrome}): {pdf_path}")
    _log.info(f"PDF rendered with Chrome engine ({chrome_version or chrome})")
    return str(pdf_path)


def make_pdf_from_html(
    html_path: str,
    pdf_path: str | None = None,
    page_margin: str = "0.1in",
    zoom: float = 1.0,
    extra_styles: Sequence[object] | None = None,
    timeout_s: int | None = None,
    debug: bool = False,
) -> str:
    """Generate a PDF from an HTML file with headless Chrome print-to-PDF.

    Args:
        html_path: Path to the source HTML document.
        pdf_path: Where to write the PDF. Defaults to ``html_path`` with a
            ``.pdf`` extension.
        page_margin: CSS margin value injected into the ``@page`` rule.
        zoom: Zoom factor (1.0 = 100%).
        extra_styles: Extra CSS to apply. These must be CSS strings or file
            paths (other objects are ignored with a warning).
        timeout_s: Wall-clock budget (seconds) before Chrome print-to-PDF is
            aborted. Defaults to the ``RS_PDF_TIMEOUT_S`` env var, or 120.
        debug: When True, keep Chrome's verbose log and the injected-source
            copy next to the PDF and add ``--enable-logging`` so a hang or
            crash can be inspected. Also enabled by ``RS_PDF_DEBUG=1``.

    Returns:
        Path to the generated PDF file.

    Raises:
        RuntimeError: If no Chrome/Chromium/Edge binary can be found, or if
            the Chrome print-to-PDF run fails or times out.
    """
    pdf_path_final = pdf_path if pdf_path else os.path.splitext(html_path)[0] + ".pdf"

    chrome = find_chrome()
    if chrome is None:
        raise RuntimeError(
            "No Chrome/Chromium/Edge binary found for PDF export. Set CHROME_PATH or install a browser."
        )

    if timeout_s is None:
        timeout_s = int(os.environ.get("RS_PDF_TIMEOUT_S", "120"))
    debug = debug or os.environ.get("RS_PDF_DEBUG") == "1"

    return _make_pdf_with_chrome(
        html_path,
        pdf_path_final,
        chrome,
        page_margin=page_margin,
        zoom=zoom,
        extra_styles=extra_styles,
        timeout_s=timeout_s,
        debug=debug,
    )
