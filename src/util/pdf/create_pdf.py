"""PDF generation from rendered report HTML.

Two engines are available:

* **Chrome/Chromium ``--headless`` print-to-PDF** (default when a browser
  binary is found). This is a real browser engine, so the PDF is laid out by
  the *same* CSS engine that renders the HTML on screen: CSS Grid, Flexbox,
  ``calc()`` with custom properties, webfonts, ``@media print`` — everything
  behaves identically. This is the only way the PDF can look exactly like the
  static HTML report for every section and control.

* **WeasyPrint** (fallback). Kept for environments with no browser binary on
  PATH/standard install locations. Note that WeasyPrint does **not** implement
  CSS Grid, so grid-based layouts (Pico's ``.grid``, ``.metric-grid``, …)
  stack vertically instead of rendering side-by-side.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

try:  # pragma: no cover - environment-dependent
    import weasyprint
except (OSError, ImportError):  # missing native libs (pango/gobject)
    weasyprint = None

logger = logging.getLogger(__name__)

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


def _make_pdf_with_chrome(
    html_path: str | os.PathLike[str],
    pdf_path: str | os.PathLike[str],
    chrome: str,
    *,
    page_margin: str = "0.1in",
    zoom: float = 1.0,
    extra_styles: Sequence[object] | None = None,
    timeout_s: int = 120,
) -> str:
    """Render ``html_path`` to ``pdf_path`` with headless Chrome print-to-PDF.

    The source HTML is copied next to itself with any layout overrides
    (``page_margin`` → ``@page`` rule, ``zoom`` → CSS ``zoom``, extra styles)
    injected into ``<head>``, so relative resources (``figures/…``) resolve
    exactly as they do in the browser. Returns ``pdf_path``.
    """
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
            logger.warning(
                "Chrome PDF engine: skipping extra style of type %s (pass CSS as a string or path; WeasyPrint CSS objects are engine-specific).",
                type(style).__name__,
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
        f"--virtual-time-budget={int(timeout_s * 1000)}",
    ]
    if (
        (hasattr(os, "geteuid") and os.geteuid() == 0)  # root (CI/docker)
        or _running_in_container()  # Docker/Podman without userns sandbox
        or os.environ.get("CHROME_NO_SANDBOX") == "1"  # explicit opt-out
    ):
        cmd.append("--no-sandbox")
    cmd.append(input_url)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s + 30)
    except subprocess.TimeoutExpired as err:  # pragma: no cover - hang guard
        raise RuntimeError(f"Chrome print-to-PDF timed out after {timeout_s}s: {err}") from err
    finally:
        shutil.rmtree(profile_dir, ignore_errors=True)
        if tmp_html is not None:  # pragma: no cover - tidy
            tmp_html.unlink(missing_ok=True)

    if result.returncode != 0 or not pdf_path.is_file() or pdf_path.stat().st_size == 0:
        detail = (result.stderr or result.stdout or "").strip()[-800:]
        raise RuntimeError(f"Chrome print-to-PDF failed (exit {result.returncode}): {detail or 'no output'}")

    version = _chrome_version(chrome)
    logger.info("PDF written with Chrome engine (%s): %s", version or chrome, pdf_path)
    _log.info(f"PDF rendered with Chrome engine ({version or chrome})")
    return str(pdf_path)


def _make_pdf_with_weasyprint(
    html_path: str | os.PathLike[str],
    pdf_path: str | os.PathLike[str],
    page_margin: str,
    zoom: float,
    extra_styles: Sequence[object] | None,
) -> str:
    """Original WeasyPrint render path (unchanged behavior)."""
    if weasyprint is None:
        raise RuntimeError("WeasyPrint is not importable (missing pango/gobject native libs?). Install a Chrome/Chromium binary for PDF generation instead.")
    margin_css = weasyprint.CSS(
        string=(f"@page {{ margin: {page_margin}; }} body {{ margin: 0 !important; padding: 0 !important; }}"),
        media_type="print",
    )
    stylesheets = [margin_css]
    if extra_styles:
        stylesheets.extend(extra_styles)
    weasyprint.HTML(filename=str(html_path), base_url=str(Path(html_path).parent)).write_pdf(
        str(pdf_path),
        stylesheets=stylesheets,
        zoom=zoom,
        presentational_hints=True,
    )
    logger.info("PDF written with WeasyPrint engine: %s", pdf_path)
    _log.info("PDF rendered with WeasyPrint engine")
    return str(pdf_path)


def make_pdf_from_html(
    html_path: str,
    pdf_path: str | None = None,
    page_margin: str = "0.1in",
    zoom: float = 1.0,
    extra_styles: Sequence[object] | None = None,
    engine: str = "auto",
) -> str:
    """Generate a PDF from an HTML file.

    Args:
        html_path: Path to the source HTML document.
        pdf_path: Where to write the PDF. Defaults to ``html_path`` with a
            ``.pdf`` extension.
        page_margin: CSS margin value injected into the ``@page`` rule.
        zoom: Zoom factor (1.0 = 100%).
        extra_styles: Extra CSS to apply. For the Chrome engine these must be
            CSS strings or file paths (WeasyPrint CSS objects are ignored with
            a warning); the WeasyPrint engine accepts WeasyPrint CSS objects.
        engine: One of ``"auto"`` (default: Chrome when available, otherwise
            WeasyPrint), ``"chrome"`` (Chrome, raising if unavailable or on
            failure) or ``"weasyprint"`` (always WeasyPrint).

    Returns:
        Path to the generated PDF file.
    """
    pdf_path_final = pdf_path if pdf_path else os.path.splitext(html_path)[0] + ".pdf"

    if engine == "weasyprint":
        return _make_pdf_with_weasyprint(html_path, pdf_path_final, page_margin, zoom, extra_styles)

    if engine not in ("auto", "chrome"):
        raise ValueError(f"Unknown PDF engine: {engine!r} (expected 'auto', 'chrome' or 'weasyprint')")

    chrome = find_chrome()
    if chrome is None and engine == "chrome":
        raise RuntimeError("No Chrome/Chromium/Edge binary found for engine='chrome'. Set CHROME_PATH or install a browser (or use engine='weasyprint').")

    if chrome is None:  # pragma: no cover - machines with browsers won't hit this
        logger.warning("No Chrome/Chromium/Edge binary found — falling back to WeasyPrint. WeasyPrint does not support CSS Grid, so grid layouts will stack instead of matching the HTML. Set CHROME_PATH to enable the Chrome engine.")
        _log.warning("No Chrome/Chromium/Edge binary found — using WeasyPrint fallback (no CSS Grid: grid layouts will stack, not match the HTML). Set CHROME_PATH to enable the Chrome engine.")
        return _make_pdf_with_weasyprint(html_path, pdf_path_final, page_margin, zoom, extra_styles)

    try:
        return _make_pdf_with_chrome(
            html_path,
            pdf_path_final,
            chrome,
            page_margin=page_margin,
            zoom=zoom,
            extra_styles=extra_styles,
        )
    except Exception as err:  # noqa: BLE001 - engine failure, try the fallback
        if engine == "chrome":
            raise
        logger.warning("Chrome PDF render failed (%s) — falling back to WeasyPrint.", err)
        _log.warning(f"Chrome PDF render failed ({err}) — falling back to WeasyPrint.")
        return _make_pdf_with_weasyprint(html_path, pdf_path_final, page_margin, zoom, extra_styles)
