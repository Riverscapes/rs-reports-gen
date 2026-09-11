"""Tests for the PDF engine (Chrome print-to-PDF)."""

import importlib
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest

from util.pdf.create_pdf import find_chrome, make_pdf_from_html

#: Grab the *module* (not the function) for monkeypatching internals.
PDF_MODULE = importlib.import_module("util.pdf.create_pdf")

TINY_HTML = """<!DOCTYPE html><html><head><title>t</title></head>
<body><div class="grid"><p>one</p><p>two</p></div></body></html>"""


class _Stream:
    """Fake pipe: the Popen stdout/stderr surface (a .read() call)."""

    def __init__(self, text: str = ""):
        self._text = text

    def read(self) -> str:
        return self._text


class _FakeProc:
    """Minimal Popen stand-in that mimics real Chrome's quirk.

    The PDF file is written, but the process never exits on its own — the
    file-stability watcher has to notice completion and terminate it.
    Also answers the ``--version`` probe that runs through subprocess.run.
    """

    print_proc: "_FakeProc | None" = None

    def __init__(self, cmd, *, stdout, stderr, text):  # noqa: ARG002
        self.args = list(cmd)
        self.stdout = _Stream("")
        self.stderr = _Stream("[fake] chrome stderr noise")
        self.returncode = 0
        self.captured = {"cmd": list(cmd), "src": ""}
        if any(a == "--version" for a in cmd):
            self.stdout = _Stream("Google Chrome 151.0.7922.34\n")
            self.stderr = _Stream("")
            return
        pdf_arg = next(a for a in cmd if a.startswith("--print-to-pdf="))
        pdf_path = Path(pdf_arg.split("=", 1)[1])
        # Chrome's actual job: the PDF file appears...
        pdf_path.write_bytes(b"%PDF-1.4 chrome")
        # ...and the injected source it rendered from exists right there.
        src = Path(unquote(urlparse(cmd[-1]).path))
        self.captured["src"] = src.read_text(encoding="utf-8")
        _FakeProc.print_proc = self

    def poll(self):
        return None  # never exits on its own — that is the hang this code fixes

    def communicate(self, input=None, timeout=None):  # noqa: ARG002
        return (self.stdout.read(), self.stderr.read())

    def terminate(self):
        pass

    def kill(self):
        pass

    def wait(self, timeout=None):  # noqa: ARG002
        return 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _HangProc:
    """Never writes the PDF and never exits: exercises the wall-clock timeout."""

    def __init__(self, cmd, *, stdout, stderr, text):  # noqa: ARG002
        self.args = list(cmd)
        self.stdout = _Stream("")
        self.stderr = _Stream("[hang] renderer waiting on font fetch")
        self.returncode = 0

    def poll(self):
        return None

    def terminate(self):
        pass

    def kill(self):
        pass

    def wait(self, timeout=None):  # noqa: ARG002
        return 0

    def communicate(self, input=None, timeout=None):  # noqa: ARG002
        return (self.stdout.read(), self.stderr.read())

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _patch_popen(monkeypatch, proc_cls=_FakeProc):
    monkeypatch.setattr(PDF_MODULE, "find_chrome", lambda: "/usr/bin/fake-chrome")
    monkeypatch.setattr(PDF_MODULE.subprocess, "Popen", proc_cls)


def test_find_chrome_prefers_chrome_path_env(monkeypatch, tmp_path):
    fake_bin = tmp_path / "chrome-for-testing"
    fake_bin.write_text("#!/bin/sh\necho ok\n", encoding="utf-8")
    fake_bin.chmod(0o755)
    monkeypatch.setenv("CHROME_PATH", str(fake_bin))
    monkeypatch.delenv("BROWSER_PATH", raising=False)
    assert find_chrome() == str(fake_bin)


def test_find_chrome_returns_none_when_nothing_found(monkeypatch, tmp_path):
    monkeypatch.delenv("CHROME_PATH", raising=False)
    monkeypatch.delenv("BROWSER_PATH", raising=False)
    monkeypatch.setattr(PDF_MODULE.shutil, "which", lambda _name: None)
    # Pretend we're on a platform with no known install locations.
    monkeypatch.setattr(PDF_MODULE.sys, "platform", "commodore-64")
    monkeypatch.setattr(PDF_MODULE, "_PLAYWRIGHT_CACHE_DIRS", (str(tmp_path / "no-playwright"),))
    assert find_chrome() is None


def test_chrome_engine_injects_margin_and_renders(monkeypatch, tmp_path):
    html = tmp_path / "report_static.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    _patch_popen(monkeypatch)

    out = make_pdf_from_html(str(html), str(tmp_path / "out.pdf"), page_margin="0.25in")

    assert Path(out).read_bytes() == b"%PDF-1.4 chrome"
    cmd = _FakeProc.print_proc.captured["cmd"]
    src = _FakeProc.print_proc.captured["src"]
    # flags that guarantee a clean, browser-like print
    assert "--no-pdf-header-footer" in cmd
    assert any(a.startswith("--print-to-pdf=") for a in cmd)
    assert "--headless=new" in cmd
    # the virtual-time budget is capped so a finished render cannot dally
    budget = next(int(a.split("=")[1]) for a in cmd if a.startswith("--virtual-time-budget="))
    assert 0 < budget <= 30_000
    # the margin is injected as an @page rule into a temp copy (browser honors it)
    assert "@page { margin: 0.25in; }" in src
    assert src.index("</head>") < src.index("@page { margin: 0.25in; }") + 100
    # temp source is cleaned up afterwards
    assert not Path(tmp_path / ".report_static.pdf-src.html").exists()


def test_chrome_engine_injects_extra_styles_from_path(monkeypatch, tmp_path):
    html = tmp_path / "report.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    extra = tmp_path / "extra.css"
    extra.write_text("body { color: red; }", encoding="utf-8")
    _patch_popen(monkeypatch)

    make_pdf_from_html(str(html), str(tmp_path / "out2.pdf"), extra_styles=[extra])

    assert "body { color: red; }" in _FakeProc.print_proc.captured["src"]


def test_chrome_engine_raises_when_no_browser(monkeypatch, tmp_path):
    html = tmp_path / "report.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    monkeypatch.setattr(PDF_MODULE, "find_chrome", lambda: None)
    with pytest.raises(RuntimeError, match="No Chrome"):
        make_pdf_from_html(str(html))


def test_timeout_reports_diagnostics(monkeypatch, tmp_path):
    html = tmp_path / "report.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    _patch_popen(monkeypatch, proc_cls=_HangProc)
    monkeypatch.setattr(PDF_MODULE.time, "sleep", lambda _s: None)  # fast test

    with pytest.raises(RuntimeError) as ei:
        make_pdf_from_html(str(html), str(tmp_path / "out.pdf"), timeout_s=1)

    msg = str(ei.value)
    assert "timed out" in msg
    assert "fake-chrome" in msg                # which binary was in use
    assert "waiting on font fetch" in msg      # Chrome's stderr surfaced
    assert "RS_PDF_DEBUG=1" in msg             # debugging pointer included


def test_debug_mode_keeps_artifacts(monkeypatch, tmp_path):
    html = tmp_path / "report.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    original_mkdtemp = PDF_MODULE.tempfile.mkdtemp

    def fake_mkdtemp(prefix, **_kwargs):
        return original_mkdtemp(prefix=prefix, dir=str(tmp_path))

    monkeypatch.setattr(PDF_MODULE.tempfile, "mkdtemp", fake_mkdtemp)
    _patch_popen(monkeypatch, proc_cls=_HangProc)
    monkeypatch.setattr(PDF_MODULE.time, "sleep", lambda _s: None)

    with pytest.raises(RuntimeError):
        make_pdf_from_html(str(html), str(tmp_path / "out.pdf"), timeout_s=1, debug=True)

    # temp injected source + chrome profile dir survive for inspection
    assert (tmp_path / ".report.pdf-src.html").exists()
    assert any(p.name.startswith("rs-pdf-profile-") for p in tmp_path.iterdir())