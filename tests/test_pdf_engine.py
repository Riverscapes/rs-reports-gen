"""Tests for the PDF engine (Chrome print-to-PDF vs WeasyPrint fallback)."""

import importlib
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest

from util.pdf.create_pdf import find_chrome, make_pdf_from_html

#: Grab the *module* (not the function) for monkeypatching internals.
PDF_MODULE = importlib.import_module("util.pdf.create_pdf")

TINY_HTML = """<!DOCTYPE html><html><head><title>t</title></head>
<body><div class="grid"><p>one</p><p>two</p></div></body></html>"""


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


class _FakeCompleted:
    def __init__(self, version=False):
        self.returncode = 0
        self.stdout = "Google Chrome 151.0.7922.34\n" if version else ""
        self.stderr = ""


def test_chrome_engine_injects_margin_and_renders(monkeypatch, tmp_path):
    html = tmp_path / "report_static.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    seen = {}

    def fake_run(cmd, capture_output, text, timeout):  # noqa: ARG001
        if any(a.startswith("--print-to-pdf=") for a in cmd):
            seen["cmd"] = cmd
            # chrome writes the pdf to --print-to-pdf=<path>; emulate that.
            pdf_arg = next(a for a in cmd if a.startswith("--print-to-pdf="))
            pdf_path = Path(pdf_arg.split("=", 1)[1])
            pdf_path.write_bytes(b"%PDF-1.4 chrome")
            # capture the patched source (the command's last arg is the file:// URL)
            src = Path(unquote(urlparse(cmd[-1]).path))
            seen["src"] = src.read_text(encoding="utf-8")
        return _FakeCompleted(version=any("--version" in a for a in cmd))

    monkeypatch.setattr(PDF_MODULE, "find_chrome", lambda: "/usr/bin/fake-chrome")
    monkeypatch.setattr(PDF_MODULE.subprocess, "run", fake_run)

    out = make_pdf_from_html(str(html), str(tmp_path / "out.pdf"), page_margin="0.25in")

    assert Path(out).read_bytes() == b"%PDF-1.4 chrome"
    # flags that guarantee a clean, browser-like print
    assert "--no-pdf-header-footer" in seen["cmd"]
    assert any(a.startswith("--print-to-pdf=") for a in seen["cmd"])
    assert "--headless=new" in seen["cmd"]
    # the margin is injected as an @page rule into a temp copy (browser honors it)
    assert "@page { margin: 0.25in; }" in seen["src"]
    assert seen["src"].index("</head>") < seen["src"].index("@page { margin: 0.25in; }") + 100
    # temp source is cleaned up afterwards
    assert not Path(tmp_path / ".report_static.pdf-src.html").exists()


def test_chrome_engine_injects_extra_styles_from_path(monkeypatch, tmp_path):
    html = tmp_path / "report.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    extra = tmp_path / "extra.css"
    extra.write_text("body { color: red; }", encoding="utf-8")
    seen = {}

    def fake_run(cmd, capture_output, text, timeout):  # noqa: ARG001
        if any(a.startswith("--print-to-pdf=") for a in cmd):
            pdf_arg = next(a for a in cmd if a.startswith("--print-to-pdf="))
            Path(pdf_arg.split("=", 1)[1]).write_bytes(b"%PDF-1.4")
            src = Path(unquote(urlparse(cmd[-1]).path))
            seen["src"] = src.read_text(encoding="utf-8")
        return _FakeCompleted(version=True)

    monkeypatch.setattr(PDF_MODULE, "find_chrome", lambda: "/usr/bin/fake-chrome")
    monkeypatch.setattr(PDF_MODULE.subprocess, "run", fake_run)
    make_pdf_from_html(str(html), str(tmp_path / "out2.pdf"), extra_styles=[extra])
    assert "body { color: red; }" in seen["src"]


def test_auto_falls_back_to_weasyprint_when_no_chrome(monkeypatch, tmp_path):
    html = tmp_path / "report.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    monkeypatch.setattr(PDF_MODULE, "find_chrome", lambda: None)
    out = make_pdf_from_html(str(html), str(tmp_path / "out3.pdf"))
    assert Path(out).read_bytes().startswith(b"%PDF")


def test_chrome_engine_raises_when_no_browser(monkeypatch, tmp_path):
    html = tmp_path / "report.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    monkeypatch.setattr(PDF_MODULE, "find_chrome", lambda: None)
    with pytest.raises(RuntimeError, match="No Chrome"):
        make_pdf_from_html(str(html), engine="chrome")


def test_invalid_engine_value_raises(monkeypatch, tmp_path):
    html = tmp_path / "report.html"
    html.write_text(TINY_HTML, encoding="utf-8")
    with pytest.raises(ValueError, match="Unknown PDF engine"):
        make_pdf_from_html(str(html), engine="skia")
