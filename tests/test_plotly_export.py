"""Tests for the Plotly figure export pipeline (kaleido, error handling)."""

import importlib
from pathlib import Path

import plotly.graph_objects as go
import pytest
from kaleido._kaleido_tab import KaleidoError

from util.plotly.export_figure import (
    _raise_error_from_worker,
    _write_placeholder_image,
    export_figure,
    write_image_with_timeout,
)

# Grab the *module* (for monkeypatching internals), not just imports above.
EXPORT_MODULE = importlib.import_module("util.plotly.export_figure")


def _tiny_fig():
    fig = go.Figure()
    fig.add_scatter(x=[1, 2, 3], y=[1, 2, 3])
    return fig


class TestRoundTripExceptionType:
    def test_kaleido_error_round_trips(self):
        with pytest.raises(KaleidoError, match="Map error"):
            _raise_error_from_worker("kaleido._kaleido_tab", "KaleidoError", "Map error.")

    def test_timeout_error_round_trips(self):
        with pytest.raises(TimeoutError):
            _raise_error_from_worker("builtins", "TimeoutError", "timed out")

    def test_unknown_module_falls_back_to_runtime_error(self):
        with pytest.raises(RuntimeError, match="Image export failed"):
            _raise_error_from_worker("no.such.module", "WeirdError", "boom")

    def test_worker_sends_type_parts_not_repr(self):
        """The child worker must transport module/name/message so the parent can
        re-raise the original exception class (KaleidoError must stay a
        KaleidoError so export_figure's graceful handler runs)."""
        received = []

        class _FakeQueue:
            def put(self, item):
                received.append(item)

        def boom(*_a, **_k):
            raise KaleidoError(0, "Map error.")

        EXPORT_MODULE.pio.from_json = boom
        try:
            EXPORT_MODULE._write_image_worker("{}", Path("unused.svg"), _FakeQueue())
        finally:
            del EXPORT_MODULE.pio.from_json
        status, module, name, message = received[0]
        assert status == "err"
        assert module == "kaleido._kaleido_tab"
        assert name == "KaleidoError"
        assert "Map error" in message


class TestRetryAndPlaceholder:
    def test_retries_on_kaleido_error_then_succeeds(self, monkeypatch, tmp_path):
        calls = {"n": 0}

        def flaky_write(fig, img_path, timeout_s=120):  # noqa: ARG001
            calls["n"] += 1
            if calls["n"] < 3:
                raise KaleidoError(0, "Map error.")
            img_path.write_bytes(b"ok")

        monkeypatch.setattr(EXPORT_MODULE, "write_image_with_timeout", flaky_write)
        monkeypatch.setattr(EXPORT_MODULE.time, "sleep", lambda _s: None)

        frag = export_figure(_tiny_fig(), tmp_path, "chart", "svg", report_dir=tmp_path)
        assert frag == '<img src="chart.svg">'
        assert (tmp_path / "chart.svg").read_bytes() == b"ok"
        assert calls["n"] == 3, "should have retried twice then succeeded"

    def test_gives_up_with_placeholder_after_max_retries(self, monkeypatch, tmp_path):
        def always_fail(fig, img_path, timeout_s=120):  # noqa: ARG001
            raise KaleidoError(0, "Map error.")

        monkeypatch.setattr(EXPORT_MODULE, "write_image_with_timeout", always_fail)
        monkeypatch.setattr(EXPORT_MODULE.time, "sleep", lambda _s: None)

        frag = export_figure(_tiny_fig(), tmp_path, "map", "svg", report_dir=tmp_path)
        assert frag == '<img src="map.svg">'  # report still builds
        placeholder = (tmp_path / "map.svg").read_text(encoding="utf-8")
        assert "Figure unavailable" in placeholder  # no broken <img>

    def test_placeholder_svg_written(self, tmp_path):
        out = tmp_path / "fig.svg"
        _write_placeholder_image(out, "svg")
        assert "Figure unavailable" in out.read_text(encoding="utf-8")

    def test_placeholder_png_written_with_pillow(self, tmp_path):
        out = tmp_path / "fig.png"
        _write_placeholder_image(out, "png")
        assert out.read_bytes().startswith(b"\x89PNG")

    def test_success_needs_no_placeholder(self, monkeypatch, tmp_path):
        def good_write(fig, img_path, timeout_s=120):  # noqa: ARG001
            img_path.write_bytes(b"ok")

        monkeypatch.setattr(EXPORT_MODULE, "write_image_with_timeout", good_write)
        frag = export_figure(_tiny_fig(), tmp_path, "ok", "svg", report_dir=tmp_path)
        assert (tmp_path / "ok.svg").read_bytes() == b"ok"
        assert frag == '<img src="ok.svg">'

    def test_placeholder_does_not_abort_on_timeout(self, monkeypatch, tmp_path):
        def always_timeout(fig, img_path, timeout_s=120):  # noqa: ARG001
            raise TimeoutError("hung")

        monkeypatch.setattr(EXPORT_MODULE, "write_image_with_timeout", always_timeout)
        monkeypatch.setattr(EXPORT_MODULE.time, "sleep", lambda _s: None)
        frag = export_figure(_tiny_fig(), tmp_path, "slow", "png", report_dir=tmp_path)
        assert frag == '<img src="slow.png">'
        assert (tmp_path / "slow.png").is_file()


class TestWriteImageWithTimeout:
    def test_success_path(self, tmp_path):
        out = tmp_path / "ok.svg"
        write_image_with_timeout(_tiny_fig(), out, timeout_s=60)
        assert out.exists()
