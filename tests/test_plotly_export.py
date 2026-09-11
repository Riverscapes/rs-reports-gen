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


class TestMapGeoFallback:
    """The WebGL/tile-free `geo` subplot fallback for MapLibre figures."""

    def _map_fig(self):
        import plotly.graph_objects as go

        fig = go.Figure()
        fig.add_trace(
            go.Choroplethmap(
                geojson={
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"id": 1},
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [[[-117.0, 44.0], [-111.0, 44.0], [-111.0, 47.0], [-117.0, 47.0], [-117.0, 44.0]]],
                            },
                        }
                    ],
                },
                locations=[1],
                z=[1],
                colorscale=[[0, "#2171a8"], [1, "#2171a8"]],
                featureidkey="properties.id",
            )
        )
        fig.add_trace(
            go.Scattermap(
                lon=[-116.5, -114.0, -116.5, None],
                lat=[44.5, 46.5, 46.5, None],
                mode="lines",
                line={"color": "red", "width": 3},
                name="AOI",
            )
        )
        return fig

    def test_is_map_figure_detects_maplibre_traces(self):
        assert EXPORT_MODULE._is_map_figure(self._map_fig())
        assert not EXPORT_MODULE._is_map_figure(_tiny_fig())

    def test_map_traces_to_geo_returns_equivalent_geo_figure(self):
        geo = EXPORT_MODULE._map_traces_to_geo(self._map_fig())
        assert geo is not None
        types = [t.type for t in geo.data]
        assert "choropleth" in types
        assert "scattergeo" in types
        # All map traces converted — no MapLibre trace survives.
        assert not any(t in types for t in ("choroplethmap", "scattermap"))
        # The geo layout is set up with land/ocean + a fitted bbox.
        assert geo.layout.geo.showland
        # bbox padded around the AOI (lat ~44-47, lon ~-117..-111)
        assert geo.layout.geo.lataxis.range[0] < 44
        assert geo.layout.geo.lataxis.range[1] > 47
        assert geo.layout.geo.lonaxis.range[0] < -117
        assert geo.layout.geo.lonaxis.range[1] > -111
        assert geo.layout.height == 500

    def test_non_map_figure_returns_none(self):
        assert EXPORT_MODULE._map_traces_to_geo(_tiny_fig()) is None

    def test_geo_fallback_writes_file_on_success(self, monkeypatch, tmp_path):
        def fake_write(fig, img_path, timeout_s=120):  # noqa: ARG001
            img_path.write_text("geo svg", encoding="utf-8")

        monkeypatch.setattr(EXPORT_MODULE, "write_image_with_timeout", fake_write)
        out = tmp_path / "map.svg"
        ok = EXPORT_MODULE._write_map_geo_fallback(self._map_fig(), out)
        assert ok
        assert out.read_text(encoding="utf-8") == "geo svg"

    def test_geo_fallback_returns_false_on_failure(self, monkeypatch, tmp_path):
        def fail_write(fig, img_path, timeout_s=120):  # noqa: ARG001
            raise KaleidoError(0, "Map error.")

        monkeypatch.setattr(EXPORT_MODULE, "write_image_with_timeout", fail_write)
        assert not EXPORT_MODULE._write_map_geo_fallback(self._map_fig(), tmp_path / "map.svg")

    def test_geo_fallback_false_for_non_map(self, tmp_path):
        assert not EXPORT_MODULE._write_map_geo_fallback(_tiny_fig(), tmp_path / "map.svg")

    def test_export_figure_uses_geo_fallback_after_retries(self, monkeypatch, tmp_path):
        """After kaleido fails on a map figure, the geo fallback snapshot is used
        instead of the gray placeholder, and the report still builds."""
        calls = {"n": 0}

        def fail_then_fallback(fig, img_path, timeout_s=120):  # noqa: ARG001
            calls["n"] += 1
            if calls["n"] <= 3:
                raise KaleidoError(0, "Map error.")
            # the 4th call is the geo fallback render
            img_path.write_text("geo fallback", encoding="utf-8")

        monkeypatch.setattr(EXPORT_MODULE, "write_image_with_timeout", fail_then_fallback)
        monkeypatch.setattr(EXPORT_MODULE.time, "sleep", lambda _s: None)

        frag = export_figure(self._map_fig(), tmp_path, "map", "svg", report_dir=tmp_path)
        assert frag == '<img src="map.svg">'
        out = (tmp_path / "map.svg").read_text(encoding="utf-8")
        assert out == "geo fallback"
        assert calls["n"] == 4

    def test_export_figure_placeholder_when_even_geo_fallback_fails(self, monkeypatch, tmp_path):
        def always_fail(fig, img_path, timeout_s=120):  # noqa: ARG001
            raise KaleidoError(0, "Map error.")

        monkeypatch.setattr(EXPORT_MODULE, "write_image_with_timeout", always_fail)
        monkeypatch.setattr(EXPORT_MODULE.time, "sleep", lambda _s: None)

        frag = export_figure(self._map_fig(), tmp_path, "map", "svg", report_dir=tmp_path)
        assert frag == '<img src="map.svg">'
        out = (tmp_path / "map.svg").read_text(encoding="utf-8")
        assert "Figure unavailable" in out  # degraded to placeholder
