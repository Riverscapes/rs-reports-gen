# import psutil  # for debugging
import base64
import importlib
import multiprocessing as mp
import os
import re
import time
import uuid
from pathlib import Path

import plotly.graph_objects as go
import plotly.io as pio
from kaleido._kaleido_tab import KaleidoError
from rsxml import Logger

#: Matches the plotly container pio.to_html(full_html=False) emits:
#: ``<div id="<uuid>" class="plotly-graph-div" ...>``. The id is a hash of the
#: figure contents, so the SAME exported fragment embedded twice in one page
#: collides (both scripts target the first div and later embeds render empty).
_PLOT_DIV_ID_RE = re.compile(r'<div id="([0-9a-fA-F-]{36})" class="plotly-graph-div"')


def unique_plot_fragment(fragment: str) -> str:
    """Rewrite the plotly div id in an exported interactive fragment to a fresh uuid.

    ``pio.to_html`` derives the container id from a hash of the figure, so the
    same exported fragment embedded twice (e.g. one figure reused in two
    sections) shares one div id: both ``Plotly.newPlot`` scripts target the
    first div and the later copy renders empty. Applying this on *every*
    embed guarantees a unique id per usage. Static (``<img>``) fragments pass
    through unchanged.

    Registered as the ``unique_plot`` Jinja global (``RSReport.render``) for
    the ``render_figure`` macro; call it on each embed, not once per figure.
    """
    match = _PLOT_DIV_ID_RE.search(fragment)
    if match is None:
        return fragment
    new_id = "fig-" + uuid.uuid4().hex
    return fragment.replace(match.group(1), new_id)


def _write_image_worker(fig_json: str, img_path: Path, q):
    """Worker runs in a separate process to avoid hangs."""
    try:
        # Reconstruct the figure in the child process
        fig = pio.from_json(fig_json)
        fig.write_image(img_path)
        q.put(("ok", None))
    except Exception as e:
        # Send the exception *type* (module + name) with its message so the
        # parent can re-raise the same class. pickling arbitrary exceptions
        # across process boundaries is unreliable, so we transport the parts
        # and rebuild with the import below.
        q.put(("err", type(e).__module__, type(e).__name__, str(e)))


def _raise_error_from_worker(err_module: str, err_name: str, err_message: str) -> None:
    """Re-raise the original exception class reported by the child worker.

    Preserves the specific error type (KaleidoError, TimeoutError, …) instead
    of collapsing everything to RuntimeError, so callers that handle known
    failure modes (e.g. transient network/WebGL hiccups) can catch them.
    """
    try:
        err_cls = getattr(importlib.import_module(err_module), err_name)
        if issubclass(err_cls, Exception):
            if err_name == "KaleidoError":
                # KaleidoError.__init__(code, message) — code is just a number.
                raise err_cls(0, err_message)
            raise err_cls(err_message)
    except (ImportError, AttributeError):
        pass
    raise RuntimeError(f"Image export failed: {err_name}: {err_message}")


#: Plotly trace types rendered by MapLibre (need WebGL + remote tiles).
#: If the tile service hiccups these fail with a generic "Map error." even
#: though WebGL is fine — we fall back to a plain `geo` subplot for those.
_MAP_TRACE_TYPES = (
    "scattermap",
    "choroplethmap",
    "densitymapbox",
    "scattermapbox",
    "choroplethmapbox",
)


def _is_map_figure(fig: go.Figure) -> bool:
    """True when the figure contains MapLibre (WebGL/tile-fetching) traces."""
    return any(t.type in _MAP_TRACE_TYPES for t in fig.data)


def _map_traces_to_geo(fig: go.Figure) -> go.Figure | None:
    """Build a WebGL-free snapshot of a map figure using the `geo` subplot.

    Convert MapLibre traces (``choroplethmap`` / ``scattermap`` and the mapbox
    variants) into ``choropleth`` / ``scattergeo`` traces, which Plotly renders
    as plain SVG. No WebGL, no remote basemap tiles — a fallback that works
    even when ``tiles.riverscapes.net`` is down. Returns ``None`` when the
    figure isn't a map or can't be converted.
    """
    if not _is_map_figure(fig):
        return None

    import plotly.graph_objects as _go

    new_fig: go.Figure = _go.Figure()
    min_lat = min_lon = 1e9
    max_lat = max_lon = -1e9
    converted = False

    def _extrude_bbox(lon: float | None, lat: float | None) -> None:
        nonlocal min_lat, min_lon, max_lat, max_lon
        if lon is None or lat is None:
            return
        min_lat = min(min_lat, lat)
        max_lat = max(max_lat, lat)
        min_lon = min(min_lon, lon)
        max_lon = max(max_lon, lon)

    for tr in fig.data:
        d = tr.to_plotly_json()
        if tr.type in ("choroplethmap", "choroplethmapbox"):
            geojson = d.get("geojson")
            locations = d.get("locations")
            # locations can be an ndarray (e.g. from px.choropleth_map) —
            # normalize to a plain list before checking emptiness (an ndarray
            # is not boolean-evaluable, so "or []" would raise).
            locations = list(locations) if locations is not None else []
            if not geojson or not locations:
                continue
            colorscale = d.get("colorscale") or [[0, "#1f5ca5"], [1, "#1f5ca5"]]
            color = colorscale[0][1]
            new_fig.add_trace(
                _go.Choropleth(
                    geojson=geojson,
                    locations=locations,
                    z=[1] * len(locations),  # solid fill from colorscale
                    colorscale=[[0, color], [1, color]],
                    featureidkey=d.get("featureidkey", "properties.id"),
                    hovertemplate=d.get("hovertemplate"),
                    hovertext=d.get("hovertext"),
                    marker={"opacity": (d.get("marker") or {}).get("opacity", 1)},
                    name=d.get("name"),
                    showlegend=d.get("showlegend", False),
                )
            )
            for feat in geojson.get("features", []):
                geom = feat.get("geometry") or {}
                if geom.get("type") == "Polygon":
                    for ring in geom["coordinates"]:
                        for lon, lat in ring:
                            _extrude_bbox(lon, lat)
                elif geom.get("type") == "MultiPolygon":
                    for poly in geom.get("coordinates", []):
                        for ring in poly:
                            for lon, lat in ring:
                                _extrude_bbox(lon, lat)
            converted = True
        elif tr.type in ("scattermap", "scattermapbox", "densitymapbox"):
            lons = list(d.get("lon") or [])
            lats = list(d.get("lat") or [])
            if not lons or not lats:
                continue
            new_fig.add_trace(
                _go.Scattergeo(
                    lon=lons,
                    lat=lats,
                    mode=d.get("mode", "lines"),
                    line=d.get("line"),
                    marker=d.get("marker"),
                    name=d.get("name"),
                    showlegend=d.get("showlegend", False),
                )
            )
            for lon, lat in zip(lons, lats, strict=False):
                _extrude_bbox(lon, lat)
            converted = True

    if not converted or min_lat >= max_lat or min_lon >= max_lon:
        return None

    pad_lat = max((max_lat - min_lat) * 0.08, 0.05)
    pad_lon = max((max_lon - min_lon) * 0.08, 0.05)

    geo_layout = {
        "projection": {"type": "natural earth"},
        "showland": True,
        "landcolor": "#eef2f7",
        "oceancolor": "#dbe6f0",
        "showcountries": True,
        "countrycolor": "#c6d3e3",
        "showframe": True,
        "framecolor": "#d5deeb",
        "lataxis": {
            "range": [min_lat - pad_lat, max_lat + pad_lat],
            "showgrid": True,
            "gridcolor": "#e5ecf6",
        },
        "lonaxis": {
            "range": [min_lon - pad_lon, max_lon + pad_lon],
            "showgrid": True,
            "gridcolor": "#e5ecf6",
        },
    }
    new_fig.update_layout(
        geo=geo_layout,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=500,
        template=fig.layout.template,
    )
    return new_fig


def _write_map_geo_fallback(fig: go.Figure, img_path: Path, timeout_s: int = 120) -> bool:
    """Render a WebGL-free snapshot of a map figure and write it to disk.

    Returns ``True`` if a fallback snapshot was written, ``False`` otherwise
    (figure isn't a map, or the fallback render also failed)."""
    geo_fig = _map_traces_to_geo(fig)
    if geo_fig is None:
        return False
    try:
        write_image_with_timeout(geo_fig, img_path, timeout_s=timeout_s)
        return True
    except (KaleidoError, TimeoutError, RuntimeError):  # noqa: BLE001 - fallback; caller degrades
        return False


def write_image_with_timeout(fig: go.Figure, img_path: Path, timeout_s: int = 120):
    """
    Write a Plotly image with a hard timeout.
    Uses a child process so we can terminate it if it hangs.
    """
    # Use 'spawn' to be safe on Windows
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(target=_write_image_worker, args=(fig.to_json(), img_path, q))
    p.start()
    p.join(timeout_s)

    if p.is_alive():
        p.terminate()
        p.join()
        raise TimeoutError(f"Writing image timed out after {timeout_s}s")

    # Collect worker result
    if not q.empty():
        status = q.get()
        if status[0] == "err":
            _raise_error_from_worker(status[1], status[2], status[3])
    else:
        # No response from worker—treat as failure
        raise RuntimeError("Image export failed: no response from worker")


def export_figure(fig: go.Figure, out_dir: str | Path, name: str, mode: str, include_plotlyjs=False, report_dir=None) -> str:
    """export plotly figure html
    either interactive, or with path to static image created at out_dir
    either way returns html fragment
    """
    log = Logger('Export fig')
    out_dir = Path(out_dir)
    if mode == "interactive":
        log.debug(f'Generating interactive fig name {name}')
        # Keep the mode bar (zoom, pan, download buttons) on maps only —
        # charts are presentation figures and shouldn't invite fiddling.
        # Hover tooltips still work everywhere; this only hides the buttons.
        config = {"displayModeBar": True} if _is_map_figure(fig) else {"displayModeBar": False}
        return pio.to_html(fig, include_plotlyjs=include_plotlyjs, full_html=False, config=config)
    # will this work? make case insensitive
    elif mode in ('png', 'jpeg', 'svg', 'pdf', 'webp'):
        img_filename = f"{name}.{mode}"
        img_path = out_dir / img_filename
        # requires kaleido (python packge) to be installed
        # and that requires Google Chrome to be installed - plotly_get_chrome or kaleido.get_chrome() or kaleido.get_chrome_sync()
        if report_dir:
            rel_path = os.path.relpath(img_path, start=report_dir)
        else:
            rel_path = img_filename
        # Figure exports are network-dependent (MapLibre basemaps/tiles are
        # fetched at render time). A single tile/style fetch hiccup inside the
        # headless browser raises a KaleidoError, so retry before degrading.
        max_attempts = 3
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                log.debug(f"Exporting figure to {img_path} (attempt {attempt}/{max_attempts})")
                write_image_with_timeout(fig, img_path, timeout_s=120)
                log.debug(" ...done")
                return f'<img src="{rel_path}">'
            except (KaleidoError, TimeoutError) as e:
                last_error = e
                log.error(f"Export failed for {img_path}: {e}")
                if attempt < max_attempts:
                    wait = 2 * attempt
                    log.warning(f"Retrying export of {img_path} in {wait}s (attempt {attempt}/{max_attempts})")
                    time.sleep(wait)
            except Exception as e:  # noqa: BLE001 - worker/browser/renderer failures
                last_error = e
                log.error(f"Export failed for {img_path}: {e}")
                # Unknown error type — don't sleep the backoff widows, just
                # retry immediately; the child-process worker can die from
                # transient browser-launch issues.
        # All attempts failed (transient network/browser/WebGL issue). For map
        # figures (MapLibre, needs WebGL + remote basemap tiles), try a
        # WebGL-free geo snapshot before degrading to a placeholder — that
        # path never touches the tile service, so it succeeds even when
        # tiles.riverscapes.net is down (HTTP 502s on low-zoom pmtiles are a
        # known real-world cause).
        if _write_map_geo_fallback(fig, img_path, timeout_s=120):
            log.warning(f"Map export failed after retries ({last_error}); wrote WebGL-free geo fallback to {img_path}")
            return f'<img src="{rel_path}">'

        _write_placeholder_image(img_path, mode)
        log.error(f"Giving up on exporting figure to {img_path} ({last_error}): wrote placeholder instead")
        html_fragment = f'<img src="{rel_path}">'
        return html_fragment
    else:
        raise NotImplementedError  # is there a better error?


#: 1x1 light-gray PNG, embedded for the (rare) case where a raster-mode
#: placeholder must be written without Pillow available.
_PLACEHOLDER_PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="


_PLACEHOLDER_SVG = """<svg xmlns="http://www.w3.org/2000/svg" width="800" height="450">
  <rect width="100%" height="100%" fill="#f1f4f9"/>
  <text x="50%" y="48%" font-family="Arial, sans-serif" font-size="20" fill="#4a5764" text-anchor="middle">Figure unavailable</text>
  <text x="50%" y="55%" font-family="Arial, sans-serif" font-size="14" fill="#6e7889" text-anchor="middle">Rendering failed after retries (likely a transient network or WebGL issue).</text>
</svg>
"""


def _write_placeholder_image(img_path: Path, mode: str) -> None:
    """Write a small 'figure unavailable' placeholder so a failed export does
    not leave a broken <img> (which would corrupt the static HTML and PDF)."""
    try:
        if mode in ("svg", "pdf"):
            img_path.write_text(_PLACEHOLDER_SVG, encoding="utf-8")
            return
        try:
            from PIL import Image, ImageDraw

            img = Image.new("RGB", (800, 450), (241, 244, 249))
            draw = ImageDraw.Draw(img)
            draw.text((400, 216), "Figure unavailable", fill=(74, 87, 100), anchor="mm")
            draw.text((400, 248), "Rendering failed after retries (likely a transient network or WebGL issue).", fill=(110, 120, 137), anchor="mm")
            img.save(img_path, format="JPEG" if mode == "jpeg" else "PNG")
        except ImportError:  # pragma: no cover - Pillow almost always present
            img_path.write_bytes(base64.b64decode(_PLACEHOLDER_PNG_B64))
    except OSError:  # pragma: no cover - filesystem troubles; never abort the build
        pass
