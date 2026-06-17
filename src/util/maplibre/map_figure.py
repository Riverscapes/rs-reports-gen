"""MapLibre GL JS map figure — standalone replacement for Plotly go.Figure maps."""

import base64
import json
import math
import re
import tempfile
from pathlib import Path

_MILES_PER_KM = 0.621371

_JS_DIR = Path(__file__).parent / "js"
_JS_ROUND_RECT = (_JS_DIR / "round_rect.js").read_text(encoding="utf-8")
_JS_LIVE_OVERLAYS = (_JS_DIR / "live_overlays.js").read_text(encoding="utf-8")
_JS_MAP_SCREENSHOT = (_JS_DIR / "map_screenshot.js").read_text(encoding="utf-8")


def _nice_scale_bar_km(span_km: float) -> float:
    """Return a 'nice' round scale bar length given the map span in km."""
    targets = [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000]
    target = span_km / 5  # aim for ~20% of the map width
    return min(targets, key=lambda t: abs(t - target))


def compute_scale_bar(aoi_gdf, zoom: float, width_px: int, height_px: int, unit_system: str = "SI") -> dict | None:
    """Compute scale bar parameters for a MapLibre canvas overlay.

    Args:
        aoi_gdf: AOI GeoDataFrame (geographic or projected CRS).
        zoom: MapLibre zoom level.
        width_px: Map canvas width in pixels.
        height_px: Map canvas height in pixels.
        unit_system: "SI" (km) or "imperial" (miles).

    Returns:
        dict with keys ``bar_px``, ``label``, ``x``, ``y``; or ``None`` if the
        AOI is empty or unavailable.
    """
    if aoi_gdf is None or aoi_gdf.empty:
        return None

    if aoi_gdf.crs is not None and not aoi_gdf.crs.is_geographic:
        aoi_gdf = aoi_gdf.to_crs("EPSG:4326")

    bounds = aoi_gdf.geometry.total_bounds  # [minx, miny, maxx, maxy]
    minx, miny, maxx, maxy = bounds
    lat_center = (miny + maxy) / 2
    lat_rad = math.radians(lat_center)
    m_per_deg_lon = 111_320 * math.cos(lat_rad)
    lon_span_km = (maxx - minx) * m_per_deg_lon / 1000

    bar_km = _nice_scale_bar_km(lon_span_km)

    if unit_system == "imperial":
        bar_miles = bar_km * _MILES_PER_KM
        nice_miles = [0.1, 0.25, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500]
        bar_miles = min(nice_miles, key=lambda m: abs(m - bar_miles))
        bar_km = bar_miles / _MILES_PER_KM
        label = f"{bar_miles:g} mi"
    else:
        label = f"{bar_km:g} km"

    # pixels_per_deg_lon at zoom z = 2^z * 512 / 360  (MapLibre uses 512px tiles)
    pixels_per_deg_lon = (2 ** zoom) * 512 / 360
    km_per_deg_lon = m_per_deg_lon / 1000
    pixels_per_km = pixels_per_deg_lon / km_per_deg_lon
    bar_px = int(bar_km * pixels_per_km)

    # Position: 15 px from bottom-left
    x = 15
    y = height_px - 20
    return {"bar_px": bar_px, "label": label, "x": x, "y": y}


class MapLibreMap:
    """Standalone MapLibre GL map — replacement for Plotly go.Figure maps."""

    def __init__(
        self,
        center: dict,
        zoom: float,
        style_url: str,
        width: int = 900,
        height: int = 500,
        geojson_data: dict | None = None,
        color_expression: list | None = None,
        opacity: float = 0.5,
        aoi_geojson: dict | None = None,
        title: str | None = None,
        legend: list[tuple[str, str]] | None = None,
        scale_bar: dict | None = None,
        unit_system: str = "SI",
    ) -> None:
        self.center = center
        self.zoom = zoom
        self.style_url = style_url
        self.width = width
        self.height = height
        self.geojson_data = geojson_data
        self.color_expression = color_expression
        self.opacity = opacity
        self.aoi_geojson = aoi_geojson
        self.title = title
        self.legend = legend or []
        self.scale_bar = scale_bar
        self.unit_system = unit_system

    # ------------------------------------------------------------------
    # HTML generation
    # ------------------------------------------------------------------

    def to_html(self) -> str:
        """Generate a full standalone HTML page with MapLibre GL JS (for Playwright export)."""
        layers_js = self._build_layers_js()
        cfg_json = json.dumps({
            "title": self.title,
            "legend": self.legend,
            "unitSystem": self.unit_system,
            "scalebar": self.scale_bar,
            "width": self.width,
            "height": self.height,
        })

        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<script src="https://cdn.jsdelivr.net/npm/maplibre-gl@4/dist/maplibre-gl.js"></script>
<link href="https://cdn.jsdelivr.net/npm/maplibre-gl@4/dist/maplibre-gl.css" rel="stylesheet">
<style>
  body {{ margin: 0; padding: 0; overflow: hidden; }}
  #map {{ width: {self.width}px; height: {self.height}px; }}
</style>
</head>
<body>
<div id="map"></div>
<script>
window.__mapConfig__ = {cfg_json};
{_JS_ROUND_RECT}
{_JS_LIVE_OVERLAYS}
{_JS_MAP_SCREENSHOT}
</script>
<script>
const map = new maplibregl.Map({{
  container: 'map',
  style: {json.dumps(self.style_url)},
  center: [{self.center['lon']}, {self.center['lat']}],
  zoom: {self.zoom},
  preserveDrawingBuffer: true,
}});

map.on('load', () => {{
{layers_js}
  try {{
    setupLiveOverlays(map, window.__mapConfig__);
  }} catch(e) {{
    console.error('setupLiveOverlays failed:', e);
  }}
  window.__mapReady = true;
}});

window.getMapScreenshot = buildGetMapScreenshot(window.__mapConfig__);
</script>
</body>
</html>"""

    def to_fragment_html(self, map_id: str) -> str:
        """Return an HTML fragment (div + script) for embedding directly in a report page.

        Requires that the MapLibre GL JS CDN and the helper JS functions
        (roundRect, setupLiveOverlays, buildGetMapScreenshot) have already been
        included earlier in the page <head> — RSReport.render() handles this.
        """
        safe_id = re.sub(r'[^A-Za-z0-9_]', '_', map_id)
        html_id = f"maplibre-{safe_id}"
        if safe_id and safe_id[0].isdigit():
            safe_id = '_' + safe_id
        layers_js = self._build_layers_js(indent="    ", map_var=f"map_{safe_id}")
        cfg_json = json.dumps({
            "title": self.title,
            "legend": self.legend,
            "unitSystem": self.unit_system,
            "scalebar": self.scale_bar,
            "width": self.width,
            "height": self.height,
            "containerId": f"#{html_id}",
        })
        return f"""<div id="{html_id}" style="width:{self.width}px; height:{self.height}px; max-width:100%;"></div>
<script>
(function() {{
  var cfg_{safe_id} = {cfg_json};
  var map_{safe_id} = new maplibregl.Map({{
    container: '{html_id}',
    style: {json.dumps(self.style_url)},
    center: [{self.center['lon']}, {self.center['lat']}],
    zoom: {self.zoom},
    preserveDrawingBuffer: true,
  }});
  map_{safe_id}.on('load', function() {{
{layers_js}
    try {{
      setupLiveOverlays(map_{safe_id}, cfg_{safe_id});
    }} catch(e) {{
      console.error('setupLiveOverlays failed:', e);
    }}
    window.__mapReady_{safe_id} = true;
  }});
  window.__getScreenshot_{safe_id} = buildGetMapScreenshot(cfg_{safe_id});
}})();
</script>"""

    def _build_layers_js(self, indent: str = "  ", map_var: str = "map") -> str:
        """Build the JS snippet that adds sources and layers inside map.on('load', ...)."""
        parts: list[str] = []

        if self.geojson_data is not None:
            color_expr = json.dumps(self.color_expression if self.color_expression else "#3388ff")
            parts.append(f"{indent}{map_var}.addSource('dgo', {{ type: 'geojson', data: {json.dumps(self.geojson_data)} }});")
            parts.append(
                f"{indent}{map_var}.addLayer({{ id: 'dgo-fill', type: 'fill', source: 'dgo', "
                f"paint: {{ 'fill-color': {color_expr}, 'fill-opacity': {self.opacity} }} }});"
            )

        if self.aoi_geojson is not None:
            parts.append(f"{indent}{map_var}.addSource('aoi', {{ type: 'geojson', data: {json.dumps(self.aoi_geojson)} }});")
            parts.append(
                f"{indent}{map_var}.addLayer({{ id: 'aoi-outline', type: 'line', source: 'aoi', "
                "paint: { 'line-color': 'red', 'line-width': 3 } });"
            )

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Head HTML helper
    # ------------------------------------------------------------------

    @staticmethod
    def head_html() -> str:
        """Return the HTML snippet (<link> + <script> tags) that must appear in the
        report <head> when one or more MapLibreMap figures are embedded interactively.

        Each helper JS file gets its own <script> tag so a parse error in one
        does not silently kill the others.
        """
        lines = [
            '<link href="https://cdn.jsdelivr.net/npm/maplibre-gl@4/dist/maplibre-gl.css" rel="stylesheet">',
            '<script src="https://cdn.jsdelivr.net/npm/maplibre-gl@4/dist/maplibre-gl.js"></script>',
        ]
        for js_content in [_JS_ROUND_RECT, _JS_LIVE_OVERLAYS, _JS_MAP_SCREENSHOT]:
            lines.append(f'<script>\n{js_content}\n</script>')
        return '\n'.join(lines)

    # ------------------------------------------------------------------
    # Image export via Playwright
    # ------------------------------------------------------------------

    def to_image(self, out_path: Path, device_scale_factor: int = 2) -> None:
        """Screenshot the map via Playwright and write a PNG to *out_path*.

        Args:
            out_path: Destination PNG file path.
            device_scale_factor: Pixel density multiplier passed to Playwright's
                Chromium.  ``2`` (the default) renders the map at 2× physical
                resolution (e.g. 1800×1000 px for a 900×500 logical viewport),
                giving ~240 DPI at full Letter page width.  Use ``3`` for
                near-print-quality output at the cost of larger file size.
        """
        from playwright.sync_api import TimeoutError as PWTimeoutError  # noqa: PLC0415
        from playwright.sync_api import sync_playwright  # noqa: PLC0415

        html = self.to_html()
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w", encoding="utf-8") as fh:
            fh.write(html)
            tmp_path = Path(fh.name)

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                try:
                    page = browser.new_page(
                        viewport={"width": self.width, "height": self.height},
                        device_scale_factor=device_scale_factor,
                    )
                    page.goto(tmp_path.as_uri())

                    try:
                        page.wait_for_function("() => window.__mapReady === true", timeout=60_000)
                    except PWTimeoutError as exc:
                        raise RuntimeError("MapLibre map.loaded() never fired") from exc

                    # Flush GPU frames
                    page.evaluate("() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))")
                    page.wait_for_timeout(800)

                    # Composite via canvas API — getMapScreenshot reads
                    # window.devicePixelRatio and sizes the output canvas to
                    # physical pixels, so the PNG is device_scale_factor× larger.
                    img_data_url: str = page.evaluate("() => window.getMapScreenshot()")
                    raw = base64.b64decode(img_data_url.split(",", 1)[1])
                    out_path.write_bytes(raw)
                finally:
                    browser.close()
        finally:
            tmp_path.unlink(missing_ok=True)
