"""Playwright-based static export for Plotly map figures.

Kaleido's headless Chrome cannot wait for MapLibre GL tile loading to finish
before it takes a screenshot, so map exports come out with a blank basemap.

This module opens the interactive figure in a real Playwright-controlled
Chromium browser, waits for MapLibre's ``map.loaded()`` to confirm that all
tile data has been decoded and the last render frame is complete, then calls
**``Plotly.toImage()``** from inside the page's own JavaScript context to
produce the PNG.

Why ``Plotly.toImage()`` instead of Playwright's ``.screenshot()``?
  Playwright captures images via Chrome's ``ReadPixels`` DevTools call.  That
  triggers a GPU stall which causes WebGL context loss — the MapLibre tile
  canvas goes blank *before* the pixels are read, so tile imagery is absent
  from the output even though it was fully loaded.

  ``Plotly.toImage()`` calls ``canvas.toDataURL()`` from *within* the page's
  JS context.  Plotly initialises the MapLibre canvas with
  ``preserveDrawingBuffer: true`` specifically to support this path.  The
  result is a correctly composited PNG: WebGL tile basemap + SVG trace
  overlay + annotations, all in one image.
"""

import tempfile
from pathlib import Path

import plotly.graph_objects as go
import plotly.io as pio
from rsxml import Logger


# ---------------------------------------------------------------------------
# JavaScript helpers injected into the Playwright page
# ---------------------------------------------------------------------------

# Polls every ~100 ms until every MapLibre map subplot inside the Plotly
# graph div has finished loading tiles and rendered the last frame.
#
# Returns ``true`` when:
#   - The graph div (_fullLayout) exists
#   - At least one map subplot has been found
#   - Every map subplot's MapLibre ``map.loaded()`` returns ``true``
#
# Handles both modern ``layout.map`` (Plotly ≥5.18) and legacy
# ``layout.mapbox`` subplots, and multi-map figures (map2, mapbox2, …).
_JS_MAP_LOADED = """\
() => {
    const gd = document.querySelector('.js-plotly-plot');
    if (!gd || !gd._fullLayout) return false;
    const layout = gd._fullLayout;
    let foundMap = false;
    for (const [key, val] of Object.entries(layout)) {
        if (typeof key !== 'string') continue;
        const isMap    = key === 'map'    || /^map\\d+$/.test(key);
        const isMapbox = key === 'mapbox' || /^mapbox\\d+$/.test(key);
        if ((isMap || isMapbox) && val && typeof val === 'object' && val._subplot) {
            const mapInst = val._subplot.map;
            if (!mapInst) continue;          // subplot exists but map not yet created
            foundMap = true;
            if (!mapInst.loaded()) return false;   // still loading
        }
    }
    return foundMap;   // false until a map subplot is found AND loaded
}
"""

# Waits for two animation frames so the WebGL canvas is composited into the
# bitmap that Chrome's screenshot mechanism will capture.  One frame is
# occasionally not enough if the GPU scheduler queues the draw slightly late.
_JS_AWAIT_ANIMATION_FRAMES = """\
() => new Promise(resolve =>
    requestAnimationFrame(() => requestAnimationFrame(resolve))
)
"""


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def is_map_figure(fig: go.Figure) -> bool:
    """Return True if the figure contains a MapLibre / Mapbox map layout.

    Plotly ≥5.18 uses ``layout.map``; older versions use ``layout.mapbox``.
    Either presence means Kaleido will struggle with external tile URLs.
    """
    layout = fig.to_dict().get("layout", {})
    return ("map" in layout) or ("mapbox" in layout)


def export_map_with_playwright(
    fig: go.Figure,
    img_path: Path,
    *,
    map_loaded_timeout_ms: int = 60_000,
    selector_timeout_ms: int = 15_000,
    post_load_pause_ms: int = 800,
) -> None:
    """Screenshot a Plotly map figure using Playwright's bundled Chromium.

    The figure is serialised to a temporary standalone HTML file, opened in a
    headless Chromium page, and the ``.js-plotly-plot`` element is screenshotted
    once MapLibre reports that all tiles in the viewport have been loaded *and*
    the final animation frame has been flushed to the WebGL canvas.

    The output is always a **PNG** regardless of what extension ``img_path``
    carries — callers should ensure the path has a ``.png`` suffix.

    Args:
        fig: The Plotly figure to export.  Must be a map figure.
        img_path: Destination path for the PNG screenshot.
        map_loaded_timeout_ms: Maximum ms to wait for ``map.loaded()`` to
            become ``True``.  Defaults to 60 000 (60 s).
        selector_timeout_ms: Maximum ms to wait for the ``.js-plotly-plot``
            selector to appear.  Defaults to 15 000 (15 s).
        post_load_pause_ms: Fixed pause (ms) inserted *after* the animation
            frames settle, as a final safety margin.  Defaults to 800 ms.

    Raises:
        ImportError: If ``playwright`` is not installed.
        RuntimeError: If the plot element is not found within
            *selector_timeout_ms*, or ``map.loaded()`` never becomes ``True``
            within *map_loaded_timeout_ms*.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
    except ImportError as exc:
        raise ImportError(
            "playwright is required for map exports with live tiles. "
            "Install it with:  uv pip install playwright  "
            "then run:  playwright install chromium"
        ) from exc

    log = Logger("playwright_export")

    # --- determine viewport size from figure layout ---------------------------
    layout = fig.to_dict().get("layout", {})
    # Plotly default when width is unset is the container width; 1000 px is a
    # reasonable stand-in that matches typical report column widths.
    vp_width = int(layout.get("width") or 1000)
    vp_height = int(layout.get("height") or 500)

    # --- serialise the figure to a temp HTML file ----------------------------
    html_content = pio.to_html(
        fig,
        include_plotlyjs=True,
        full_html=True,
        config={"displayModeBar": False},
    )

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            suffix=".html", delete=False, mode="w", encoding="utf-8"
        ) as fh:
            fh.write(html_content)
            tmp_path = Path(fh.name)

        log.debug(f"Playwright temp HTML → {tmp_path}")

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": vp_width, "height": vp_height})

                # Suppress the expected WebGL noise that occurs when Playwright's
                # ReadPixels-based screenshot triggers a GPU stall and Chrome loses
                # the WebGL context.  This happens AFTER the pixel data is captured
                # so it has no effect on the output image.
                _WEBGL_NOISE = (
                    "GPU stall due to ReadPixels",
                    "CONTEXT_LOST_WEBGL",
                    "object does not belong to this context",
                    "glDrawElements",
                )
                page.on(
                    "console",
                    lambda msg: (
                        log.debug(f"[browser] {msg.text}")
                        if not any(s in msg.text for s in _WEBGL_NOISE)
                        else None
                    ),
                )

                page.goto(tmp_path.as_uri())

                # ── Step 1: Plotly has mounted the map div ────────────────────
                try:
                    page.wait_for_selector(".js-plotly-plot", timeout=selector_timeout_ms)
                except PWTimeoutError as exc:
                    raise RuntimeError(
                        "Plotly plot element (.js-plotly-plot) did not appear within "
                        f"{selector_timeout_ms} ms — the figure may have failed to render."
                    ) from exc

                # ── Step 2: MapLibre map.loaded() — tiles decoded & last frame done
                try:
                    page.wait_for_function(_JS_MAP_LOADED, timeout=map_loaded_timeout_ms)
                    log.debug("MapLibre map.loaded() confirmed")
                except PWTimeoutError as exc:
                    raise RuntimeError(
                        f"MapLibre map.loaded() did not become True within "
                        f"{map_loaded_timeout_ms} ms.  Tiles may be unreachable or "
                        "the map subplot path has changed in this version of Plotly."
                    ) from exc

                # ── Step 3: flush WebGL canvas — two rAF callbacks ────────────
                page.evaluate(_JS_AWAIT_ANIMATION_FRAMES)

                # ── Step 4: fixed safety pause ────────────────────────────────
                page.wait_for_timeout(post_load_pause_ms)

                # ── Step 5: composite via Plotly.toImage() ────────────────────
                log.debug("Calling Plotly.toImage() inside page…")
                try:
                    page.set_default_timeout(map_loaded_timeout_ms)
                    img_data_url: str = page.evaluate(
                        """([w, h]) => {
                            const gd = document.querySelector('.js-plotly-plot');
                            return Plotly.toImage(gd, {format: 'png', width: w, height: h});
                        }""",
                        [vp_width, vp_height],
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"Plotly.toImage() failed inside Playwright page: {exc}"
                    ) from exc

                import base64 as _base64
                raw_bytes = _base64.b64decode(img_data_url.split(",", 1)[1])
                img_path.write_bytes(raw_bytes)
                log.debug(f"Plotly.toImage() → {img_path}")

            finally:
                browser.close()

    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()
