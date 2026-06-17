import os
from pathlib import Path
# import psutil  # for debugging
import multiprocessing as mp

import plotly.graph_objects as go
import plotly.io as pio
from kaleido._kaleido_tab import KaleidoError

from rsxml import Logger
from util.maplibre.map_figure import MapLibreMap
from util.plotly.playwright_export import is_map_figure, export_map_with_playwright


def _write_image_worker(fig_json: str, img_path: Path, q):
    """Worker runs in a separate process to avoid hangs."""
    try:
        # Reconstruct the figure in the child process
        fig = pio.from_json(fig_json)
        fig.write_image(img_path)
        q.put(("ok", None))
    except Exception as e:
        q.put(("err", repr(e)))


def _strip_external_map_styles(fig: go.Figure) -> go.Figure:
    """Replace any external URL map styles with a Kaleido-compatible built-in.

    This is a Kaleido-only fallback used for non-map figures that happen to
    carry an external style URL, or as a last-resort if the Playwright path
    fails.  Map figures are normally handled by
    :func:`~util.plotly.playwright_export.export_map_with_playwright` which
    renders live tiles correctly.
    """
    fig_dict = fig.to_dict()
    layout = fig_dict.get("layout", {})
    # Plotly >=5.18 uses layout.map; older versions use layout.mapbox
    for key in ("map", "mapbox"):
        if key in layout:
            style = layout[key].get("style", "")
            if isinstance(style, str) and style.startswith("http"):
                layout[key]["style"] = "carto-positron"
    return go.Figure(fig_dict)


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
        status, payload = q.get()
        if status == "err":
            raise RuntimeError(f"Image export failed: {payload}")
    else:
        # No response from worker—treat as failure
        raise RuntimeError("Image export failed: no response from worker")


def export_figure(fig: go.Figure | MapLibreMap, out_dir: str | Path, name: str, mode: str,
                  include_plotlyjs=False, report_dir=None) -> str:
    """export plotly figure html
    either interactive, or with path to static image created at out_dir
    either way returns html fragment
    """
    log = Logger('Export fig')
    out_dir = Path(out_dir)

    # --- MapLibreMap branch ---
    if isinstance(fig, MapLibreMap):
        if mode == "interactive":
            # Embed directly in the report — no separate file, no iframe
            return fig.to_fragment_html(name)
        else:
            img_path = out_dir / f"{name}.png"
            if report_dir:
                rel = os.path.relpath(img_path, start=report_dir)
            else:
                rel = f"{name}.png"
            log.debug(f"Exporting MapLibre map to {img_path}")
            fig.to_image(img_path)
            log.debug(" ...done")
            return f'<img src="{rel}">'

    if mode == "interactive":
        # Enable mode bar for interactivity (zoom, pan, etc.)
        log.debug(f'Generating interactive fig name {name}')
        return pio.to_html(
            fig,
            include_plotlyjs=include_plotlyjs,
            full_html=False,
            config={"displayModeBar": True}
        )
    # will this work? make case insensitive
    elif mode in ('png', 'jpeg', 'svg', 'pdf', 'webp'):
        # Map figures are exported via Playwright so that MapLibre tile loading
        # completes before the screenshot is taken.  All other figures continue
        # to use Kaleido.
        #
        # Playwright always outputs PNG, so regardless of the requested mode we
        # write (and reference) a .png file for map figures.  This is fine for
        # both the static HTML report (WeasyPrint renders PNG <img> tags fine)
        # and the standalone PNG download.
        if is_map_figure(fig) and mode in ('png', 'svg'):
            img_path = out_dir / f"{name}.png"
            if report_dir:
                rel_path = os.path.relpath(img_path, start=report_dir)
            else:
                rel_path = f"{name}.png"
            log.debug(f"Exporting map figure via Playwright → {img_path}")
            try:
                export_map_with_playwright(fig, img_path)
                log.debug(" ...done")
            except Exception as e:
                log.error(
                    f"Playwright map export failed for {img_path}: {e}. "
                    "Falling back to Kaleido with carto-positron basemap."
                )
                export_fig = _strip_external_map_styles(fig)
                try:
                    write_image_with_timeout(export_fig, img_path, timeout_s=120)
                except Exception as fallback_err:
                    log.error(f"Kaleido fallback also failed: {fallback_err}")
                    raise fallback_err
            return f'<img src="{rel_path}">'

        # --- Kaleido path for all non-map figures (and jpeg/pdf/webp modes) ---
        img_filename = f"{name}.{mode}"
        img_path = out_dir / img_filename
        if report_dir:
            rel_path = os.path.relpath(img_path, start=report_dir)
        else:
            rel_path = img_filename
        try:
            log.debug(f"Exporting figure to {img_path}")
            # Strip external map style URLs before handing to Kaleido — it
            # cannot fetch remote MapLibre styles from its headless browser.
            export_fig = _strip_external_map_styles(fig)
            write_image_with_timeout(export_fig, img_path, timeout_s=120)
            log.debug(" ...done")
        except KaleidoError as e:
            log.error(f"KaleidoError: {e}. May be due to network and we should add retrying ability...")
        except TimeoutError as e:
            log.error(f"Timed out exporting figure to {img_path}: {e}")
        except Exception as e:
            log.error(f"Error exporting figure to {img_path}: {e}")
            raise e
        html_fragment = f'<img src="{rel_path}">'
        return html_fragment
    else:
        raise NotImplementedError  # is there a better error?
