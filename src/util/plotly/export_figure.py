import os
from pathlib import Path
# import psutil  # for debugging
import multiprocessing as mp

import plotly.graph_objects as go
import plotly.io as pio
from kaleido._kaleido_tab import KaleidoError

from rsxml import Logger


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

    Kaleido's headless browser cannot load external MapLibre style URLs
    (e.g. https://tiles.riverscapes.net/mapStyles/topo.json), which causes
    a 'Map error.' at export time.  Swapping to 'carto-positron' gives a
    clean, self-contained basemap that Kaleido can render without network
    access.
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


def export_figure(fig: go.Figure, out_dir: str | Path, name: str, mode: str,
                  include_plotlyjs=False, report_dir=None) -> str:
    """export plotly figure html
    either interactive, or with path to static image created at out_dir
    either way returns html fragment
    """
    log = Logger('Export fig')
    out_dir = Path(out_dir)
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
        img_filename = f"{name}.{mode}"
        img_path = out_dir / img_filename
        # requires kaleido (python packge) to be installed
        # and that requires Google Chrome to be installed - plotly_get_chrome or kaleido.get_chrome() or kaleido.get_chrome_sync()
        if report_dir:
            rel_path = os.path.relpath(img_path, start=report_dir)
        else:
            rel_path = img_filename
        # I've seen this transiently fail - probably network connection issue -
        try:
            # process = psutil.Process(os.getpid())
            # mem_mb = process.memory_info().rss / 1024 / 1024
            # log.debug(f"Memory usage before image export: {mem_mb:.2f} MB")
            log.debug(f"Exporting figure to {img_path}")
            # Strip external map style URLs before handing to Kaleido — it
            # cannot fetch remote MapLibre styles from its headless browser.
            export_fig = _strip_external_map_styles(fig)
            # ---- the only behavioral change: enforce timeout cross-platform ----
            write_image_with_timeout(export_fig, img_path, timeout_s=120)
            # mem_mb_after = process.memory_info().rss / 1024 / 1024
            # log.debug(f"Memory usage after image export: {mem_mb_after:.2f} MB")
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
