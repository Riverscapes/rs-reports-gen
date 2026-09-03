# import psutil  # for debugging
import multiprocessing as mp
import os
import re
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
        q.put(("err", repr(e)))


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
            # ---- the only behavioral change: enforce timeout cross-platform ----
            write_image_with_timeout(fig, img_path, timeout_s=120)
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
