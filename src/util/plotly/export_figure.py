# System imports
import os
import plotly.graph_objects as go
import plotly.io as pio
from kaleido._kaleido_tab import KaleidoError
from rsxml import Logger


def export_figure(fig: go.Figure, out_dir: str, name: str, mode: str, include_plotlyjs=False, report_dir=None) -> str:
    """export plotly figure html
    either interactive, or with path to static image created at out_dir
    either way returns html fragment
    """
    log = Logger()
    if mode == "interactive":
        # Enable mode bar for interactivity (zoom, pan, etc.)
        return pio.to_html(
            fig,
            include_plotlyjs=include_plotlyjs,
            full_html=False,
            config={"displayModeBar": True}
        )
    # will this work? make case insensitive
    elif mode in ('png', 'jpeg', 'svg', 'pdf', 'webp'):
        img_filename = f"{name}.{mode}"
        img_path = os.path.join(out_dir, img_filename)
        # requires kaleido (python packge) to be installed
        # and that requires Google Chrome to be installed - plotly_get_chrome or kaleido.get_chrome() or kaleido.get_chrome_sync()
        if report_dir:
            rel_path = os.path.relpath(img_path, start=report_dir)
        else:
            rel_path = img_filename
        # I've seen this transiently fail - probably network connection issue -
        try:
            log.debug(f"Exporting figure to {img_path}")
            fig.write_image(img_path)  # scale of 4 is equivalent to about dpi of 300 for 800x600 image. This should keep the image snappy on print and big screens
            log.debug(" ...done")
        except KaleidoError as e:
            log.error(f"KaleidoError: {e}. May be due to network and we should add retrying ability...")
        except TimeoutError as e:
            log.error(f"Timed out exporting figure to {img_path}: {e}")
            raise e
        except Exception as e:
            log.error(f"Error exporting figure to {img_path}: {e}")
            raise e
        html_fragment = f'<img src="{rel_path}">'
        return html_fragment
    else:
        raise NotImplementedError  # is there a better error?
