"""Build the DEMO style-guide report.

This exercises the exact pipeline the real reports use
(:class:`~util.html.RSReport.RSReport`` renders
``templates/body.html`` plus all shared macros and base.css)
with deterministic sample data.

Usage (installed)::

    rs-report-demo [--output-dir PATH] [--html-only]

Usage (from source)::

    uv run rs-report-demo --output-dir demo_output

    # or via the convenience wrapper:
    scripts/build_demo.sh --html-only
"""

import argparse
from pathlib import Path

from rsxml import Logger

from util.html.demo.sample_data import (
    sample_figures,
    sample_highlight_cards,
    sample_metric_cards,
    sample_progress_cards,
    sample_progress_groups,
    sample_tables,
    sample_widgets,
)
from util.html.RSReport import RSReport
from util.pdf.create_pdf import make_pdf_from_html

TEMPLATE_DIR = Path(__file__).parent / "templates"
DEMO_BODY = TEMPLATE_DIR / "body.html"
DEMO_CSS = TEMPLATE_DIR / "demo.css"
DEMO_VERSION = "0.1.0"


def build_demo(output_dir: Path, html_only: bool = False, pdf_timeout: int | None = None, pdf_debug: bool = False) -> list[str]:
    """Render the demo report. Returns the list of generated artifact paths."""
    log = Logger("demo builder")

    report = RSReport(
        report_name="DEMO Style Guide",
        report_subtitle="Shared component catalog",
        report_type="Template Sandbox",
        report_dir=output_dir,
        report_version=DEMO_VERSION,
        body_template_path=TEMPLATE_DIR / "body.html",
        css_paths=[DEMO_CSS] if DEMO_CSS.exists() else [],
    )

    for name, fig in sample_figures().items():
        report.add_figure(name, fig)
    report.add_html_elements("tables", sample_tables())
    report.add_html_elements("cards", sample_metric_cards())
    report.add_html_elements("highlight_cards", sample_highlight_cards())
    report.add_html_elements("progress_groups", sample_progress_groups())
    report.add_html_elements("progress_cards", sample_progress_cards())
    report.add_html_elements("widgets", sample_widgets())

    outputs: list[str] = []

    interactive_path = report.render(fig_mode="interactive")
    outputs.append(interactive_path)
    log.info(f"Interactive DEMO report: {interactive_path}")

    if html_only:
        log.info("--html-only: skipping static HTML and PDF outputs")
        return outputs

    try:
        static_path = report.render(fig_mode="svg", suffix="_static")
        outputs.append(static_path)
        log.info(f"Static DEMO report: {static_path}")
        try:
            pdf_path = make_pdf_from_html(static_path, timeout_s=pdf_timeout, debug=pdf_debug)
            outputs.append(pdf_path)
            log.info(f"PDF DEMO report: {pdf_path}")
        except Exception as err:  # noqa: BLE001
            log.error(f"PDF generation failed: {err}")
            log.error("Re-run with --pdf-debug (or RS_PDF_DEBUG=1) to keep Chrome's log + the injected HTML source for diagnosis.")
    except Exception as err:  # noqa: BLE001
        log.warning(f"Static render failed (need kaleido + Chrome?): {err}")

    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-dir", type=Path, default=Path("demo_output"), help="Where the demo report will be written (default: ./demo_output)")
    parser.add_argument("--html-only", action="store_true", help="Build only the interactive HTML variant (fast; skips kaleido/Chrome and the PDF)")
    parser.add_argument("--pdf-timeout", type=int, default=None, help="Wall-clock seconds before Chrome print-to-PDF aborts (default: 120 or $RS_PDF_TIMEOUT_S)")
    parser.add_argument("--pdf-debug", action="store_true", help="Keep Chrome's verbose log + injected HTML source next to the PDF for diagnosing hangs (same as $RS_PDF_DEBUG=1)")
    args = parser.parse_args()

    outputs = build_demo(args.output_dir, html_only=args.html_only, pdf_timeout=args.pdf_timeout, pdf_debug=args.pdf_debug)
    for path in outputs:
        print(f"  ✔ {path}")


if __name__ == "__main__":
    main()
