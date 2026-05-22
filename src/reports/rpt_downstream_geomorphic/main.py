"""Downstream Geomorphic report – longitudinal profile charts of RME metrics along level paths.

Queries RME intersection data for an AOI, groups by level path, and generates
Plotly line charts showing how geomorphic indicators change along segment distance
(headwater → mouth).

Copilot-generated module.
"""

import argparse
import shutil
from pathlib import Path

import pandas as pd
from rsxml import Logger

from reports.rpt_downstream_geomorphic import __version__ as report_version
from reports.rpt_downstream_geomorphic.dataprep import (
    prepare_profile_data,
    prepare_summary_data_top_n,
    prepare_summary_for_report,
    query_rme_data,
)
from reports.rpt_downstream_geomorphic.figures import build_profile_figures
from util.athena import get_field_metadata
from util.html import RSReport
from util.pandas import RSFieldMeta, load_gdf_from_pq
from util.pdf import make_pdf_from_html
from util.report_entrypoint import (
    add_parquet_cli_args,
    init_report_logging,
    parse_report_args,
    report_main_wrapper,
)

REPORT_SLUG = "rpt-downstream-geomorphic"
RPT_RME_LAYER_ID = "rpt_rme"


def define_fields(unit_system: str = "SI") -> None:
    """Set up field metadata and unit system for this report.

    Copilot-generated function.
    """
    fm = RSFieldMeta()
    registry_field_meta = get_field_metadata(
        authority="data-exchange-scripts",
        tool_schema_name="*",
        layer_id="raw_rme,rpt_rme",
    )

    # Consolidate RME rows under one layer id so metadata lookups are never ambiguous.
    registry_field_meta.loc[registry_field_meta["layer_id"].isin(["raw_rme", "rpt_rme"]), "layer_id"] = RPT_RME_LAYER_ID
    registry_field_meta = registry_field_meta.drop_duplicates(subset=["layer_id", "name"], keep="first")

    fm.field_meta = registry_field_meta
    fm.unit_system = unit_system
    fm.set_display_unit("centerline_length", "kilometer", RPT_RME_LAYER_ID)


def make_report(
    profile_df: pd.DataFrame,
    lp_summary_df: pd.DataFrame,
    mode: str,
    report_dir: Path,
    report_name: str,
    *,
    include_pdf: bool = False,
) -> None:
    """Render the HTML (and optional PDF) report.

    Copilot-generated function.
    """
    log = Logger("make_report")

    figure_dir = report_dir / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)

    figures = build_profile_figures(profile_df)

    # Build HTML
    report = RSReport(
        report_name=report_name,
        report_type="Downstream Geomorphic",
        report_dir=report_dir,
        body_template_path=Path(__file__).parent / "templates" / "body.html",
        # css_paths=[Path(__file__).parent / "templates" / "report.css"],
        report_version=report_version,
    )
    for name, fig in figures.items():
        report.add_figure(name, fig)

    total_centerline_length_header = str(lp_summary_df.attrs.get("total_centerline_length_header", "Total Centerline Length"))
    context = {
        'selection_mode': mode,
        'total_centerline_length_header': total_centerline_length_header,
    }
    lp_summary = lp_summary_df.reset_index().to_dict(orient="records")

    report.add_html_elements('context', context)
    report.add_html_elements('lp_summary', lp_summary)
    report.render(fig_mode="interactive")
    log.info(f"HTML report written to {report_dir}")

    if include_pdf:
        make_pdf_from_html(str(report_dir))


def orchestrate(
    unit_system: str,
    dgoid: str,
    report_name: str,
    include_pdf: bool,
    output_path: Path,
    parquet_path: Path | None = None,
    keep_parquet: bool = False,
) -> None:
    """Main orchestration: query → prepare → chart → render."""
    define_fields(unit_system)
    log = Logger('Orchestrate')
    staging_path = output_path / "staging"

    # MODE - level_path
    mode = 'Whole Level Path'

    if parquet_path:
        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet path '{parquet_path}' does not exist")
        log.info(f"Using supplied Parquet data files at {parquet_path}")
        rme_df = load_gdf_from_pq(parquet_path)
    else:
        staging_path.mkdir(parents=True, exist_ok=True)
        rme_df = query_rme_data(mode, dgoid, staging_path)

    # Use one report-wide layer context for all presentation-oriented metadata lookups.
    rme_df.attrs["layer_id"] = RPT_RME_LAYER_ID

    summary_df = prepare_summary_data_top_n(rme_df, 9)
    summary_df = prepare_summary_for_report(summary_df)
    profile_df = prepare_profile_data(rme_df)
    profile_df, _applied_units = RSFieldMeta().apply_units(profile_df)
    profile_df.attrs["layer_id"] = RPT_RME_LAYER_ID

    make_report(profile_df, summary_df, mode, output_path, report_name, include_pdf=include_pdf)

    if not parquet_path and not keep_parquet and staging_path.exists():
        shutil.rmtree(staging_path)
        log.info("Staging data removed")


def main() -> None:
    """CLI entry point for the downstream geomorphic report."""
    parser = argparse.ArgumentParser(description="Downstream Geomorphic Longitudinal Profile Report")
    parser.add_argument("output_path", help="Folder to write outputs (will be created)", type=Path)
    parser.add_argument("dgoid", help="Latkey_Lonkey", type=str)
    parser.add_argument("report_name", help="Human-readable name for this export")
    parser.add_argument("--include_pdf", help="Include a PDF version of the report", action="store_true", default=False)
    parser.add_argument("--unit_system", help="Unit system: SI or imperial", type=str, default="SI")
    add_parquet_cli_args(parser)

    args, output_path = parse_report_args(parser)
    log = init_report_logging(output_path, REPORT_SLUG)
    report_main_wrapper(
        log,
        lambda: orchestrate(
            args.unit_system,
            args.dgoid,
            args.report_name,
            args.include_pdf,
            output_path,
            args.parquet_path,
            args.keep_parquet,
        ),
        debug=True,
    )


if __name__ == "__main__":
    main()
