"""PBR Explorer

Queries the PBR explorer public API
eventually for an area of interest (AOI), but non-geo queries should be possible too
supports cache-first parquet inputs,
and renders a minimal HTML report with baseline summary tables and charts.

Created 2026-07-13, Lorin Gaertner (started with stub version of rpt_beaver_restoration_potential)
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import TypedDict

import geopandas as gpd
import pandas as pd
import pint_pandas
from rsxml import Logger

from reports.rpt_pbrexplorer import __version__ as report_version
from reports.rpt_pbrexplorer.dataprep import (
    PBR_PROJECTS_LAYER_ID,
    define_fields,
    load_cached_pbr_data,
    load_live_pbr_data,
)
from util import prepare_gdf_for_athena
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.pdf import make_pdf_from_html
from util.report_entrypoint import (
    init_report_logging,
    parse_report_args,
    report_main_wrapper,
)

REPORT_SLUG = "rpt-pbr-explorer"


class MetricCard(TypedDict):
    title: str
    value: str
    details: str | None


def _summary_table_to_html(summary_df: pd.DataFrame) -> str:
    """Convert a summary dataframe into an HTML table for Jinja rendering."""
    if summary_df.empty:
        return "<p>No rows were available for this summary.</p>"
    rs_df = RSGeoDataFrame(summary_df.copy())
    layer_id = summary_df.attrs.get("layer_id") if hasattr(summary_df, "attrs") else None
    return rs_df.to_html(
        index=False,
        classes="table table-striped",
        border=0,
        include_units=True,
        use_friendly=True,
        layer_id=layer_id,
    )


def make_report(
    data_df: pd.DataFrame,
    # summary_tables: dict[str, pd.DataFrame],
    cards: MetricCard,
    report_dir: Path,
    report_name: str,
    *,
    include_pdf: bool = False,
) -> None:
    """Render the HTML report and optional PDF."""
    log = Logger("Make Report")
    data_dir = report_dir / "data"
    figure_dir = report_dir / "figures"
    data_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)

    # Export raw and summary tables for downstream inspection.
    raw_export_df = data_df.copy()
    for col in raw_export_df.columns:
        if isinstance(raw_export_df[col].dtype, pint_pandas.PintType):
            raw_export_df[col] = raw_export_df[col].pint.magnitude

    # Parquet is significantly faster and smaller than CSV for large raw exports.
    # raw_export_df.to_parquet(data_dir / "beaver_restoration_potential_raw.parquet", index=False)
    # for compatibility with older systems we could also, or based on a CLI flag export CSV
    # raw_export_df.to_csv(data_dir / "beaver_restoration_potential_raw.csv", index=False)
    # for key, summary_df in summary_tables.items():
    #     summary_df.to_csv(data_dir / f"{key}_summary.csv", index=False)

    # figures = build_beaver_figures(summary_tables)
    # summary_tables_html = {name: _summary_table_to_html(df) for name, df in summary_tables.items()}

    report = RSReport(
        report_name=report_name,
        report_type="Beaver Restoration Potential",
        report_dir=report_dir,
        body_template_path=Path(__file__).parent / "templates" / "body.html",
        report_version=report_version,
    )

    # for name, fig in figures.items():
    #     report.add_figure(name, fig)

    # report.add_html_elements("summary_tables", summary_tables_html)
    report.add_html_elements("cards", cards)
    report.render(fig_mode="interactive")
    log.info(f"HTML report written to {report_dir}")

    if include_pdf:
        make_pdf_from_html(str(report_dir))


def summary_stats(data_df: pd.DataFrame) -> MetricCard:
    """Prepare formatted summary data for report - a combination of dataprep and figure"""
    project_count = len(data_df)
    return {
        "number_of_projects": {
            "title": "Number of Projects",
            "value": f"{project_count:,}",
            "details": "Projects returned by PBR Explorer for this run.",
        },
    }


def orchestrate(
    output_path: Path,
    path_to_shape: Path,
    report_name: str,
    unit_system: str,
    include_pdf: bool,
    raw_data_path: Path | None = None,
    keep_raw_data: bool = False,
) -> None:
    """Execute the query -> summarize -> render workflow for this report."""
    log = Logger("Orchestrate")
    staging_path = output_path / "staging"

    if raw_data_path and not raw_data_path.exists():
        raise FileNotFoundError(f"Path '{raw_data_path}' does not exist")
    if raw_data_path and not raw_data_path.is_dir():
        raise NotADirectoryError(f"Expected a directory of paged JSON cache files, got '{raw_data_path}'")

    aoi_gdf = gpd.read_file(path_to_shape)
    # CHECK: we aren't querying Athena, but we may still want to simplify inputs
    query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
    if not simplification_results.success:
        log.warning("AOI geometry could not be simplified to the preferred size.")
    if simplification_results.simplified:
        log.warning(f"AOI geometry simplified with tolerance {simplification_results.tolerance_m} meters for query execution.")

    if not raw_data_path:
        staging_path.mkdir(parents=True, exist_ok=True)

    # load_meta_from_parquet, metadata_cachefile_path = _resolve_metadata_cachefile_path(raw_data_path, staging_path, keep_raw_data)
    # define_fields(unit_system, load_from_parquet=load_meta_from_parquet, metadata_cachefile_path=metadata_cachefile_path)

    if raw_data_path:
        log.info(f"Using supplied raw data at {raw_data_path}")
        data_df = load_cached_pbr_data(raw_data_path)
    else:
        data_df = load_live_pbr_data(staging_path)

    if data_df.empty:
        log.warning("No AOI rows were returned. Rendering a stub report with no-data placeholders.")
    else:
        define_fields(unit_system)
        data_df.attrs["layer_id"] = PBR_PROJECTS_LAYER_ID
        try:
            data_df, _applied_units = RSFieldMeta().apply_units(data_df, layer_id=PBR_PROJECTS_LAYER_ID)
        except Exception as exc:
            log.warning(f"Unable to apply units for all fields: {exc}")

    cards = summary_stats(data_df)
    make_report(
        data_df,
        # summary_tables,
        cards,
        output_path,
        report_name,
        include_pdf=include_pdf,
    )

    if not raw_data_path and not keep_raw_data and staging_path.exists():
        shutil.rmtree(staging_path)
        log.info("Staging cache removed")

    log.info(f"Report Path: {output_path}")


def main() -> None:
    """CLI entry point for PBR Explorer Report generatin."""
    parser = argparse.ArgumentParser(description="PBR Explorer report")
    parser.add_argument("output_path", help="Folder to write outputs (will be created)", type=Path)
    parser.add_argument("path_to_shape", help="Path to the AOI GeoJSON file", type=Path)
    parser.add_argument("report_name", help="Human-readable report name")
    parser.add_argument("--include_pdf", help="Include a PDF version of the report", action="store_true", default=False)
    parser.add_argument("--unit_system", help="Unit system: SI or imperial", type=str, default="SI")
    parser.add_argument(
        "--use-raw-cache",
        dest="raw_cache_path",
        type=Path,
        default=None,
        help="Use an existing directory of pbr_projects_page_*.json files instead of querying the PBR API",
    )
    parser.add_argument("--keep-raw", dest="keep_raw", action="store_true", help="Keep downloaded staged page JSON files")

    args, output_path = parse_report_args(parser)
    log = init_report_logging(output_path, REPORT_SLUG)
    log.info(f"AOI shape: {args.path_to_shape}")
    log.info(f"Report name: {args.report_name}")
    log.info(f"Report version: {report_version}")

    report_main_wrapper(
        log,
        lambda: orchestrate(
            output_path=output_path,
            path_to_shape=Path(args.path_to_shape),
            report_name=args.report_name,
            unit_system=args.unit_system,
            include_pdf=args.include_pdf,
            raw_data_path=args.raw_cache_path,
            keep_raw_data=args.keep_raw,
        ),
        debug=True,
    )


if __name__ == "__main__":
    main()
