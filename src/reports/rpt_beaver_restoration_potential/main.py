"""Beaver Restoration Potential report stub.

Queries beaver-related RME fields for an AOI, supports cache-first parquet inputs,
and renders a minimal HTML report with baseline summary tables and charts.

Created 2026-07-07.
Created by copilot.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pint_pandas
from rsxml import Logger

from reports.rpt_beaver_restoration_potential import __version__ as report_version
from reports.rpt_beaver_restoration_potential.dataprep import (
    RPT_RME_LAYER_ID,
    load_cached_beaver_data,
    query_beaver_data_for_aoi,
    summarize_beaver_potential,
)
from reports.rpt_beaver_restoration_potential.figures import build_beaver_figures, high_rp_statistics, main_statistics
from util import prepare_gdf_for_athena
from util.athena import get_field_metadata_lakehouse_ref
from util.figures import make_aoi_outline_map, metric_cards
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame, load_meta_from_file, save_meta_to_file
from util.pdf import make_pdf_from_html
from util.report_entrypoint import (
    add_parquet_cli_args,
    init_report_logging,
    parse_report_args,
    report_main_wrapper,
)

REPORT_SLUG = "rpt-beaver-restoration-potential"


def _resolve_metadata_cachefile_path(parquet_path: Path | None, staging_path: Path, keep_parquet: bool) -> tuple[bool, Path | None]:
    """Determine metadata cache mode and cache file location for this run."""
    if parquet_path:
        candidate = parquet_path / "registry_field_meta.parquet"
        if candidate.exists():
            return True, candidate
        Logger("Define Fields").warning(f"No metadata cache found at {candidate}. Falling back to Athena metadata.")
        return False, None

    if keep_parquet:
        return False, staging_path / "registry_field_meta.parquet"

    return False, None


def define_fields(unit_system: str = "SI", load_from_parquet: bool = False, metadata_cachefile_path: Path | None = None) -> None:
    """Load metadata, normalize layer ids, and set display-unit preferences."""
    log = Logger("Define Fields")
    registry_field_meta: pd.DataFrame | None = None

    if load_from_parquet and metadata_cachefile_path and metadata_cachefile_path.exists():
        log.info(f"Loading field metadata from {metadata_cachefile_path}")
        registry_field_meta = load_meta_from_file(metadata_cachefile_path)

    if registry_field_meta is None:
        registry_field_meta = get_field_metadata_lakehouse_ref(lakehouse_ref="rs_rpt.nasa_ba_rme_join")
        if metadata_cachefile_path:
            log.info(f"Saving metadata cache to {metadata_cachefile_path}")
            save_meta_to_file(registry_field_meta, metadata_cachefile_path)

    registry_field_meta["layer_id"] = RPT_RME_LAYER_ID

    field_meta = RSFieldMeta()
    field_meta.field_meta = registry_field_meta
    field_meta.unit_system = unit_system
    field_meta.set_display_unit("centerline_length", "kilometer", RPT_RME_LAYER_ID)
    field_meta.set_display_unit("segment_area", "kilometer ** 2", RPT_RME_LAYER_ID)


def _build_report_context(df: pd.DataFrame, report_name: str, path_to_shape: Path) -> dict[str, str | int]:
    """Build template context values for top-level report metadata."""
    level_path_count = int(df["level_path"].nunique())
    huc10_count = int(df["huc10"].nunique())
    total_riverscape_area = "N/A"
    if "segment_area" in df.columns and not df.empty:
        total_area_value = df["segment_area"].sum()
        total_riverscape_area = RSFieldMeta().format_scalar(
            "segment_area",
            total_area_value,
            layer_id=RPT_RME_LAYER_ID,
            decimals=2,
        )

    return {
        "report_name": report_name,
        "aoi_input": str(path_to_shape),
        "row_count": len(df),
        "level_path_count": level_path_count,
        "huc10_count": huc10_count,
        "total_riverscape_area": total_riverscape_area,
    }


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
    aoi_df: gpd.GeoDataFrame,
    summary_tables: dict[str, pd.DataFrame],
    context: dict[str, str | int],
    report_dir: Path,
    report_name: str,
    *,
    include_pdf: bool = False,
) -> None:
    """Render the Beaver Restoration Potential HTML report and optional PDF."""
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
    raw_export_df.to_parquet(data_dir / "beaver_restoration_potential_raw.parquet", index=False)
    # for compatibility with older systems we could also, or based on a CLI flag export CSV
    # raw_export_df.to_csv(data_dir / "beaver_restoration_potential_raw.csv", index=False)
    for key, summary_df in summary_tables.items():
        summary_df.to_csv(data_dir / f"{key}_summary.csv", index=False)

    figures = {
        "map": make_aoi_outline_map(aoi_df),
        **build_beaver_figures(summary_tables),
    }
    summary_tables_html = {name: _summary_table_to_html(df) for name, df in summary_tables.items()}

    report = RSReport(
        report_name=report_name,
        report_type="Beaver Restoration Potential",
        report_dir=report_dir,
        body_template_path=Path(__file__).parent / "templates" / "body.html",
        report_version=report_version,
    )

    for name, fig in figures.items():
        report.add_figure(name, fig)

    summary_stats = main_statistics(data_df)
    high_rp_stats = high_rp_statistics(data_df)
    metrics_for_summary_cards = ["total_dam_capacity", "total_dams", "realized_capacity"]
    metric_data_for_cards = {key: summary_stats[key] for key in metrics_for_summary_cards}
    metric_data_for_cards.update(high_rp_stats)

    report.add_html_elements("summary_tables", summary_tables_html)
    report.add_html_elements("context", context)
    report.add_html_elements("cards", metric_cards(metric_data_for_cards))
    report.render(fig_mode="interactive")
    log.info(f"HTML report written to {report_dir}")

    if include_pdf:
        make_pdf_from_html(str(report_dir))


def orchestrate(
    output_path: Path,
    path_to_shape: Path,
    report_name: str,
    unit_system: str,
    include_pdf: bool,
    parquet_path: Path | None = None,
    keep_parquet: bool = False,
) -> None:
    """Execute the query -> summarize -> render workflow for this report."""
    log = Logger("Orchestrate")
    staging_path = output_path / "staging"

    if parquet_path and not parquet_path.exists():
        raise FileNotFoundError(f"Parquet path '{parquet_path}' does not exist")

    aoi_gdf = gpd.read_file(path_to_shape)
    query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
    if not simplification_results.success:
        log.warning("AOI geometry could not be simplified to the preferred size; Athena query may fail.")
    if simplification_results.simplified:
        log.warning(f"AOI geometry simplified with tolerance {simplification_results.tolerance_m} meters for Athena query execution.")

    if not parquet_path:
        staging_path.mkdir(parents=True, exist_ok=True)

    load_meta_from_parquet, metadata_cachefile_path = _resolve_metadata_cachefile_path(parquet_path, staging_path, keep_parquet)
    define_fields(unit_system, load_from_parquet=load_meta_from_parquet, metadata_cachefile_path=metadata_cachefile_path)

    if parquet_path:
        log.info(f"Using supplied parquet data at {parquet_path}")
        data_df = load_cached_beaver_data(parquet_path)
    else:
        data_df = query_beaver_data_for_aoi(query_gdf, staging_path)

    if data_df.empty:
        log.warning("No AOI rows were returned. Rendering a stub report with no-data placeholders.")
    else:
        data_df.attrs["layer_id"] = RPT_RME_LAYER_ID
        try:
            data_df, _applied_units = RSFieldMeta().apply_units(data_df, layer_id=RPT_RME_LAYER_ID)
        except Exception as exc:
            log.warning(f"Unable to apply units for all fields: {exc}")

    summary_tables = summarize_beaver_potential(data_df)
    context = _build_report_context(data_df, report_name, path_to_shape)
    make_report(
        data_df,
        aoi_gdf,
        summary_tables,
        context,
        output_path,
        report_name,
        include_pdf=include_pdf,
    )

    if not parquet_path and not keep_parquet and staging_path.exists():
        shutil.rmtree(staging_path)
        log.info("Staging parquet removed")

    log.info(f"Report Path: {output_path}")


def main() -> None:
    """CLI entry point for Beaver Restoration Potential."""
    parser = argparse.ArgumentParser(description="Beaver Restoration Potential report")
    parser.add_argument("output_path", help="Folder to write outputs (will be created)", type=Path)
    parser.add_argument("path_to_shape", help="Path to the AOI GeoJSON file", type=Path)
    parser.add_argument("report_name", help="Human-readable report name")
    parser.add_argument("--include_pdf", help="Include a PDF version of the report", action="store_true", default=False)
    parser.add_argument("--unit_system", help="Unit system: SI or imperial", type=str, default="SI")
    add_parquet_cli_args(parser)

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
            parquet_path=args.parquet_path,
            keep_parquet=args.keep_parquet,
        ),
        debug=True,
    )


if __name__ == "__main__":
    main()
