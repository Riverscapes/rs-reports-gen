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
import sqlite3
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pint_pandas
from rsxml import Logger

from reports.rpt_pbr_explorer import __version__ as report_version
from reports.rpt_pbr_explorer.dataprep import (
    PBR_DATE_ENUMS,
    PBR_PROJECTS_LAYER_ID,
    SUMMARY_METRICS_LAYER_ID,
    _ensure_actions_metric_metadata,
    build_actions_metrics,
    build_project_extents_gdf,
    build_summary_metrics,
    count_projects_with_actions,
    define_fields,
    load_cached_pbr_data,
    load_live_pbr_data,
    normalize_affiliates_table,
    parse_actions_to_columns,
    parse_dates_to_columns,
)
from reports.rpt_pbr_explorer.figures import build_pbr_figures
from util import prepare_gdf_for_athena
from util.figures import (
    MetricCards,
    metric_cards,
)
from util.html import RSReport
from util.pandas import RSFieldMeta
from util.pdf import make_pdf_from_html
from util.report_entrypoint import (
    init_report_logging,
    parse_report_args,
    report_main_wrapper,
)

REPORT_SLUG = "rpt-pbr-explorer"
PBR_EXPORT_GPKG_FILENAME = "pbr_projects.gpkg"
PBR_EXPORT_LAYER_PROJECTS = "projects"
PBR_EXPORT_LAYER_EXTENTS = "project_extents"
PBR_EXPORT_TABLE_AFFILIATES = "project_affiliates"


def make_report(
    data_df: pd.DataFrame,
    aoi_df: gpd.GeoDataFrame,
    cards: MetricCards,
    actions_cards: MetricCards | None,
    report_dir: Path,
    report_name: str,
    *,
    include_pdf: bool = False,
    no_data_message: str | None = None,
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

    figures = build_pbr_figures(data_df, aoi_df)
    # summary_tables_html = {name: _summary_table_to_html(df) for name, df in summary_tables.items()}

    report = RSReport(
        report_name=report_name,
        report_type="Beaver Restoration Potential",
        report_dir=report_dir,
        body_template_path=Path(__file__).parent / "templates" / "body.html",
        report_version=report_version,
    )

    for name, fig in figures.items():
        report.add_figure(name, fig)

    # report.add_html_elements("summary_tables", summary_tables_html)
    report.add_html_elements("cards", cards)
    report.add_html_elements("actions_cards", actions_cards)
    report.add_html_elements("no_data_message", no_data_message)
    outpath = report.render(fig_mode="interactive")
    log.info(f"HTML report written to {outpath}")

    if include_pdf:
        # make a static version as well, so we get the figure
        static_path = report.render(fig_mode="svg", suffix="_static")
        pdf_path = make_pdf_from_html(static_path)
        log.info(f'PDF report built from static at {pdf_path}')


def _strip_pint_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pint columns to magnitudes for file export compatibility."""
    export_df = df.copy()
    for col in export_df.columns:
        if isinstance(export_df[col].dtype, pint_pandas.PintType):
            export_df[col] = export_df[col].pint.magnitude
    return export_df


def export_column_metadata_csv(project_df: pd.DataFrame, output_path: Path) -> Path:
    """Export column metadata for project-layer fields used in the package.

    Created by copilot.
    """
    metadata_path = output_path / "column_metadata.csv"
    layer_id = project_df.attrs.get("layer_id", PBR_PROJECTS_LAYER_ID)
    meta = RSFieldMeta()

    records: list[dict[str, str | None]] = []
    for column_name in project_df.columns:
        field_meta = meta.get_field_meta(column_name, layer_id=layer_id)
        records.append(
            {
                "table_name": PBR_EXPORT_LAYER_PROJECTS,
                "column_name": column_name,
                "friendly_name": field_meta.friendly_name if field_meta else column_name,
                "data_unit": str(field_meta.data_unit) if field_meta and field_meta.data_unit is not None else None,
                "description": field_meta.description if field_meta else None,
            }
        )

    pd.DataFrame(records).to_csv(metadata_path, index=False)
    return metadata_path


def _register_gpkg_attribute_table(conn: sqlite3.Connection, table_name: str) -> None:
    """Register a non-spatial table in GeoPackage metadata for QGIS discovery.

    Created by copilot.
    """
    table_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if table_exists is None:
        return

    conn.execute("DELETE FROM gpkg_contents WHERE table_name = ?", (table_name,))
    conn.execute(
        """
        INSERT INTO gpkg_contents (
            table_name,
            data_type,
            identifier,
            description,
            last_change,
            min_x,
            min_y,
            max_x,
            max_y,
            srs_id
        )
        VALUES (
            ?,
            'attributes',
            ?,
            ?,
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
        )
        """,
        (table_name, table_name, "PBR project affiliates (attribute table)"),
    )


def export_data_gpkg(data_df: pd.DataFrame, output_path: Path) -> Path:
    """Export PBR projects, extents, and affiliates to a single GeoPackage.

    Created by copilot.
    """
    log = Logger("Export PBR Data")
    data_dir = output_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = data_dir / PBR_EXPORT_GPKG_FILENAME

    export_df = _strip_pint_columns(data_df)
    export_df = parse_dates_to_columns(export_df)
    affiliates_df = normalize_affiliates_table(export_df)
    extents_gdf = build_project_extents_gdf(export_df)

    # Keep only flat project columns in the main export layer.
    drop_columns = [
        "orgAffiliates",
        "pbrAffiliates",
        "extent",
        "budget.items",
    ]
    project_df = export_df.drop(columns=[c for c in drop_columns if c in export_df.columns], errors="ignore").copy()
    extent_prefixed_columns = [c for c in project_df.columns if c == "extent" or c.startswith("extent.")]
    if extent_prefixed_columns:
        project_df = project_df.drop(columns=extent_prefixed_columns)

    # Ensure date columns are included even if all null for this AOI.
    for date_enum in PBR_DATE_ENUMS:
        date_col = f"date_{date_enum}"
        if date_col not in project_df.columns:
            project_df[date_col] = pd.NA

    projects_gdf = gpd.GeoDataFrame(
        project_df,
        geometry=gpd.points_from_xy(project_df["location.longitude"], project_df["location.latitude"]),
        crs="EPSG:4326",
    )
    projects_gdf.to_file(gpkg_path, layer=PBR_EXPORT_LAYER_PROJECTS, driver="GPKG")

    if not extents_gdf.empty:
        extents_gdf.to_file(gpkg_path, layer=PBR_EXPORT_LAYER_EXTENTS, driver="GPKG")

    with sqlite3.connect(gpkg_path) as conn:
        affiliates_df.to_sql(PBR_EXPORT_TABLE_AFFILIATES, conn, if_exists="replace", index=False)
        _register_gpkg_attribute_table(conn, PBR_EXPORT_TABLE_AFFILIATES)

    metadata_path = export_column_metadata_csv(project_df, output_path / "data")
    log.info(f"Exported projects to {gpkg_path}")
    log.info(f"Exported metadata to {metadata_path}")
    return gpkg_path


def summary_stats(data_df: pd.DataFrame) -> MetricCards:
    """Build and render summary metrics for the report cards."""
    return metric_cards(build_summary_metrics(data_df), layer_id=SUMMARY_METRICS_LAYER_ID)


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
        data_df = load_live_pbr_data(staging_path, query_gdf)

    define_fields(unit_system)

    if data_df.empty:
        fetch_status = data_df.attrs.get("fetch_status")
        if fetch_status == "error":
            no_data_message = "No data returned because the PBR API request failed. See logs for details."
            log.warning("No rows available due to a PBR API fetch error. Rendering a short no-data report.")
        else:
            no_data_message = "No data returned for this request."
            log.warning("No AOI rows were returned. Rendering a short no-data report.")

        make_report(
            data_df,
            aoi_gdf,
            {},
            {},
            output_path,
            report_name,
            include_pdf=include_pdf,
            no_data_message=no_data_message,
        )
    else:
        data_df.attrs["layer_id"] = PBR_PROJECTS_LAYER_ID
        # Parse nested arrays before applying units so metadata resolves correctly.
        data_df = parse_actions_to_columns(data_df)
        data_df = parse_dates_to_columns(data_df)
        try:
            data_df, _applied_units = RSFieldMeta().apply_units(data_df, layer_id=PBR_PROJECTS_LAYER_ID)
        except Exception as exc:
            log.warning(f"Unable to apply units for all fields: {exc}")

        export_data_gpkg(data_df, output_path)

        cards = summary_stats(data_df)

        # Build actions metric cards and enrich with project counts.
        _ensure_actions_metric_metadata()
        actions_sums = build_actions_metrics(data_df)
        actions_counts = count_projects_with_actions(data_df)
        actions_cards = metric_cards(actions_sums, layer_id=SUMMARY_METRICS_LAYER_ID)
        for action_key, count in actions_counts.items():
            if action_key in actions_cards:
                actions_cards[action_key]["details"] = f"{count} project{'s' if count != 1 else ''}"

        make_report(
            data_df,
            aoi_gdf,
            cards,
            actions_cards,
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
