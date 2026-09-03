"""Generate the Inventory of Resources report.

Created 2026-08-13.
Created by copilot.
"""

import argparse
import logging
import os
import shutil
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from reports.rpt_inventory_of_resources import __version__ as report_version
from reports.rpt_inventory_of_resources.dataprep import (
    build_metric_cards,
    build_report_summaries,
    data_for_aoi_to_parquet,
)
from reports.rpt_inventory_of_resources.figures import hypsometry_fig
from util import prepare_gdf_for_athena
from util.athena import athena_unload_to_dataframe, get_field_metadata
from util.figures import bar_total_x_by_ybins, horizontal_split_bar_chart, make_aoi_outline_map, project_id_list, split_bar_chart_by_bins
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame, load_gdf_from_pq, ureg
from util.pdf import make_pdf_from_html


def _table_html(summary_df: pd.DataFrame, top_n: int = 20) -> str:
    """Return a bounded, readable HTML table for a report section.

    Args:
            summary_df: Full summary dataframe.
            top_n: Maximum number of rows included in the HTML table.

    Returns:
            An HTML table fragment.

    Created by copilot.
    """
    display_df = summary_df.head(top_n).copy()
    unit_system = RSFieldMeta().unit_system
    length_unit = ureg.kilometer if unit_system == "SI" else ureg.mile
    length_label = "km" if unit_system == "SI" else "mi"
    area_unit = ureg.hectare if unit_system == "SI" else ureg.acre
    area_label = "ha" if unit_system == "SI" else "acres"
    length_categories = {"Stream Network", "Perennial", "Non-Perennial"}
    area_categories = {"Waterbodies", "Riverscape Area", "Anthropogenic LULC", "Riparian-Wetland"}
    display_df[["Total Inventory", "Total BLM", "BLM Managment"]] = display_df[["Total Inventory", "Total BLM", "BLM Managment"]].astype(object)

    for row_index, category in display_df["Resource Category"].items():
        target_unit = length_unit if category in length_categories else area_unit if category in area_categories else None
        if target_unit is not None:
            for column in ("Total Inventory", "Total BLM"):
                value = display_df.at[row_index, column]
                if hasattr(value, "to"):
                    display_df.at[row_index, column] = f"{value.to(target_unit).magnitude:,.2f} {length_label if category in length_categories else area_label}"
        ratio = display_df.at[row_index, "BLM Managment"]
        if pd.notna(ratio):
            display_df.at[row_index, "BLM Managment"] = f"{float(ratio) * 100:,.2f}%"

    return display_df.to_html(index=False, classes="dataframe", border=0)


def define_fields(unit_system: str = "SI"):
    """Set up the fields and units for this report"""
    meta = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()
    meta.field_meta = get_field_metadata(authority='data-exchange-scripts', tool_schema_name='*', layer_id="raw_rme,rpt_rme")  # Set the field metadata for the report
    meta.unit_system = unit_system  # Set the unit system for the report

    # Here's where we can set any preferred units that differ from the data unit
    meta.set_display_unit('centerline_length', 'kilometer')

    return


def make_report(
    data_df: pd.DataFrame,
    aoi_gdf: gpd.GeoDataFrame,
    huc_df: pd.DataFrame,
    report_dir: Path,
    report_name: str,
    include_static: bool = True,
    include_pdf: bool = True,
) -> None:
    """Render interactive and optional static Inventory of Resources reports.

    Args:
            data_df: Normalized raw inventory data.
            aoi_gdf: Source area of interest for the overview map.
            report_dir: Directory containing report outputs.
            report_name: User-facing area name.
            include_static: Render a static HTML report.
            include_pdf: Render a PDF from static HTML.

    """
    log = Logger('make report')

    data_df['ownership_binary'] = data_df['ownership_desc'].apply(lambda x: 'BLM Managed' if x == 'Bureau of Land Management' else 'Non-BLM')
    data_df['fcode_binary'] = data_df['fcode'].apply(lambda x: 'Perennial' if x in (46006, 55800) else 'Non-Perennial')
    data_perennial = data_df[data_df['fcode_binary'] == 'Perennial']
    data_non_perennial = data_df[data_df['fcode_binary'] == 'Non-Perennial']
    RSFieldMeta().add_field_meta(name='ownership_binary', friendly_name='Ownership (BLM vs Non-BLM)', description='Binary classification of ownership: BLM vs Non-BLM')
    RSFieldMeta().add_field_meta(name='fcode_binary', friendly_name='Flow Type', description='Binary classification of stream type: Perennial vs Non-Perennial')
    RSFieldMeta().set_friendly_name('prim_channel_gradient', 'Channel Slope')
    RSFieldMeta().add_field_meta(name='waterbody_type_desc', friendly_name='Waterbody Type', description='Type of waterbody within the riverscape')

    summaries = build_report_summaries(data_df)
    figures = {
        "map": make_aoi_outline_map(aoi_gdf),
        "streams_by_type": horizontal_split_bar_chart(data_df, "fcode_binary", "stream_length", "ownership_binary", color_discrete_map={"BLM Managed": "#1f77b4", "Non-BLM": "#797979"}),
        "streams_by_order": horizontal_split_bar_chart(data_df, "stream_order", "stream_length", "ownership_binary", color_discrete_map={"BLM Managed": "#1f77b4", "Non-BLM": "#797979"}),
        "channel_slope_perennial": split_bar_chart_by_bins(data_perennial, "prim_channel_gradient", "stream_length", "ownership_binary", color_discrete_map={"BLM Managed": "#1f77b4", "Non-BLM": "#797979"}),
        "channel_slope_non_perennial": split_bar_chart_by_bins(data_non_perennial, "prim_channel_gradient", "stream_length", "ownership_binary", color_discrete_map={"BLM Managed": "#1f77b4", "Non-BLM": "#797979"}),
        "streams_by_valley_confinement": split_bar_chart_by_bins(data_df, "confinement_ratio", "stream_length", "ownership_binary", color_discrete_map={"BLM Managed": "#1f77b4", "Non-BLM": "#797979"}),
        "waterbodies": horizontal_split_bar_chart(data_df, "waterbody_type_desc", "waterbody_extent", "ownership_binary", color_discrete_map={"BLM Managed": "#1f77b4", "Non-BLM": "#797979"}),
        "prop_riparian": bar_total_x_by_ybins(data_df, 'segment_area', ['lf_riparian_prop']),
        "hypsometry_fig": hypsometry_fig(huc_df),
    }
    tables = {name: _table_html(summary) for name, summary in summaries.items()}
    appendices = {
        "project_ids": project_id_list(data_df),
    }

    for name, summary in summaries.items():
        summary.to_csv(report_dir / "data" / f"{name}_summary.csv", index=False)

    figure_dir = report_dir / "figures"
    safe_makedirs(str(figure_dir))

    report = RSReport(
        report_name=report_name,
        report_type="Inventory of Resources",
        report_dir=report_dir,
        report_version=report_version,
        figure_dir=figure_dir,
        body_template_path=os.path.join(os.path.dirname(__file__), 'templates', 'body.html'),
        css_paths=[os.path.join(os.path.dirname(__file__), 'templates', 'report.css')],
    )
    for name, figure in figures.items():
        log.info(f"Adding figure '{name}' to the report.")
        report.add_figure(name, figure)
    report.add_html_elements("cards", build_metric_cards(data_df))
    report.add_html_elements("tables", tables)
    report.add_html_elements("appendices", appendices)

    report.render(fig_mode="interactive", suffix="")
    static_path = None
    pdf_path = None
    if include_static:
        static_path = report.render(fig_mode="svg", suffix="_static")
        log.info(f"Generated static HTML report at '{static_path}'.")
        if include_pdf:
            pdf_path = make_pdf_from_html(static_path)
            log.info(f"Generated PDF report at '{pdf_path}'.")


def load_huc_data(hucs: list[str]) -> pd.DataFrame:
    """Queries rscontext_huc10 for all the huc10 watersheds that intersect the aoi
    * this could be a spatial query but we already have the huc12 from data_gdf so this is much faster
    * FUTURE ENHANCEMENT - take the aoi and join with huc geometries to produce some statistics about the amount of intersection between them
    * FUTURE ENHANCEMENT: check if we got data for all the hucs we were looking for
    """
    log = Logger("Load HUC data")

    if not hucs or len(hucs) == 0:
        log.error("No hucs provided to load_huc_data")
        return pd.DataFrame()  # return empty dataframe

    # Basic input sanitation: ensure all hucs are strings, length 10, digits only, and unique
    clean_hucs = {h for h in hucs if isinstance(h, str) and len(h) == 10 and h.isdigit()}
    if not clean_hucs or (len(clean_hucs) != len(hucs)):
        log.error("No hucs, duplicate huc or unexpected value in huc list")

    # Prepare SQL-safe quoted list
    huc_sql = "(" + ",".join([f"'{h}'" for h in clean_hucs]) + ")"
    sql_str = f"SELECT huc, project_id, hucname, hucareasqkm, dem_bins FROM rs_context_huc10 WHERE huc IN {huc_sql}"

    df = athena_unload_to_dataframe(sql_str)
    return df


def make_report_orchestrator(
    report_name: str,
    report_dir: Path,
    path_to_shape: str,
    include_pdf: bool = True,
    unit_system: str = "SI",
    parquet_override: Path | None = None,
    keep_parquet: bool = False,
) -> None:
    """Load the AOI, retrieve or load inventory data, export it, and render.

    Args:
            report_name: User-facing name for the selected area.
            report_dir: Output directory.
            path_to_shape: Path to the input AOI file.
            include_pdf (bool, optional): Whether to generate a PDF version of the report. Defaults to True.
            unit_system (str, optional): The unit system to use ("SI" or "imperial"). Defaults to "SI".
            parquet_override (Path or None): for running multiple times in developement/test can supply path to previously downloaded data and skip athena query
            keep_parquet (bool, default False): keep parquet files, e.g. for debugging purposes
    """
    log = Logger("Make report orchestrator")
    log.info("Starting report orchestration")

    aoi_gdf = gpd.read_file(path_to_shape)
    safe_makedirs(str(report_dir / "data"))
    csv_path = report_dir / "data" / "data.csv"

    # Start tasks in background
    # 1. Get metadata (Athena query)
    meta_future = None
    executor = ThreadPoolExecutor(max_workers=2)

    # Start Metadata definition immediately
    meta_future = executor.submit(define_fields, unit_system)  # This is where all the initialization happens for fields and units

    if parquet_override:
        parquet_data_source = Path(parquet_override)
        if not parquet_data_source.exists():
            raise FileNotFoundError(f"Parquet override file not found at '{parquet_data_source}'")
        log.info(f"Using parquet override files at '{parquet_data_source}'")
    else:
        parquet_data_source = report_dir / "pq"
        query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
        if not simplification_results.success:
            raise ValueError("Unable to simplify input geometry sufficiently to insert into an Athena query")
        if simplification_results.simplified:
            log.warning(f"Input geometry simplified with tolerance {simplification_results.tolerance_m} metres for the Athena query.")

        data_for_aoi_to_parquet(query_gdf, parquet_data_source)

    data_gdf = load_gdf_from_pq(parquet_data_source, geometry_col='dgo_polygon_geom')
    data_gdf.to_csv(csv_path, index=False)

    # Ensure metadata is loaded before applying units
    try:
        meta_future.result()
        log.info("Metadata loaded successfully.")
    except Exception as e:
        log.error(f"Failed to load field metadata: {e}")
        raise e

    data_gdf, _ = RSFieldMeta().apply_units(data_gdf)

    # Export the data to Excel
    RSGeoDataFrame(data_gdf).export_excel(report_dir / 'data' / 'data.xlsx')

    unique_huc10 = data_gdf['watershed_id'].astype(str).unique().tolist()
    huc_data_df = load_huc_data(unique_huc10)

    make_report(data_gdf, aoi_gdf, huc_data_df, report_dir, report_name, include_static=include_pdf, include_pdf=include_pdf)

    if not keep_parquet:
        try:
            if parquet_data_source.exists():
                shutil.rmtree(parquet_data_source)
                log.info(f"Removed parquet data source at '{parquet_data_source}'")
        except Exception as e:
            log.warning(f"Failed to remove parquet data source: {e}")


def main() -> None:
    """Parse CLI arguments and generate an Inventory of Resources report.

    Created by copilot.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("output_path", type=Path, help="Folder to store the report outputs")
    parser.add_argument("path_to_shape", type=str, help="Path to the AOI GeoJSON, shapefile, or other vector input")
    parser.add_argument("report_name", help="Name for the report area")
    parser.add_argument("--include-pdf", action="store_true", help="Include static HTML and PDF outputs")
    parser.add_argument('--unit_system', help='Unit system to use: SI or imperial', type=str, default='SI')
    parser.add_argument('--use-parquet', dest='parquet_path', type=Path, default=None, help='Use an existing Parquet file or directory instead of running the Athena AOI query')
    parser.add_argument('--keep-parquet', action='store_true', help='Keep the downloaded AOI Parquet files instead of deleting the pq folder')
    args = dotenv.parse_args_env(parser)

    safe_makedirs(str(args.output_path))
    log = Logger("Inventory of Resources")
    log.setup(log_path=args.output_path / "report.log", log_level=logging.DEBUG)
    log.title("rs-rpt-inventory-of-resources")
    log.info(f"Report version: {report_version}")
    try:
        make_report_orchestrator(
            report_name=args.report_name,
            report_dir=args.output_path,
            path_to_shape=args.path_to_shape,
            include_pdf=args.include_pdf,
            unit_system=args.unit_system,
            parquet_override=args.parquet_path,
            keep_parquet=args.keep_parquet,
        )
    except Exception as exc:
        log.error(exc)
        traceback.print_exc(file=sys.stdout)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
