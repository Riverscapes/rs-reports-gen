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
from util import prepare_gdf_for_athena
from util.athena import get_field_metadata
from util.figures import make_aoi_outline_map, project_id_list
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame, load_gdf_from_pq
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
    return summary_df.head(top_n).to_html(index=False, classes="dataframe", border=0)


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

    summaries = build_report_summaries(data_df)
    figures = {
        "map": make_aoi_outline_map(aoi_gdf),
    }
    tables = {name: _table_html(summary) for name, summary in summaries.items()}
    appendices = {
        "project_ids": project_id_list(aoi_gdf),
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

    make_report(data_gdf, aoi_gdf, report_dir, report_name, include_static=include_pdf, include_pdf=include_pdf)

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
    parser.add_argument("--csv", type=Path, default=None, help="Use a local inventory CSV instead of querying Athena")
    parser.add_argument("--include-pdf", action="store_true", help="Include static HTML and PDF outputs")
    args = dotenv.parse_args_env(parser)

    safe_makedirs(str(args.output_path))
    log = Logger("Inventory of Resources")
    log.setup(log_path=args.output_path / "report.log", log_level=logging.DEBUG)
    log.title("rs-rpt-inventory-of-resources")
    log.info(f"Report version: {report_version}")
    try:
        make_report_orchestrator(args.report_name, args.output_path, args.path_to_shape, args.csv, args.include_pdf)
    except Exception as exc:
        log.error(exc)
        traceback.print_exc(file=sys.stdout)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
