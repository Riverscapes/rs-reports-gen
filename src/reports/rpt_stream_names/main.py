"""Main module for Riverscapes Stream Names Report"""

# System imports
import argparse
import logging
import shutil
import sys
import traceback
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from reports.rpt_stream_names import __version__ as report_version
from reports.rpt_stream_names.dataprep import build_highlight_cards_data, get_wcdata_for_aoi
from reports.rpt_stream_names.figures import aoi_polygon_svg, word_cloud
from util import prepare_gdf_for_athena
from util.figures import (
    HighlightCard,
    make_aoi_outline_map,
)
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.pdf import make_pdf_from_html


def define_fields(unit_system: str = "SI") -> None:
    """Register field metadata and configure unit system for this report.

    Args:
        unit_system (str): Unit system to use ("SI" or "imperial"). Defaults to "SI".

    Created by copilot.
    """
    meta = RSFieldMeta()
    meta.unit_system = unit_system

    # total_riverscape_length arrives from Athena in metres; display in km or miles
    meta.add_field_meta(
        name="total_riverscape_length",
        friendly_name="Total Riverscape Length",
        data_unit="meter",
        dtype="REAL",
        description="Sum of riverscape centerline length for all level paths with this stream name.",
        preferred_format="{:,.1f}",
    )
    # Set km display; RSFieldMeta will auto-convert to miles when unit_system is imperial
    meta.set_display_unit("total_riverscape_length", "kilometer")

    meta.add_field_meta(
        name="level_path_count",
        friendly_name="Distinct Paths",
        data_unit=None,
        dtype="INTEGER",
        description="Number of distinct level paths with this stream name.",
    )
    meta.add_field_meta(
        name="stream_name",
        friendly_name="Stream Name",
        data_unit=None,
        dtype="TEXT",
        description="Name of the stream.",
    )
    meta.add_field_meta(
        name="rank",
        friendly_name="Rank",
        data_unit=None,
        dtype="INTEGER",
        description="Rank by distinct paths (ties broken by total length).",
    )
    meta.add_field_meta(
        name="pct_of_paths",
        friendly_name="% of Named Paths",
        data_unit=None,
        dtype="REAL",
        description="Percent of all distinct, named, level paths in the area of interest with this stream name.",
        preferred_format="{:.2f}",
    )
    meta.add_field_meta(
        name="pct_of_length",
        friendly_name="% of Named Length",
        data_unit=None,
        dtype="REAL",
        description="Percent of total named riverscape length in the area of interest accounted for by this stream name.",
        preferred_format="{:.2f}",
    )


def build_top_names_by_path_count_table(df: pd.DataFrame, top_n: int = 10) -> str:
    """Build an HTML table of the top N stream names ranked by distinct level-path count then total riverscape length.

    Args:
        df (pd.DataFrame): The stream names dataframe from dataprep.
        top_n (int): Number of top rows to include. Defaults to 10.

    Returns:
        str: HTML table fragment.

    Created by copilot.
    """
    total_paths = df["level_path_count"].sum()

    ranked = (
        df[["stream_name", "level_path_count", "total_riverscape_length"]]
        .copy()
        .sort_values(
            by=["level_path_count", "total_riverscape_length", "stream_name"],
            ascending=[False, False, True],
            kind="mergesort",
        )
        .head(top_n)
        .reset_index(drop=True)
    )
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    ranked["pct_of_paths"] = ranked["level_path_count"] / total_paths * 100

    # Reorder columns for display
    display_df = RSGeoDataFrame(ranked[["rank", "stream_name", "level_path_count", "pct_of_paths", "total_riverscape_length"]])
    return display_df.to_html(index=False, escape=False)


def build_top_names_by_riverscape_length_table(df: pd.DataFrame, top_n: int = 10) -> str:
    """Build an HTML table of the top N stream names ranked by total riverscape length then distinct level-path count.

    Args:
        df (pd.DataFrame): The stream names dataframe from dataprep.
        top_n (int): Number of top rows to include. Defaults to 10.

    Returns:
        str: HTML table fragment.
    """
    total_length = df["total_riverscape_length"].sum()

    ranked = (
        df[["stream_name", "level_path_count", "total_riverscape_length"]]
        .copy()
        .sort_values(
            by=["total_riverscape_length", "level_path_count", "stream_name"],
            ascending=[False, False, True],
            kind="mergesort",
        )
        .head(top_n)
        .reset_index(drop=True)
    )
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    ranked["pct_of_length"] = ranked["total_riverscape_length"] / total_length * 100

    display_df = RSGeoDataFrame(ranked[["rank", "stream_name", "total_riverscape_length", "pct_of_length", "level_path_count"]])
    return display_df.to_html(index=False, escape=False)


def sort_dataframe_for_deterministic_output(df: pd.DataFrame) -> pd.DataFrame:
    """Return a deterministically sorted copy of a dataframe for stable file outputs."""
    if df.empty:
        return df.reset_index(drop=True)

    sort_columns = list(df.columns)
    return df.sort_values(by=sort_columns, kind="mergesort", na_position="last").reset_index(drop=True)


def make_report(
    df: pd.DataFrame,
    report_dir: Path,
    report_name: str,
    aoi_gdf: gpd.GeoDataFrame,
    include_static: bool = True,
    include_pdf: bool = True,
    unit_system: str = "SI",
    guessed_name: str = "",
):
    """
    Generates HTML report(s) in report_dir.
    Args:
        df (pandas DataFrame): The main data dataframe for the report.
        report_dir (Path): The directory where the report will be saved.
        report_name (str): The name of the report.
        aoi_gdf (gpd.GeoDataFrame): Polygon used for the AOI map and SVG graphic.
            May be the raw AOI shape or the simplified query polygon.
        include_static (bool, optional): Whether to include a static version of the report. Defaults to True.
        include_pdf (bool, optional): Whether to include a PDF version of the report. Defaults to True.
        unit_system (str, optional): Unit system for display values ("SI" or "imperial"). Defaults to "SI".
        guessed_name (str, optional): Users guess on most common name, will be included in report if not empty

    Note: define_fields() must be called before this function to configure units.
    """
    log = Logger('make report')

    log.info(f"Generating report in {report_dir}")
    figures = {
        "map": make_aoi_outline_map(aoi_gdf),
    }

    figure_dir = report_dir / 'figures'
    safe_makedirs(str(figure_dir))

    header_svg = aoi_polygon_svg(aoi_gdf, figure_dir)
    tables = {
        "top_names_by_path_count": build_top_names_by_path_count_table(df),
        "top_names_by_riverscape_length": build_top_names_by_riverscape_length_table(df),
    }

    word_cloud(df, figure_dir, frequency_field='total_riverscape_length')
    word_cloud(df, figure_dir, frequency_field='level_path_count')

    highlight_cards: list[HighlightCard] = build_highlight_cards_data(df, unit_system=unit_system)

    report = RSReport(
        report_name="What are the most common names of our streams and rivers?",
        report_subtitle=report_name,
        report_type="Riverscapes Word Cloud",
        report_dir=report_dir,
        report_version=report_version,
        body_template_path=Path(__file__).parent / 'templates' / 'body.html',
        css_paths=[Path(__file__).parent / 'templates' / 'report.css'],
    )
    for name, fig in figures.items():
        report.add_figure(name, fig)

    report.set_header_svg(header_svg)
    report.add_html_elements("tables", tables)
    report.add_html_elements("highlight_cards", highlight_cards)
    report.add_html_elements("user_guess", guessed_name)

    interactive_path = report.render(fig_mode="interactive", suffix="")
    static_path = None
    pdf_path = None
    if include_static:
        static_path = report.render(fig_mode="svg", suffix="_static")
        if include_pdf:
            pdf_path = make_pdf_from_html(static_path)
            log.info(f'PDF report built from static at {pdf_path}')

    log.title('Report Generation Complete')
    log.info(f'Interactive: {interactive_path}')
    if static_path:
        log.info(f'Static: {static_path}')
    if pdf_path:
        log.info(f'PDF: {pdf_path}')


def make_report_orchestrator(
    report_name: str,
    report_dir: Path,
    path_to_shape: str,
    existing_csv_path: Path | None = None,
    include_pdf: bool = True,
    unit_system: str = "SI",
    guessed_name: str = "",
):
    """Orchestrates the report generation process:

    Args:
        report_name (str): The name of the report.
        report_dir (str): The directory where the report will be saved.
        path_to_shape (str): The path to the shapefile for the area of interest.
        existing_csv_path (str | None, optional): Path to an existing CSV file to use instead of querying Athena. Defaults to None.
        include_pdf (bool, optional): Whether to generate a PDF version of the report. Defaults to True.
        unit_system (str, optional): The unit system to use ("SI" or "imperial"). Defaults to "SI".
    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")

    # Initialize field metadata and unit system for this report
    define_fields(unit_system)

    # make place for the data to go (as csv)
    safe_makedirs(str(report_dir / 'data'))
    csv_data_path = report_dir / 'data' / 'data.csv'

    # load shape as gdf
    aoi_gdf = gpd.read_file(path_to_shape)
    query_gdf = None  # populated below when Athena query is run

    if existing_csv_path:
        log.info(f"Using supplied csv file at {csv_data_path}")
        if existing_csv_path != csv_data_path:
            shutil.copyfile(existing_csv_path, csv_data_path)
        data_df = pd.read_csv(csv_data_path)
        query_gdf = aoi_gdf  # no Athena simplification; use raw AOI shape for SVG
    else:
        # use shape to query Athena
        query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
        if not simplification_results.success:
            raise ValueError("Unable to simplify input geometry sufficiently to insert into Athena query")
        if simplification_results.simplified:
            log.warning(
                f"""Input polygon was simplified using tolerance of {simplification_results.tolerance_m} metres for the purpose of intersecting with DGO geometries in the database.
                If you require a higher precision extract, please contact support@riverscapes.freshdesk.com."""
            )

        log.info("Querying athena for data for AOI")
        data_df = get_wcdata_for_aoi(query_gdf)

    data_df = sort_dataframe_for_deterministic_output(data_df)
    data_df.to_csv(csv_data_path, index=False)
    # given the data groups by stream name and there are only ~80k distinct stream names in CONUS this shouldn't blow up
    data_df.to_excel(report_dir / 'data' / 'data.xlsx', index=False)

    # make html report
    # If we aren't including pdf we just make interactive report. No need for the static one
    make_report(
        data_df,
        report_dir,
        report_name,
        aoi_gdf=query_gdf,
        include_static=include_pdf,
        include_pdf=include_pdf,
        unit_system=unit_system,
        guessed_name=guessed_name,
    )

    log.info(f"Report Path: {report_dir}")


def main():
    """Main function to parse arguments and generate the report"""

    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=Path)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=str)
    parser.add_argument('report_name', help='name for the report (usually name of the area selected)')
    parser.add_argument('--include_pdf', help='Include a pdf version of the report', action='store_true', default=False)
    parser.add_argument('--unit_system', help='Unit system to use: SI or imperial', type=str, default='SI')
    parser.add_argument('--csv', help='Path to a local CSV of downloaded data for the AOI to use instead of querying Athena', type=str, default=None)
    parser.add_argument('--guessed_name', help='What name do you think is most common in this area?', type=str, default='')
    # NOTE: IF WE CHANGE THESE VALUES PLEASE UPDATE ./launch.py

    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    output_path = Path(args.output_path)
    # new version of safe_makedirs will take a Path but for now all Paths are converted to string for this function
    safe_makedirs(str(output_path))

    log = Logger('Setup')
    log_path = output_path / 'report.log'
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-stream-names')
    log.info(f"Output path: {output_path}")
    log.info(f"AOI shape: {args.path_to_shape}")
    log.info(f"Region name: {args.report_name}")
    log.info(f"Report Version: {report_version}")
    if args.csv:
        csvpath = Path(args.csv)
        log.info(f"Using existing CSV: {csvpath}")
    else:
        log.debug("No existing CSV provided, will query Athena")
        csvpath = None

    try:
        make_report_orchestrator(args.report_name, output_path, args.path_to_shape, csvpath, args.include_pdf, args.unit_system, args.guessed_name)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
