"""Main module for Riverscapes Stream Names Report"""
# System imports
import argparse
import logging
import os
from pathlib import Path
import sys
import shutil
import traceback
import pandas as pd
import geopandas as gpd
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from util import prepare_gdf_for_athena
from util.pandas import load_gdf_from_csv
from util.athena import get_data_for_aoi, athena_unload_to_dataframe
from util.rme.field_metadata import get_field_metadata
from util.athena.athena import S3_ATHENA_BUCKET, get_wcdata_for_aoi
from util.color import DEFAULT_FCODE_COLOR_MAP

from util.figures import (
    make_map_with_aoi,
    table_total_x_by_y,
    project_id_list,
)
from util.pdf import make_pdf_from_html
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame
from reports.rpt_stream_names import __version__ as report_version
from reports.rpt_stream_names.figures import word_cloud


def make_report(gdf: gpd.GeoDataFrame,
                report_dir: Path, report_name: str,
                include_static: bool = True,
                include_pdf: bool = True
                ) -> dict[str, str]:
    """
    Generates HTML report(s) in report_dir.
    Args:
        gdf (gpd.GeoDataFrame): The main data geodataframe for the report.
        aoi_df (gpd.GeoDataFrame): The area of interest geodataframe.
        report_dir (str): The directory where the report will be saved.
        report_name (str): The name of the report.
        include_static (bool, optional): Whether to include a static version of the report. Defaults to True.
        include_pdf (bool, optional): Whether to include a PDF version of the report. Defaults to True.
    """
    log = Logger('make report')

    log.info(f"Generating report in {report_dir}")
    figures = {
        # "map": make_map_with_aoi(gdf, aoi_df, color_discrete_map=DEFAULT_FCODE_COLOR_MAP),
    }

    figure_dir = report_dir / 'figures'
    safe_makedirs(str(figure_dir))

    word_cloud(gdf, figure_dir)

    report = RSReport(
        report_name=report_name,
        report_type="Riverscapes Stream Names",
        report_dir=report_dir,
        report_version=report_version,
        figure_dir=figure_dir,
        body_template_path=os.path.join(os.path.dirname(__file__), 'templates', 'body.html'),
        css_paths=[os.path.join(os.path.dirname(__file__), 'templates', 'report.css')],
    )
    for (name, fig) in figures.items():
        report.add_figure(name, fig)

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


def make_report_orchestrator(report_name: str, report_dir: Path, path_to_shape: str,
                             existing_csv_path: Path | None = None, include_pdf: bool = True):
    """ Orchestrates the report generation process:

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

    # This is where all the initialization happens for fields and units
    # we really don't need them for this report
    # define_fields(unit_system)  # ensure fields are defined

    # make place for the data to go (as csv)
    safe_makedirs(str(report_dir / 'data'))
    csv_data_path = report_dir / 'data' / 'data.csv'

    # load shape as gdf
    aoi_gdf = gpd.read_file(path_to_shape)

    if existing_csv_path:
        log.info(f"Using supplied csv file at {csv_data_path}")
        if existing_csv_path != csv_data_path:
            shutil.copyfile(existing_csv_path, csv_data_path)
        data_df = pd.read_csv(csv_data_path)
    else:
        # use shape to query athena
        query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
        if not simplification_results.success:
            raise ValueError("Unable to simplify input geometry sufficiently to insert into Athena query")
        if simplification_results.simplified:
            log.warning(
                f"""Input polygon was simplified using tolerance of {simplification_results.tolerance_m} metres for the purpose of intersecting with DGO geometries in the database.
                If you require a higher precision extract, please contact support@riverscapes.freshdesk.com.""")

        log.info("Querying athena for data for AOI")
        data_df = get_wcdata_for_aoi(query_gdf)
        data_df.to_csv(csv_data_path)

    data_df.to_excel(report_dir / 'data' / 'data.xlsx', index=False)

    # make html report
    # If we aren't including pdf we just make interactive report. No need for the static one
    make_report(data_df, report_dir, report_name,
                include_static=include_pdf,
                include_pdf=include_pdf
                )

    log.info(f"Report Path: {report_dir}")


def main():
    """ Main function to parse arguments and generate the report
"""

    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=str)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=str)
    parser.add_argument('report_name', help='name for the report (usually description of the area selected)')
    parser.add_argument('--include_pdf', help='Include a pdf version of the report', action='store_true', default=False)
    parser.add_argument('--unit_system', help='Unit system to use: SI or imperial', type=str, default='SI')
    parser.add_argument('--csv', help='Path to a local CSV of AOI data to use instead of querying Athena', type=str, default=None)
    # NOTE: IF WE CHANGE THESE VALUES PLEASE UPDATE ./launch.py

    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    output_path = Path(args.output_path)
    # new version of safe_makedirs will take a Path but for now all Paths are converted to string for this function
    safe_makedirs(str(output_path))

    log = Logger('Setup')
    log_path = output_path / 'report.log'
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-riverscapes-inventory')
    log.info(f"Output path: {output_path}")
    log.info(f"AOI shape: {args.path_to_shape}")
    log.info(f"Report name: {args.report_name}")
    log.info(f"Report Version: {report_version}")
    if args.csv:
        csvpath = Path(args.csv)
        log.info(f"Using existing CSV: {csvpath}")
    else:
        log.info("No existing CSV provided, will query Athena")
        csvpath = None

    try:
        make_report_orchestrator(args.report_name,
                                 output_path,
                                 args.path_to_shape,
                                 csvpath,
                                 args.include_pdf
                                 )

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
