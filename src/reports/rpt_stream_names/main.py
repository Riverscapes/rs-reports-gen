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


def define_fields(unit_system: str = "SI"):
    """Set up the fields and units for this report"""
    _FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()
    _FIELD_META.field_meta = get_field_metadata()  # Set the field metadata for the report
    _FIELD_META.unit_system = unit_system  # Set the unit system for the report

    # Here's where we can set any preferred units that differ from the data unit
    _FIELD_META.set_display_unit('centerline_length', 'kilometer')
    _FIELD_META.set_display_unit('segment_area', 'kilometer ** 2')

    return


def make_report(gdf: gpd.GeoDataFrame, aoi_df: gpd.GeoDataFrame,
                report_dir, report_name,
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
    
    word_cloud(gdf, os.path.join(report_dir, 'figures'))

    tables = {
        "river_names": table_total_x_by_y(gdf, 'centerline_length', ['stream_name']),
    }
    # appendices = {
    #     "project_ids": project_id_list(gdf),
    # }
    figure_dir = os.path.join(report_dir, 'figures')
    safe_makedirs(figure_dir)

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

    report.add_html_elements('tables', tables)

    # report.add_html_elements('appendices', appendices)

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


def make_report_orchestrator(report_name: str, report_dir: str, path_to_shape: str,
                             existing_csv_path: str | None = None, include_pdf: bool = True, unit_system: str = "SI"):
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
    define_fields(unit_system)  # ensure fields are defined

    # load shape as gdf
    aoi_gdf = gpd.read_file(path_to_shape)
    # get data first as csv
    safe_makedirs(os.path.join(report_dir, 'data'))
    csv_data_path = os.path.join(report_dir, 'data', 'data.csv')

    if existing_csv_path:
        log.info(f"Using supplied csv file at {csv_data_path}")
        if existing_csv_path != csv_data_path:
            shutil.copyfile(existing_csv_path, csv_data_path)
    else:
        log.info("Querying athena for data for AOI")
        get_wcdata_for_aoi(aoi_gdf, csv_data_path)

    data_gdf = load_gdf_from_csv(csv_data_path)
    data_gdf, _ = RSFieldMeta().apply_units(data_gdf)  # this is still a geodataframe but we will need to be more explicit about it for type checking

    # Export the data to Excel
    RSGeoDataFrame(data_gdf).export_excel(os.path.join(report_dir, 'data', 'data.xlsx'))

    # make html report
    # If we aren't including pdf we just make interactive report. No need for the static one
    make_report(data_gdf, aoi_gdf, report_dir, report_name,
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
    output_path = args.output_path
    # if want each iteration to be saved add datetimestamp to path
    # dt_str = datetime.now().strftime("%y%m%d_%H%M")
    # dt_str = ""
    safe_makedirs(output_path)

    log = Logger('Setup')
    log_path = os.path.join(output_path, 'report.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-riverscapes-inventory')
    log.info(f"Output path: {output_path}")
    log.info(f"AOI shape: {args.path_to_shape}")
    log.info(f"Report name: {args.report_name}")
    log.info(f"Report Version: {report_version}")
    if args.csv:
        log.info(f"Using existing CSV: {args.csv}")
    else:
        log.info("No existing CSV provided, will query Athena")

    try:
        make_report_orchestrator(args.report_name,
                                 output_path,
                                 args.path_to_shape,
                                 args.csv,
                                 args.include_pdf,
                                 args.unit_system)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
