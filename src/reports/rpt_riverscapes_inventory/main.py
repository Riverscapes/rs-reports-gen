# System imports
import argparse
import logging
import os
import sys
import shutil
from datetime import datetime
from importlib import resources
import traceback
# 3rd party imports
import geopandas as gpd
import pandas as pd
import pint
from shapely import wkt

from jinja2 import Template

from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from util.athena import get_s3_file, run_aoi_athena_query
from util.athena.athena import athena_query_get_parsed

from util.pdf.create_pdf import make_pdf_from_html
from util.plotly.export_figure import export_figure


def make_report_orchestrator(report_name: str, report_dir: str, path_to_shape: str,
                             existing_csv_path: str | None = None, include_pdf: bool = True):
    """ Orchestrates the report generation process:

    Args:
        report_name (str): The name of the report.
        report_dir (str): The directory where the report will be saved.
        path_to_shape (str): The path to the shapefile for the area of interest.
    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")


def main():
    """ Main function to parse arguments and generate the report
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=str)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=str)
    parser.add_argument('report_name', help='name for the report (usually description of the area selected)')
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

    try:
        make_report_orchestrator(args.report_name, output_path, args.path_to_shape, args.csv)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
