"""Main entry point"""

# Standard library imports
import os
import argparse
import logging
import sys
import traceback
import tempfile
import geopandas as gpd
# Third party imports
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs
from util.athena.athena import get_s3_file
# Local imports
from .athena_query_aoi import run_aoi_athena_query
from .athenacsv_to_rme import create_gpkg_igos_from_csv, create_igos_project


def get_and_process_aoi(path_to_shape, s3_bucket, spatialite_path, project_dir, project_name, log_path):
    """ Get and process AOI orchestrator

    Args:
        path_to_shape (str): Path to the AOI shapefile.
        s3_bucket (str): Name of the S3 bucket.
        spatialite_path (str): Path to the mod_spatialite library.
        project_dir (str): Directory for the project.
        project_name (str): Name of the project. 
        log_path (str): Path to the log file.

    Raises:
        ValueError: If no valid S3 path is returned from Athena query.
    """
    log = Logger('Get and Process AOI orchestrator')
    aoi_gdf = gpd.read_file(path_to_shape)
    path_to_results = run_aoi_athena_query(aoi_gdf, s3_bucket)
    if not isinstance(path_to_results, str):
        log.error('Did not get result from run_aoi_athena_query that we were expecting')
        raise ValueError("No valid S3 path returned from Athena query; cannot download file.")
    log.info('Athena query to extract data for AOI completed successfully.')
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile:  # could change to True for production?
        local_csv_path = tmpfile.name
        get_s3_file(path_to_results, local_csv_path)
        log.info('Downloaded results csv from s3 successfully.')
        gpkg_path = create_gpkg_igos_from_csv(project_dir, spatialite_path, local_csv_path)
        create_igos_project(project_dir, project_name, spatialite_path, gpkg_path, log_path, aoi_gdf)


def main():
    """
    main rpt-igo-project routine
    get an AOI geometry and query athena raw_rme for data within
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=str)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=str)
    parser.add_argument('project_name', help='name for the new project')

    args = dotenv.parse_args_env(parser)

    s3_bucket = "riverscapes-athena"

    # Set up some reasonable folders to store things
    output_path = args.output_path
    safe_makedirs(output_path)

    log = Logger('Setup')
    log_path = os.path.join(output_path, 'report.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rpt-igo-project')

    try:
        get_and_process_aoi(args.path_to_shape, s3_bucket, args.spatialite_path, output_path, args.project_name, log_path)
        # print(path_to_results)
        print("done")
        sys.exit(0)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


def env_launch_params():
    """Default parameters for launching from the report launcher (Development env only).

    """
    return [
        "spatialite_path",
        "{env:DATA_ROOT}/rpt-igo-project",
        "../src/reports/rpt_igo_project/example/althouse_smaller_selection.geojson",
        "Althouse Creek 2",
        # "--csv",
        # "/tmp/tmphcfn8l6q.csv",
    ]


if __name__ == '__main__':
    main()
