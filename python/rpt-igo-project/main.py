"""Main entry point"""

import os
import argparse
import logging
import sys
import traceback
import tempfile
import geopandas as gpd
from rsxml import dotenv, Logger
from rsxml.util import safe_makedirs
from util.athena import get_s3_file
from athena_query_aoi import run_aoi_athena_query
from athenacsv_to_rme import create_gpkg_igos_from_csv, create_igos_project

def get_and_process_aoi(path_to_shape, s3_bucket, spatialite_path, project_dir, project_name, log_path):
    log = Logger('Get and Process AOI orchestrator')
    aoi_gdf = gpd.read_file(path_to_shape)
    path_to_results = run_aoi_athena_query(aoi_gdf, s3_bucket)
    if not isinstance(path_to_results,str) :
        log.error('Did not get result from run_aoi_athena_query that we were expecting')
        raise ValueError ("No valid S3 path returned from Athena query; cannot download file.")
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile: # change to True after dubgging
        local_csv_path = tmpfile.name
        get_s3_file(path_to_results, local_csv_path)
        gpkg_path = create_gpkg_igos_from_csv (project_dir, spatialite_path, local_csv_path)
        create_igos_project(project_dir, project_name, spatialite_path, gpkg_path, log_path, aoi_gdf)

def main():
    """
    main rpt-igo-project routine
    get an AOI geometry and query athena raw_rme for data within
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=str)
    parser.add_argument('project_name', help='name for the new project')
    
    args = dotenv.parse_args_env(parser)
    
    s3_bucket = "riverscapes-athena"

    # Set up some reasonable folders to store things
    working_folder = args.working_folder
    project_dir = os.path.join(working_folder, 'project')  # , 'outputs', 'riverscapes_metrics.gpkg')
    safe_makedirs(project_dir)

    log = Logger('Setup')
    log_path = os.path.join(project_dir, 'athena-rme-scrape.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rpt-igo-project')

    try: 
        get_and_process_aoi(args.path_to_shape, s3_bucket, args.spatialite_path, project_dir, args.project_name, log_path)
        # print(path_to_results)
        print ("done")
        sys.exit(0)
    
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

if __name__ == '__main__':
    main()