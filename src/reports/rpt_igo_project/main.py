"""Main entry point"""

# Standard library imports
import os
import argparse
import logging
import sys
import traceback
import tempfile
import shutil
# Third party imports
import geopandas as gpd
from jinja2 import Template  # or maybe use util.html RSReport instead
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs
# Local imports
from util.athena.athena import get_s3_file, S3_ATHENA_BUCKET
from util.athena import run_aoi_athena_query
# from util.html import RSReport
from .athenacsv_to_rme import create_gpkg_igos_from_csv, create_igos_project, list_of_source_projects
from .__version__ import __version__


def generate_report(project_dir: str, local_csv_path: str):
    """Make a readme file"""
    src_dir = os.path.dirname(__file__)
    template_path = os.path.join(src_dir, 'templates', 'template_readme.md')
    with open(template_path, encoding='utf-8') as f:
        template = Template(f.read())
    context = {
        "appendices": {
            "project_ids": list_of_source_projects(local_csv_path)
        },
        "report_version": __version__
    }
    readme_contents = template.render(context)
    with open(os.path.join(project_dir, 'README.md'), 'w', encoding='utf-8') as f:
        f.write(readme_contents)
    # shutil.copyfile(template_path, os.path.join(project_dir, 'README.md'))


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
    fields_we_need = "rme_version, rme_version_int, rme_date_created_ts, level_path, seg_distance, centerline_length, segment_area, fcode, longitude, latitude, ownership, state, county, drainage_area, watershed_id, stream_name, stream_order, headwater, stream_length, waterbody_type, waterbody_extent, ecoregion3, ecoregion4, elevation, geology, huc12, prim_channel_gradient, valleybottom_gradient, rel_flow_length, confluences, diffluences, tributaries, tribs_per_km, planform_sinuosity, lowlying_area, elevated_area, channel_area, floodplain_area, integrated_width, active_channel_ratio, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, confinement_ratio, constriction_ratio, confining_margins, constricting_margins, lf_evt, lf_bps, lf_agriculture_prop, lf_agriculture, lf_conifer_prop, lf_conifer, lf_conifer_hardwood_prop, lf_conifer_hardwood, lf_developed_prop, lf_developed, lf_exotic_herbaceous_prop, lf_exotic_herbaceous, lf_exotic_tree_shrub_prop, lf_exotic_tree_shrub, lf_grassland_prop, lf_grassland, lf_hardwood_prop, lf_hardwood, lf_riparian_prop, lf_riparian, lf_shrubland_prop, lf_shrubland, lf_sparsely_vegetated_prop, lf_sparsely_vegetated, lf_hist_conifer_prop, lf_hist_conifer, lf_hist_conifer_hardwood_prop, lf_hist_conifer_hardwood, lf_hist_grassland_prop, lf_hist_grassland, lf_hist_hardwood_prop, lf_hist_hardwood, lf_hist_hardwood_conifer_prop, lf_hist_hardwood_conifer, lf_hist_peatland_forest_prop, lf_hist_peatland_forest, lf_hist_peatland_nonforest_prop, lf_hist_peatland_nonforest, lf_hist_riparian_prop, lf_hist_riparian, lf_hist_savanna_prop, lf_hist_savanna, lf_hist_shrubland_prop, lf_hist_shrubland, lf_hist_sparsely_vegetated_prop, lf_hist_sparsely_vegetated, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, riparian_veg_departure, ag_conversion, develop, grass_shrub_conversion, conifer_encroachment, invasive_conversion, riparian_condition, qlow, q2, splow, sphigh, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent, brat_capacity, brat_hist_capacity, brat_risk, brat_opportunity, brat_limitation, brat_complex_size, brat_hist_complex_size, dam_setting, rme_project_id"
    path_to_results = run_aoi_athena_query(aoi_gdf, s3_bucket,
                                           fields_str=fields_we_need,
                                           source_table='raw_rme_pq2',
                                           bbox_field='dgo_geom_bbox'
                                           )
    if not isinstance(path_to_results, str):
        log.error('Did not get result from run_aoi_athena_query that we were expecting')
        raise ValueError("No valid S3 path returned from Athena query; cannot download file.")
    log.info('Athena query to extract data for AOI completed successfully.')
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile:
        local_csv_path = tmpfile.name
    get_s3_file(path_to_results, local_csv_path)
    log.info('Downloaded results csv from s3 successfully.')
    gpkg_path = create_gpkg_igos_from_csv(project_dir, spatialite_path, local_csv_path)
    create_igos_project(project_dir, project_name, spatialite_path, gpkg_path, log_path, aoi_gdf)
    generate_report(project_dir, local_csv_path)

    log.info(f'IGO project created successfully in {project_dir}.')


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
    # NOTE: IF WE CHANGE THESE VALUES PLEASE UPDATE ./launch.py

    args = dotenv.parse_args_env(parser)

    s3_bucket = S3_ATHENA_BUCKET

    # Set up some reasonable folders to store things
    output_path = args.output_path
    safe_makedirs(output_path)

    log = Logger('Setup')
    log_path = os.path.join(output_path, 'report.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rpt-igo-project')
    log.info(f"Version: {__version__}")

    try:
        get_and_process_aoi(args.path_to_shape, s3_bucket, args.spatialite_path, output_path, args.project_name, log_path)
        # print(path_to_results)
        print("done")
        sys.exit(0)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == '__main__':
    main()
