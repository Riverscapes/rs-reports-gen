"""Main entry point"""

# Standard library imports
import os
from pathlib import Path
import argparse
import logging
import sys
import traceback
import shutil
# Third party imports
import pandas as pd
import geopandas as gpd
from jinja2 import Template  # or maybe use util.html RSReport instead
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs
# Local imports
from util import prepare_gdf_for_athena
from util.athena import aoi_query_to_local_parquet
from util.rme.field_metadata import get_field_metadata
from .athenacsv_to_rme import create_gpkg_igos_from_parquet, create_igos_project, list_of_source_projects
from .__version__ import __version__


def field_metadata_to_file(output_path: str):
    """Export field metadata from Athena to a CSV file."""
    df = get_field_metadata("WHERE table_name='raw_rme'")
    # users won't understand all columsn
    df = df[['name', 'theme_name', 'friendly_name', 'data_unit', 'description']].copy()
    df = df.rename(columns={'name': 'column_name'})
    df.to_csv(output_path, index=False)


def project_list_to_file(local_csv_path, output_path: str):
    """export the list of projects to a file"""
    sp_list = list_of_source_projects(local_csv_path)
    df = pd.DataFrame(sp_list)
    df.to_csv(output_path, index=False)


def generate_report(project_dir: Path, local_csv_path: str | None):
    """Make a readme file plus metadata artifacts; project list is optional if already written."""
    # column_meta
    field_metadata_to_file(os.path.join(project_dir, 'column_metadata.csv'))
    # list of projects
    if local_csv_path:
        project_list_to_file(local_csv_path, os.path.join(project_dir, 'source_projects.csv'))
    # build readme
    src_dir = os.path.dirname(__file__)
    template_path = os.path.join(src_dir, 'templates', 'template_readme.md')
    with open(template_path, encoding='utf-8') as f:
        template = Template(f.read())
    context = {
        "report_version": __version__
    }
    readme_contents = template.render(context)
    with open(os.path.join(project_dir, 'README.md'), 'w', encoding='utf-8') as f:
        f.write(readme_contents)


def get_and_process_aoi(
    path_to_shape: Path,
    spatialite_path,
    project_dir: Path,
    project_name: str,
    log_path: Path,
    parquet_override: Path | None = None,
    keep_parquet: bool = False,
):
    """ Get and process AOI orchestrator

    Args:
        path_to_shape (Path): Path to the AOI shapefile.
        spatialite_path (str): Path to the mod_spatialite library.
        project_dir (Path): Directory for the project.
        project_name (str): Name of the project. 
        log_path (str): Path to the log file.
        parquet_override (Path or None): for running multiple times in developement/test can supply path to previously downloaded data and skip athena query
        keep_parquet (bool, default False): keep parquet files, e.g. for debugging purposes

    Raises:
        ValueError: If no valid S3 path is returned from Athena query.
    """
    log = Logger('Get and Process AOI orchestrator')
    log.info("Report orchestration begun")

    aoi_gdf = gpd.read_file(path_to_shape)

    if parquet_override:
        parquet_data_source = Path(parquet_override)
        if not parquet_data_source.exists():
            raise FileNotFoundError(f"Parquet path '{parquet_data_source}' does not exist")
        log.info(f"Using supplied Parquet data files at {parquet_data_source}")
    else:
        parquet_data_source = project_dir / "pq"
        # use shape to query Athena
        query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
        if not simplification_results.success:
            raise ValueError("Unable to simplify input geometry sufficiently to insert into Athena query")
        if simplification_results.simplified:
            log.warning(
                f"Input polygon was simplified using tolerance of {simplification_results.tolerance_m} metres for the purpose of intersecting with DGO geometries in the database. If you require a higher precision extract, please contact support@riverscapes.freshdesk.com.")

        log.info(f"Running AOI query and writing Parquet output to {parquet_data_source}")
        fields_we_need = "rme_version, rme_version_int, rme_date_created_ts, level_path, seg_distance, centerline_length, segment_area, fcode, longitude, latitude, ownership, state, county, drainage_area, watershed_id, stream_name, stream_order, headwater, stream_length, waterbody_type, waterbody_extent, ecoregion3, ecoregion4, elevation, geology, huc12, prim_channel_gradient, valleybottom_gradient, rel_flow_length, confluences, diffluences, tributaries, tribs_per_km, planform_sinuosity, lowlying_area, elevated_area, channel_area, floodplain_area, integrated_width, active_channel_ratio, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, confinement_ratio, constriction_ratio, confining_margins, constricting_margins, lf_evt, lf_bps, lf_agriculture_prop, lf_agriculture, lf_conifer_prop, lf_conifer, lf_conifer_hardwood_prop, lf_conifer_hardwood, lf_developed_prop, lf_developed, lf_exotic_herbaceous_prop, lf_exotic_herbaceous, lf_exotic_tree_shrub_prop, lf_exotic_tree_shrub, lf_grassland_prop, lf_grassland, lf_hardwood_prop, lf_hardwood, lf_riparian_prop, lf_riparian, lf_shrubland_prop, lf_shrubland, lf_sparsely_vegetated_prop, lf_sparsely_vegetated, lf_hist_conifer_prop, lf_hist_conifer, lf_hist_conifer_hardwood_prop, lf_hist_conifer_hardwood, lf_hist_grassland_prop, lf_hist_grassland, lf_hist_hardwood_prop, lf_hist_hardwood, lf_hist_hardwood_conifer_prop, lf_hist_hardwood_conifer, lf_hist_peatland_forest_prop, lf_hist_peatland_forest, lf_hist_peatland_nonforest_prop, lf_hist_peatland_nonforest, lf_hist_riparian_prop, lf_hist_riparian, lf_hist_savanna_prop, lf_hist_savanna, lf_hist_shrubland_prop, lf_hist_shrubland, lf_hist_sparsely_vegetated_prop, lf_hist_sparsely_vegetated, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, riparian_veg_departure, ag_conversion, develop, grass_shrub_conversion, conifer_encroachment, invasive_conversion, riparian_condition, qlow, q2, splow, sphigh, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent, brat_capacity, brat_hist_capacity, brat_risk, brat_opportunity, brat_limitation, brat_complex_size, brat_hist_complex_size, dam_setting, rme_project_id"
        query_str = f"SELECT {fields_we_need} FROM raw_rme_pq2 WHERE {{prefilter_condition}} AND {{intersects_condition}}"

        aoi_query_to_local_parquet(
            query_str,
            'ST_GeomFromBinary(dgo_geom)',
            'dgo_geom_bbox',
            query_gdf,
            parquet_data_source
        )

    gpkg_path = create_gpkg_igos_from_parquet(project_dir, spatialite_path, parquet_data_source)
    create_igos_project(project_dir, project_name, gpkg_path, log_path, aoi_gdf)
    generate_report(project_dir, None)

    if not keep_parquet:
        try:
            if parquet_data_source.exists():
                shutil.rmtree(parquet_data_source)
                log.info(f"Deleted Parquet staging folder {parquet_data_source}")
        except Exception as cleanup_err:
            log.warning(f"Failed to delete Parquet folder {parquet_data_source}: {cleanup_err}")

    log.info(f'IGO project created successfully in {project_dir}.')


def main():
    """
    main rpt-igo-project routine
    get an AOI geometry and query athena raw_rme for data within
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=Path)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=Path)
    parser.add_argument('project_name', help='name for the new project')
    parser.add_argument(
        '--use-parquet',
        dest='parquet_path',
        type=Path,
        default=None,
        help='Use an existing Parquet file or directory instead of running the Athena AOI query'
    )
    parser.add_argument(
        '--keep-parquet',
        action='store_true',
        help='Keep the downloaded AOI Parquet files instead of deleting the pq folder'
    )
    # NOTE: IF WE CHANGE THESE VALUES PLEASE UPDATE ./launch.py

    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    output_path = Path(args.output_path)
    safe_makedirs(str(output_path))

    log = Logger('Setup')
    log_path = output_path / 'report.log'
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rpt-igo-project')
    log.info(f"Version: {__version__}")

    try:
        get_and_process_aoi(
            args.path_to_shape,
            args.spatialite_path,
            output_path,
            args.project_name,
            log_path,
            parquet_override=args.parquet_path,
            keep_parquet=args.keep_parquet,
        )
        # print(path_to_results)
        print("done")
        sys.exit(0)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == '__main__':
    main()
