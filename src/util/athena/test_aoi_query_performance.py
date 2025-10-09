"""this is to compare querying using parquet vs csv versions of raw_rme
needs to be fixed since have changed the athena.py considerably
"""

import time
import geopandas as gpd
import logging
from rsxml import Logger
from util.athena import run_aoi_athena_query, run_athena_aoi_query
import pandas as pd
from util.athena.athena import get_s3_file
import os

S3_BUCKET = "riverscapes-athena"
RAW_RME_TABLE = "raw_rme"
RAW_RME_PQ_TABLE = "raw_rme_pq"

# Example fields string (adjust as needed)
FIELDS_STR = "level_path, seg_distance, centerline_length, segment_area, fcode, fcode*2 as squarecode, longitude, latitude, ownership, state, county, drainage_area, stream_name, stream_order, stream_length, huc12, rel_flow_length, channel_area, integrated_width, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, lf_agriculture_prop, lf_agriculture, lf_developed_prop, lf_developed, lf_riparian_prop, lf_riparian, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, develop, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent"


def test_aoi_query_performance(path_to_geojson):
    log = Logger('AOI Query Test')
    aoi_gdf = gpd.read_file(path_to_geojson)

    # Test raw_rme (TSV, no bbox)
    log.info("Testing raw_rme (TSV, no bbox)...")
    start1 = time.time()
    s3_csv_path1 = run_aoi_athena_query(aoi_gdf, S3_BUCKET, fields_str=FIELDS_STR, source_table=RAW_RME_TABLE)
    end1 = time.time()
    log.info(f"raw_rme query time: {end1 - start1:.2f} seconds")
    if s3_csv_path1:
        # Get number of rows from Athena (using athena_query_get_parsed)
        # This assumes the result is a CSV file on S3
        # You may want to download and count rows, but here we just log the S3 path
        log.info(f"raw_rme S3 result: {s3_csv_path1}")
    else:
        log.error("raw_rme query failed.")

    # Test raw_rme_pq (GeoParquet, has bbox)
    log.info("Testing raw_rme_pq (GeoParquet, has bbox)...")
    start2 = time.time()
    s3_csv_path2 = run_athena_aoi_query(
        aoi_gdf,
        S3_BUCKET,
        select_fields=FIELDS_STR,
        select_from=RAW_RME_PQ_TABLE,
        geometry_field_nm="dgo_geom",
        geometry_bbox_field_nm="dgo_geom_bbox"
    )
    end2 = time.time()
    log.info(f"raw_rme_pq query time: {end2 - start2:.2f} seconds")
    if s3_csv_path2:
        log.info(f"raw_rme_pq S3 result: {s3_csv_path2}")
    else:
        log.error("raw_rme_pq query failed.")

    # Optionally, download both CSVs and compare row counts
    def get_row_count(s3_path, label):
        if not s3_path:
            return None
        local_path = os.path.join(os.path.dirname(__file__), f"_tmp_{label}.csv")
        try:
            get_s3_file(s3_path, local_path)
            df = pd.read_csv(local_path)
            row_count = len(df)
            # Do not remove the file
            return row_count
        except Exception as e:
            log.error(f"Failed to get row count for {s3_path}: {e}")
            return None

    count1 = get_row_count(s3_csv_path1, "raw_rme")
    count2 = get_row_count(s3_csv_path2, "raw_rme_pq")
    log.info(f"raw_rme row count: {count1}")
    log.info(f"raw_rme_pq row count: {count2}")
    if count1 is not None and count2 is not None:
        log.info(f"Row counts match: {count1 == count2}")
    else:
        log.info("Row counts not available (implement S3 download to compare)")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python test_aoi_query_performance.py <path_to_geojson>")
        sys.exit(1)
    log_path = os.path.join(r'c:\nardata\localcode\rs-reports-gen', 'test_aoi_query_performance.log')
    log = Logger('Setup')
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('test_aoi_query_performance')
    test_aoi_query_performance(sys.argv[1])
