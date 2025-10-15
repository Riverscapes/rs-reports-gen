""" Utility functions to get data from AWS Athena and return it in useful formats
a version/copy of these functions can be found in multiple Riverscapes repositories
* data-exchange-scripts
* rs-reports-gen (this one)
* athena 
* cybercastor_scripts

Consider porting any improvements to these other repositories. 

---

## Summary of 'public' functions

| Function Name                  | Query Type | Output Format | Use Case                        |
|------------------------------- |------------|--------------|---------------------------------|
| athena_select_to_dict          | SELECT     | list[dict]    | Small/simple queries            |
| athena_select_to_dataframe     | SELECT     | DataFrame     | Small/simple queries            |
| athena_unload_to_dict          | UNLOAD     | list[dict]    | Large/complex/nested data       |
| athena_unload_to_dataframe     | UNLOAD     | DataFrame     | Large/complex/nested data       |

"""
import time
import re
import gzip
import io
import json
import uuid
import tempfile
import csv
import math
from urllib.parse import urlparse
import boto3
import pandas as pd
from rsxml import Logger
import geopandas as gpd
from util import simplify_gdf

# buffer, in decimal degrees, on centroids to capture DGO.
# value of 0.47 is based on an analysis of distance between centroid and corners of bounding boxes of raw_rme 2025-09-08
BUFFER_CENTROID_TO_BB_DD = 0.47  # DEPRECATED - USE BOUNDING BOX STRUCT instead
S3_ATHENA_BUCKET = "riverscapes-athena-output"
ATHENA_WORKGROUP = "primary"
AWS_REGION = "us-west-2"


def _run_athena_query(
    s3_output_path: str,
    query: str,
    max_wait: int = 600
) -> tuple[str, str] | None:
    """
    Run an Athena query and wait for completion.

    Args:
        s3_bucket (str): S3 bucket AND prefix for Athena output e.g. s3://my-bucket/my-prefix . For UNLOAD-type queries has to be empty folder. 
        query (str): SQL query string.
        max_wait (int): Maximum wait time in seconds.

    Returns:
        tuple[str, str] | None: (output_path, query_execution_id) on success, or None on failure.

    This is core function called by `athena_query_get_path` and `athena_query_get_rows` and
    """
    log = Logger("Athena query")

    # Debugging output
    query_length = len(query.encode('utf-8'))
    if query_length < 2000:
        log.debug(f'Query:\n{query}')
    else:
        log.debug(f'Query is {query_length} bytes')
        log.debug(f"Query starts with: {query[:1900]}")
        log.debug(f"Query ends with: {repr(query[-100:])}")

    athena = boto3.client('athena', region_name=AWS_REGION)

    # s3_output should be a full s3://bucket/prefix or s3://bucket/file.csv
    # and this has to be empty for unload queries
    # will fail if not

    response = athena.start_query_execution(
        QueryString=query,
        WorkGroup=ATHENA_WORKGROUP,
        QueryExecutionContext={
            'Database': 'default',
            'Catalog': 'AwsDataCatalog'
        },
        ResultConfiguration={
            'OutputLocation': s3_output_path
        }
    )
    query_execution_id = response['QueryExecutionId']
    start_time = time.time()
    log.debug(f"Query started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

    # Poll for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        if time.time() - start_time > max_wait:
            log.error(f"Timed out waiting for Athena query to complete. Waited {max_wait} seconds.")
            log.error(f"Check the Athena console for QueryExecutionId {query_execution_id} for more details.")
            log.error(f"Query string (truncated): {query[:500]}{'...' if len(query) > 500 else ''}")
            output_path = status['QueryExecution']['ResultConfiguration'].get('OutputLocation', '')
            log.error(f"S3 OutputLocation (may be empty): {output_path}")
            return None
        time.sleep(2)

    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
        log.error(f"Athena query failed or was cancelled: {state}. Reason: {reason}")
        return None

    log.debug(f'Query completed at: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}')
    output_path = status['QueryExecution']['ResultConfiguration'].get('OutputLocation', '')
    return output_path, query_execution_id


def fix_s3_uri(argstr: str) -> str:
    """the parser is messing up s3 paths. this should fix them
    launch.json value (a valid s3 string): "s3://riverscapes-athena/athena_query_results/d40eac38-0d04-4249-8d55-ad34901fee82.csv" 
    agrstr (input to this function) 's3:\\\\riverscapes-athena\\\\athena_query_results\\\\d40eac38-0d04-4249-8d55-ad34901fee82.csv'
    Returns: a valid s3 string 
    """
    # Replace all backslashes with slashes
    uri = argstr.replace("\\", "/")
    # Ensure exactly two slashes after 's3:'
    uri = re.sub(r'^s3:/+', 's3://', uri)
    return uri


def get_s3_file(s3path: str, localpath: str):
    """Download a file from S3 to local path, fixing S3 URI if needed.
    """
    s3_uri = fix_s3_uri(s3path)
    download_file_from_s3(s3_uri, localpath)


def download_file_from_s3(s3_uri: str, local_path: str) -> None:
    """
    Download a file from S3 to a local path.

    Args:
        s3_uri (str): S3 URI of the file, e.g. 's3://bucket/key'.
        local_path (str): Local filesystem path to save the downloaded file.

    Raises:
        ValueError: If s3_uri is not a valid S3 URI.

    Example:
        download_file_from_s3('s3://riverscapes-athena/adhoc/yct_sample4.csv', '/tmp/yct_sample4.csv')
    """
    log = Logger('Download File')
    # Validate and parse S3 URI
    if not isinstance(s3_uri, str) or not s3_uri.startswith('s3://'):
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with 's3://'")
    parts = s3_uri[5:].split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must be in format 's3://bucket/key'")
    s3_bucket, s3_key = parts

    log.info(f"Downloading {s3_key} from bucket {s3_bucket} to {local_path}")
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=s3_bucket, Key=s3_key)
    size_bytes = response['ContentLength']
    log.info(f"ContentType: {response['ContentType']}\t File size: {size_bytes} bytes")
    # TODO if very large, add progress bar
    s3.download_file(s3_bucket, s3_key, local_path)
    log.info("Download complete.")


def parse_athena_results(rows):
    """Convert Athena result rows to a list of dicts.

    Args: 
        rows (list): Raw Athena result rows.

    Returns:
        list[dict]: List of dictionaries representing query results.
    """
    if not rows or len(rows) < 2:
        return []
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    data = []
    for row in rows[1:]:
        values = [col.get('VarCharValue', None) for col in row['Data']]
        data.append(dict(zip(headers, values)))
    return data


def _parse_csv_from_s3(s3_uri: str) -> list[dict]:
    """Download and parse a CSV file from S3 into a list of dicts."""
    log = Logger('Parse CSV from S3')
    with tempfile.NamedTemporaryFile(delete=True, suffix='.csv') as tmp:
        download_file_from_s3(s3_uri, tmp.name)
        with open(tmp.name, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            log.debug(f"Parsed {len(rows)} rows from {s3_uri}")
            return rows


def _parse_json_from_s3_prefix(s3_prefix: str) -> list[dict]:
    """Download and parse all JSON files from an S3 prefix into a list of dicts. 
    Handles both .json and .json.gz files, and paginates for large result sets.
    """

    log = Logger('Parse JSON from S3 prefix')
    s3 = boto3.client('s3')
    parsed = urlparse(s3_prefix)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')

    # Use paginator for scalability

    paginator = s3.get_paginator("list_objects_v2")
    file_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") or key.endswith(".gz"):
                file_keys.append(key)

    rows = []
    for key in file_keys:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        if key.endswith(".gz"):
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
                    for raw_line in gz:
                        line = raw_line.decode("utf-8").strip()
                        if line:
                            rows.append(json.loads(line))
            except gzip.BadGzipFile:
                log.error(f"File {key} is not a valid gzip file")
        else:
            for line in body.decode("utf-8").splitlines():
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    log.info(f"Parsed {len(rows)} rows from {len(file_keys)} files in {s3_prefix}")
    return rows


# === Public API ===
def athena_select_to_dict(query: str, s3_output: str | None = None, max_wait: int = 600) -> list[dict]:
    """Run a SELECT query and return results as a list of dicts."""
    if s3_output is None:
        s3_output = f's3://{S3_ATHENA_BUCKET}/athena_query_results'
    result = _run_athena_query(s3_output, query, max_wait)
    if result is None:
        raise RuntimeError("Did not get a valid result from the query. check logs")
    output_path, _executionid = result
    return _parse_csv_from_s3(output_path)


def athena_select_to_dataframe(query: str, s3_output: str | None = None, max_wait: int = 600) -> pd.DataFrame:
    """Run a SELECT query and return results as a pandas DataFrame."""
    rows = athena_select_to_dict(query, s3_output, max_wait)
    return pd.DataFrame(rows)


def athena_unload_to_dict(query: str, s3_output: str | None = None, max_wait: int = 600) -> list[dict]:
    """Run an UNLOAD query and return results as a list of dicts."""
    if s3_output is None:
        s3_output = f's3://{S3_ATHENA_BUCKET}/athena_unload/{uuid.uuid4()}/'
    # Compose UNLOAD SQL
    cleaned_query = query.strip().rstrip(';')
    unload_sql = f"UNLOAD ({cleaned_query}) TO '{s3_output}' WITH (format='JSON', compression='GZIP')"
    _run_athena_query(s3_output, unload_sql, max_wait)
    # Now we need to list and fetch the results. We CAN use the manifest file but it's easier for now to just
    # to list all the .gz files and read them in order we find them.
    return _parse_json_from_s3_prefix(s3_output)


def athena_unload_to_dataframe(query: str, s3_output: str | None = None, max_wait: int = 600) -> pd.DataFrame:
    """Run an UNLOAD query and return results as a pandas DataFrame."""
    rows = athena_unload_to_dict(query, s3_output, max_wait)
    return pd.DataFrame(rows)


# def athena_query_get_parsed(s3_bucket: str, query: str, max_wait: int = 600) -> list[dict] | None:
#     """
#     Run an Athena query and return parsed results as a list of dictionaries.

#     Args:
#         s3_bucket (str): S3 bucket for Athena output.
#         query (str): SQL query string.
#         max_wait (int): Maximum wait time in seconds.

#     Returns:
#         list[dict] | None: Parsed Athena results, or None on failure.
#     """
#     rows = athena_query_get_rows(s3_bucket, query, max_wait)
#     if rows:
#         return parse_athena_results(rows)
#     return None

# === SPECIALIZED QUERIES FOR SPATIAL INTERSECTION =====

def get_data_for_aoi(s3_bucket: str | None, gdf: gpd.GeoDataFrame, output_path: str):
    """given aoi in gdf format (assume 4326), run SELECTion from raw_rme_pq 
    side effect: populate output_path (local path) with the data csv file
    TO DO -  fix we're not using s3_bucket because it's better to let the downstream function decide where to put it
    """
    log = Logger('Run AOI query on Athena')
    # TODO improve from this temporary approach --
    #  should use report-type specific CTAS (or view if it's just a select without calcs) and report-specific UNLOAD statement
    #  parameterize the fields it's bad practice to load everything
    fields_str = "level_path, seg_distance, centerline_length, segment_area, fcode, fcode_desc, longitude, latitude, ownership, ownership_desc, state, county, drainage_area, stream_name, stream_order, stream_length, huc12, rel_flow_length, channel_area, integrated_width, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, lf_agriculture_prop, lf_agriculture, lf_developed_prop, lf_developed, lf_riparian_prop, lf_riparian, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, develop, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent, confinement_ratio, brat_capacity,brat_hist_capacity, riparian_veg_departure, riparian_condition, rme_project_id, rme_project_name"
    s3_csv_path = run_aoi_athena_query(gdf, None, fields_str=fields_str, source_table="rpt_rme_pq")
    if s3_csv_path is None:
        log.error("Didn't get a result from athena")
        raise NotImplementedError
    get_s3_file(s3_csv_path, output_path)
    return


def get_aoi_geom_sql_expression(gdf: gpd.GeoDataFrame, max_size_bytes=261000) -> str | None:
    """check the geometry, union 
    maximum size of athena query is 256 kb or 262144 bytes
    the default max_size assumes we need just under 5000 bytes for the rest of the query so this part needs to be under
    if it is too big returns None 
    returns a SQL expression for the geometry
    future improvement - check but i think it's almost *always* shorter as WKB so we don't really need to do both every time
    """
    log = Logger('check aoi')
    # Get the union of all geometries in the GeoDataFrame
    aoi_geom = gdf.union_all()

    # Convert to WKT
    aoi_wkt = aoi_geom.wkt
    wkt_length = len(aoi_wkt.encode('utf-8')) + 23  # this is to account for the text ST_GeometryFromText('')

    # WKB representation (as hex string)
    aoi_wkb = aoi_geom.wkb.hex()
    wkb_length = len(aoi_wkb) + 31  # this is to account for the text ST_GeomFromBinary(from_hex(''))

    log.debug(f"WKT length: {wkt_length} bytes")
    log.debug(f"WKB hex length: {wkb_length} bytes")

    if wkt_length > max_size_bytes and wkb_length > max_size_bytes:
        log.error(f"aoi geometry results in too big a query (greater than {max_size_bytes} bytes) - simplify it and try again")
        return

    # return the smaller representation as an SQL expression for Athena
    if wkt_length <= wkb_length:
        log.info("returning WKT as it is smaller ")
        return f"ST_GeometryFromText('{aoi_wkt}')"
    else:
        return f"ST_GeomFromBinary(from_hex('{aoi_wkb}'))"


def generate_sql_where_clause_for_bounds(gdf: gpd.GeoDataFrame) -> str:
    """
    Get the total bounds (minx, miny, maxx, maxy)
    'buffer' by BUFFER_CENTROID_TO_BB_DD and 
    return SQL where clause for latitude and longitude within this expanded box
    we round to 6 decimal places (<10cm) to control size of the SQL string 

    Assumes the geodataframe is in decimal degrees 
    Todo: check the assumption 

    Previously: tried buffering the bounding box with buffer, but 
    that produces rounded corners and smooth edges - no longer a straightforward box
    # from shapely.geometry import box
    # Create a bounding box geometry
    # bbox = box(minx, miny, maxx, maxy)

    # print("Unbuffered bounding box WKT:", bbox.wkt)

    # buffered_bbox = bbox.buffer(0.01)
    # print("Buffered bounding box WKT:", buffered_bbox.wkt)
    """

    # these are fun variable names :-)
    minx, miny, maxx, maxy = gdf.total_bounds
    bufminx = round(float(minx) - BUFFER_CENTROID_TO_BB_DD, 6)
    bufminy = round(float(miny) - BUFFER_CENTROID_TO_BB_DD, 6)
    bufmaxx = round(float(maxx) + BUFFER_CENTROID_TO_BB_DD, 6)
    bufmaxy = round(float(maxy) + BUFFER_CENTROID_TO_BB_DD, 6)

    bounds_where_clause = f'WHERE (latitude between {bufminy} AND {bufmaxy}) AND (longitude between {bufminx} AND {bufmaxx})'
    return bounds_where_clause


def round_down(val, decimals=6):
    factor = 10 ** decimals
    return math.floor(val * factor) / factor


def round_up(val, decimals=6):
    factor = 10 ** decimals
    return math.ceil(val * factor) / factor


def generate_sql_bbox_where_clause_for_bounds(aoi_gdf: gpd.GeoDataFrame, bbox_fld_nm: str) -> str:
    """return SQL clause for to find records where the bounding box of the record overlaps with bounding box of the aoi

    Assumes both are in the same spatial reference system (probably degrees), and rounds to 6 decimal places

    Args:
        aoi_gdf (gpd.GeoDataFrame): geodataframe containing area of interest
        bbox_fld_nm (str): name of the bounding box field

    Returns:
        str: sql WHERE clause (including the word WHERE)
    """

    minx, miny, maxx, maxy = aoi_gdf.total_bounds
    minx = round_down(float(minx), 6)
    miny = round_down(float(miny), 6)
    maxx = round_up(float(maxx), 6)
    maxy = round_up(float(maxy), 6)
    bounds_where_clause = f'WHERE {bbox_fld_nm}.xmax >= {minx} AND {bbox_fld_nm}.xmin <= {maxx} AND {bbox_fld_nm}.ymax >= {miny} AND {bbox_fld_nm}.ymin <= {maxy}'
    return bounds_where_clause


def run_aoi_athena_query(aoi_gdf: gpd.GeoDataFrame, s3_bucket: str | None = None, fields_str: str = "", source_table: str = "raw_rme_pq", geometry_field_clause: str | None = 'ST_GeomFromBinary(dgo_geom)', bbox_field: str | None = None) -> str | None:
    """Run Athena query `select (field_str) from (source_table)` on supplied AOI geojson
    also includes the dgo geometry (polygon) 
    the source table must have fields: 
     * latitude, longitude 
     * dgo_geom (WKB)
    return path to results on S3
    returns None if the shape can't be converted to suitably sized geometry sql expression. 
    Future Enhancements: 
    - if the source has bounds struct field (comes default with qgis geoparquet exports) we can use that instead of the BUFFER 
    - use the same multiple-resizing strategy from simplify_to_size in `rs_geo_helpers.py`
    """
    log = Logger('Run AOI Query on Athena')

    if s3_bucket is None:
        s3_output_path = f's3://{S3_ATHENA_BUCKET}/aoi_query_results/{uuid.uuid4()}/'
    else:
        s3_output_path = f's3://{s3_bucket}/aoi_query_results/{uuid.uuid4()}/'

    if bbox_field:
        prefilter_where_clause = generate_sql_bbox_where_clause_for_bounds(aoi_gdf, bbox_field)
    else:
        # we are going to filter the centroids, but clip to polygon
        # So we need to buffer by the maximum distance between a centroid and its bounding polygon
        prefilter_where_clause = generate_sql_where_clause_for_bounds(aoi_gdf)

    # count the prefiltered records - these 3 lines are only needed for debugging
    # TODO: IN production comment this out for better performance
    query_str = f'SELECT count(*) AS record_count FROM {source_table} {prefilter_where_clause}'
    results = athena_select_to_dict(query_str, s3_output_path)
    log.debug(f'Prefiltered records: {results}')

    # Try with original geometry
    aoi_geom_str = get_aoi_geom_sql_expression(aoi_gdf)
    simplified = False

    # If too large, try with simplified geometry
    if not aoi_geom_str:
        log.info("AOI geometry too large, simplifying and retrying...")
        aoi_gdf = simplify_gdf(aoi_gdf, tolerance_meters=11)  # Adjust tolerance as needed
        aoi_geom_str = get_aoi_geom_sql_expression(aoi_gdf)
        simplified = True

    if not aoi_geom_str:
        log.error('Could not create suitable geometry from supplied value, even after simplification.')
        return

    log.info(f'Built AOI geometry string for query. Length {len(aoi_geom_str):,} bytes ({"simplified" if simplified else "did not need to simplify"})')

    # For a query to pull just the lat/lon of DGOs that intersect, use this instead
    # fields_str = "longitude, latitude"
    # query to get all the details _except_ the dgo geometry
    if fields_str == "":
        fields_str = "rme_version, rme_version_int, rme_date_created_ts, level_path, seg_distance, centerline_length, segment_area, fcode, longitude, latitude, ownership, state, county, drainage_area, watershed_id, stream_name, stream_order, headwater, stream_length, waterbody_type, waterbody_extent, ecoregion3, ecoregion4, elevation, geology, huc12, prim_channel_gradient, valleybottom_gradient, rel_flow_length, confluences, diffluences, tributaries, tribs_per_km, planform_sinuosity, lowlying_area, elevated_area, channel_area, floodplain_area, integrated_width, active_channel_ratio, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, confinement_ratio, constriction_ratio, confining_margins, constricting_margins, lf_evt, lf_bps, lf_agriculture_prop, lf_agriculture, lf_conifer_prop, lf_conifer, lf_conifer_hardwood_prop, lf_conifer_hardwood, lf_developed_prop, lf_developed, lf_exotic_herbaceous_prop, lf_exotic_herbaceous, lf_exotic_tree_shrub_prop, lf_exotic_tree_shrub, lf_grassland_prop, lf_grassland, lf_hardwood_prop, lf_hardwood, lf_riparian_prop, lf_riparian, lf_shrubland_prop, lf_shrubland, lf_sparsely_vegetated_prop, lf_sparsely_vegetated, lf_hist_conifer_prop, lf_hist_conifer, lf_hist_conifer_hardwood_prop, lf_hist_conifer_hardwood, lf_hist_grassland_prop, lf_hist_grassland, lf_hist_hardwood_prop, lf_hist_hardwood, lf_hist_hardwood_conifer_prop, lf_hist_hardwood_conifer, lf_hist_peatland_forest_prop, lf_hist_peatland_forest, lf_hist_peatland_nonforest_prop, lf_hist_peatland_nonforest, lf_hist_riparian_prop, lf_hist_riparian, lf_hist_savanna_prop, lf_hist_savanna, lf_hist_shrubland_prop, lf_hist_shrubland, lf_hist_sparsely_vegetated_prop, lf_hist_sparsely_vegetated, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, riparian_veg_departure, ag_conversion, develop, grass_shrub_conversion, conifer_encroachment, invasive_conversion, riparian_condition, qlow, q2, splow, sphigh, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent, brat_capacity, brat_hist_capacity, brat_risk, brat_opportunity, brat_limitation, brat_complex_size, brat_hist_complex_size, dam_setting"
    query_str = f"""
WITH pre_filtered_rme AS (
    SELECT
        {fields_str}
        , {geometry_field_clause} AS dgo_geom_obj
    FROM
        {source_table}
    {prefilter_where_clause}
    )
SELECT
    * 
FROM 
    pre_filtered_rme AS t1
WHERE 
    ST_Intersects(
        t1.dgo_geom_obj,
        {aoi_geom_str}
    );
"""

    result = _run_athena_query(s3_output_path, query_str)
    if not result:
        raise RuntimeError("Did not get a valid result from query")
    result_path, _ = result
    return result_path

# ==== TESTING FUNCTIONS ============


def test_unload_query():
    query_str = """
        SELECT * FROM rs_context_huc10 LIMIT 300
        """
    result = athena_unload_to_dict(query_str)
    print(json.dumps(result, indent=2))


def test_run_aoi_athena_query():
    """get an AOI geometry and query athena raw_rme for data within
    not intended to be called except for isolated testing these functions
    """
    # path_to_shape = r"C:\nardata\work\rme_extraction\20250827-rkymtn\physio_rky_mtn_system.geojson"
    path_to_shape = r"C:\nardata\work\rme_extraction\Price-riv\pricehuc10s.geojson"
    s3_bucket = "riverscapes-athena"
    aoi_gdf = gpd.read_file(path_to_shape)
    path_to_results = run_aoi_athena_query(aoi_gdf, s3_bucket)
    print(path_to_results)


if __name__ == '__main__':
    test_unload_query()

# =================
# CRUFT TO REMOVE WHEN TESTED
# =================

# def run_athena_aoi_query(aoi_gdf: gpd.GeoDataFrame, s3_bucket: str, select_fields: str, select_from: str,
#                          geometry_field_nm: str = "geometry", geometry_bbox_field_nm: str = "geometry_bbox"):
#     """
#     Executes an Athena query to select features from select_from table that intersect the AOI geometry,
#     using geometry_bbox for efficient prefiltering.

#     Args:
#         aoi_gdf: AOI as GeoDataFrame
#         s3_bucket: S3 bucket for Athena results
#         select_fields: Comma-separated fields to select
#         select_from: Table name
#         geometry_field_nm: Name of geometry field (default 'geometry')
#         geometry_bbox_field_nm: Name of bbox field (default 'geometry_bbox')
#     Returns:
#         Athena results (parsed or S3 path)
#     """
#     log = Logger('run_athena_aoi_query')
#     # Get AOI bounding box
#     minx, miny, maxx, maxy = aoi_gdf.total_bounds

#     bbox_where = (
#         f"WHERE "
#         f"{geometry_bbox_field_nm}.xmax >= {minx} AND "
#         f"{geometry_bbox_field_nm}.xmin <= {maxx} AND "
#         f"{geometry_bbox_field_nm}.ymax >= {miny} AND "
#         f"{geometry_bbox_field_nm}.ymin <= {maxy}"
#     )

#     # Get AOI geometry SQL expression (WKT or WKB)
#     aoi_geom_str = get_aoi_geom_sql_expression(aoi_gdf)
#     if not aoi_geom_str:
#         log.error('Could not create suitable geometry from AOI.')
#         return None

#     # Compose query
#     query_str = f"""
# WITH pre_filtered AS (
#     SELECT {select_fields}, ST_GeomFromBinary({geometry_field_nm}) AS {geometry_field_nm}_geom
#     FROM {select_from}
#     {bbox_where}
# )
# SELECT * FROM pre_filtered
# WHERE ST_Intersects({geometry_field_nm}_geom, {aoi_geom_str});
# """

#     log.info("Running Athena AOI query with bbox prefilter and spatial intersection.")
#     # Use athena_query_get_path to execute and get S3 path
#     results = athena_query_get_path(s3_bucket, query_str)
#     return results
