""" Utility functions to get data from AWS Athena and return it in useful formats
a version/copy of these functions can be found in multiple Riverscapes repositories
* data-exchange-scripts
* rs-reports-gen (this one)
* athena 
* cybercastor_scripts

Consider porting any improvements to these other repositories. 
"""
import time
import re
import boto3
import pandas as pd
from rsxml import Logger
import geopandas as gpd
from util import simplify_gdf

# buffer, in decimal degrees, on centroids to capture DGO.
# value of 0.47 is based on an analysis of distance between centroid and corners of bounding boxes of raw_rme 2025-09-08
BUFFER_CENTROID_TO_BB_DD = 0.47
S3_ATHENA_BUCKET = "riverscapes-athena"
ATHENA_WORKGROUP = "primary"


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


def athena_query_get_path(s3_bucket: str, query: str, max_wait: int = 600) -> str | None:
    """
    Run an Athena query and return the S3 output path to the CSV result file.

    Args:
        s3_bucket (str): S3 bucket for Athena output.
        query (str): SQL query string.
        max_wait (int): Maximum wait time in seconds.

    Returns:
        str | None: S3 path to the CSV result file, or None on failure.
    """
    result = _run_athena_query(s3_bucket, query, max_wait)
    return result[0] if result else None


def athena_query_get_rows(s3_bucket: str, query: str, max_wait: int = 600) -> list | None:
    """
    Run an Athena query and return the raw result rows.

    Args:
        s3_bucket (str): S3 bucket for Athena output.
        query (str): SQL query string.
        max_wait (int): Maximum wait time in seconds.

    Returns:
        list | None: Raw Athena result rows, or None on failure.
    """
    result = _run_athena_query(s3_bucket, query, max_wait)
    if not result:
        return None
    _, query_execution_id = result

    log = Logger("Athena query")
    athena = boto3.client('athena', region_name='us-west-2')
    results = []
    next_token = None
    while True:
        if next_token:
            response = athena.get_query_results(QueryExecutionId=query_execution_id, NextToken=next_token)
        else:
            response = athena.get_query_results(QueryExecutionId=query_execution_id)
        results.extend(response['ResultSet']['Rows'])
        next_token = response.get('NextToken')
        if not next_token:
            break

    if results and len(results) > 1:
        return results
    return None


def athena_query_get_parsed(s3_bucket: str, query: str, max_wait: int = 600) -> list[dict] | None:
    """
    Run an Athena query and return parsed results as a list of dictionaries.

    Args:
        s3_bucket (str): S3 bucket for Athena output.
        query (str): SQL query string.
        max_wait (int): Maximum wait time in seconds.

    Returns:
        list[dict] | None: Parsed Athena results, or None on failure.
    """
    rows = athena_query_get_rows(s3_bucket, query, max_wait)
    if rows:
        return parse_athena_results(rows)
    return None


def get_field_metadata() -> pd.DataFrame:
    """
    Query Athena for column metadata from rme_table_column_defs and return as a DataFrame.

    # For the shape of what needs to be returned refer to:
        src/util/pandas/RSFieldMeta.py

    Returns:
        pd.DataFrame - DataFrame of metadata

    Example:
        metadata_df = get_field_metadata()
    """
    log = Logger('Get field metadata')
    log.info("Getting field metadata from athena")

    # TODO: Athena table needs the following column changes:
    # 1. Rename `type` => `dtype`.
    # 2. Add Column: `no_convert` => boolean (True/False) - default False
    # 3. Rename `unit` => `data_unit`
    # 4. Add Column `display_unit` (same data type as data_unit)

    query = """
        SELECT table_name, name, friendly_name, type AS dtype, unit AS data_unit, description
        FROM rme_table_column_defs
    """
    result = athena_query_get_parsed(S3_ATHENA_BUCKET, query)
    if result is not None:
        return pd.DataFrame(result)
    raise RuntimeError("Railed to retrieve metadata from Athena.")


def get_data_for_aoi(s3_bucket: str, gdf: gpd.GeoDataFrame, output_path: str):
    """given aoi in gdf format (assume 4326), just get all the raw_rme (for now)
    returns: local path to the data csv file"""
    log = Logger('Run AOI query on Athena')
    # temporary approach -- later try using report-type specific CTAS and report-specific UNLOAD statement
    fields_str = "level_path, seg_distance, centerline_length, segment_area, fcode, fcode_desc, longitude, latitude, ownership, ownership_desc, state, county, drainage_area, stream_name, stream_order, stream_length, huc12, rel_flow_length, channel_area, integrated_width, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, lf_agriculture_prop, lf_agriculture, lf_developed_prop, lf_developed, lf_riparian_prop, lf_riparian, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, develop, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent, rme_project_id, rme_project_name"
    s3_csv_path = run_aoi_athena_query(gdf, s3_bucket, fields_str=fields_str, source_table="rpt_rme")
    if s3_csv_path is None:
        log.error("Didn't get a result from athena")
        raise NotImplementedError
    get_s3_file(s3_csv_path, output_path)
    return


def _run_athena_query(
    s3_bucket: str,
    query: str,
    max_wait: int = 600
) -> tuple[str, str] | None:
    """
    Run an Athena query and wait for completion.

    Args:
        s3_bucket (str): S3 bucket for Athena output.
        query (str): SQL query string.
        max_wait (int): Maximum wait time in seconds.

    Returns:
        tuple[str, str] | None: (output_path, query_execution_id) on success, or None on failure.

    This is core function called by `athena_query_get_path` and `athena_query_get_rows`
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
    # print("Full query:")
    # print(query_str)
    # with open("athena_query.sql", "w", encoding="utf-8") as f:
    #     f.write(query_str)

    athena = boto3.client('athena', region_name='us-west-2')
    response = athena.start_query_execution(
        QueryString=query,
        WorkGroup=ATHENA_WORKGROUP,
        QueryExecutionContext={
            'Database': 'default',
            'Catalog': 'AwsDataCatalog'
        },
        ResultConfiguration={
            'OutputLocation': f's3://{s3_bucket}/athena_query_results'
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

    Assumes the geojson is in decimal degrees 
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


def run_athena_aoi_query(aoi_gdf: gpd.GeoDataFrame, s3_bucket: str, select_fields: str, select_from: str,
                         geometry_field_nm: str = "geometry", geometry_bbox_field_nm: str = "geometry_bbox"):
    """
    Executes an Athena query to select features from select_from table that intersect the AOI geometry,
    using geometry_bbox for efficient prefiltering.

    Args:
        aoi_gdf: AOI as GeoDataFrame
        s3_bucket: S3 bucket for Athena results
        select_fields: Comma-separated fields to select
        select_from: Table name
        geometry_field_nm: Name of geometry field (default 'geometry')
        geometry_bbox_field_nm: Name of bbox field (default 'geometry_bbox')
    Returns:
        Athena results (parsed or S3 path)
    """
    log = Logger('run_athena_aoi_query')
    # Get AOI bounding box
    minx, miny, maxx, maxy = aoi_gdf.total_bounds

    bbox_where = (
        f"WHERE "
        f"{geometry_bbox_field_nm}.xmax >= {minx} AND "
        f"{geometry_bbox_field_nm}.xmin <= {maxx} AND "
        f"{geometry_bbox_field_nm}.ymax >= {miny} AND "
        f"{geometry_bbox_field_nm}.ymin <= {maxy}"
    )

    # Get AOI geometry SQL expression (WKT or WKB)
    aoi_geom_str = get_aoi_geom_sql_expression(aoi_gdf)
    if not aoi_geom_str:
        log.error('Could not create suitable geometry from AOI.')
        return None

    # Compose query
    query_str = f"""
WITH pre_filtered AS (
    SELECT {select_fields}, ST_GeomFromBinary({geometry_field_nm}) AS {geometry_field_nm}_geom
    FROM {select_from}
    {bbox_where}
)
SELECT * FROM pre_filtered
WHERE ST_Intersects({geometry_field_nm}_geom, {aoi_geom_str});
"""

    log.info("Running Athena AOI query with bbox prefilter and spatial intersection.")
    # Use athena_query_get_path to execute and get S3 path
    results = athena_query_get_path(s3_bucket, query_str)
    return results


def run_aoi_athena_query(aoi_gdf: gpd.GeoDataFrame, s3_bucket: str, fields_str: str = "", source_table: str = "raw_rme") -> str | None:
    """Run Athena query `select (field_str) from (source_table)` on supplied AOI geojson
    also includes the dgo geometry (polygon) 
    the source table must have fields: 
     * latitude, longitude 
     * dgo_geom (which is WKT but with , replaced by |)
    return path to results on S3
    returns None if the shape can't be converted to suitably sized geometry sql expression. 
    Future Enhancements: 
    - change to using a WKB field instead of dgo_geom
    - if the source has bounds struct field (comes default with qgis geoparquet exports) we can use that instead of the BUFFER 
    - note there is a copy of this function (whole module) in `src\reports\rpt_igo_project\athena_query_aoi.py`
    - use the same multiple-resizing strategy from simplify_to_size in `rs_geo_helpers.py`
    """
    log = Logger('Run AOI Query on Athena')

    # we are going to filter the centroids, but clip to polygon
    # So we need to buffer by the maximum distance between a centroid and its bounding polygon
    prefilter_where_clause = generate_sql_where_clause_for_bounds(aoi_gdf)

    # count the prefiltered records - these 3 lines are only needed for debugging -- comment out for better performance
    query_str = f'SELECT count(*) AS record_count FROM {source_table} {prefilter_where_clause}'
    results = (athena_query_get_parsed(s3_bucket, query_str))
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
        , ST_GeometryFromText(REPLACE({source_table}.dgo_geom, '|', ',')) AS dgo_geom_obj
    FROM
        {source_table}
    {prefilter_where_clause}
    )
SELECT
    * -- was fields_str
FROM 
    pre_filtered_rme AS t1
WHERE 
    ST_Intersects(
        t1.dgo_geom_obj,
        {aoi_geom_str}
    );
"""

    results = athena_query_get_path(s3_bucket, query_str)
    return results


def main():
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
    main()
