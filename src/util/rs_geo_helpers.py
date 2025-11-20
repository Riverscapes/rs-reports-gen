import json
import apsw
from rsxml import Logger
import geopandas as gpd


def simplify_gdf(gdf: gpd.GeoDataFrame, tolerance_meters: float = 11) -> gpd.GeoDataFrame:
    """ Simplifies the geometries in a GeoDataFrame using a specified tolerance in meters.

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame to be simplified.
        tolerance_meters (float, optional): Tolerance in meters for simplification. Defaults to 11.

    Returns:
        gpd.GeoDataFrame: Simplified GeoDataFrame.
    """
    # check original crs
    crs = gdf.crs

    # Reproject to a projected CRS (EPSG:5070 is good for CONUS)
    gdf_proj = gdf.to_crs(epsg=5070)
    # Simplify geometries in projected CRS (tolerance in meters)
    gdf_proj["geometry"] = gdf_proj.geometry.simplify(tolerance=tolerance_meters, preserve_topology=True)
    # Reproject back to original crs or if none, EPSG:4326 for Athena
    if crs:
        gdf_simplified = gdf_proj.to_crs(crs)
    else:
        gdf_simplified = gdf_proj.to_crs(epsg=4326)
    return gdf_simplified


def simplify_gdf_to_size(
        gdf: gpd.GeoDataFrame,
        size_bytes: int,
        format: str = "geojson",
        start_tolerance_m: float = 5.0,
        max_attempts=3,
) -> tuple[gpd.GeoDataFrame, dict]:
    """
    Simplifies gdf so its serialized size (GeoJSON or WKB) is under size_limit_bytes.
    Returns: (simplified_gdf, tolerance_used, success)

    If if needed, tries to simplify it to get to within provided size 
    each attempt the tolerance grows exponentially starting at start_tolerance_m e.g. 5m x 1, x4, x9, x16 etc. so 5, 20, 45, 80 etc. 
    returns the geodataframe, the final simplification tolerance used (0 means none), and boolean if is under
    TODO: size as geojson not the same thing as size as WKB - perhaps take a parameter
    TODO: no point in converting to desired output format to check the size, but then passing back gdf and then calling function has to convert again
    """
    log = Logger("simplify to size")
    geojson_geom = gdf.to_json()
    size = len(geojson_geom.encode('utf-8'))

    if size <= size_bytes:
        log.debug(f'Supplied gdf is {size:,} which is less than {size_bytes:,}. All good')
        return gdf, 0, True

    for attempt in range(1, max_attempts + 1):
        tolerance_m = start_tolerance_m * (attempt ** 2)
        log.debug(f'GeoJSON size {size:,} bytes exceeds {size_bytes:,}, attempt {attempt} to simplify will use {tolerance_m} m tolerance')
        simplified_gdf = simplify_gdf(gdf, tolerance_m)
        geojson_geom = simplified_gdf.to_json()
        size = len(geojson_geom.encode('utf-8'))
        if size <= size_bytes:
            log.debug(f'After {attempt} attempts at resizing, final size is {size:,} which is less than {size_bytes:,}.')
            return simplified_gdf, tolerance_m, True

    log.warning(f'GeoJSON size {size:,} bytes still exceeds {size_bytes:,} bytes. Stopping after {max_attempts} attempts.')
    metadata = {
        "tolerance_m": tolerance_m,
        "simplified": tolerance_m > 0,
        "success": True,
        "final_size_bytes": size,
        "format": format,
    }
    return simplified_gdf, metadata


def prepare_gdf_for_athena(
        gdf: gpd.GeoDataFrame,
        size_bytes: int = 261_000,
        max_attempts: int = 5
) -> tuple[gpd.GeoDataFrame, dict]:
    """Simplify a GeoDataFrame for Athena if needed and report the simplification metadata.
    """
    sized_gdf, tolerance_m, success = simplify_to_size(gdf, size_bytes, max_attempts)
    final_size = len(sized_gdf.to_json().encode('utf-8'))
    metadata = {
        "tolerance_m": tolerance_m,
        "simplified": tolerance_m > 0,
        "success": success,
        "final_size_bytes": final_size,
    }
    return sized_gdf, metadata


def get_bounds_from_gdf(
        gdf: gpd.GeoDataFrame
) -> tuple[dict, tuple[float, float], tuple[float, float, float, float]]:
    """
    Unions/dissolves all the features of the geodataframe 
    simplifies if needed 
    Assumes already in correct CRS, 4326
    Returns: GeoJSOn dictionary, the centroid, the bounding box
    Future enhancement - split functions, it's doing too many things. 
    """
    log = Logger('get bounds from gdf')
    union_geom = gdf.union_all()
    MAX_GEOJSON_SIZE: int = 300_000
    # check the size as a geojson is under 300 kb (docs says "well under 500 kb")

    union_gdf = gpd.GeoDataFrame(geometry=[union_geom], crs=gdf.crs)

    sized_gdf, _tolerance_m, success = simplify_to_size(union_gdf, MAX_GEOJSON_SIZE, 3)
    if not success:
        log.warning('Bounds may be too big (complex) for Riverscapes Projects use.')

    json_str = sized_gdf.to_json()
    geojson_output = json.loads(json_str)

    centroid = union_geom.centroid
    bounds = union_geom.bounds  # (minx, miny, maxx, maxy)

    return geojson_output, (centroid.x, centroid.y), bounds
