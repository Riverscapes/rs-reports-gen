import geopandas as gpd
import pandas as pd
from shapely import wkt
from rsxml import Logger
import pint

ureg = pint.UnitRegistry()


def load_gdf_from_csv(csv_path) -> gpd.GeoDataFrame:
    """ load csv from athena query into gdf

    Args:
        csv_path (_type_): _path to csv

    Returns:
        GeoDataFrame
    """
    log = Logger('load gdf from csv')
    log.debug('Reading CSV')
    df = pd.read_csv(csv_path, dtype={'huc12': str})
    df['dgo_polygon_geom'] = df['dgo_geom_obj'].apply(wkt.loads)  # pyright: ignore[reportArgumentType, reportCallIssue]
    gdf = gpd.GeoDataFrame(df, geometry='dgo_polygon_geom', crs='EPSG:4326')
    gdf = gdf.drop(columns=['dgo_geom_obj'])
    return gdf
