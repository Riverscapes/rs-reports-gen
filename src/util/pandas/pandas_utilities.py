import geopandas as gpd
import pandas as pd
from shapely import wkt
import pint

ureg = pint.UnitRegistry()


def load_gdf_from_csv(csv_path):
    """ load csv from athena query into gdf

    Args:
        csv_path (_type_): _path to csv

    Returns:
        _type_: gdf
    """
    df = pd.read_csv(csv_path)
    df.describe()  # outputs some info for debugging
    df['dgo_polygon_geom'] = df['dgo_geom_obj'].apply(wkt.loads)  # pyright: ignore[reportArgumentType, reportCallIssue]
    gdf = gpd.GeoDataFrame(df, geometry='dgo_polygon_geom', crs='EPSG:4326')
    # print(gdf)
    return gdf


def convert_gdf_units(gdf: gpd.GeoDataFrame, unit_system: str = "US"):
    """convert all measures according to unit system 
    does not work yet

    Args:
        gdf (gpd.GeoDataFrame): data_gpd
        unit_system (str, optional): Unit system. Defaults to "US".

    Returns:
        same data frame but with units converted
    """
    ureg.default_system = unit_system
    for col in gdf.columns:
        if hasattr(gdf[col], 'pint'):
            # convert each unit DOES NOT WORK this way
            # pint.to_unit(foot) etc. does work -- we'll need to know which units
            gdf[col] = gdf[col].pint.to_base_units()
    return gdf


def add_calculated_cols(df: pd.DataFrame) -> pd.DataFrame:
    """ Add any calculated columns to the dataframe

    Args:
        df (pd.DataFrame): Input dataframe

    Returns:
        pd.DataFrame: DataFrame with calculated columns added
    """
    # TODO: add metadata for any added columns
    df['channel_length'] = df['rel_flow_length']*df['centerline_length']
    return df
