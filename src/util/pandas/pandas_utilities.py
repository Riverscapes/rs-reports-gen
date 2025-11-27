from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely import wkt, wkb
from rsxml import Logger
import pint
import pyarrow.parquet as pq
from util.athena.athena_unload_utils import list_athena_unload_payload_files
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


def load_gdf_from_pq(
    pq_path: Path,
    geometry_col: str | None = None,
    crs: str = 'EPSG:4326',
) -> pd.DataFrame | gpd.GeoDataFrame:
    """
    Load a DataFrame or GeoDataFrame from local parquet file or folder of files.

    Args:
        pq_path: Path to parquet file or directory.
        geometry_col: Name of geometry column to use (optional).
        crs: CRS to assign if returning GeoDataFrame.

    Returns:
        pd.DataFrame or gpd.GeoDataFrame
    """
    log = Logger('load gdf from parquet')
    log.debug(f'Reading from {pq_path}')
    if pq_path.is_file():
        parquet_files = [pq_path]
    else:
        parquet_files = list_athena_unload_payload_files(pq_path)
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {pq_path}")
    dfs = [pq.ParquetFile(p).read().to_pandas() for p in parquet_files]
    df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
    if geometry_col and geometry_col in df.columns:
        # Detect and convert WKT or WKB
        sample = df[geometry_col].dropna().iloc[0] if not df[geometry_col].dropna().empty else None
        if sample is not None:
            if isinstance(sample, str):
                df[geometry_col] = df[geometry_col].apply(wkt.loads)
            elif isinstance(sample, (bytes, bytearray)):
                df[geometry_col] = df[geometry_col].apply(wkb.loads)
        gdf = gpd.GeoDataFrame(df, geometry=geometry_col, crs=crs)
        return gdf
    return df
