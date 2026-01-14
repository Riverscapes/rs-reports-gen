from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely import wkt, wkb
from rsxml import Logger
import pint
import pyarrow.parquet as pq
from util.athena.athena_unload_utils import list_athena_unload_payload_files
from util.pandas import RSFieldMeta
ureg = pint.UnitRegistry()


def pprint_df_meta(df: pd.DataFrame | gpd.GeoDataFrame, table_name: str | None = None):
    """Pretty print a summary of a dataframe AND the metadata
    the table_name is used for metadata disambiguation, if provided
    """
    print(f'DataFrame: {table_name if table_name else "Unnamed"}')
    print('-' * 120)
    print(f'Shape (rows, cols): {df.shape}\n')
    meta = RSFieldMeta()

    # --- TABLE 1: Metadata ---
    print("METADATA SUMMARY")
    # Define widths for Table 1
    w_idx = 4
    w_col = 40
    w_type = 20
    w_table = 15
    w_friendly = 30
    w_unit = 15
    w_fmt = 20

    header1 = (
        f"{'#':<{w_idx}} {'Column':<{w_col}} {'Type':<{w_type}} {'Table':<{w_table}} "
        f"{'Friendly Name':<{w_friendly}} {'Unit':<{w_unit}} {'Preferred Fmt':<{w_fmt}}"
    )
    print(header1)
    print('-' * len(header1))

    for i, col in enumerate(df.columns):
        col_str = str(col)
        dtype_str = str(df[col].dtype)

        try:
            m = meta.get_field_meta(column_name=col_str, table_name=table_name)
        except Exception:
            m = None

        t_name = ""
        f_name = ""
        d_unit = ""
        p_fmt = ""

        if m:
            t_name = m.table_name if m.table_name else ""
            f_name = m.friendly_name if m.friendly_name else ""
            d_unit = str(m.data_unit) if m.data_unit else ""
            p_fmt = m.preferred_format if m.preferred_format else ""

        print(
            f"{i:<{w_idx}} {col_str[:w_col - 1]:<{w_col}} {dtype_str[:w_type - 1]:<{w_type}} "
            f"{t_name[:w_table - 1]:<{w_table}} {f_name[:w_friendly - 1]:<{w_friendly}} "
            f"{d_unit[:w_unit - 1]:<{w_unit}} {p_fmt[:w_fmt - 1]:<{w_fmt}}"
        )
    print('\n')

    # --- TABLE 2: Data Statistics & Samples ---
    print("DATA STATISTICS & SAMPLES")
    # Define widths for Table 2
    # w_idx defined above
    # w_col defined above
    w_na = 10
    w_raw = 30
    w_formatted = 30

    header2 = (
        f"{'#':<{w_idx}} {'Column':<{w_col}} {'NA Count':<{w_na}} "
        f"{'Sample (Raw)':<{w_raw}} {'Sample (Formatted)':<{w_formatted}}"
    )
    print(header2)
    print('-' * len(header2))

    for i, col in enumerate(df.columns):
        col_str = str(col)
        na_count = df[col].isna().sum()

        # Get a sample value (first valid)
        first_valid_index = df[col].first_valid_index()
        if first_valid_index is not None:
            raw_sample = df[col].loc[first_valid_index]
        else:
            raw_sample = None

        formatted_sample = ""
        if raw_sample is not None:
            try:
                formatted_sample = meta.format_scalar(column_name=col_str, value=raw_sample, table_name=table_name)
            except Exception:
                formatted_sample = "<Format Error>"

        raw_sample_str = str(raw_sample) if raw_sample is not None else "<All NA>"

        print(
            f"{i:<{w_idx}} {col_str[:w_col - 1]:<{w_col}} {str(na_count):<{w_na}} "
            f"{raw_sample_str[:w_raw - 1]:<{w_raw}} {formatted_sample[:w_formatted - 1]:<{w_formatted}}"
        )
    print('\n')


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
