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


def pprint_df_meta(df: pd.DataFrame | gpd.GeoDataFrame, layer_id: str | None = None):
    """Pretty print a summary of a dataframe AND the metadata
    the layer_id is used for metadata disambiguation, if provided
    """
    print(f'DataFrame: {layer_id if layer_id else "Unnamed"}')
    print('-' * 120)
    print(f'Shape (rows, cols): {df.shape}\n')
    meta = RSFieldMeta()

    rows = []
    errors = []

    # Collection Phase
    for i, col in enumerate(df.columns):
        col_str = str(col)
        dtype_str = str(df[col].dtype)

        # Statistics
        na_count = df[col].isna().sum()
        distinct_count = df[col].nunique()

        # Sampling
        first_valid_index = df[col].first_valid_index()
        raw_sample = None
        raw_sample_str = "<All NA>"
        if first_valid_index is not None:
            # Use loc to get value
            raw_sample = df[col].loc[first_valid_index]
            raw_sample_str = str(raw_sample)

        # Metadata
        m = None
        try:
            m = meta.get_field_meta(column_name=col_str, layer_id=layer_id)
        except Exception as e:
            errors.append(f"Meta lookup error for '{col_str}': {e}")

        t_name = m.layer_id if m and m.layer_id else ""
        f_name = m.friendly_name if m and m.friendly_name else ""
        d_unit = str(m.data_unit) if m and m.data_unit else ""
        p_fmt = m.preferred_format if m and m.preferred_format else ""

        # Formatting
        formatted_sample = ""
        if raw_sample is not None:
            try:
                formatted_sample = meta.format_scalar(column_name=col_str, value=raw_sample, layer_id=layer_id)
            except Exception as e:
                formatted_sample = "<Format Error>"
                errors.append(f"Format error for '{col_str}': {e}")

        rows.append({
            'i': i,
            'col': col_str,
            'dtype': dtype_str,
            'table': t_name,
            'friendly': f_name,
            'unit': d_unit,
            'fmt': p_fmt,
            'na': str(na_count),
            'distinct': str(distinct_count),
            'raw': raw_sample_str,
            'formatted': formatted_sample
        })

    # --- TABLE 1: Metadata ---
    print("METADATA SUMMARY")
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

    for r in rows:
        print(
            f"{r['i']:<{w_idx}} {r['col'][:w_col - 1]:<{w_col}} {r['dtype'][:w_type - 1]:<{w_type}} "
            f"{r['table'][:w_table - 1]:<{w_table}} {r['friendly'][:w_friendly - 1]:<{w_friendly}} "
            f"{r['unit'][:w_unit - 1]:<{w_unit}} {r['fmt'][:w_fmt - 1]:<{w_fmt}}"
        )
    print('\n')

    # --- TABLE 2: Data Statistics & Samples ---
    print("DATA STATISTICS & SAMPLES")
    w_na = 10
    w_distinct = 10
    w_raw = 30
    w_formatted = 30

    header2 = (
        f"{'#':<{w_idx}} {'Column':<{w_col}} {'NA Count':<{w_na}} {'Distinct':<{w_distinct}} "
        f"{'Sample (Raw)':<{w_raw}} {'Sample (Formatted)':<{w_formatted}}"
    )
    print(header2)
    print('-' * len(header2))

    for r in rows:
        print(
            f"{r['i']:<{w_idx}} {r['col'][:w_col - 1]:<{w_col}} {r['na']:<{w_na}} {r['distinct']:<{w_distinct}} "
            f"{r['raw'][:w_raw - 1]:<{w_raw}} {r['formatted'][:w_formatted - 1]:<{w_formatted}}"
        )
    print('\n')

    if errors:
        print("ERRORS ENCOUNTERED:")
        for err in errors:
            print(f"- {err}")
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
