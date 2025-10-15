from collections import defaultdict
import pandas as pd
from rsxml import Logger
import plotly.graph_objects as go
import pint
import geopandas as gpd
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.figures import common_statistics

ureg = pint.get_application_registry()


def hypsometry_data(huc_df: pd.DataFrame, bin_size: int = 100) -> pd.DataFrame:
    """
    Aggregate dem_bins from all rows, summing cell_count for each bin.
    Returns a DataFrame with columns: bin, total_cell_count.
    Fills missing bins (using bin_size) with zeros, sorted descending by bin.
    """
    log = Logger('hypsometry_data')
    log.info(f"Processing hypsometry data with bin size {bin_size}")
    if 'dem_bins' not in huc_df.columns:
        log.warning("No 'dem_bins' column found in DataFrame.")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=['bin', 'total_cell_count'])

    combined_bins = defaultdict(int)
    for dem_bin_dict in huc_df['dem_bins']:
        for b in dem_bin_dict.get('bins', []):
            combined_bins[b['bin']] += b['cell_count']

    if not combined_bins:
        return pd.DataFrame(columns=['bin', 'total_cell_count'])

    min_bin = min(combined_bins)
    max_bin = max(combined_bins)
    all_bins = list(range(min_bin, max_bin + bin_size, bin_size))

    filled_bins = {
        'bin': all_bins,
        'total_cell_count': [combined_bins.get(b, 0) for b in all_bins]
    }

    result_df = pd.DataFrame(filled_bins)
    result_df = result_df.sort_values('bin', ascending=True).reset_index(drop=True)
    return result_df


def hypsometry_fig(huc_df: pd.DataFrame) -> go.Figure:
    """
    Plot hypsometry as a bar chart: total_cell_count vs. bin.
    """
    df = hypsometry_data(huc_df)
    print('HYPSOMETRY DATA')
    print(df)  # debug only

    fig = go.Figure(
        go.Bar(
            x=df['total_cell_count'],
            y=df['bin'],
            orientation='h',
            marker_color='steelblue'
        )
    )
    fig.update_layout(
        title="Hypsometry: Total Cell Count by Elevation",
        xaxis_title="Total Cell Count",
        yaxis_title="Elevation (m)",
        template="plotly_white"
    )
    return fig


def statistics(gdf: gpd.GeoDataFrame) -> dict[str, pint.Quantity]:
    """ Calculate and return key statistics as a dictionary
    Args:
        gdf (GeoDataFrame): data_gdf input WITH UNITS APPLIED

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """
    common_stats = common_statistics(gdf)

    # create any statistics specific to this report
    # copy a subset df to make sure we don't accidentally change the incoming df
    subset_df = RSGeoDataFrame(gdf[["huc12",]].copy())

    # e.g. Calculate totals
    count_huc12s = subset_df['huc12'].nunique()
    count_huc12s = count_huc12s * ureg('count')  # 'count' is a unitless unit in pint

    # if you want different units or descriptions then give them different names and add rsfieldmeta
    # Add field meta if not already present
    RSFieldMeta().add_field_meta(
        name='count_huc12',
        friendly_name='Number of HUC-12',
        data_unit='',
        dtype='INTEGER',
        description='The number of different HUC12 having Riverscape data in the Area of Interest.'
    )

    # Compose result dictionary
    stats = {
        **common_stats,
        'count_huc12s': count_huc12s
    }
    return stats
