from collections import defaultdict
import pandas as pd
from rsxml import Logger
import plotly.graph_objects as go
import pint
import geopandas as gpd
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.figures import format_value


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
        title="Hypsometry: Total Cell Count by Elevation Bin",
        xaxis_title="Total Cell Count",
        yaxis_title="Elevation Bin (m)",
        template="plotly_white"
    )
    return fig


def statistics(gdf: gpd.GeoDataFrame) -> dict[str, pint.Quantity]:
    """ Calculate and return key statistics as a dictionary
    TODO: integrated should be calculated from the totals, not at row level
    Args:
        gdf (GeoDataFrame): data_gdf input WITH UNITS APPLIED

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """
    subset = RSGeoDataFrame(gdf[["segment_area", "centerline_length", "channel_length"]].copy())
    # Calculate totals
    total_segment_area = subset["segment_area"].sum()
    total_centerline_length = subset["centerline_length"].sum()
    total_channel_length = subset["channel_length"].sum()

    # Calculate integrated valley bottom width as ratio of totals
    integrated_valley_bottom_width = total_segment_area / total_centerline_length if total_centerline_length != 0 else float('nan')

    # if you want different units or descriptions then give them different names and add rsfieldmeta
    # Add field meta if not already present
    RSFieldMeta().add_field_meta(
        name='integrated_valley_bottom_width',
        friendly_name='Integrated Valley Bottom Width',
        data_unit='m',
        dtype='REAL',
        description='Total segment area divided by total centerline length, representing average valley bottom width'
    )

    # Compose result dictionary
    stats = {
        'segment_area': total_segment_area.to('kilometer ** 2'),  # acres and hectares will be interchangeable based on unit system
        'centerline_length': total_centerline_length.to('kilometer'),  # miles and km will be interchangeable based on unit system
        'channel_length': total_channel_length.to('kilometer'),  # miles and km will be interchangeable based on unit system
        # Here we specify yards (because yards converts to meters but meters converts to feet and we want yards for the imperial system)
        'integrated_valley_bottom_width': integrated_valley_bottom_width.to('yards'),
    }
    return stats
