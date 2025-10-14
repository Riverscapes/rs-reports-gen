from collections import defaultdict
import pandas as pd
from rsxml import Logger
import plotly.graph_objects as go
from util.pandas import RSFieldMeta
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


def metric_cards(metrics: dict) -> list[tuple[str, str, str]]:
    """transform a statistics dictionary into list of metric elements

    Args: 
        metrics (dict): metric_id, Quantity
        **uses Friendly name and description if they have been added to the RSFieldMeta**

    Returns:
        list of card elements: 
            * friendly metric name (title)
            * formatted metric value, including units
            * additional description (optional)

    Uses the order of the dictionary (guaranteed to be insertion order from Python 3.7 and later)
    FUTURE ENHANCEMENT - Should be modified to handle different number of decimal places depending on the metric
    """
    cards = []
    meta = RSFieldMeta()
    log = Logger('metric_cards')
    for key, value in metrics.items():
        friendly = meta.get_friendly_name(key)
        desc = meta.get_description(key)
        log.info(f"metric: {key}, friendly: {friendly}, desc: {desc}")
        # Make sure the value respects the unit system
        system_value = RSFieldMeta().get_system_units(value)
        formatted = format_value(system_value, 0)
        cards.append((friendly, formatted, desc))
    return cards


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
