"""Figure builders for the Inventory of Resources report.

Created 2026-08-13.
Created by copilot.
"""

from collections import defaultdict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from rsxml import Logger


def length_bar_chart(summary_df: pd.DataFrame, category_column: str, title: str) -> go.Figure:
    """Build a horizontal stream-length chart from a summary table.

    Args:
            summary_df: Summary dataframe containing stream length by category.
            category_column: Category column to display on the y axis.
            title: Figure title.

    Returns:
            A Plotly bar chart.

    Created by copilot.
    """
    display_data = summary_df.head(15).sort_values("stream_length", ascending=True)
    figure = px.bar(
        display_data,
        x="stream_length",
        y=category_column,
        orientation="h",
        labels={"stream_length": "Total Stream Length (m)", category_column: category_column.replace("_", " ").title()},
        title=title,
    )
    figure.update_layout(margin={"r": 20, "t": 50, "l": 20, "b": 20}, showlegend=False)
    return figure


def distribution_chart(data_df: pd.DataFrame, value_column: str, title: str, x_label: str) -> go.Figure:
    """Build a histogram for a numeric inventory attribute.

    Args:
            data_df: Normalized inventory dataframe.
            value_column: Numeric source field.
            title: Figure title.
            x_label: Human-readable axis label.

    Returns:
            A Plotly histogram, including an empty-state annotation when needed.

    Created by copilot.
    """
    values = data_df.get(value_column, pd.Series(dtype=float)).dropna()
    figure = px.histogram(values, x=value_column, nbins=30, labels={value_column: x_label, "count": "Inventory Records"}, title=title)
    if values.empty:
        figure.add_annotation(text="No values reported for this area of interest.", showarrow=False)
    figure.update_layout(margin={"r": 20, "t": 50, "l": 20, "b": 20}, showlegend=False)
    return figure


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
        'total_cell_count': [combined_bins.get(b, 0) for b in all_bins],
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
            marker_color='steelblue',
        )
    )
    fig.update_layout(
        title="Hypsometry: Total Cell Count by Elevation",
        xaxis_title="Total Cell Count",
        yaxis_title="Elevation (m)",
        template="plotly_white",
    )
    return fig
