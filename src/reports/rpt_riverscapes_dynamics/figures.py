"""functions to generate figures specifically for rpt_riverscapes_dynamics reports"""

import pandas as pd
import pint
import plotly.graph_objects as go
import plotly.express as px

from util.pandas import RSFieldMeta
from util.figures import bar_group_x_by_y


def linechart(df_metrics: pd.DataFrame, metric_colnm: str) -> go.Figure:
    """rsdynamics metric df by time and confidence
    TODO: use metadata for col including units
    TODO: probably move filtering upstream so don't have to redo it for every metric
    """
    # Filter data where epoch_length is 5 (including all confidence levels)
    filtered_df = df_metrics[(df_metrics['epoch_length'] == '5')].copy()
    # however when you do only that the epoch_length category still includes the epoch_length 30

    # 1. Isolate the Series for the 5-year epochs
    # Need to make sure epoch_name is categorical before using .cat accessor
    if df_metrics['epoch_name'].dtype == 'object':
        df_metrics['epoch_name'] = df_metrics['epoch_name'].astype('category')

    subset_series = df_metrics.loc[df_metrics['epoch_length'] == '5', 'epoch_name']

    # 2. Remove unused categories from the SERIES, then grab the remaining categories
    #    This returns a Pandas Index, which set_categories() accepts happily.
    valid_5yr_epochs = subset_series.cat.remove_unused_categories().cat.categories

    # 3. Apply to your filtered dataframe
    filtered_df['epoch_name'] = filtered_df['epoch_name'].cat.set_categories(valid_5yr_epochs)

    metric_summary = filtered_df.groupby(['landcover', 'epoch_name', 'confidence'], observed=False)[metric_colnm].sum().reset_index()
    metric_summary = metric_summary.sort_values('epoch_name')

    # Try to resolve layer_id from dataframe attributes and add to new dataframe
    layer_id = df_metrics.attrs.get('layer_id') if hasattr(df_metrics, 'attrs') else None
    metric_summary.attrs['layer_id'] = layer_id
    baked_chart_data, _baked_headers = RSFieldMeta().bake_units(metric_summary)

    title = f'{metric_colnm.capitalize()} by Landcover over Time (Epoch Length 5)'
    # Create line chart
    fig = px.line(baked_chart_data, x='epoch_name', y=metric_colnm, color='landcover', line_dash='confidence',
                  title=title,
                  line_dash_map={'95': 'solid', '68': 'dash'})
    return fig


def area_histogram(df_metrics: pd.DataFrame) -> go.Figure:
    """
    Generate a histogram of area for the 30-year epoch (1989_2024, confidence 95).
    """
    df_30yr_area = df_metrics[
        (df_metrics['epoch_name'] == '1989_2024') &
        (df_metrics['confidence'] == '95')
    ].copy()

    if df_30yr_area.empty:
        raise ValueError("No data for 30-year epoch found")

    # Use magnitude for cutting to ensure clean numeric labels
    area_vals = df_30yr_area['area']
    if hasattr(area_vals, 'pint'):
        area_vals = area_vals.pint.magnitude

    # Calculate bins: 1 bin if no variation, else 10 bins
    n_bins = 10 if (area_vals.max() - area_vals.min()) > 10 else 1

    df_30yr_area['area_bin_raw'] = pd.cut(area_vals, bins=n_bins)

    # Turn the bins into categories so we can show empty bins and format the labels
    if isinstance(df_30yr_area['area_bin_raw'].dtype, pd.CategoricalDtype):
        new_labels = [f"{i.left:,.0f} - {i.right:,.0f}" for i in df_30yr_area['area_bin_raw'].cat.categories]
        df_30yr_area['area_bins'] = df_30yr_area['area_bin_raw'].cat.rename_categories(new_labels)
    else:
        df_30yr_area['area_bins'] = df_30yr_area['area_bin_raw'].astype(str)

    df_30yr_area['record_count'] = 1
    return bar_group_x_by_y(df_30yr_area, total_col='record_count', group_by_cols=['area_bins', 'landcover'])


def statistics(df: pd.DataFrame) -> dict[str, pint.Quantity]:
    """Calculate and return key statistics for rsdynamics"""
    count_dgos = len(df)

    total_segment_area = df["segment_area"].sum()
    total_centerline_length = df["centerline_length"].sum()
    # Calculate integrated valley bottom width as ratio of totals
    integrated_valley_bottom_width = total_segment_area / total_centerline_length if total_centerline_length != 0 else float('nan') * total_segment_area.units / total_centerline_length.units

    stats = {
        'count_dgos': count_dgos,
        'total_segment_area': total_segment_area,
        'total_centerline_length': total_centerline_length,
        'integrated_valley_bottom_width': integrated_valley_bottom_width
    }
    return stats
