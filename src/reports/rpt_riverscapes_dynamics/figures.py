"""functions to generate figures specifically for rpt_riverscapes_dynamics reports"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from util.pandas import RSFieldMeta


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
