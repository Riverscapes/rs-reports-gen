"""Figure generation for Stream Names Report
"""
import pandas as pd
import plotly.graph_objects as go
from util.pandas import RSFieldMeta


def word_cloud_data(df: pd.DataFrame) -> pd.DataFrame:
    """process to go from raw df to just what we need"""
    meta = RSFieldMeta()
    df_copy = df[['stream_name', 'centerline_length']].copy()
    agg_data = df_copy.groupby('stream_name', as_index=False, observed=False)['centerline_length'].sum()

    df_baked, _headers = meta.bake_units(agg_data)
    return df_baked


def word_cloud(indf: pd.DataFrame) -> go.Figure:
    """
    Plot word cloud.
    """
    print('WORD CLOUD')
    print(indf)  # debug only
    df = word_cloud_data(indf)

    fig = go.Figure(
        go.Bar(
            x=df['centerline_length'],
            y=df['stream_name'],
            orientation='h',
            marker_color='steelblue'
        )
    )
    fig.update_layout(
        title="Word Cloud",
        xaxis_title="centerline length",
        yaxis_title="stream name",
        template="plotly_white"
    )
    return fig
