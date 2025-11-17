"""Figure generation for Stream Names Report
"""
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from wordcloud import WordCloud, STOPWORDS

from util.pandas import RSFieldMeta


def word_cloud_data(df: pd.DataFrame) -> pd.DataFrame:
    """process to go from raw df to just what we need"""
    meta = RSFieldMeta()
    df_copy = df[['stream_name', 'centerline_length']].copy()
    agg_data = df_copy.groupby('stream_name', as_index=False, observed=False)[
        'centerline_length'].sum()

    df_baked, _headers = meta.bake_units(agg_data)
    return df_baked


def word_cloud(indf: pd.DataFrame) -> go.Figure:
    """
    Plot word cloud.
    """
    print('WORD CLOUD')
    print(indf)  # debug only

    # 1. Prepare aggregated / unit-baked data
    df = word_cloud_data(indf)

    # Safety checks
    if df.empty or 'stream_name' not in df.columns or 'centerline_length' not in df.columns:
        # Fallback tiny dummy word cloud so the report doesn't break
        freq_dict = {"no_stream_names": 1.0}
    else:
        # 2. Build frequency dict: { stream_name: centerline_length }
        #    centerline_length is already aggregated & unit-baked.
        freq_series = (
            df
            .dropna(subset=['stream_name', 'centerline_length'])
            .set_index('stream_name')['centerline_length']
        )

        # Convert to plain dict with float values
        freq_dict = {name: float(val) for name,
                     val in freq_series.items() if float(val) > 0}

        if not freq_dict:
            freq_dict = {"no_stream_names": 1.0}

    # 3. Generate word cloud from frequencies
    wc = WordCloud(
        width=800,
        height=400,
        background_color='white',
        stopwords=STOPWORDS,
        max_words=100,
        colormap='viridis',
        prefer_horizontal=0.1,  # mix of horizontal & vertical words
    ).generate_from_frequencies(freq_dict)

    # 4. Convert to array for Plotly
    wc_array = wc.to_array()

    # 5. Display with Plotly (image-based figure)
    fig = px.imshow(wc_array)
    fig.update_layout(
        title="Stream Name Word Cloud",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        margin=dict(l=0, r=0, t=0, b=0),
        template="plotly_white",
    )
    fig.update_xaxes(showticklabels=False)
    fig.update_yaxes(showticklabels=False)

    return fig
