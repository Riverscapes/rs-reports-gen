"""Figure generation for Stream Names Report
"""
import os

import pandas as pd
from wordcloud import WordCloud, STOPWORDS
import plotly.express as px

from util.pandas import RSFieldMeta
from reports.rpt_stream_names.colour_gradient import stream_colour_from_order


def word_cloud_data(df: pd.DataFrame) -> pd.DataFrame:
    """process to go from raw df to just what we need"""
    meta = RSFieldMeta()
    df_copy = df[['stream_name', 'centerline_length', 'stream_order']].copy()
    agg_data = df_copy.groupby('stream_name', as_index=False, observed=False).agg({
        'centerline_length': 'sum',
        'stream_order': max
    })
    agg_data['stream_order_colour'] = agg_data['stream_order'].apply(
        stream_colour_from_order
    )
    df_baked, _headers = meta.bake_units(agg_data)
    return df_baked


def word_cloud(indf: pd.DataFrame, output_dir: str) -> str:
    """
    Generate a word cloud from stream names and:

      * write one SVG to `output_dir`
      * write 3 PNGs of different sizes to `output_dir`
      * return a single <img> tag string referencing the SVG

    Args:
        indf (pd.DataFrame): Input dataframe with at least stream_name, centerline_length, stream_order
        output_dir (str): Directory where SVG and PNG files will be written

    Returns:
        str: HTML <img> tag pointing at the generated SVG (relative filename)
    """
    print('WORD CLOUD')
    print(indf)  # debug only

    os.makedirs(output_dir, exist_ok=True)

    # 1. Prepare aggregated / unit-baked data
    df = word_cloud_data(indf)

    # Safety checks
    if df.empty or 'stream_name' not in df.columns or 'centerline_length' not in df.columns:
        # Fallback tiny dummy word cloud so the report doesn't break
        freq_dict = {"no_stream_names": 1.0}
        colour_lookup = {"no_stream_names": "#000000"}
    else:
        # 2. Build frequency dict: { stream_name: centerline_length }
        #    centerline_length is already aggregated & unit-baked.
        freq_series = (
            df
            .dropna(subset=['stream_name', 'centerline_length'])
            .set_index('stream_name')['centerline_length']
        )

        # Convert to plain dict with float values
        freq_dict = {
            name: float(val)
            for name, val in freq_series.items()
            if float(val) > 0
        }

        if not freq_dict:
            freq_dict = {"no_stream_names": 1.0}

        # 2b. Build colour lookup dict: { stream_name: stream_order_colour }
        if 'stream_order_colour' in df.columns:
            colour_lookup = {
                row['stream_name']: row['stream_order_colour']
                for _, row in df.iterrows()
                if isinstance(row['stream_order_colour'], str)
            }
        else:
            # Fallback: default to black if no colour column
            colour_lookup = {name: "#000000" for name in freq_dict.keys()}

    # 3. Generate word cloud from frequencies
    wc = WordCloud(
        width=800,
        height=400,
        scale=2,  # higher-res rendering to reduce blur
        background_color='white',
        stopwords=STOPWORDS,
        max_words=100,
        # colormap is effectively overridden by colour func below
        colormap='viridis',
        prefer_horizontal=0.1,  # mix of horizontal & vertical words
    ).generate_from_frequencies(freq_dict)

    # 3b. Apply per-word colours from colour_lookup
    def colour_func(word, font_size, position, orientation, font_path, random_state):
        # If a colour is defined for this word, use it; else default to black
        return colour_lookup.get(word, "#000000")

    wc = wc.recolor(color_func=colour_func)

    # 4. Save SVG and PNGs to disk
    # Use a timestamped base name so multiple runs don't collide
    base_name = "stream_names"

    # 4a. Create SVG markup and write to file
    svg_xml = wc.to_svg()
    svg_filename = f"{base_name}.svg"
    svg_path = os.path.join(output_dir, svg_filename)
    with open(svg_path, "w", encoding="utf-8") as f:
        f.write(svg_xml)

    # ============= Returning Plotly figure for completion, we actually use the svg written to the file system for now ==============
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
