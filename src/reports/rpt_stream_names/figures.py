"""Figure generation for Stream Names Report"""

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from wordcloud import WordCloud

from reports.rpt_stream_names.colour_gradient import stream_colour_from_order


def word_cloud_data(df: pd.DataFrame, frequency_field: str) -> pd.DataFrame:
    """process to go from raw df to just what we need"""
    df_copy = df[['stream_name', frequency_field, 'max_stream_order']].copy()
    df_copy['stream_order_colour'] = df_copy['max_stream_order'].apply(stream_colour_from_order)

    return df_copy


def word_cloud(indf: pd.DataFrame, output_dir: Path, frequency_field: str):
    """
    Generate a word cloud from stream names and:

      * write one SVG to `output_dir` named stream_names.svg

    Args:
        indf (pd.DataFrame): Input dataframe with at least stream_name, {frequency_field}, max_stream_order
        output_dir (str): Directory where SVG and PNG files will be written (assumed to already exist)

    Returns:
        str: HTML <img> tag pointing at the generated SVG (relative filename)
    """
    print('WORD CLOUD')
    # print(indf)  # debug only

    # 1. Prepare aggregated / unit-baked data
    df = word_cloud_data(indf, frequency_field)

    # Safety checks
    if df.empty or 'stream_name' not in df.columns or frequency_field not in df.columns:
        # Fallback tiny dummy word cloud so the report doesn't break
        freq_dict = {"no_stream_names": 1.0}
        colour_lookup = {"no_stream_names": "#000000"}
    else:
        # 2. Build frequency dict: { stream_name: {frequency_field} }
        #    Assumes frequency field is already aggregated & unit-baked (we don't need units).
        freq_series = df.dropna(subset=['stream_name', frequency_field]).set_index('stream_name')[frequency_field]

        # Convert to plain dict with float values
        freq_dict = {name: float(val) for name, val in freq_series.items() if float(val) > 0}

        if not freq_dict:
            freq_dict = {"no_stream_names": 1.0}

        # print(freq_dict)  # debug only

        # 2b. Build colour lookup dict: { stream_name: stream_order_colour }
        if 'stream_order_colour' in df.columns:
            colour_lookup = {row['stream_name']: row['stream_order_colour'] for _, row in df.iterrows() if isinstance(row['stream_order_colour'], str)}
        else:
            # Fallback: default to black if no colour column
            colour_lookup = {name: "#000000" for name in freq_dict.keys()}

    module_dir = Path(__file__).parent
    font_path = module_dir / "fonts" / "JetBrainsMono-VariableFont_wght.ttf"

    # 3b. Apply per-word colours from colour_lookup
    def colour_func(word, **kwargs):
        # If a colour is defined for this word, use it; else default to black
        return colour_lookup.get(word, "#000000")

    # 3. Generate word cloud from frequencies
    # generate 3 different resolutions
    scales = [2, 5, 9]
    for scale in scales:
        wc = WordCloud(
            width=800,
            height=500,
            scale=scale,  # higher-res rendering to reduce blur but increase file size. 2 is too low and 10 too high
            font_path=font_path,
            background_color='white',
            stopwords=None,  # ignored anyway since we are using generate_from_frequencies
            max_words=200,  # this is the default anyway
            prefer_horizontal=0.1,  # mix of horizontal & vertical words
        ).generate_from_frequencies(freq_dict)

        wc = wc.recolor(color_func=colour_func)

        # 4. Save to disk
        outputfilename = f"stream_names_{frequency_field}_scale{scale}.png"

        # PNG output
        wc.to_file(output_dir / outputfilename)

    # 4a. Create SVG markup and write to file
    # SVG doesn't look good, unless we embed the same font somehow
    # svg_xml = wc.to_svg()
    # svg_path = output_dir / f"{base_name}.svg"
    # with open(svg_path, "w", encoding="utf-8") as f:
    #     f.write(svg_xml)


def aoi_polygon_svg(query_gdf: gpd.GeoDataFrame, output_dir: Path) -> Path:
    """Render the simplified AOI polygon as an SVG file for inclusion in the report.

    Draws a clean, minimal outline of the polygon geometry — no basemap, no axes,
    just the shape itself. The SVG is written to ``output_dir/aoi_polygon.svg``.

    Args:
        query_gdf (gpd.GeoDataFrame): Simplified GeoDataFrame used as the Athena
            query polygon.  Must have a valid geometry column.
        output_dir (Path): Directory where the SVG will be written (must already
            exist).

    Returns:
        Path: Absolute path to the written SVG file.

    Created by copilot.
    """
    # Reproject to WGS-84 for consistent aspect ratio, if not already
    gdf = query_gdf.to_crs(epsg=4326) if query_gdf.crs is not None else query_gdf

    fig, ax = plt.subplots(figsize=(4, 4))
    gdf.plot(
        ax=ax,
        facecolor=(1, 1, 1, 0.15),  # semi-transparent white fill — legible on dark header
        edgecolor="white",
        linewidth=1.5,
    )
    ax.set_aspect("equal")
    ax.axis("off")
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)

    svg_path = output_dir / "aoi_polygon.svg"
    fig.savefig(svg_path, format="svg", bbox_inches="tight", transparent=True)
    plt.close(fig)
    return svg_path
