"""Figure generation for Stream Names Report
"""
from pathlib import Path
import pandas as pd
from wordcloud import WordCloud

from reports.rpt_stream_names.colour_gradient import stream_colour_from_order


def word_cloud_data(df: pd.DataFrame, frequency_field: str) -> pd.DataFrame:
    """process to go from raw df to just what we need"""
    df_copy = df[['stream_name', frequency_field, 'max_stream_order']].copy()
    df_copy['stream_order_colour'] = df_copy['max_stream_order'].apply(
        stream_colour_from_order
    )

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
        freq_series = (
            df
            .dropna(subset=['stream_name', frequency_field])
            .set_index('stream_name')[frequency_field]
        )

        # Convert to plain dict with float values
        freq_dict = {
            name: float(val)
            for name, val in freq_series.items()
            if float(val) > 0
        }

        if not freq_dict:
            freq_dict = {"no_stream_names": 1.0}

        print(freq_dict)  # debug only

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
