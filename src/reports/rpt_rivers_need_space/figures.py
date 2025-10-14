from __future__ import annotations
from typing import List
from ast import Tuple
import json
import os
import plotly.graph_objects as go
import plotly.express as px
import geopandas as gpd
import pint
import pandas as pd
import numpy as np

from rsxml import Logger
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.figures import format_value

# assume pint registry has been set up already

# =========================
# Helpers
# =========================


def get_bins_legend(key: str) -> list:
    bins_path = os.path.join(os.path.dirname(__file__), "bins.json")
    with open(bins_path, "r") as f:
        bins_dict = json.load(f)
    return bins_dict[key]

# =========================
# Figures - generate specific figures
# take a (geo)dataframe and return a plotly graph object
# =========================


# def low_lying_ratio_bins(df: pd.DataFrame) -> go.Figure:
#     # "legend" array from https://github.com/Riverscapes/RiverscapesXML/blob/master/Symbology/web/Shared/Low_Lying_Ratio.json
#     bins_json = """[
#     ["rgb(247, 252, 245)", "< 2%"],
#     ["rgb(226, 244, 221)", "2% to 5%"],
#     ["rgb(192, 230, 185)", "5% to 10%"],
#     ["rgb(148, 211, 144)", "10% to 15%"],
#     ["rgb(96, 186, 108)", "15% to 25%"],
#     ["rgb(50, 155, 81)", "25% to 50%"],
#     ["rgb(12, 120, 53)", "50% to 75%"],
#     ["rgb(0, 68, 27)", "> 75%"]
#     ]"""

#     chart_data = df[['low_lying_ratio', 'segment_area']].copy()
#     bins = [0, 0.02, 0.05, 0.10, 0.15, 0.25, 0.50, 0.75, 1]
#     labels = extract_labels_from_legend(bins_json)
#     colours = extract_colours_from_legend(bins_json)
#     # Bin the low_lying_ratio values - cut creates Categorical dtype
#     chart_data['bin'] = pd.cut(chart_data['low_lying_ratio'], bins=bins, labels=labels, include_lowest=True)
#     # Aggregate segment_area by bin
#     agg_data = chart_data.groupby('bin', as_index=False, observed=False)['segment_area'].sum()

#     baked_header_lookup = RSFieldMeta().get_headers_dict(agg_data)
#     baked_agg_data, baked_headers = RSFieldMeta().bake_units(agg_data)

#     baked_header_lookup['bin'] = 'Low Lying Ratio'
#     baked_header_lookup['segment_area'] = 'Total Riverscape Area'

#     fig = px.bar(
#         baked_agg_data,
#         x='bin',
#         y='segment_area',
#         color='bin',
#         color_discrete_sequence=colours,
#         title='Total Riverscape Area by Low Lying Ratio Bin',
#         labels=baked_header_lookup,
#         height=400
#     )
#     fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
#     return fig


# def prop_riparian_bins(df: pd.DataFrame) -> go.Figure:
#     """NOT NEEDED JUST KEEPING TO SHOW WHAT WE DID"""
#     # "legend" array from https://github.com/Riverscapes/RiverscapesXML/blob/master/Symbology/web/Shared/Prop_Rip.json
#     bins_json = """[
#     ["rgb(67, 41, 0)", "0%"],
#     ["rgb(98, 73, 0)", "0 - 5%"],
#     ["rgb(89, 83, 0)", "5 - 15%"],
#     ["rgb(76, 93, 0)", "15 - 30%"],
#     ["rgb(53, 103, 0)", "30 - 60%"],
#     ["rgb(9, 112, 0)", "> 60%"]
#     ]"""

#     chart_data = df[['lf_riparian_prop', 'segment_area']].copy()
#     bins = [0, 0.000000001, 0.05, 0.15, 0.3, 0.6, 1]
#     labels = extract_labels_from_legend(bins_json)
#     colours = extract_colours_from_legend(bins_json)
#     # Bin the low_lying_ratio values - cut creates Categorical dtype
#     chart_data['bin'] = pd.cut(chart_data['lf_riparian_prop'], bins=bins, labels=labels, include_lowest=True)
#     # Aggregate segment_area by bin
#     agg_data = chart_data.groupby('bin', as_index=False, observed=False)['segment_area'].sum()
#     # Plot bar chart
#     baked_header_lookup = RSFieldMeta().get_headers_dict(agg_data)
#     baked_agg_data, baked_headers = RSFieldMeta().bake_units(agg_data)    # Plot bar chart

#     baked_header_lookup['bin'] = 'Low Lying Ratio'
#     baked_header_lookup['segment_area'] = 'Total Riverscape Area'

#     fig = px.bar(
#         baked_agg_data,
#         x='bin',
#         y='segment_area',
#         color='bin',
#         color_discrete_sequence=colours,
#         title='Total Riverscape Area by Proportion Riparian Bin',
#         labels=baked_header_lookup,
#         height=400
#     )
#     fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
#     return fig


# def make_rs_area_by_owner(gdf: gpd.GeoDataFrame) -> go.Figure:
#     """ Create bar chart of total segment area by ownership

#     Args:
#         gdf (GeoDataFrame): _geodataframe with 'ownership' and 'segment_area' columns_

#     Returns:
#         _type_: _plotly figure object_
#     """
#     # Create horizontal bar chart (sum of segment_area by ownership)
#     chart_data = gdf.groupby('ownership_desc', as_index=False)['segment_area'].sum()

#     baked_header_lookup = RSFieldMeta().get_headers_dict(chart_data)
#     baked_chart_data, baked_headers = RSFieldMeta().bake_units(chart_data)

#     bar_fig = px.bar(
#         baked_chart_data,
#         y="ownership_desc",
#         x="segment_area",
#         orientation="h",
#         title="Total Riverscape Area by Ownership",
#         labels=baked_header_lookup,
#         height=400
#     )
#     bar_fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
#     bar_fig.update_xaxes(tickformat=",")
#     return bar_fig


def format_hover(df: pd.DataFrame, nice_headers: List[str]) -> str:
    """
    Generate a Plotly hovertemplate based on column dtypes.
    Returns a string suitable for `update_traces(hovertemplate=...)`.
    """
    lines = []
    for i, (col, dtype) in enumerate(df.dtypes.items()):
        col_name = nice_headers[i]
        if np.issubdtype(dtype, np.number):
            lines.append(f"{col_name}: %{{customdata[{i}]:,.2f}}")
        elif np.issubdtype(dtype, np.datetime64):
            lines.append(f"{col_name}: %{{customdata[{i}]|%Y-%m-%d %H:%M}}")
        else:
            lines.append(f"{col_name}: %{{customdata[{i}]}}")

    hover = "<br>".join(lines) + "<extra></extra>"
    return hover
