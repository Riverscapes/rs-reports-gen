from __future__ import annotations
from typing import List
import json
import os
import geopandas as gpd
import pint
import pandas as pd
import numpy as np

from util.pandas import RSFieldMeta, RSGeoDataFrame

# assume pint registry has been set up already

# =========================
# Helpers
# =========================


def get_bins_legend(key: str) -> list:
    """ Get 

    Args:
        key (str): _description_

    Returns:
        list: _description_
    """
    bins_path = os.path.join(os.path.dirname(__file__), "bins.json")
    with open(bins_path, "r", encoding="utf-8") as f:
        bins_dict = json.load(f)
    return bins_dict[key]


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


def statistics(gdf: gpd.GeoDataFrame) -> dict[str, pint.Quantity]:
    """ Calculate and return key statistics as a dictionary
    TODO: integrated should be calculated from the totals, not at row level
    Args:
        gdf (GeoDataFrame): data_gdf input WITH UNITS APPLIED

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """
    subset = RSGeoDataFrame(gdf[["segment_area", "centerline_length", "channel_length"]].copy())
    # Calculate totals
    total_segment_area = subset["segment_area"].sum()
    total_centerline_length = subset["centerline_length"].sum()
    total_channel_length = subset["channel_length"].sum()

    # Calculate integrated valley bottom width as ratio of totals
    integrated_valley_bottom_width = total_segment_area / total_centerline_length if total_centerline_length != 0 else float('nan')

    # if you want different units or descriptions then give them different names and add rsfieldmeta
    # Add field meta if not already present
    RSFieldMeta().add_field_meta(
        name='total_segment_area',
        friendly_name='Total Segment Area',
        data_unit='kilometer ** 2',
        dtype='REAL',
        description='Total area of all segments in the segment'
    )
    RSFieldMeta().add_field_meta(
        name='total_centerline_length',
        friendly_name='Total Centerline Length',
        data_unit='kilometer',
        dtype='REAL',
        description='Total length of all centerlines in the segment'
    )
    RSFieldMeta().add_field_meta(
        name='total_channel_length',
        friendly_name='Total Channel Length',
        data_unit='kilometer',
        dtype='REAL',
        description='Total length of all channels in the segment'
    )
    RSFieldMeta().add_field_meta(
        name='integrated_valley_bottom_width',
        friendly_name='Integrated Valley Bottom Width',
        data_unit='m',
        dtype='REAL',
        description='Total segment area divided by total centerline length, representing average valley bottom width'
    )

    # Compose result dictionary
    stats = {
        'total_segment_area': total_segment_area.to('kilometer ** 2'),  # acres and hectares will be interchangeable based on unit system
        'total_centerline_length': total_centerline_length.to('kilometer'),  # miles and km will be interchangeable based on unit system
        'total_channel_length': total_channel_length.to('kilometer'),  # miles and km will be interchangeable based on unit system
        # Here we specify yards (because yards converts to meters but meters converts to feet and we want yards for the imperial system)
        'integrated_valley_bottom_width': integrated_valley_bottom_width.to('yards'),
    }
    return stats
