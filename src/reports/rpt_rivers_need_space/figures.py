from __future__ import annotations
from typing import List
import json
import os
import geopandas as gpd
import pint
import pandas as pd
import numpy as np

from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.figures import common_statistics

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
    Args:
        gdf (GeoDataFrame): data_gdf input WITH UNITS APPLIED

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """
    common_stats = common_statistics(gdf)
    # any statistics needed for this report specifically go here

    # Compose result dictionary
    stats = {
        **common_stats,
    }
    return stats
