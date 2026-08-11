from __future__ import annotations

import json
import os

import geopandas as gpd
import numpy as np
import pandas as pd
import pint

from util.figures import common_statistics
from util.pandas import RSFieldMeta, RSGeoDataFrame

# assume pint registry has been set up already

# =========================
# Helpers
# =========================


def get_bins_legend(key: str) -> list:
    """Get

    Args:
        key (str): _description_

    Returns:
        list: _description_
    """
    bins_path = os.path.join(os.path.dirname(__file__), "bins.json")
    with open(bins_path, encoding="utf-8") as f:
        bins_dict = json.load(f)
    return bins_dict[key]


def format_hover(df: pd.DataFrame, nice_headers: list[str]) -> str:
    """
    Generate a Plotly hovertemplate based on column dtypes.
    Returns a string suitable for `update_traces(hovertemplate=...)`.
    """
    lines = []
    for i, (_col, dtype) in enumerate(df.dtypes.items()):
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
    """Calculate and return key statistics as a dictionary
    Args:
        gdf (GeoDataFrame): data_gdf input WITH UNITS APPLIED

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """
    common_stats = common_statistics(gdf)
    # any statistics needed for this report specifically go here

    subset_df = RSGeoDataFrame(gdf[["segment_area", "centerline_length", "elevated_ratio", "lf_agriculture", "lf_developed", "access_fldpln_extent"]].copy())
    # Calculate totals
    total_segment_area = subset_df["segment_area"].sum()
    total_centerline_length = subset_df["centerline_length"].sum()
    elevated_ratio = sum(subset_df["elevated_ratio"] * subset_df["segment_area"]) / total_segment_area if total_segment_area != 0 else float('nan') * total_segment_area.units / total_segment_area.units
    lf_agriculture_ratio = subset_df["lf_agriculture"].sum() / total_segment_area if total_segment_area != 0 else float('nan') * total_segment_area.units / total_segment_area.units
    lf_developed_ratio = subset_df["lf_developed"].sum() / total_segment_area if total_segment_area != 0 else float('nan') * total_segment_area.units / total_segment_area.units
    inaccessible_fldpln_ratio = 1 - (subset_df["access_fldpln_extent"].sum() / total_segment_area) if total_segment_area != 0 else float('nan') * total_segment_area.units / total_segment_area.units

    if total_centerline_length != 0:
        integrated_valley_bottom_area_per_length = total_segment_area / total_centerline_length
    else:
        integrated_valley_bottom_area_per_length = float('nan') * total_segment_area.units / total_centerline_length.units

    RSFieldMeta().add_field_meta(
        name='integrated_valley_bottom_area_per_length',
        friendly_name='Area per Length of Riverscape',
        data_unit='acre / mile' if RSFieldMeta().unit_system == 'imperial' else 'hectare / kilometer',
        dtype='REAL',
        description='Total riverscape area divided by total riverscape length.',
    )
    RSFieldMeta().add_field_meta(
        name='inaccessible_fldpln_ratio',
        friendly_name='Inaccessible Floodplain Ratio',
        data_unit='',
        dtype='REAL',
        description='Proportion of the floodplain that is inaccessible.',
        preferred_format='{:.1%}',
    )

    for ratio_field in ['elevated_ratio', 'lf_agriculture_prop', 'lf_developed_prop']:
        try:
            RSFieldMeta().set_preferred_format(ratio_field, '{:.1%}')
        except Exception:
            # Some schemas may omit a field; add a minimal metadata row so cards still format correctly.
            RSFieldMeta().add_field_meta(
                name=ratio_field,
                data_unit='',
                dtype='REAL',
                preferred_format='{:.1%}',
            )

    # Compose result dictionary
    stats = {
        **common_stats,
        'integrated_valley_bottom_area_per_length': integrated_valley_bottom_area_per_length.to('acre / mile' if RSFieldMeta().unit_system == 'imperial' else 'hectare / kilometer'),
        'elevated_ratio': elevated_ratio,
        'lf_agriculture_prop': lf_agriculture_ratio,
        'lf_developed_prop': lf_developed_ratio,
        'inaccessible_fldpln_ratio': inaccessible_fldpln_ratio,
    }
    return stats
