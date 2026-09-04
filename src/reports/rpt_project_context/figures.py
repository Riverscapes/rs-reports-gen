from __future__ import annotations

import json
import os

import geopandas as gpd
import numpy as np
import pandas as pd
import pint

from util.figures import common_statistics
from util.pandas import RSFieldMeta

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


def statistics(gdf: gpd.GeoDataFrame, tot_area: pint.Quantity) -> dict[str, pint.Quantity]:
    """Calculate and return key statistics as a dictionary
    Args:
        gdf (GeoDataFrame): data_gdf input WITH UNITS APPLIED
        tot_area (pint.Quantity): total area of the area of interest

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """
    df = gdf.copy()

    common_stats = common_statistics(df)
    # any statistics needed for this report specifically go here

    # Calculate totals
    total_segment_area = common_stats["total_segment_area"]
    total_centerline_length = common_stats["total_centerline_length"]
    proportion_riverscape = total_segment_area / tot_area if tot_area != 0 else float('nan') * total_segment_area.units / tot_area.units
    elevated_ratio = sum(df["elevated_ratio"] * (df["segment_area"] / 1000)) / total_segment_area if total_segment_area != 0 else float('nan') * total_segment_area.units / total_segment_area.units
    low_lying_ratio = sum(df["low_lying_ratio"] * (df["segment_area"] / 1000)) / total_segment_area if total_segment_area != 0 else float('nan') * total_segment_area.units / total_segment_area.units
    lf_agriculture_ratio = (df["lf_agriculture"].sum() / 1000) / total_segment_area if total_segment_area != 0 else float('nan') * total_segment_area.units / total_segment_area.units
    lf_developed_ratio = (df["lf_developed"].sum() / 1000) / total_segment_area if total_segment_area != 0 else float('nan') * total_segment_area.units / total_segment_area.units
    inaccessible_fldpln_ratio = 1 - ((df["access_fldpln_extent"].sum() / 1000) / total_segment_area) if total_segment_area != 0 else float('nan') * total_segment_area.units / total_segment_area.units

    if total_centerline_length != 0:
        integrated_valley_bottom_area_per_length = total_segment_area / total_centerline_length
        min_size = min(df["segment_area"] / df["centerline_length"])
        max_size = max(df["segment_area"] / df["centerline_length"])
    else:
        integrated_valley_bottom_area_per_length = float('nan') * total_segment_area.units / total_centerline_length.units
        min_size = float('nan') * total_segment_area.units / total_centerline_length.units
        max_size = float('nan') * total_segment_area.units / total_centerline_length.units

    RSFieldMeta().add_field_meta(
        name='proportion_riverscape',
        friendly_name='Proportion of Selected Area in Riverscape',
        data_unit='',
        dtype='REAL',
        description='Proportion of the selected area that is in the riverscape.',
        preferred_format='{:.1%}',
    )

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
        description='Proportion of the floodplain that is inaccessible due to transportation infrastructure.',
        preferred_format='{:.1%}',
    )
    RSFieldMeta().add_field_meta(
        name='min_size',
        friendly_name='Minimum Area per Length of Riverscape',
        data_unit='acre / mile' if RSFieldMeta().unit_system == 'imperial' else 'hectare / kilometer',
        dtype='REAL',
        description='Minimum segment area divided by its centerline length.',
    )
    RSFieldMeta().add_field_meta(
        name='max_size',
        friendly_name='Maximum Area per Length of Riverscape',
        data_unit='acre / mile' if RSFieldMeta().unit_system == 'imperial' else 'hectare / kilometer',
        dtype='REAL',
        description='Maximum segment area divided by its centerline length.',
    )
    RSFieldMeta().add_field_meta(
        name='owner_area',
        friendly_name='Primary Ownership/Administration of Riverscape',
        data_unit='acre' if RSFieldMeta().unit_system == 'imperial' else 'hectare',
        dtype='REAL',
        description='Area of riverscape owned/administered by the primary ownership/administration.',
    )
    RSFieldMeta().add_field_meta(
        name='anthro_lulc',
        friendly_name='Anthropogenic Land Cover',
        data_unit='',
        dtype='REAL',
        description='Proportion of the riverscape that is anthropogenic land cover (agriculture and developed).',
        preferred_format='{:.1%}',
    )

    for ratio_field in ['elevated_ratio', 'low_lying_ratio', 'lf_agriculture_prop', 'lf_developed_prop']:
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
        'proportion_riverscape': proportion_riverscape,
        'integrated_valley_bottom_area_per_length': integrated_valley_bottom_area_per_length.to('acre / mile' if RSFieldMeta().unit_system == 'imperial' else 'hectare / kilometer'),
        'min_size': min_size.to('acre / mile' if RSFieldMeta().unit_system == 'imperial' else 'hectare / kilometer'),
        'max_size': max_size.to('acre / mile' if RSFieldMeta().unit_system == 'imperial' else 'hectare / kilometer'),
        'elevated_ratio': elevated_ratio,
        'low_lying_ratio': low_lying_ratio,
        'lf_agriculture_prop': lf_agriculture_ratio,
        'lf_developed_prop': lf_developed_ratio,
        'anthro_lulc': lf_agriculture_ratio + lf_developed_ratio,
        'inaccessible_fldpln_ratio': inaccessible_fldpln_ratio,
        'owner_area': df.groupby("ownership_desc")["segment_area"].sum().max(),
        'ownership_desc': df.groupby("ownership_desc")["segment_area"].sum().idxmax(),
        'perennial_classification': df.groupby("perennial_classification")["segment_area"].sum().idxmax(),
        'stream_name': df.groupby("stream_name")["segment_area"].sum().idxmax(),
        'drainage_area': df['drainage_area'].max(),
        'stream_order': df['stream_order'].max(),
        'min_gradient': df['prim_channel_gradient'].min(),
        'max_gradient': df['prim_channel_gradient'].max(),
        'prim_channel_gradient': (df['elevation'].max() - df['elevation'].min()) / common_stats['total_stream_length'].to('m')
        if common_stats['total_stream_length'] != 0
        else float('nan') * df['elevation'].max().units / common_stats['total_stream_length'].units,
        'planform_sinuosity': common_stats['total_stream_length'] / common_stats['total_centerline_length']
        if common_stats['total_centerline_length'] != 0
        else float('nan') * common_stats['total_stream_length'].units / common_stats['total_centerline_length'].units,
        'confinement_ratio': sum(df['confinement_ratio'] * (df['segment_area'] / total_segment_area.to('m ** 2'))) if total_segment_area != 0 else float('nan') * df['confinement_ratio'].max().units,
        'hist_riparian': df['lf_hist_riparian'].sum() / total_segment_area.to('m ** 2') if total_segment_area != 0 else float('nan') * df['lf_hist_riparian'].max().units / total_segment_area.units,
        'ex_riparian': df['lf_riparian'].sum() / total_segment_area.to('m ** 2') if total_segment_area != 0 else float('nan') * df['lf_riparian'].max().units / total_segment_area.units,
        'hist_brat_cap': sum(df['brat_hist_capacity'] * df['centerline_length']),
        'ex_brat_cap': sum(df['brat_capacity'] * df['centerline_length']),
    }
    return stats
