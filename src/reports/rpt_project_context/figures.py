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


def statistics(gdf: gpd.GeoDataFrame) -> dict[str, pint.Quantity]:
    """Calculate and return key statistics as a dictionary
    Args:
        gdf (GeoDataFrame): data_gdf input WITH UNITS APPLIED

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """
    df = gdf.copy()

    common_stats = common_statistics(df)
    # any statistics needed for this report specifically go here

    if RSFieldMeta().unit_system == 'imperial':
        total_segment_area = common_stats["total_segment_area"].to('ft ** 2')
        total_centerline_length = common_stats["total_centerline_length"].to('ft')
        total_stream_length = common_stats["total_stream_length"].to('ft')
        valley_width = common_stats["integrated_valley_bottom_width"].to('ft')
    else:
        total_segment_area = common_stats["total_segment_area"].to('m ** 2')
        total_centerline_length = common_stats["total_centerline_length"].to('m')
        total_stream_length = common_stats["total_stream_length"].to('m')
        valley_width = common_stats["integrated_valley_bottom_width"].to('m')

    elevated_ratio = sum(df["elevated_area"]) / total_segment_area if total_segment_area != 0 else float('nan')
    low_lying_ratio = sum(df["lowlying_area"]) / total_segment_area if total_segment_area != 0 else float('nan')
    lf_agriculture_ratio = df["lf_agriculture"].sum() / total_segment_area if total_segment_area != 0 else float('nan')
    lf_developed_ratio = df["lf_developed"].sum() / total_segment_area if total_segment_area != 0 else float('nan')
    inaccessible_fldpln_ratio = 1 - (df["access_fldpln_extent"].sum() / total_segment_area) if total_segment_area != 0 else float('nan')
    confinement_ratio = (df["confinement_ratio"] * (df["segment_area"] / total_segment_area)).sum() if total_segment_area != 0 else float('nan')

    if total_centerline_length != 0:
        if RSFieldMeta().unit_system == 'imperial':
            integrated_valley_bottom_area_per_length = (total_segment_area / total_centerline_length).to('acre / mile')
            min_size = min(df["segment_area"] / df["centerline_length"]).to('acre / mile')
            max_size = max(df["segment_area"] / df["centerline_length"]).to('acre / mile')
        else:
            integrated_valley_bottom_area_per_length = (total_segment_area / total_centerline_length).to('hectare / kilometer')
            min_size = min(df["segment_area"] / df["centerline_length"]).to('hectare / kilometer')
            max_size = max(df["segment_area"] / df["centerline_length"]).to('hectare / kilometer')
    else:
        integrated_valley_bottom_area_per_length = float('nan')
        min_size = float('nan')
        max_size = float('nan')

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

    for ratio_field in ['elevated_ratio', 'low_lying_ratio', 'lf_agriculture_prop', 'lf_developed_prop', 'hist_riparian', 'ex_riparian']:
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

    for ratio_field in ['planform_sinuosity', 'confinement_ratio']:
        try:
            RSFieldMeta().set_preferred_format(ratio_field, '{:.2f}')
        except Exception:
            # Some schemas may omit a field; add a minimal metadata row so cards still format correctly.
            RSFieldMeta().add_field_meta(
                name=ratio_field,
                data_unit='',
                dtype='REAL',
                preferred_format='{:.2f}',
            )

    for gradient_field in ['prim_channel_gradient', 'min_gradient', 'max_gradient']:
        try:
            RSFieldMeta().set_preferred_format(gradient_field, '{:.2%}')
        except Exception:
            RSFieldMeta().add_field_meta(
                name=gradient_field,
                data_unit='',
                dtype='REAL',
                preferred_format='{:.2%}',
            )

    # Compose result dictionary
    stats = {
        'total_segment_area': total_segment_area,
        'total_centerline_length': total_centerline_length,
        'total_stream_length': total_stream_length,
        'integrated_valley_bottom_width': valley_width,
        'integrated_valley_bottom_area_per_length': integrated_valley_bottom_area_per_length,
        'min_size': min_size,
        'max_size': max_size,
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
        'min_gradient': df['prim_channel_gradient'].min() if not df['prim_channel_gradient'].empty else float('nan'),
        'max_gradient': df['prim_channel_gradient'].max() if not df['prim_channel_gradient'].empty else float('nan'),
        'prim_channel_gradient': (df['elevation'].max() - df['elevation'].min()) / total_stream_length if total_stream_length != 0 else float('nan'),
        'planform_sinuosity': total_stream_length / total_centerline_length if total_centerline_length != 0 else float('nan') * total_stream_length.units / total_centerline_length.units,
        'confinement_ratio': confinement_ratio,
        'hist_riparian': df['lf_hist_riparian'].sum() / total_segment_area if total_segment_area != 0 else float('nan'),
        'ex_riparian': df['lf_riparian'].sum() / total_segment_area if total_segment_area != 0 else float('nan'),
        'hist_brat_cap': sum(df['brat_hist_capacity'] * df['centerline_length']),
        'ex_brat_cap': sum(df['brat_capacity'] * df['centerline_length']),
    }
    return stats
