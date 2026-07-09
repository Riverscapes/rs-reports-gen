"""Data preparation for the Beaver Restoration Potential report.

Created 2026-07-07.
Created by copilot.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pint_pandas
from rsxml import Logger

from util.athena import aoi_query_to_local_parquet
from util.figures import get_bins_info
from util.pandas import load_gdf_from_pq

RPT_RME_LAYER_ID = "rpt_beaver_restoration_potential"
SUMMARY_TOTAL_FIELD = "segment_area"
SUMMARY_COUNT_FIELD = "segment_count"

BEAVER_FIELDS = (
    "level_path, seg_distance, centerline_length, segment_area, stream_name, stream_order, "
    "fcode, fcode_desc, ownership, ownership_desc, huc10, "
    "brat_capacity, brat_hist_capacity, brat_opportunity, brat_limitation, brat_risk, dam_setting, dam_ct, dam_density"
)


def query_beaver_data_for_aoi(aoi_gdf: gpd.GeoDataFrame, staging_path: Path) -> pd.DataFrame:
    """Query Athena for beaver-focused RME fields intersecting the AOI."""
    log = Logger("QueryBeaverData")
    log.info("Querying Athena for Beaver Restoration Potential data ...")

    query_template = f"SELECT {BEAVER_FIELDS} FROM input_geom, rs_rpt.nasa_ba_rme_join WHERE {{prefilter_condition}} AND {{intersects_condition}}"
    aoi_query_to_local_parquet(
        query_template,
        geometry_field_expression="ST_GeomFromBinary(geom_wkb)",
        geom_bbox_field=None,
        aoi_gdf=aoi_gdf,
        local_path=staging_path,
    )
    df = load_cached_beaver_data(staging_path)
    log.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from staging parquet")
    return df


def load_cached_beaver_data(parquet_path: Path) -> pd.DataFrame:
    """Load cached parquet output from a previous AOI query."""
    df = load_gdf_from_pq(parquet_path)
    df.attrs["layer_id"] = RPT_RME_LAYER_ID
    return df


def _sum_metric_by_group(df: pd.DataFrame, group_field: str, metric_field: str = SUMMARY_TOTAL_FIELD) -> pd.DataFrame:
    """Aggregate metric totals and segment count by a categorical group."""
    if group_field not in df.columns or metric_field not in df.columns:
        return pd.DataFrame(columns=["group", metric_field, SUMMARY_COUNT_FIELD])

    summary_df = df[[group_field, metric_field]].copy()
    summary_df[group_field] = summary_df[group_field].fillna("Unknown").astype(str)
    if isinstance(summary_df[metric_field].dtype, pint_pandas.PintType):
        summary_df[metric_field] = summary_df[metric_field].pint.magnitude
    summary_df[metric_field] = pd.to_numeric(summary_df[metric_field], errors="coerce").fillna(0)

    summary_df = (
        summary_df.groupby(group_field, as_index=False)
        .agg(
            **{
                metric_field: (metric_field, "sum"),
                SUMMARY_COUNT_FIELD: (metric_field, "size"),
            }
        )
        .rename(columns={group_field: "group"})
        .sort_values(metric_field, ascending=False)
    )
    summary_df.attrs["layer_id"] = RPT_RME_LAYER_ID
    summary_df.attrs["group_field"] = group_field
    summary_df.attrs["total_field"] = metric_field
    return summary_df


def _sum_metric_by_binned_numeric(
    df: pd.DataFrame,
    value_field: str,
    bin_lookup: str,
    metric_field: str = SUMMARY_TOTAL_FIELD,
) -> pd.DataFrame:
    """Aggregate metric totals by bins.json categories for a numeric field."""
    if value_field not in df.columns or metric_field not in df.columns:
        return pd.DataFrame(columns=["group", metric_field, SUMMARY_COUNT_FIELD])

    summary_df = df[[value_field, metric_field]].copy()

    if isinstance(summary_df[value_field].dtype, pint_pandas.PintType):
        summary_df[value_field] = summary_df[value_field].pint.magnitude
    if isinstance(summary_df[metric_field].dtype, pint_pandas.PintType):
        summary_df[metric_field] = summary_df[metric_field].pint.magnitude

    summary_df[value_field] = pd.to_numeric(summary_df[value_field], errors="coerce")
    summary_df[metric_field] = pd.to_numeric(summary_df[metric_field], errors="coerce").fillna(0)

    edges, labels, _colours = get_bins_info(bin_lookup)
    bins = pd.cut(summary_df[value_field], bins=edges, labels=labels, include_lowest=True)
    summary_df["group"] = bins.astype(object).where(bins.notna(), "Out of Range / Unknown").astype(str)

    summary_df = (
        summary_df.groupby("group", as_index=False)
        .agg(
            **{
                metric_field: (metric_field, "sum"),
                SUMMARY_COUNT_FIELD: (metric_field, "size"),
            }
        )
        .sort_values(metric_field, ascending=False)
    )
    summary_df.attrs["layer_id"] = RPT_RME_LAYER_ID
    summary_df.attrs["group_field"] = value_field
    summary_df.attrs["total_field"] = metric_field
    return summary_df


def summarize_beaver_potential(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Create simple summary tables used by the Beaver Restoration Potential stub report."""
    if df.empty:
        empty = pd.DataFrame(columns=["group", SUMMARY_TOTAL_FIELD, SUMMARY_COUNT_FIELD])
        empty.attrs["layer_id"] = RPT_RME_LAYER_ID
        empty.attrs["total_field"] = SUMMARY_TOTAL_FIELD
        return {
            "capacity": empty.copy(),
            "opportunity": empty.copy(),
            "limitation": empty.copy(),
            "risk": empty.copy(),
        }

    return {
        "capacity": _sum_metric_by_binned_numeric(df, "brat_capacity", "brat_capacity"),
        "opportunity": _sum_metric_by_group(df, "brat_opportunity"),
        "limitation": _sum_metric_by_group(df, "brat_limitation"),
        "risk": _sum_metric_by_group(df, "brat_risk"),
    }
