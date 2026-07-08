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
from util.pandas import load_gdf_from_pq

RPT_RME_LAYER_ID = "rpt_beaver_restoration_potential"
SUMMARY_LAYER_ID = "rpt_beaver_restoration_potential_summary"

BEAVER_FIELDS = (
    "level_path, seg_distance, centerline_length, segment_area, stream_name, stream_order, "
    "fcode_desc, ownership_desc, huc12, "
    "brat_capacity, brat_hist_capacity, brat_opportunity, brat_limitation, brat_risk, dam_setting, "
    "rme_project_id, rme_project_name"
)


def query_beaver_data_for_aoi(aoi_gdf: gpd.GeoDataFrame, staging_path: Path) -> pd.DataFrame:
    """Query Athena for beaver-focused RME fields intersecting the AOI."""
    log = Logger("QueryBeaverData")
    log.info("Querying Athena for Beaver Restoration Potential data ...")

    query_template = f"SELECT {BEAVER_FIELDS} FROM input_geom, rs_rpt.rpt_rme_intersections WHERE {{prefilter_condition}} AND {{intersects_condition}}"
    aoi_query_to_local_parquet(
        query_template,
        geometry_field_expression="ST_GeomFromBinary(dgo_geom)",
        geom_bbox_field="dgo_geom_bbox",
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


def _sum_centerline_by_group(df: pd.DataFrame, group_field: str) -> pd.DataFrame:
    """Aggregate centerline totals and segment count by a categorical group."""
    if group_field not in df.columns or "centerline_length" not in df.columns:
        return pd.DataFrame(columns=["group", "total_centerline_length", "segment_count"])

    summary_df = df[[group_field, "centerline_length"]].copy()
    summary_df[group_field] = summary_df[group_field].fillna("Unknown").astype(str)
    if isinstance(summary_df["centerline_length"].dtype, pint_pandas.PintType):
        summary_df["centerline_length"] = summary_df["centerline_length"].pint.magnitude
    summary_df["centerline_length"] = pd.to_numeric(summary_df["centerline_length"], errors="coerce").fillna(0)

    summary_df = (
        summary_df.groupby(group_field, as_index=False)
        .agg(
            total_centerline_length=("centerline_length", "sum"),
            segment_count=("centerline_length", "size"),
        )
        .rename(columns={group_field: "group"})
        .sort_values("total_centerline_length", ascending=False)
    )
    summary_df.attrs["layer_id"] = SUMMARY_LAYER_ID
    return summary_df


def summarize_beaver_potential(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Create simple summary tables used by the Beaver Restoration Potential stub report."""
    if df.empty:
        empty = pd.DataFrame(columns=["group", "total_centerline_length", "segment_count"])
        return {
            "capacity": empty.copy(),
            "opportunity": empty.copy(),
            "limitation": empty.copy(),
            "risk": empty.copy(),
        }

    return {
        "capacity": _sum_centerline_by_group(df, "brat_capacity"),
        "opportunity": _sum_centerline_by_group(df, "brat_opportunity"),
        "limitation": _sum_centerline_by_group(df, "brat_limitation"),
        "risk": _sum_centerline_by_group(df, "brat_risk"),
    }
