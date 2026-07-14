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
from util.summary import summarize_metric_by_binned_numeric, summarize_metric_by_group

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


def _summarize_by_group(df: pd.DataFrame, group_field: str, *, include_name_field: bool = True) -> pd.DataFrame:
    """Summarize dam counts and capacity by a grouping field."""
    output_columns = [group_field, "dam_ct", "dam_capacity", "percent_capacity"]
    if include_name_field:
        output_columns.insert(1, "stream_name")

    if df.empty:
        empty = pd.DataFrame(columns=output_columns)
        empty.attrs["layer_id"] = RPT_RME_LAYER_ID
        empty.attrs["total_field"] = "dam_capacity"
        return empty

    required_columns = {group_field, "dam_ct", "brat_capacity", "centerline_length"}
    if include_name_field:
        required_columns.add("stream_name")
    missing_columns = required_columns.difference(df.columns)
    if missing_columns:
        raise KeyError(f"Missing required columns for {group_field} summary: {sorted(missing_columns)}")

    summary_df = df[list(required_columns)].copy()
    summary_df = summary_df.dropna(subset=[group_field])
    summary_df["dam_capacity"] = summary_df["brat_capacity"] * summary_df["centerline_length"]

    aggregations = {
        "dam_ct": ("dam_ct", "sum"),
        "dam_capacity": ("dam_capacity", "sum"),
    }
    if include_name_field:
        aggregations = {
            "stream_name": ("stream_name", lambda series: series.dropna().iloc[0] if not series.dropna().empty else pd.NA),
            **aggregations,
        }

    result = summary_df.groupby(group_field, as_index=False, observed=False).agg(**aggregations)
    result = result[result["dam_ct"] > 0].reset_index(drop=True)
    if isinstance(result["dam_capacity"].dtype, pint_pandas.PintType):
        result["dam_capacity"] = result["dam_capacity"].pint.magnitude
    percent_capacity = (result["dam_ct"] / result["dam_capacity"]).where(result["dam_capacity"] > 0)
    result["dam_capacity"] = result["dam_capacity"].round().astype(int)
    result["percent_capacity"] = percent_capacity.map(lambda value: f"{value:.1%}" if pd.notna(value) else pd.NA)
    result = result.sort_values("dam_capacity", ascending=False).reset_index(drop=True)
    result = result.loc[:, output_columns]

    result.attrs["layer_id"] = RPT_RME_LAYER_ID
    result.attrs["total_field"] = "dam_capacity"
    return result


def summarize_by_level_path(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize beaver potential metrics by level path."""
    return _summarize_by_group(df, "level_path")


def summarize_by_watershed(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize beaver potential metrics by watershed."""
    return _summarize_by_group(df, "huc10", include_name_field=False)


def summarize_beaver_potential(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Create simple summary tables used by the Beaver Restoration Potential stub report."""
    if df.empty:
        empty = pd.DataFrame(columns=["group", SUMMARY_TOTAL_FIELD, SUMMARY_COUNT_FIELD])
        empty.attrs["layer_id"] = RPT_RME_LAYER_ID
        empty.attrs["total_field"] = SUMMARY_TOTAL_FIELD
        level_paths = pd.DataFrame(columns=["level_path", "stream_name", "dam_ct", "dam_capacity", "percent_capacity"])
        level_paths.attrs["layer_id"] = RPT_RME_LAYER_ID
        level_paths.attrs["total_field"] = "dam_capacity"
        return {
            "level_paths": level_paths,
            "capacity": empty.copy(),
            "opportunity": empty.copy(),
            "limitation": empty.copy(),
            "risk": empty.copy(),
        }

    return {
        "level_paths": summarize_by_level_path(df),
        "hucs": summarize_by_watershed(df),
        "capacity": summarize_metric_by_binned_numeric(
            df,
            value_field="brat_capacity",
            bin_lookup="brat_capacity",
            metric_field=SUMMARY_TOTAL_FIELD,
            count_field=SUMMARY_COUNT_FIELD,
            layer_id=RPT_RME_LAYER_ID,
        ),
        "opportunity": summarize_metric_by_group(
            df,
            group_field="brat_opportunity",
            metric_field=SUMMARY_TOTAL_FIELD,
            count_field=SUMMARY_COUNT_FIELD,
            layer_id=RPT_RME_LAYER_ID,
        ),
        "limitation": summarize_metric_by_group(
            df,
            group_field="brat_limitation",
            metric_field=SUMMARY_TOTAL_FIELD,
            count_field=SUMMARY_COUNT_FIELD,
            layer_id=RPT_RME_LAYER_ID,
        ),
        "risk": summarize_metric_by_group(
            df,
            group_field="brat_risk",
            metric_field=SUMMARY_TOTAL_FIELD,
            count_field=SUMMARY_COUNT_FIELD,
            layer_id=RPT_RME_LAYER_ID,
        ),
    }
