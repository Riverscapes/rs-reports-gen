"""Data preparation for the Beaver Restoration Potential report.

Created 2026-07-07.
Created by copilot.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pint_pandas
from rsxml import Logger

from util.athena import aoi_query_to_dataframe, aoi_query_to_local_parquet
from util.pandas import load_gdf_from_pq
from util.summary import summarize_metric_by_binned_numeric, summarize_metric_by_group

RPT_RME_LAYER_ID = "rpt_beaver_restoration_potential"
SUMMARY_TOTAL_FIELD = "segment_area"
SUMMARY_COUNT_FIELD = "segment_count"

BEAVER_FIELDS = (
    "level_path, seg_distance, centerline_length, segment_area, stream_name, stream_order, fcode, fcode_desc, ownership, ownership_desc, huc10, brat_capacity, brat_hist_capacity, brat_opportunity, brat_limitation, brat_risk, dam_setting"
)

# Actual, field-verified beaver dam locations (as opposed to the modeled dam_ct field on the RME join above).
ACTUAL_DAM_TABLE = "rs_rpt.qris_beaver_activity"


def query_beaver_data_for_aoi(aoi_gdf: gpd.GeoDataFrame, staging_path: Path) -> pd.DataFrame:
    """Query Athena for beaver-focused RME fields intersecting the AOI."""
    log = Logger("QueryBeaverData")
    log.info("Querying Athena for Beaver Restoration Potential data ...")

    query_template = f"SELECT {BEAVER_FIELDS} FROM input_geom, rs_rpt.rme_datamart_base_vw WHERE {{prefilter_condition}} AND {{intersects_condition}}"
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


def query_actual_dam_points_for_aoi(aoi_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Query Athena for actual (surveyed) beaver dam points intersecting the AOI.

    Each matched point is spatially joined to the RME segment it falls within so the
    resulting rows can be grouped by level_path/huc10, in addition to computing an
    overall total (``len(result)``). qris_beaver_activity has no bbox field of its own, so
    the AOI prefilter/intersects test is applied to the joined RME segment (rme_datamart_base_vw)
    instead of the dam point itself.
    """
    log = Logger("QueryActualDamPoints")
    log.info("Querying Athena for actual beaver dam locations intersecting AOI ...")

    query_template = (
        "SELECT r.level_path AS level_path, r.huc10 AS huc10, r.fcode as fcode, r.brat_opportunity as brat_opportunity, q.geom_wkb AS geom_wkb "
        "FROM input_geom, rs_rpt.rme_datamart_base_vw r "
        f"JOIN {ACTUAL_DAM_TABLE} q ON ST_Intersects(ST_GeomFromBinary(q.geom_wkb), ST_GeomFromBinary(r.dgo_geom)) "
        "WHERE {prefilter_condition} AND {intersects_condition}"
    )
    df = aoi_query_to_dataframe(
        query_template,
        geometry_field_expression="ST_GeomFromBinary(r.dgo_geom)",
        geom_bbox_field="r.dgo_geom_bbox",
        aoi_gdf=aoi_gdf,
    )
    log.info(f"Found {len(df)} actual dam points intersecting AOI")
    return df


# Cached in its own subdirectory so it isn't picked up by list_athena_unload_payload_files
# when it falls back to listing every file alongside the RME unload's own parquet/manifest.
ACTUAL_DAM_POINTS_CACHE_DIRNAME = "actual_dam_points"
ACTUAL_DAM_POINTS_CACHE_FILENAME = "actual_dam_points.parquet"


def save_actual_dam_points(df: pd.DataFrame, staging_path: Path) -> None:
    """Cache actual dam points alongside RME staging data so cached/offline runs can reuse them."""
    cache_dir = staging_path / ACTUAL_DAM_POINTS_CACHE_DIRNAME
    cache_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_dir / ACTUAL_DAM_POINTS_CACHE_FILENAME, index=False)


def load_cached_actual_dam_points(parquet_path: Path) -> gpd.GeoDataFrame | None:
    """Load actual dam points cached by save_actual_dam_points, or None if no cache is present."""
    cache_file = parquet_path / ACTUAL_DAM_POINTS_CACHE_DIRNAME / ACTUAL_DAM_POINTS_CACHE_FILENAME
    if not cache_file.exists():
        return None
    return load_gdf_from_pq(cache_file, geometry_col="geom_wkb")


def _summarize_by_group(df: pd.DataFrame, group_field: str, actual_dam_counts: pd.Series | None = None, *, include_name_field: bool = True) -> pd.DataFrame:
    """Summarize dam counts and modeled capacity by a grouping field.

    actual_dam_counts: Series of actual (surveyed) dam counts indexed by group_field value,
    e.g. from grouping the output of query_actual_dam_points_for_aoi. Missing groups default
    to 0. If None (the actual dam query was not run, e.g. cached/offline mode), falls back to
    summing the modeled dam_ct field on df.
    """
    output_columns = [group_field, "dam_ct", "dam_capacity", "percent_capacity"]
    if include_name_field:
        output_columns.insert(1, "stream_name")

    if df.empty:
        empty = pd.DataFrame(columns=output_columns)
        empty.attrs["layer_id"] = RPT_RME_LAYER_ID
        empty.attrs["total_field"] = "dam_capacity"
        return empty

    required_columns = {group_field, "brat_capacity", "centerline_length"}
    if actual_dam_counts is None:
        required_columns.add("dam_ct")
    if include_name_field:
        required_columns.add("stream_name")
    missing_columns = required_columns.difference(df.columns)
    if missing_columns:
        raise KeyError(f"Missing required columns for {group_field} summary: {sorted(missing_columns)}")

    summary_df = df[df['fcode'].isin([46006, 55800])].copy()  # Only perennial streams
    summary_df = summary_df.dropna(subset=[group_field])
    summary_df["dam_capacity"] = summary_df["brat_capacity"] * summary_df["centerline_length"]

    aggregations = {
        "dam_capacity": ("dam_capacity", "sum"),
    }
    if actual_dam_counts is None:
        aggregations["dam_ct"] = ("dam_ct", "sum")
    if include_name_field:
        aggregations = {
            "stream_name": ("stream_name", lambda series: series.dropna().iloc[0] if not series.dropna().empty else pd.NA),
            **aggregations,
        }

    result = summary_df.groupby(group_field, as_index=False, observed=False).agg(**aggregations)
    if actual_dam_counts is not None:
        result["dam_ct"] = result[group_field].map(actual_dam_counts).fillna(0).astype(int)
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


def summarize_by_level_path(df: pd.DataFrame, actual_dam_counts: pd.Series | None = None) -> pd.DataFrame:
    """Summarize beaver potential metrics by level path."""
    return _summarize_by_group(df, "level_path", actual_dam_counts)


def summarize_by_watershed(df: pd.DataFrame, actual_dam_counts: pd.Series | None = None) -> pd.DataFrame:
    """Summarize beaver potential metrics by watershed."""
    return _summarize_by_group(df, "huc10", actual_dam_counts, include_name_field=False)


def summarize_beaver_potential(df: pd.DataFrame, actual_dam_points: pd.DataFrame | None = None) -> dict[str, pd.DataFrame]:
    """Create simple summary tables used by the Beaver Restoration Potential stub report.

    actual_dam_points: output of query_actual_dam_points_for_aoi, one row per surveyed dam
    point with level_path/huc10 columns, used to attribute actual dam counts to groups. If
    None (e.g. cached/offline mode where the actual dam query was not run), group summaries
    fall back to summing the modeled dam_ct field.
    """
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

    if actual_dam_points is not None:
        level_path_dam_counts = actual_dam_points.groupby("level_path").size() if not actual_dam_points.empty else pd.Series(dtype="int64")
        huc10_dam_counts = actual_dam_points.groupby("huc10").size() if not actual_dam_points.empty else pd.Series(dtype="int64")
    else:
        level_path_dam_counts = None
        huc10_dam_counts = None

    return {
        "level_paths": summarize_by_level_path(df, level_path_dam_counts),
        "hucs": summarize_by_watershed(df, huc10_dam_counts),
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
