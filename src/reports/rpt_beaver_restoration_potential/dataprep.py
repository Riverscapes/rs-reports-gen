"""Data preparation for the Beaver Restoration Potential report.

Created 2026-07-07.
Created by copilot.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
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
