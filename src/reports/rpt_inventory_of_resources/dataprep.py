"""Query and summarize Inventory of Resources data.

Created 2026-08-13.
Created by copilot.
"""

import geopandas as gpd
import pandas as pd

from util.athena import aoi_query_to_local_parquet

INVENTORY_FIELDS = "segment_area, centerline_length, watershed_id, ownership, ownership_desc, drainage_area, stream_name, stream_order, stream_length, waterbody_type, waterbody_type_desc, waterbody_extent, prim_channel_gradient, valleybottom_gradient, fcode, fcode_desc, confinement_ratio, constriction_ratio, lf_riparian_prop, rme_project_id, rme_project_name"


def data_for_aoi_to_parquet(aoi_gdf: gpd.GeoDataFrame, parquet_path: str) -> None:
    """Query inventory data for an AOI and save it to a local Parquet file.

    Args:
            aoi_gdf: Area of interest used to filter RME DGO records.
            parquet_path: Path to the output Parquet file.
    """
    query = f"""
SELECT {INVENTORY_FIELDS}
FROM input_geom, rs_rpt.rme_datamart_base_vw
WHERE {{prefilter_condition}} AND {{intersects_condition}}
"""
    return aoi_query_to_local_parquet(
        query,
        geometry_field_expression="ST_GeomFromBinary(dgo_geom)",
        geom_bbox_field="dgo_geom_bbox",
        aoi_gdf=aoi_gdf,
        local_path=parquet_path,
    )


def summarize_by_length(data_df: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """Aggregate stream length and record count by one or more inventory fields.

    Args:
            data_df: Normalized inventory data.
            group_columns: Fields that define each output row.

    Returns:
            Summary sorted by total stream length, then record count.
    """
    required_columns = [*group_columns, "stream_length"]
    missing_columns = [column for column in required_columns if column not in data_df]
    if missing_columns:
        raise ValueError(f"Inventory data is missing required columns: {', '.join(missing_columns)}")

    summary = (
        data_df.groupby(group_columns, dropna=False, as_index=False)
        .agg(stream_length=("stream_length", "sum"), record_count=("stream_length", "size"))
        .sort_values(["stream_length", "record_count", *group_columns], ascending=[False, False, *([True] * len(group_columns))], kind="mergesort")
        .reset_index(drop=True)
    )
    return summary


def build_report_summaries(data_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build the tables used by the Inventory of Resources report.

    Args:
            data_df: Normalized inventory data.

    Returns:
            Named report summary tables.
    """

    # summary_df = data_df.copy()
    # perennial = summary_df[summary_df["fcode"].isin([46006, 55800])]
    # non_perennial = summary_df[~summary_df["fcode"].isin([46006, 55800])]
    # perennial_row = [perennial["stream_length"].sum(), perennial[perennial['ownership']=='BLM']["stream_length"].sum()]

    return {
        "ownership": summarize_by_length(data_df, ["ownership_desc"]),
        "feature_type": summarize_by_length(data_df, ["fcode_desc"]),
        "stream": summarize_by_length(data_df, ["stream_name", "stream_order"]),
        "watershed": summarize_by_length(data_df, ["watershed_id"]),
        "waterbody": summarize_by_length(data_df, ["waterbody_type"]),
    }


def build_metric_cards(data_df: pd.DataFrame) -> dict[str, dict[str, str]]:
    """Build display-ready key indicators from normalized inventory data.

    Args:
            data_df: Normalized inventory data.

    Returns:
            Card payloads consumed by the shared metric-grid template macro.
    """
    total_length = data_df["stream_length"].sum(min_count=1) if "stream_length" in data_df else None
    watershed_count = data_df["watershed_id"].nunique() if "watershed_id" in data_df else 0
    named_stream_count = data_df.loc[data_df["stream_name"] != "Not specified", "stream_name"].nunique() if "stream_name" in data_df else 0
    median_drainage = data_df["drainage_area"].median() if "drainage_area" in data_df else None

    return {
        "records": {"title": "Inventory Records", "value": f"{len(data_df):,}", "details": "RME records intersecting the area of interest"},
        "stream_length": {"title": "Total Stream Length", "value": "Not available" if pd.isna(total_length) else f"{total_length:,.0f} m", "details": "Sum of reported stream lengths"},
        "watersheds": {"title": "Watersheds", "value": f"{watershed_count:,}", "details": "Distinct watershed identifiers represented"},
        "stream_names": {"title": "Named Streams", "value": f"{named_stream_count:,}", "details": "Distinct non-empty stream names"},
        "drainage_area": {"title": "Median Drainage Area", "value": "Not available" if pd.isna(median_drainage) else f"{median_drainage:,.2f}", "details": "Reported drainage-area value per inventory record"},
    }
