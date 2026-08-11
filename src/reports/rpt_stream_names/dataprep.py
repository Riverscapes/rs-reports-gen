from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pint
from rsxml import Logger

from util.athena import QueryStatus, aoi_query_to_dataframe_result
from util.figures import HighlightCard
from util.rs_geo_helpers import total_aoi_area_m2


def get_wcdata_for_aoi(aoi_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """get word cloud data for an area of interest
    and return dataframe
    -- stream order goes from 1 to 11.
    """
    log = Logger("Run AOI query on Athena WC edition")
    log.debug("Get word cloud data")
    geom_field_clause = "ST_GeomFromBinary(dgo_geom)"  # must be a geometry, not a WKT or WKB
    geom_bbox_field = "dgo_geom_bbox"
    querystr = """
SELECT stream_name, round(sum(centerline_length),0) AS total_riverscape_length, max(stream_order) AS max_stream_order, count(distinct level_path) as level_path_count
FROM input_geom, raw_rme_pq2
WHERE {prefilter_condition} AND {intersects_condition} AND (stream_name IS NOT NULL)
GROUP BY stream_name
"""
    query_result = aoi_query_to_dataframe_result(querystr, geom_field_clause, geom_bbox_field, aoi_gdf)

    if query_result.status == QueryStatus.ERROR:
        message = query_result.message or "Unknown Athena query error"
        raise RuntimeError(f"Failed to query stream-name data from Athena: {message}") from query_result.error

    df = query_result.data if query_result.data is not None else pd.DataFrame()
    if query_result.status == QueryStatus.EMPTY or df.empty:
        df = pd.DataFrame(columns=["stream_name", "total_riverscape_length", "max_stream_order", "level_path_count", "rs_area_per_length"], data=[["No stream names found", 10.0, 3, 1, 1.0]])
        df["stream_name"] = df["stream_name"].astype(str)
        df["total_riverscape_length"] = df["total_riverscape_length"].astype(float)
        df["max_stream_order"] = df["max_stream_order"].astype(int)
        df["level_path_count"] = df["level_path_count"].astype(int)
        df["rs_area_per_length"] = df["rs_area_per_length"].astype(float)
    return df


def build_highlight_cards_data(data_df: pd.DataFrame, unit_system: str = "SI") -> list[HighlightCard]:
    """Build highlight card payloads from the stream names dataframe.

    Produces two cards:
    - Most Repeated Name  : top name by distinct level-path count.
    - Most Riverscape Length : top name by total riverscape length.

    Args:
        data_df (pd.DataFrame): Stream names dataframe with columns
            ``stream_name``, ``level_path_count``, and ``total_riverscape_length``.
        unit_system (str): ``"SI"`` (km) or ``"imperial"`` (miles).

    Returns:
        list[dict]: List of HighlightCard-shaped dicts ready for the template.
    """
    if data_df.empty:
        return []

    total_paths = data_df["level_path_count"].sum()
    total_length_m = data_df["total_riverscape_length"].sum()

    # --- Most repeated name (by distinct level paths) ---
    # Sort matches table: level_path_count desc, total_riverscape_length desc, stream_name asc
    paths_sorted = data_df.sort_values(
        by=["level_path_count", "total_riverscape_length", "stream_name"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    top_by_paths = paths_sorted.iloc[0]
    paths_count: int = int(top_by_paths["level_path_count"])
    paths_pct: float = paths_count / total_paths * 100 if total_paths else 0.0
    paths_is_tie: bool = (data_df["level_path_count"] == paths_count).sum() > 1

    # --- Most riverscape length ---
    # Sort: total_riverscape_length desc, level_path_count desc, stream_name asc
    length_sorted = data_df.sort_values(
        by=["total_riverscape_length", "level_path_count", "stream_name"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    top_by_length = length_sorted.iloc[0]
    length_m: float = float(top_by_length["total_riverscape_length"])
    length_pct: float = length_m / total_length_m * 100 if total_length_m else 0.0
    length_is_tie: bool = (data_df["total_riverscape_length"] == length_m).sum() > 1

    if unit_system == "imperial":
        length_value: float = length_m / 1000 * 0.621371
        length_unit = "miles"
    else:
        length_value = length_m / 1000
        length_unit = "km"

    return [
        {
            "theme": "blue",
            "icon": "emoji_events",
            "header": "MOST REPEATED NAME",
            "primary_value": str(top_by_paths["stream_name"]) + (" (tie)" if paths_is_tie else ""),
            "secondary_stat": {
                "icon": "waves",
                "text": f"{paths_count:,} distinct named systems",
            },
            "footer": {
                "metric": f"{paths_pct:.1f}%",
                "label": "of all named systems",
            },
        },
        {
            "theme": "green",
            "icon": "water",
            "header": "MOST RIVERSCAPE LENGTH",
            "primary_value": str(top_by_length["stream_name"]) + (" (tie)" if length_is_tie else ""),
            "secondary_stat": {
                "icon": "straighten",
                "text": f"{length_value:,.1f} {length_unit}",
            },
            "footer": {
                "metric": f"{length_pct:.1f}%",
                "label": "of all named riverscape length",
            },
        },
    ]


def additional_stats(aoi_gdf: gpd.GeoDataFrame, df: pd.DataFrame) -> dict[str, pint.Quantity]:
    """summary statsitics including area of AOI

    # TODO: Additional stats:
    Total channel length
    Total riverscape length
    Total number of systems or level paths
    Named channel length
    Unnamed channel length
    Percentage of channel length named
    Percentage of riverscape length associated with a named stream
    Relative flow length = channel length ÷ riverscape length

    """
    aoi_area = total_aoi_area_m2(aoi_gdf)

    stats = {"aoi_area": aoi_area}
    return stats
