import pandas as pd
import geopandas as gpd
from rsxml import Logger
from util.athena.athena import get_anydata_for_aoi


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
SELECT stream_name, round(sum(centerline_length),0) AS total_riverscape_length, max(stream_order) AS max_stream_order, count(distinct level_path) as level_path_count, round(sum(segment_area) / sum(centerline_length),1) as rs_area_per_length
FROM raw_rme_pq2
{prefilter_clause} AND {intersects_clause} AND (stream_name IS NOT NULL)
GROUP BY stream_name
"""
    df = get_anydata_for_aoi(querystr, geom_field_clause, geom_bbox_field, aoi_gdf)
    if df.empty:
        df = pd.DataFrame(
            columns=["stream_name", "total_riverscape_length", "max_stream_order", "level_path_count", "rs_area_per_length"],
            data=[["No stream names found", 10.0, 3, 1]]
        )
        df["stream_name"] = df["stream_name"].astype(str)
        df["total_riverscape_length"] = df["total_riverscape_length"].astype(float)
        df["max_stream_order"] = df["max_stream_order"].astype(int)
        df["level_path_count"] = df["level_path_count"].astype(int)
        df["rs_area_per_length"] = df["rs_area_per_length"].astype(float)
    return df
