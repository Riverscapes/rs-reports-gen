"""data preparation for Rivers Need Space"""

import pandas as pd

from util.athena.athena import aoi_query_to_local_parquet
from util.binning import get_bins_info
from util.pandas import RSFieldMeta


def get_parquet_data(query_gdf, parquet_data_source):

    fields_we_need = "level_path, seg_distance, centerline_length, segment_area, fcode, fcode_desc, longitude, latitude, ownership, ownership_desc, state, county, drainage_area, stream_name, stream_order, stream_length, huc12, rel_flow_length, channel_area, integrated_width, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, lf_agriculture_prop, lf_agriculture, lf_developed_prop, lf_developed, lf_riparian_prop, lf_riparian, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, develop, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent, confinement_ratio, brat_capacity, brat_hist_capacity, riparian_veg_departure, riparian_condition, rme_project_id, rme_project_name, dgo_geom AS dgo_polygon_geom"
    query_str = f"SELECT {fields_we_need} FROM input_geom, rpt_rme_pq WHERE {{prefilter_condition}} AND {{intersects_condition}}"

    aoi_query_to_local_parquet(query_str, geometry_field_expression='ST_GeomFromBinary(dgo_geom)', geom_bbox_field='dgo_geom_bbox', aoi_gdf=query_gdf, local_path=parquet_data_source)


def add_calculated_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Add any calculated columns to the dataframe
    These could be bins.
    When adding columns to this function, add metadata at the same time
    Args:
        df (pd.DataFrame): Input dataframe

    Returns:
        pd.DataFrame: DataFrame with calculated columns added
    """
    # Example:
    # df['channel_length'] = df['rel_flow_length']*df['centerline_length']
    # RSFieldMeta().add_field_meta(name='channel_length',
    #                              friendly_name='Channel Length',
    #                              data_unit='m',
    #                              dtype='REAL'
    #                              )
    meta = RSFieldMeta()

    # bin

    unbinnedfldnm = 'riparian_veg_departure'
    binnedflnm = unbinnedfldnm + '_bins'  # default
    binned_friendly_nm = 'Riparian Vegetation Departure'  # non-default
    binlookupnm = unbinnedfldnm  # default
    edges, labels, colours = get_bins_info(binlookupnm)

    df[binnedflnm] = pd.cut(df[unbinnedfldnm], bins=edges, labels=labels, include_lowest=True)
    meta.add_field_meta(name=binnedflnm, friendly_name=binned_friendly_nm)  # the type usually be categorical text

    return df
