"""Data preparation for the Watershed Summary report.

Owns all Athena queries, field metadata registration, and derived-column
construction.  make_report_orchestrator in main.py calls these functions and
should not need to know the details of how data is fetched or described.

Created 2026-07-28.
"""

import geopandas as gpd
import pandas as pd
from rsxml import Logger

from util.athena import aoi_query_to_dataframe, get_field_metadata, query_to_dataframe
from util.athena.athena import get_aoi_geom_sql_expression
from util.pandas import RSFieldMeta
from util.rs_geo_helpers import prepare_gdf_for_athena

LAYER_ID = 'rpt_watershed_summary'


def get_intersecting_hucs(aoi_gdf: gpd.GeoDataFrame) -> list[str]:
    """Query wbdhu10_cleaned for the HUC10 codes that intersect the AOI polygon."""
    log = Logger("Get intersecting HUCs")
    query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
    if not simplification_results.success:
        raise ValueError("Unable to simplify input geometry sufficiently to intersect with HUC10 boundaries.")

    query_str = "SELECT huc10 FROM input_geom, wbdhu10_cleaned WHERE {prefilter_condition} AND {intersects_condition}"
    df = aoi_query_to_dataframe(query_str, geometry_field_expression='ST_GeomFromBinary(geometry)', geom_bbox_field='geometry_bbox', aoi_gdf=query_gdf)
    huc_list = sorted(df['huc10'].dropna().unique().tolist()) if not df.empty else []
    log.info(f"Found {len(huc_list)} intersecting HUC10(s).")
    return huc_list


def define_fields(unit_system: str = "SI") -> None:
    """Load base field metadata from Athena schema and set the unit system.

    Must be called once before any data-fetching or unit-conversion functions.
    """
    meta = RSFieldMeta()
    meta.field_meta = get_field_metadata(tool_schema_name=['rscontext_to_athena', 'rpt_rme'], layer_id=['rs_context_huc10', 'rpt_rme'])
    meta.unit_system = unit_system

    # Here's where we can set any preferred units that differ from the data unit
    # meta.set_display_unit('centerline_length', 'kilometer')
    # meta.set_display_unit('segment_area', 'kilometer ** 2')


def register_context_fields() -> None:
    """Register report-context string fields in RSFieldMeta.

    These are non-numeric, single-value fields derived from the query inputs
    (not from Athena row data), but they are described here — alongside the
    other derived metrics — so that build_named_values() in excel.py can look
    up their friendly_name and description from the singleton without the
    caller having to supply that metadata inline.
    """
    meta = RSFieldMeta()
    meta.add_field_meta(
        name='state_abbreviations',
        layer_id=LAYER_ID,
        friendly_name='State Abbreviations',
        description='Comma-separated alphabetical list of two-letter state abbreviations for the selected area',
        dtype='TEXT',
    )
    meta.add_field_meta(
        name='huc_codes',
        layer_id=LAYER_ID,
        friendly_name='HUC Codes',
        description='Comma-separated list of HUC codes used to generate this report',
        dtype='TEXT',
    )


def get_ownership_data(aoi_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Query and return ownership summary data (unnested from the ownership field)."""
    log = Logger("Get ownership data")

    aoi_sql_geom = get_aoi_geom_sql_expression(aoi_gdf)
    if aoi_sql_geom is None:
        raise ValueError("AOI geometry exceeds Athena query size limit. Simplify the AOI and try again.")

    query_str = f"""
SELECT lu_blm_o.edomvd AS ownership_desc, ST_AsBinary(ST_GeomFromBinary(ext_rpt.us_blm_sma_ownership.geom_wkb)) AS geom
FROM ext_rpt.us_blm_sma_ownership
         LEFT JOIN lu_blm_ownership lu_blm_o ON upper(ext_rpt.us_blm_sma_ownership.admin_agency_code) = upper(lu_blm_o.edomv)
WHERE ST_Intersects(ST_GeomFromBinary(geom_wkb), {aoi_sql_geom})
"""
    df = query_to_dataframe(query_str, "ownership")
    if df.empty:
        log.info("No ownership polygons intersect the AOI.")
        return gpd.GeoDataFrame(columns=["ownership_desc", "geometry"], geometry="geometry", crs=aoi_gdf.crs)

    gdf = gpd.GeoDataFrame(df.drop(columns=["geom"]), geometry=gpd.GeoSeries.from_wkb(df["geom"]), crs=aoi_gdf.crs)
    return gpd.clip(gdf, aoi_gdf)


def get_geology_data(aoi_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Query geology polygons intersecting the AOI and clip them to its extent.

    Returns a GeoDataFrame of geology polygons (rock_type, unit_name, geometry) clipped
    to the boundary of aoi_gdf. The table has no bounding-box column, so no prefilter
    condition is available and ST_Intersects is applied directly.
    """
    log = Logger("Get geology data for AOI")

    aoi_sql_geom = get_aoi_geom_sql_expression(aoi_gdf)
    if aoi_sql_geom is None:
        raise ValueError("AOI geometry exceeds Athena query size limit. Simplify the AOI and try again.")

    query_str = f"""
SELECT major1 AS rock_type, unit_name, ST_AsBinary(ST_GeomFromBinary(geom_wkb)) AS geom
FROM ext_rpt.us_sgmc_geology
WHERE ST_Intersects(ST_GeomFromBinary(geom_wkb), {aoi_sql_geom})
"""
    df = query_to_dataframe(query_str, "geology")
    if df.empty:
        log.info("No geology polygons intersect the AOI.")
        return gpd.GeoDataFrame(columns=["rock_type", "unit_name", "geometry"], geometry="geometry", crs=aoi_gdf.crs)

    gdf = gpd.GeoDataFrame(df.drop(columns=["geom"]), geometry=gpd.GeoSeries.from_wkb(df["geom"]), crs=aoi_gdf.crs)
    return gpd.clip(gdf, aoi_gdf)


def get_ecoregion_data(aoi_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Query ecoregion polygons intersecting the AOI and clip them to its extent.

    Returns a GeoDataFrame of ecoregion polygons (ecoregion_iv, ecoregion_iii, geometry) clipped
    to the boundary of aoi_gdf. The table has no bounding-box column, so no prefilter
    condition is available and ST_Intersects is applied directly.
    """
    log = Logger("Get ecoregion data for AOI")

    aoi_sql_geom = get_aoi_geom_sql_expression(aoi_gdf)
    if aoi_sql_geom is None:
        raise ValueError("AOI geometry exceeds Athena query size limit. Simplify the AOI and try again.")

    query_str = f"""
SELECT us_l4name AS ecoregion_iv, us_l3name AS ecoregion_iii, ST_AsBinary(ST_GeomFromBinary(geom_wkb)) AS geom
FROM ext_rpt.us_epa_ecoregions_l4
WHERE ST_Intersects(ST_GeomFromBinary(geom_wkb), {aoi_sql_geom})
"""
    df = query_to_dataframe(query_str, "ecoregions")
    if df.empty:
        log.info("No ecoregion polygons intersect the AOI.")
        return gpd.GeoDataFrame(columns=["ecoregion_iv", "ecoregion_iii", "geometry"], geometry="geometry", crs=aoi_gdf.crs)

    gdf = gpd.GeoDataFrame(df.drop(columns=["geom"]), geometry=gpd.GeoSeries.from_wkb(df["geom"]), crs=aoi_gdf.crs)
    return gpd.clip(gdf, aoi_gdf)


def get_states(huc_condition: str) -> pd.DataFrame:
    """Query and return distinct states (name + two-letter abbreviation) for the selection."""
    query_str = f"""
SELECT DISTINCT state_name, t.state AS state_abbrev
FROM rs_context_huc10
CROSS JOIN UNNEST(split(hucstates, ',')) AS t (state)
JOIN ext_rpt.us_states on t.state = us_states.alphacode
where {huc_condition}
ORDER BY state_name
"""
    return query_to_dataframe(query_str, "states")


def _add_agg_field_meta(fields: list[str], agg_type: str) -> None:
    """Register metadata for aggregated columns following the naming convention.

    Args:
        fields: Source field names (before the agg_ prefix).
        agg_type: Aggregation prefix used in column names (sum, min, max, countdistinct).
    """
    meta = RSFieldMeta()
    friendly_prefix = {"sum": "Total", "min": "Minimum", "max": "Maximum", "count": "Count", "countdistinct": "Count distinct"}.get(agg_type, agg_type.title())
    for orig_fld_nm in fields:
        orig_meta = meta.get_field_meta(orig_fld_nm)
        agg_col = f"{agg_type}_{orig_fld_nm}"

        if orig_meta:
            friendly_name = f"{friendly_prefix} {orig_meta.friendly_name}"
            data_unit = orig_meta.data_unit
            dtype = orig_meta.dtype
        else:
            friendly_name = f"{friendly_prefix} {orig_fld_nm.replace('_', ' ').title()}"
            data_unit = None
            dtype = 'REAL'  # could be int or something else but seems like a safe guess

        # Special case: count fields should always have unit 'count' & data type int
        if agg_type in ('count', 'countdistinct'):
            data_unit = "count"
            dtype = 'INT'

        meta.add_field_meta(name=agg_col, layer_id='rs_context_huc10', data_unit=data_unit, dtype=dtype, friendly_name=friendly_name)


def get_aggregated_data(huc_condition: str) -> pd.DataFrame:
    """Query and return all HUC-level summary data: flowline, waterbody, DEM, slope, etc.

    Returns an empty DataFrame if no rows match the condition.
    """
    log = Logger("Get aggregated data")
    sum_fields = [
        'hucareasqkm',
        'flowlineLengthPerennialKm',
        'flowlineLengthIntermittentKm',
        'flowlineLengthEphemeralKm',
        'flowlineLengthCanalsKm',
        'flowlineLengthAllKm',
        'flowlineFeatureCount',
        'waterbodyAreaSqKm',
        'waterbodyFeatureCount',
        'waterbodyLakesPondsAreaSqKm',
        'waterbodyLakesPondsFeatureCount',
        'waterbodyReservoirAreaSqKm',
        'waterbodyReservoirFeatureCount',
        'waterbodyEstuariesAreaSqKm',
        'waterbodyEstuariesFeatureCount',
        'waterbodyPlayaAreaSqKm',
        'waterbodyPlayaFeatureCount',
        'waterbodySwampMarshAreaSqKm',
        'waterbodySwampMarshFeatureCount',
        'waterbodyIceSnowAreaSqKm',
        'waterbodyIceSnowFeatureCount',
        'demsum',
        'demcount',
        'slopesum',
        'slopecount',
        'precipsum',
        'precipcount',
        'catchmentlength',
        'catchmentarea',
        'catchmentperimeter',
    ]
    min_fields = [
        'demminimum',
        'slopeminimum',
        'precipminimum',
        'circularityRatio',
        'elongationRatio',
        'formFactor',
        'hucName',
    ]
    max_fields = [
        'demmaximum',
        'slopemaximum',
        'precipmaximum',
    ]
    countdistinct_fields = [
        'huc',
    ]

    sum_expression = ','.join([f"SUM({f}) AS sum_{f}" for f in sum_fields])
    min_expression = ','.join([f"MIN({f}) AS min_{f}" for f in min_fields])
    max_expression = ','.join([f"MAX({f}) AS max_{f}" for f in max_fields])
    countdistinct_expression = ','.join([f"COUNT(DISTINCT {f}) AS countdistinct_{f}" for f in countdistinct_fields])
    query_str = f"""
SELECT {sum_expression}, {min_expression}, {max_expression}, {countdistinct_expression}
FROM rs_context_huc10
WHERE {huc_condition}
"""
    df = query_to_dataframe(query_str, "aggregates")

    if df.dropna(how="all").empty or df['countdistinct_huc'].iloc[0] == 0:
        log.error(f"No results returned for the query (ie nothing matching {huc_condition})")
        return pd.DataFrame()

    _add_agg_field_meta(sum_fields, "sum")
    _add_agg_field_meta(min_fields, "min")
    _add_agg_field_meta(max_fields, "max")
    _add_agg_field_meta(countdistinct_fields, "countdistinct")

    return df
