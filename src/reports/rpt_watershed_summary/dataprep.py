"""Data preparation for the Watershed Summary report.

Owns all Athena queries, field metadata registration, and derived-column
construction.  make_report_orchestrator in main.py calls these functions and
should not need to know the details of how data is fetched or described.

Created 2026-07-28.
"""

import pandas as pd
from rsxml import Logger

from util.athena import get_field_metadata, query_to_dataframe
from util.pandas import RSFieldMeta

LAYER_ID = 'rpt_watershed_summary'


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


def get_ownership_data(huc_condition: str) -> pd.DataFrame:
    """Query and return ownership summary data (unnested from the ownership field)."""
    log = Logger("Get ownership data")
    query_str = f"""
SELECT lu_blm_o.edomvd AS ownership_desc, sum(o.ownership_area) AS sum_ownership_area
FROM rs_context_huc10
         CROSS JOIN UNNEST(ownership) AS o (ownership_code, ownership_area)
         LEFT JOIN lu_blm_ownership lu_blm_o ON upper(o.ownership_code) = upper(lu_blm_o.edomv)
WHERE {huc_condition}
GROUP BY lu_blm_o.edomvd
ORDER BY lu_blm_o.edomvd
"""
    df = query_to_dataframe(query_str, "ownership")
    # TODO - Units for data in athena should be defined in athena, not here
    log.debug("Units for ownership area assumed to be m**2.")
    meta = RSFieldMeta()
    meta.add_field_meta(name='sum_ownership_area', friendly_name='Total Area', data_unit='m**2', display_unit='kilometer ** 2')
    return df


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
