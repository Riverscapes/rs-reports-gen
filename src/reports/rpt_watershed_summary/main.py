"""Watershed Summary Report main entry point"""
import argparse
import logging
from pathlib import Path

import pandas as pd

from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs
from util.athena import query_to_dataframe
from util.pandas import RSFieldMeta, RSGeoDataFrame


def get_field_metadata(fields: str = '*',
                       authority: str = 'data-exchange-scripts',
                       authority_name: str = 'rscontext_to_athena',
                       layer_id='rs_context_huc10'
                       ) -> pd.DataFrame:
    """new version of util.rme.field_metadata.py 
    TODO: move to util.athena (not rme) once tested & generalized
    Query athena for metadata 

    Returns: 
        pd.DataFrame - DataFrame of metadata
    """
    log = Logger('Get metadata')
    log.info("Getting metadata from Athena")

    if fields == '*':
        and_name = ""
    else:
        and_name = "AND name in ({fields})"
    query = f"""
SELECT layer_id, layer_name, name, friendly_name, data_unit, description
FROM layer_definitions_latest
WHERE authority = {authority} AND authority_name = {authority_name} AND layer_id = {layer_id}
{and_name}
"""
    df = query_to_dataframe(query)
    if df.empty:
        raise RuntimeError("Failed to retrieve metadata from Athena.")
    return df


def define_fields(unit_system: str = "SI"):
    """Set up the fields and units for this report"""
    _FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()
    _FIELD_META.field_meta = get_field_metadata()  # Set the field metadata for the report
    _FIELD_META.unit_system = unit_system  # Set the unit system for the report

    # Here's where we can set any preferred units that differ from the data unit
    _FIELD_META.set_display_unit('centerline_length', 'kilometer')
    _FIELD_META.set_display_unit('segment_area', 'kilometer ** 2')

    return


def get_waterbody_data(huc_condition: str) -> pd.DataFrame:
    """get waterbody summaries"""
    sum_fields = [
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
    ]
    sum_expression = ','.join([f"SUM({f}) AS sum_{f}" for f in sum_fields])
    query_str = f"""
SELECT {sum_expression}
FROM rs_context_huc10
WHERE {huc_condition}
"""
    df = query_to_dataframe(query_str)
    # TODO get the metadata

    return df


def get_data(huc_condition: str) -> pd.DataFrame:
    """get the data for the huc_condition"""
    log = Logger('get data')

    fields = "huc,project_id,hucname,hucstates,hucareasqkm,ownership"
    query_str = f"""
SELECT {fields}
FROM rs_context_huc10
WHERE {huc_condition}
"""
    df = query_to_dataframe(query_str)
    return df


def make_report_orchestrator(report_name: str, report_dir: Path, hucs: str,
                             include_pdf: bool = True, unit_system: str = "both"):
    """Orcestratest the report generation process: 
    * get the data
    * make the report

    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")
    huc_condition = parse_hucs(hucs, 'huc', 10)
    log.debug(f"{huc_condition}")
    df = get_data(huc_condition)
    df_waterbodies = get_waterbody_data(huc_condition)
    print(df)
    print(df_waterbodies)


def parse_hucs(hucs: str, field_identifier='huc10', field_length: int = 10) -> str:
    """
    Build a SQL condition for a list of HUC codes (2/4/6/8/10/12 digits).
    Handles both huc10 and huc12 fields.
    Raises ValueError for mixed lengths or invalid codes.

    Arguments: 
    * hucs (str): comma-separated list of HUC codes, all of the same length
    * field_identifier: the name of the field that we ware searching
    * field_length: what the field_identifier contains (e.g. huc10 has 10, huc12 has 12)

    Returns condition that can be added to a where clause e.g. 
        "HUC10 IN ('1234567890')"
        "substr(HUC10,1,8) IN ('12345678','87654321')"

    See test_parse_hucs for more examples. 
    This is similar to `get_huc_sql_filter` in cybercastor_scripts scripts/add_batch_athena.py
    """
    huc_list = [h.strip() for h in hucs.split(',') if h.strip()]
    if not huc_list:
        raise ValueError("No HUCs provided.")

    lengths = set(len(huc) for huc in huc_list)
    if len(lengths) > 1:
        raise NotImplementedError("All HUCs must have the same length.")

    huc_len = lengths.pop()
    if not all(huc.isdigit() for huc in huc_list):
        raise ValueError("All HUCs must be numeric.")

    if huc_len > field_length:
        raise ValueError(f"HUC length must be <= {field_length} for field {field_identifier}.")

    if huc_len == field_length:
        condition = f"{field_identifier} IN ({','.join(repr(huc) for huc in huc_list)})"
    else:
        condition = f"substr({field_identifier},1,{huc_len}) IN ({','.join(repr(huc) for huc in huc_list)})"
    return condition


def main():
    """ Main function to parse arguments and generate the report
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=Path)
    parser.add_argument('huc_list', help='comma separated list of huc codes', type=str)
    parser.add_argument('report_name', help='name for the report (usually description of the area selected)')
    parser.add_argument('--include_pdf', help='Include a pdf version of the report', action='store_true', default=False)
    parser.add_argument('--unit_system', help='Unit system to use: SI or imperial', type=str, default='SI')

    args = dotenv.parse_args_env(parser)
    # Set up some reasonable folders to store things
    output_path = Path(args.output_path)
    # new version of safe_makedirs will take a Path but for now all Paths are converted to string for this function
    safe_makedirs(str(output_path))

    log = Logger('Setup')
    log_path = output_path / 'report.log'
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-watershed-summary')

    make_report_orchestrator(args.report_name,
                             output_path,
                             args.huc_list,
                             args.include_pdf,
                             args.unit_system)


if __name__ == "__main__":
    main()
