"""Watershed Summary Report main entry point"""
import argparse
import logging
from pathlib import Path
# 3rd party imports
import pandas as pd
# RSxml imports
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs
# Repo imports
from util.athena import query_to_dataframe
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.pdf import make_pdf_from_html
# Report type imports
from reports.rpt_watershed_summary import __version__ as report_version
from reports.rpt_watershed_summary.figures import waterbody_summary_table


def get_field_metadata(fields: str = '*',
                       authority: str = 'data-exchange-scripts',
                       authority_name: str = 'rscontext_to_athena',
                       layer_id='rs_context_huc10'
                       ) -> pd.DataFrame:
    """new version of util.rme.field_metadata.py 
    TODO: move to util.athena (not rme) once tested & generalized
    Query athena for metadata. 

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
SELECT layer_id, layer_name AS table_name, name, friendly_name, data_unit, description
FROM layer_definitions_latest
WHERE authority = '{authority}' AND authority_name = '{authority_name}' AND layer_id = '{layer_id}'
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
    # _FIELD_META.set_display_unit('centerline_length', 'kilometer')
    # _FIELD_META.set_display_unit('segment_area', 'kilometer ** 2')

    return


def make_report(df: pd.DataFrame, report_dir: Path, report_name: str,
                include_static: bool = True,
                include_pdf: bool = True):
    """
    Generates HTML report(s) in report_dir.
    Args:
        df (pd.DataFrame): The main data dataframe for the report.
        report_dir (Path): The directory where the report will be saved.
        report_name (str): The name of the report.
        include_static (bool, optional): Whether to include a static version of the report. Defaults to True.
        include_pdf (bool, optional): Whether to include a PDF version of the report. Defaults to True.
    """
    log = Logger('make report')

    # figures must be plotly objects
    figures = {
    }
    # tables are html strings
    tables = {
        "waterbodies": waterbody_summary_table(df)
    }

    figure_dir = report_dir / 'figures'
    report = RSReport(
        report_name=report_name,
        report_type="Watershed Summary",
        report_dir=report_dir,
        figure_dir=figure_dir,
        report_version=report_version,
        body_template_path=Path(__file__).parent / 'templates' / 'body.html',
        css_paths=[Path(__file__).parent / 'templates' / 'report.css']
    )
    for (name, fig) in figures.items():
        report.add_figure(name, fig)

    report.add_html_elements('tables', tables)

    interactive_path = report.render(fig_mode="interactive", suffix="")
    static_path = None
    pdf_path = None
    if include_static:
        static_path = report.render(fig_mode="svg", suffix="_static")
        if include_pdf:
            pdf_path = make_pdf_from_html(static_path)
            log.info(f'PDF report built from static at {pdf_path}')

    log.title('Report Generation Complete')
    log.info(f'Interactive: {interactive_path}')
    if static_path:
        log.info(f'Static: {static_path}')
    if pdf_path:
        log.info(f'PDF: {pdf_path}')


def get_waterbody_data(huc_condition: str) -> pd.DataFrame:
    """get waterbody summary data"""
    meta = RSFieldMeta()
    sum_fields = [
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
    ]
    sum_expression = ','.join([f"SUM({f}) AS sum_{f}" for f in sum_fields])
    query_str = f"""
SELECT {sum_expression}
FROM rs_context_huc10
WHERE {huc_condition}
"""
    df = query_to_dataframe(query_str)
    # transfer/add metadata for the new aggregated columns
    for orig_fld_nm in sum_fields:
        meta.add_field_meta(name=f'sum_{orig_fld_nm}',
                            table_name='rs_context_huc10',
                            data_unit=meta.get_field_unit(orig_fld_nm),
                            friendly_name=f"Total {meta.get_friendly_name(orig_fld_nm)}"
                            )

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
                             include_pdf: bool = True, unit_system: str = "SI"):
    """Orcestratest the report generation process: 
    * get the data
    * make the report

    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")
    meta = RSFieldMeta()
    huc_condition = parse_hucs(hucs, 'huc', 10)
    log.debug(f"huc condition: {huc_condition}")
    define_fields(unit_system)
    # df = get_data(huc_condition)
    # print(df)
    df_waterbodies = get_waterbody_data(huc_condition)
    df_waterbodies, _ = meta.apply_units(df_waterbodies)
    print(df_waterbodies)
    make_report(df_waterbodies, report_dir, report_name, include_pdf, include_pdf)
    safe_makedirs(str(report_dir / 'data'))
    # Export the data to Excel
    RSGeoDataFrame(df_waterbodies).export_excel(report_dir / 'data' / 'data.xlsx')


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
