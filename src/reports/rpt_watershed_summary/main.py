"""Watershed Summary Report main entry point"""
import argparse
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
# 3rd party imports
import pandas as pd
import plotly.graph_objects as go
# rsxml imports
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs
# Repo imports
from util.athena import query_to_dataframe, get_field_metadata
from util.figures import metric_cards
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.pdf import make_pdf_from_html
# Report type imports
from reports.rpt_watershed_summary import __version__ as report_version
from reports.rpt_watershed_summary.figures import (
    waterbody_summary_table,
    ownership_summary_table,
    hydrography_table,
    statistics
)


def define_fields(unit_system: str = "SI"):
    """Set up the fields and units for this report"""
    _FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()
    _FIELD_META.field_meta = get_field_metadata(authority_name=['rscontext_to_athena', 'rpt_rme'],
                                                layer_id=['rs_context_huc10', 'rpt_rme'])  # Set the field metadata for the report
    _FIELD_META.unit_system = unit_system  # Set the unit system for the report

    # Here's where we can set any preferred units that differ from the data unit
    # _FIELD_META.set_display_unit('centerline_length', 'kilometer')
    # _FIELD_META.set_display_unit('segment_area', 'kilometer ** 2')

    return


def make_report(aggregate_data_df: pd.DataFrame, ownership_df: pd.DataFrame, states_df: pd.DataFrame,
                report_dir: Path, report_name: str,
                include_static: bool = True,
                include_pdf: bool = True,
                error_message: str | None = None):
    """
    Generates HTML report(s) in report_dir.
    Args:
        df (pd.DataFrame): The main data dataframe for the report.
        report_dir (Path): The directory where the report will be saved.
        report_name (str): The name of the report.
        include_static (bool, optional): Whether to include a static version of the report. Defaults to True.
        include_pdf (bool, optional): Whether to include a PDF version of the report. Defaults to True.
        error_message: display to user *instead* of any figures
    """
    log = Logger('make report')

    figures: dict[str, go.Figure] = {}
    tables: dict[str, str] = {}

    if error_message is None:
        tables = {
            "waterbodies": waterbody_summary_table(aggregate_data_df),
            "ownership": ownership_summary_table(ownership_df),
            "hydrography": hydrography_table(aggregate_data_df),
        }

    report = RSReport(
        report_name=report_name,
        report_type="Watershed Summary",
        report_dir=report_dir,
        figure_dir=report_dir / 'figures',
        report_version=report_version,
        body_template_path=Path(__file__).parent / 'templates' / 'body.html',
        css_paths=[Path(__file__).parent / 'templates' / 'report.css']
    )
    for (name, fig) in figures.items():
        report.add_figure(name, fig)

    if error_message:
        report.add_html_elements('message', {'text': error_message})

    report.add_html_elements('tables', tables)
    report.add_html_elements('states', states_df['state_name'].tolist())
    cards = metric_cards(statistics(aggregate_data_df))
    report.add_html_elements('cards', cards)

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


def get_ownership_data(huc_condition: str) -> pd.DataFrame:
    """get ownership summary data (Unnest the ownership field)"""
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
    meta = RSFieldMeta()
    meta.add_field_meta(name='sum_ownership_area',
                        friendly_name='Total Area',
                        data_unit='m**2',
                        display_unit='kilometer ** 2')
    return df


def get_states(huc_condition: str) -> pd.DataFrame:
    """Get list of distinct states"""
    query_str = f"""
SELECT DISTINCT state_name
FROM rs_context_huc10
CROSS JOIN UNNEST(split(hucstates, ',')) AS t (state)
JOIN us_states on t.state = us_states.alphacode
where {huc_condition}
ORDER BY state_name
"""
    df = query_to_dataframe(query_str, "states")
    return df


def add_agg_field_meta(fields, agg_type: str):
    """Helper to transfer/add metadata for aggregated columns.
    assumes the new fields follow naming convention
    Args:
        agg_type - the prefix used for the aggregation type (sum, min, max)
    """
    meta = RSFieldMeta()
    for orig_fld_nm in fields:
        orig_meta = meta.get_field_meta(orig_fld_nm)
        # NAMING CONVENTION:
        agg_col = f"{agg_type}_{orig_fld_nm}"
        friendly_prefix = {
            "sum": "Total",
            "min": "Minimum",
            "max": "Maximum",
            "count": "Count",
            "countdistinct": "Count (distinct)"
        }.get(agg_type, agg_type.title())

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

        meta.add_field_meta(
            name=agg_col,
            table_name='rs_context_huc10',
            data_unit=data_unit,
            dtype=dtype,
            friendly_name=friendly_name
        )


def get_aggregated_data(huc_condition: str) -> pd.DataFrame:
    """get all summary data: flowline, waterbody, dem, slope, etc """
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
    ]
    max_fields = [
        'demmaximum',
        'slopemaximum',
        'precipmaximum',
    ]
    countdistinct_fields = [
        'huc',
    ]
    # NAMING CONVENTION
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

    if df.dropna(how="all").empty:
        log.error("No results returned for the query")
        # short-circuit report generation
        return pd.DataFrame()  # an empty DataFrame

    # transfer/add metadata for the new aggregated columns
    add_agg_field_meta(sum_fields, "sum")
    add_agg_field_meta(min_fields, "min")
    add_agg_field_meta(max_fields, "max")
    add_agg_field_meta(countdistinct_fields, "countdistinct")

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
    df_aggregatedata = get_aggregated_data(huc_condition)

    if df_aggregatedata.empty:
        # we send 3 empty dataframes
        make_report(df_aggregatedata, df_aggregatedata, df_aggregatedata, report_dir, report_name,
                    error_message="No results found for selection.")
    else:
        # although it doesn't make much difference with these quick queries, parallelizing is good practice
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_owners = executor.submit(get_ownership_data, huc_condition)
            future_states = executor.submit(get_states, huc_condition)
            df_owners = future_owners.result()
            df_states = future_states.result()

        df_aggregatedata, _ = meta.apply_units(df_aggregatedata)
        df_owners, _ = meta.apply_units(df_owners)

        make_report(df_aggregatedata, df_owners, df_states, report_dir, report_name,
                    include_pdf, include_pdf)
        safe_makedirs(str(report_dir / 'data'))
        # Export the data to Excel
        RSGeoDataFrame(df_aggregatedata).export_excel(report_dir / 'data' / 'data.xlsx')


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
