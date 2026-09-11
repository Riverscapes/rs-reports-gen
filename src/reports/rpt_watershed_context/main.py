"""Watershed Summary Report main entry point"""

import argparse
import logging
from pathlib import Path

import geopandas as gpd

# 3rd party imports
import pandas as pd
import plotly.graph_objects as go

# rsxml imports
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

# Report type imports
from reports.rpt_watershed_context import __version__ as report_version
from reports.rpt_watershed_context.dataprep import define_fields, get_aggregated_data, get_ecoregion_data, get_geology_data, get_intersecting_hucs, get_ownership_data, get_states, register_context_fields
from reports.rpt_watershed_context.excel import NamedValue, build_named_values, make_template, render_excel  # noqa: F401
from reports.rpt_watershed_context.figures import hydrography_table, hypsometry_fig, ownership_summary_table, statistics, waterbody_summary_table

# Repo imports
from util.athena.athena import athena_unload_to_dataframe
from util.figures import make_aoi_outline_map, metric_cards
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.pdf import make_pdf_from_html


def load_huc_data(hucs: list[str]) -> pd.DataFrame:
    """Queries rscontext_huc10 for all the huc10 watersheds that intersect the aoi
    * this could be a spatial query but we already have the huc12 from data_gdf so this is much faster
    * FUTURE ENHANCEMENT - take the aoi and join with huc geometries to produce some statistics about the amount of intersection between them
    * FUTURE ENHANCEMENT: check if we got data for all the hucs we were looking for
    """
    log = Logger("Load HUC data")

    if not hucs or len(hucs) == 0:
        log.error("No hucs provided to load_huc_data")
        return pd.DataFrame()  # return empty dataframe

    # Basic input sanitation: ensure all hucs are strings, length 10, digits only, and unique
    clean_hucs = {h for h in hucs if isinstance(h, str) and len(h) == 10 and h.isdigit()}
    if not clean_hucs or (len(clean_hucs) != len(hucs)):
        log.error("No hucs, duplicate huc or unexpected value in huc list")

    # Prepare SQL-safe quoted list
    huc_sql = "(" + ",".join([f"'{h}'" for h in clean_hucs]) + ")"
    sql_str = f"SELECT huc, project_id, hucname, hucareasqkm, dem_bins FROM rs_context_huc10 WHERE huc IN {huc_sql}"

    df = athena_unload_to_dataframe(sql_str)
    return df


def make_report(
    aoi_gdf: gpd.GeoDataFrame,
    aggregate_data_df: pd.DataFrame,
    ownership_df: pd.DataFrame,
    geology_df: gpd.GeoDataFrame,
    ecoregion_df: gpd.GeoDataFrame,
    states_df: pd.DataFrame,
    hucs_df: gpd.GeoDataFrame,
    report_dir: Path,
    report_name: str,
    stats: dict | None = None,
    include_static: bool = True,
    include_pdf: bool = True,
    error_message: str | None = None,
):
    """
    Generates HTML report(s) in report_dir.
    Args:
        aggregate_data_df: The main data dataframe for the report.
        ownership_df: Ownership summary dataframe.
        states_df: States dataframe.
        report_dir (Path): The directory where the report will be saved.
        report_name (str): The name of the report.
        stats: Pre-computed statistics dict from figures.statistics().  If None,
            statistics() is called internally (display-unit df path, legacy behaviour).
        include_static (bool, optional): Whether to include a static version of the report. Defaults to True.
        include_pdf (bool, optional): Whether to include a PDF version of the report. Defaults to True.
        error_message: display to user *instead* of any figures
    """
    log = Logger('make report')

    figures: dict[str, go.Figure] = {'map': make_aoi_outline_map(aoi_gdf), 'hysometry': hypsometry_fig(hucs_df)}
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
        css_paths=[Path(__file__).parent / 'templates' / 'report.css'],
    )
    for name, fig in figures.items():
        report.add_figure(name, fig)

    if error_message:
        report.add_html_elements('error_message', {'text': error_message})

    else:
        report.add_html_elements('tables', tables)
        report.add_html_elements('states', states_df['state_name'].tolist())
        effective_stats: dict[str, object] = stats if stats is not None else statistics(aggregate_data_df)  # type: ignore[assignment]
        cards = metric_cards(effective_stats)
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


def make_report_orchestrator(report_name: str, report_dir: Path, aoi_path: Path, include_pdf: bool = True, unit_system: str = "SI"):
    """Orcestratest the report generation process:
    * get the data
    * make the report

    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")
    meta = RSFieldMeta()

    aoi_gdf = gpd.read_file(aoi_path)
    huc_list = get_intersecting_hucs(aoi_gdf)
    if not huc_list:
        raise ValueError("No HUC10 watersheds intersect the supplied AOI.")
    huc_condition = parse_hucs(','.join(huc_list), 'huc', 10)
    log.debug(f"huc condition: {huc_condition}")

    define_fields(unit_system)
    df_aggregatedata = get_aggregated_data(huc_condition)

    if df_aggregatedata.empty:
        # we send empty dataframes and error_message
        make_report(aoi_gdf, df_aggregatedata, df_aggregatedata, df_aggregatedata, df_aggregatedata, report_dir, report_name, error_message="No results found for selection.")
    else:
        # although it doesn't make much difference with these quick queries, parallelizing is good practice
        # with ThreadPoolExecutor(max_workers=2) as executor:
        #     future_owners = executor.submit(get_ownership_data, huc_condition)
        #     future_states = executor.submit(get_states, huc_condition)
        #     df_owners = future_owners.result()
        #     df_states = future_states.result()
        df_states = get_states(huc_condition)
        df_owners = get_ownership_data(aoi_gdf)
        df_geology = get_geology_data(aoi_gdf)
        df_ecoregion = get_ecoregion_data(aoi_gdf)
        df_hucs = load_huc_data(huc_list)

        # apply_units must run first so statistics() receives Pint-typed columns.
        # build_named_values then converts derived stats back to SI data_unit for Excel.
        df_aggregatedata, _ = meta.apply_units(df_aggregatedata)
        stats = statistics(df_aggregatedata)
        df_owners, _ = meta.apply_units(df_owners)

        register_context_fields()

        # Build string values for the registered fields
        state_abbrevs = ', '.join(sorted(df_states['state_abbrev'].dropna().str.strip().unique()))
        huc_codes_str = ', '.join(huc_list)
        extra_named_values: dict[str, NamedValue] = {
            'state_abbreviations': NamedValue(value=state_abbrevs),
            'huc_codes': NamedValue(value=huc_codes_str),
        }
        named_values = build_named_values(df_aggregatedata, stats, extra=extra_named_values)

        make_report(aoi_gdf, df_aggregatedata, df_owners, df_geology, df_ecoregion, df_states, df_hucs, report_dir, report_name, stats=stats, include_static=include_pdf, include_pdf=include_pdf)
        safe_makedirs(str(report_dir / 'data'))
        # Export the data to Excel (simple dumb export)
        RSGeoDataFrame(df_aggregatedata).export_excel(report_dir / 'data' / 'data.xlsx')
        # rebuild template one time or when data schema changes (e.g. new column added to query)
        # make_template(named_values)
        # Inject the data into smart Excel template (SI units; stats include derived metrics)
        render_excel(named_values, df_owners, report_dir / 'report.xlsx')


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
    """Main function to parse arguments and generate the report"""
    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=Path)
    parser.add_argument('aoi_path', help='Path to a polygon file (e.g. geojson/shapefile) defining the area of interest', type=Path)
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

    make_report_orchestrator(args.report_name, output_path, args.aoi_path, args.include_pdf, args.unit_system)


if __name__ == "__main__":
    main()
