# System imports
import argparse
import logging
import os
import sys
import shutil
import traceback
import pandas as pd
import geopandas as gpd
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from util.pandas import load_gdf_from_csv
from util.athena import get_data_for_aoi
from util.rme.field_metadata import get_field_metadata
from util.athena import athena_unload_to_dataframe
from util.athena.athena import S3_ATHENA_BUCKET

from util.pdf import make_pdf_from_html
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.figures import (
    table_total_x_by_y,
    bar_group_x_by_y,
    bar_total_x_by_ybins,
    bar_total_x_by_ybins_h,
    make_map_with_aoi,
    make_rs_area_by_featcode,
    prop_ag_dev,
    dens_road_rail,
    project_id_list,
    metric_cards,
)
from reports.rpt_riverscapes_inventory import __version__ as report_version
from reports.rpt_rivers_need_space.dataprep import add_calculated_cols
from reports.rpt_riverscapes_inventory.figures import hypsometry_fig, statistics


def define_fields(unit_system: str = "SI"):
    """Set up the fields and units for this report"""
    _FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()
    _FIELD_META.field_meta = get_field_metadata()  # Set the field metadata for the report
    _FIELD_META.unit_system = unit_system  # Set the unit system for the report

    # Here's where we can set any preferred units that differ from the data unit
    _FIELD_META.set_display_unit('centerline_length', 'kilometer')
    _FIELD_META.set_display_unit('segment_area', 'kilometer ** 2')

    return


def make_report(gdf: gpd.GeoDataFrame, huc_df: pd.DataFrame, aoi_df: gpd.GeoDataFrame, report_dir, report_name, mode="interactive"):
    """
    Generates HTML report(s) in report_dir.
    mode: "interactive", "static", or "both"
    Returns path(s) to the generated html file(s).
    """
    log = Logger('make report')

    # TODO: Check - beaver_dam_capacity only applies to perennieal - so may need to use filter gdf before building beaver_dam_capacity_bar
    # also can we make the units dams per km or dams per mile
    log.info(f"Generating report in {report_dir} with mode={mode}")
    figures = {
        "map": make_map_with_aoi(gdf, aoi_df),
        "owner_bar": bar_group_x_by_y(gdf, 'segment_area', ['ownership_desc', 'fcode_desc']),
        "pie": make_rs_area_by_featcode(gdf),
        "low_lying_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['low_lying_ratio']),
        "prop_riparian_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['lf_riparian_prop']),
        "floodplain_access_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['fldpln_access']),
        "land_use_intensity_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['land_use_intens']),
        "prop_ag_dev": prop_ag_dev(gdf),
        "dens_road_rail": dens_road_rail(gdf),
        "hypsometry": hypsometry_fig(huc_df),
        "confinement_length_bar": bar_total_x_by_ybins(gdf, 'centerline_length', ['confinement_ratio', 'fcode_desc']),
        "confinement_area_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['confinement_ratio', 'fcode_desc']),
        "beaver_dam_capacity_historical_bar": bar_total_x_by_ybins_h(gdf, 'centerline_length', ['brat_hist_capacity']),
        "beaver_dam_capacity_current_bar": bar_total_x_by_ybins_h(gdf, 'centerline_length', ['brat_capacity']),
        "stream_order_bar": bar_group_x_by_y(gdf, 'centerline_length', ['stream_order']),
        "riparian_condition_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['riparian_condition']),
        "riparian_departure_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['riparian_veg_departure'])  # need to check these bins, also reverse them

    }
    tables = {
        "river_names": table_total_x_by_y(gdf, 'centerline_length', ['stream_name']),
        "owners": table_total_x_by_y(gdf, 'centerline_length', ['ownership', 'ownership_desc']),
        "table_of_fcodes": table_total_x_by_y(gdf, 'centerline_length', ['fcode_desc'])
    }
    appendices = {
        "project_ids": project_id_list(gdf),
    }
    figure_dir = os.path.join(report_dir, 'figures')
    safe_makedirs(figure_dir)

    report = RSReport(
        report_name=report_name,
        report_type="Riverscapes Inventory",
        report_dir=report_dir,
        report_version=report_version,
        figure_dir=figure_dir,
        body_template_path=os.path.join(os.path.dirname(__file__), 'templates', 'body.html'),
        css_paths=[os.path.join(os.path.dirname(__file__), 'templates', 'report.css')],
    )
    for (name, fig) in figures.items():
        report.add_figure(name, fig)

    report.add_html_elements('tables', tables)

    all_stats = statistics(gdf)
    metrics_for_key_indicators = ['total_segment_area', 'total_centerline_length', 'total_stream_length', 'integrated_valley_bottom_width']
    metric_data_for_key_indicators = {k: all_stats[k] for k in metrics_for_key_indicators if k in all_stats}
    report.add_html_elements('cards', metric_cards(metric_data_for_key_indicators))
    report.add_html_elements('appendices', appendices)

    if mode == "both":
        interactive_path = report.render(fig_mode="interactive", suffix="")
        static_path = report.render(fig_mode="png", suffix="_static")
        return {"interactive": interactive_path, "static": static_path}
    elif mode == "static":
        return report.render(fig_mode="png", suffix="_static")
    else:
        return report.render(fig_mode="interactive", suffix="")
    log.info("Report generation complete")


def load_huc_data(hucs: list[str]) -> pd.DataFrame:
    """Queries rscontext_huc10 for all the huc10 watersheds that intersect the aoi
    * this could be a spatial query but we already have the huc12 from data_gdf so this is much faster
    * FUTURE ENHANCEMENT - take the aoi and join with huc geometries to produce some statistics about the amount of intersection between them
    * FUTURE ENHANCEMENT - use unload instead of select
    """
    log = Logger('Load HUC data')

    if not hucs or len(hucs) == 0:
        log.error('No hucs provided to load_huc_data')
        return pd.DataFrame()  # return empty dataframe

    # Basic input sanitation: ensure all hucs are strings, length 10, digits only, and unique
    clean_hucs = {h for h in hucs if isinstance(h, str) and len(h) == 10 and h.isdigit()}
    if not clean_hucs or (len(clean_hucs) != len(hucs)):
        log.error('No hucs, duplicate huc or unexpected value in huc list')

    # Prepare SQL-safe quoted list
    huc_sql = '(' + ','.join([f"'{h}'" for h in clean_hucs]) + ')'
    sql_str = f"SELECT huc, project_id, hucname, hucareasqkm, dem_bins FROM rs_context_huc10 WHERE huc IN {huc_sql}"

    df = athena_unload_to_dataframe(sql_str)
    return df


def make_report_orchestrator(report_name: str, report_dir: str, path_to_shape: str,
                             existing_csv_path: str | None = None, include_pdf: bool = True, unit_system: str = "SI"):
    """ Orchestrates the report generation process:

    Args:
        report_name (str): The name of the report.
        report_dir (str): The directory where the report will be saved.
        path_to_shape (str): The path to the shapefile for the area of interest.
        existing_csv_path (str | None, optional): Path to an existing CSV file to use instead of querying Athena. Defaults to None.
        include_pdf (bool, optional): Whether to generate a PDF version of the report. Defaults to True.
        unit_system (str, optional): The unit system to use ("SI" or "imperial"). Defaults to "SI".
    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")

    # This is where all the initialization happens for fields and units
    define_fields(unit_system)  # ensure fields are defined

    # load shape as gdf
    aoi_gdf = gpd.read_file(path_to_shape)
    # get data first as csv
    safe_makedirs(os.path.join(report_dir, 'data'))
    csv_data_path = os.path.join(report_dir, 'data', 'data.csv')

    if existing_csv_path:
        log.info(f"Using supplied csv file at {csv_data_path}")
        if existing_csv_path != csv_data_path:
            shutil.copyfile(existing_csv_path, csv_data_path)
    else:
        log.info("Querying athena for data for AOI")
        get_data_for_aoi(S3_ATHENA_BUCKET, aoi_gdf, csv_data_path)

    data_gdf = load_gdf_from_csv(csv_data_path)
    data_gdf = add_calculated_cols(data_gdf)
    data_gdf, _ = RSFieldMeta().apply_units(data_gdf)  # this is still a geodataframe but we will need to be more explicit about it for type checking

    unique_huc10 = data_gdf['huc12'].astype(str).str[:10].unique().tolist()
    huc_data_df = load_huc_data(unique_huc10)
    print(huc_data_df)  # DEBUG ONLY

    # Export the data to Excel
    RSGeoDataFrame(data_gdf).export_excel(os.path.join(report_dir, 'data', 'data.xlsx'))

    # stop at just dynamic report for testing
    # make_report(data_gdf, huc_data_df, aoi_gdf, report_dir, report_name)
    # return

    # make html report
    report_paths = make_report(data_gdf, huc_data_df, aoi_gdf, report_dir, report_name, mode="both")
    html_path = report_paths["interactive"]
    static_path = report_paths["static"]
    log.info(f'Interactive HTML report built at {html_path}')
    log.info(f'Static HTML report built at {static_path}')

    if include_pdf:
        pdf_path = make_pdf_from_html(static_path)
        log.info(f'PDF report built from static at {pdf_path}')

    log.info(f"Report orchestration complete. Report is available in {report_dir}")
    return


def main():
    """ Main function to parse arguments and generate the report
"""

    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=str)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=str)
    parser.add_argument('report_name', help='name for the report (usually description of the area selected)')
    parser.add_argument('--include_pdf', help='Include a pdf version of the report', action='store_true', default=False)
    parser.add_argument('--unit_system', help='Unit system to use: SI or imperial', type=str, default='SI')
    parser.add_argument('--csv', help='Path to a local CSV of AOI data to use instead of querying Athena', type=str, default=None)
    # NOTE: IF WE CHANGE THESE VALUES PLEASE UPDATE ./launch.py

    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    output_path = args.output_path
    # if want each iteration to be saved add datetimestamp to path
    # dt_str = datetime.now().strftime("%y%m%d_%H%M")
    # dt_str = ""
    safe_makedirs(output_path)

    log = Logger('Setup')
    log_path = os.path.join(output_path, 'report.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-riverscapes-inventory')
    log.info(f"Output path: {output_path}")
    log.info(f"AOI shape: {args.path_to_shape}")
    log.info(f"Report name: {args.report_name}")
    log.info(f"Report Version: {report_version}")
    if args.csv:
        log.info(f"Using existing CSV: {args.csv}")
    else:
        log.info("No existing CSV provided, will query Athena")

    try:
        make_report_orchestrator(args.report_name,
                                 output_path,
                                 args.path_to_shape,
                                 args.csv,
                                 args.include_pdf,
                                 args.unit_system)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
