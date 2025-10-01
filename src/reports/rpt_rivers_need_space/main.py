# System imports
import argparse
import logging
import os
import sys
import shutil
import traceback
# 3rd party imports
import geopandas as gpd
import pandas as pd
import pint
from shapely import wkt

from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from util.athena import get_s3_file, run_aoi_athena_query, get_field_metadata

from util.html import RSReport
from util.pandas import RSFieldMeta
# Local imports
from reports.rpt_rivers_need_space.figures import (make_rs_area_by_owner,
                                                   make_rs_area_by_featcode,
                                                   make_map_with_aoi,
                                                   statistics,
                                                   table_of_river_names,
                                                   table_of_ownership,
                                                   low_lying_ratio_bins,
                                                   prop_riparian_bins,
                                                   floodplain_access,
                                                   land_use_intensity,
                                                   prop_ag_dev,
                                                   dens_road_rail,
                                                   project_id_table,
                                                   )


S3_BUCKET = "riverscapes-athena"
_FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()
ureg = pint.UnitRegistry()


def make_report(gdf: gpd.GeoDataFrame, aoi_df: gpd.GeoDataFrame, report_dir, report_name, mode="interactive"):
    """
    Generates HTML report(s) in report_dir.
    mode: "interactive", "static", or "both"
    Returns path(s) to the generated html file(s).
    """
    log = Logger('make report')

    figures = {
        "map": make_map_with_aoi(gdf, aoi_df),
        "bar": make_rs_area_by_owner(gdf),
        "pie": make_rs_area_by_featcode(gdf),
        "low_lying": low_lying_ratio_bins(gdf),
        "prop_riparian": prop_riparian_bins(gdf),
        "floodplain_access": floodplain_access(gdf),
        "land_use_intensity": land_use_intensity(gdf),
        "prop_ag_dev": prop_ag_dev(gdf),
        "dens_road_rail": dens_road_rail(gdf),
    }
    tables = {
        "river_names": table_of_river_names(gdf),
        "owners": table_of_ownership(gdf),
        "project_id_table": project_id_table(gdf),
    }
    figure_dir = os.path.join(report_dir, 'figures')
    safe_makedirs(figure_dir)

    report = RSReport(
        report_name=report_name,
        report_type="Rivers Need Space",
        report_dir=report_dir,
        figure_dir=figure_dir,
        body_template_path=os.path.join(os.path.dirname(__file__), 'templates', 'body.html'),
    )
    for (name, fig) in figures.items():
        report.add_figure(name, fig)

    report.add_html_elements('tables', tables)
    report.add_html_elements('kpis', statistics(gdf))

    if mode == "both":
        interactive_path = report.render(fig_mode="interactive", suffix="")
        static_path = report.render(fig_mode="png", suffix="_static")
        return {"interactive": interactive_path, "static": static_path}
    elif mode == "static":
        return report.render(fig_mode="png", suffix="_static")
    else:
        return report.render(fig_mode="interactive", suffix="")


def load_gdf_from_csv(csv_path):
    """ load csv from athena query into gdf

    Args:
        csv_path (_type_): _path to csv

    Returns:
        _type_: gdf
    """
    df = pd.read_csv(csv_path)
    df.describe()  # outputs some info for debugging
    df['dgo_polygon_geom'] = df['dgo_geom_obj'].apply(wkt.loads)  # pyright: ignore[reportArgumentType, reportCallIssue]
    gdf = gpd.GeoDataFrame(df, geometry='dgo_polygon_geom', crs='EPSG:4326')
    # print(gdf)
    return gdf


def get_data_for_aoi(gdf: gpd.GeoDataFrame, output_path: str):
    """given aoi in gdf format (assume 4326), just get all the raw_rme (for now)
    returns: local path to the data csv file"""
    log = Logger('Run AOI query on Athena')
    # temporary approach -- later try using report-type specific CTAS and report-specific UNLOAD statement
    fields_str = "level_path, seg_distance, centerline_length, segment_area, fcode, fcode_desc, longitude, latitude, ownership, ownership_desc, state, county, drainage_area, stream_name, stream_order, stream_length, huc12, rel_flow_length, channel_area, integrated_width, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, lf_agriculture_prop, lf_agriculture, lf_developed_prop, lf_developed, lf_riparian_prop, lf_riparian, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, develop, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent, rme_project_id, rme_project_name"
    s3_csv_path = run_aoi_athena_query(gdf, S3_BUCKET, fields_str=fields_str, source_table="rpt_rme")
    if s3_csv_path is None:
        log.error("Didn't get a result from athena")
        raise NotImplementedError
    get_s3_file(s3_csv_path, output_path)
    return


def add_calculated_cols(df: pd.DataFrame) -> pd.DataFrame:
    """ Add any calculated columns to the dataframe

    Args:
        df (pd.DataFrame): Input dataframe

    Returns:
        pd.DataFrame: DataFrame with calculated columns added
    """
    df['channel_length'] = df['rel_flow_length'] * df['centerline_length']

    meta = RSFieldMeta()
    existing_meta = meta.field_meta
    if existing_meta is None or 'channel_length' not in existing_meta.index:
        table_name = ''
        data_unit = ''
        display_unit = ''
        dtype = ''
        if existing_meta is not None and 'centerline_length' in existing_meta.index:
            base_meta = existing_meta.loc['centerline_length']
            table_name_val = base_meta.get('table_name', '')
            data_unit_obj = base_meta.get('data_unit')
            display_unit_obj = base_meta.get('display_unit')
            dtype_val = base_meta.get('dtype', '')
            if pd.notna(table_name_val):
                table_name = str(table_name_val)
            if pd.notna(data_unit_obj):
                data_unit = str(data_unit_obj)
            if pd.notna(display_unit_obj):
                display_unit = str(display_unit_obj)
            if pd.notna(dtype_val):
                dtype = str(dtype_val)
        meta.add_field_meta(
            name='channel_length',
            table_name=table_name,
            friendly_name='Channel Length',
            data_unit=data_unit,
            display_unit=display_unit,
            dtype=dtype or 'DOUBLE',
        )
    return df


def make_report_orchestrator(report_name: str, report_dir: str, path_to_shape: str,
                             existing_csv_path: str | None = None,
                             include_pdf: bool = True,
                             unit_system: str = "SI"):
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

    _FIELD_META.field_meta = get_field_metadata()  # Set the field metadata for the report
    _FIELD_META.unit_system = unit_system  # Set the unit system for the report

    # load shape as gdf
    aoi_gdf = gpd.read_file(path_to_shape)
    # get data first as csv
    safe_makedirs(os.path.join(report_dir, 'data'))
    csv_data_path = os.path.join(report_dir, 'data', 'data.csv')

    if existing_csv_path:
        log.info(f"Using supplied csv file at {csv_data_path}")
        shutil.copyfile(existing_csv_path, csv_data_path)
    else:
        log.info("Querying athena for data for AOI")
        get_data_for_aoi(aoi_gdf, csv_data_path)
    data_gdf = load_gdf_from_csv(csv_data_path)

    add_calculated_cols(data_gdf)

    # excel version
    export_excel(data_gdf, os.path.join(report_dir, 'data', 'data.xlsx'))

    # make html report

    report_paths = make_report(data_gdf, aoi_gdf, report_dir, report_name, mode="both")
    html_path = report_paths["interactive"]
    static_path = report_paths["static"]
    log.info(f'Interactive HTML report built at {html_path}')
    log.info(f'Static HTML report built at {static_path}')

    # if include_pdf:
    #     pdf_path = make_pdf_from_html(static_path)
    #     log.info(f'PDF report built from static at {pdf_path}')

    log.info(f"Report orchestration complete. Report is available in {report_dir}")
    return


def export_excel(gdf: gpd.GeoDataFrame, output_path: str):
    """ Export the GeoDataFrame to an Excel file with metadata.

    Args:
        gdf (gpd.GeoDataFrame): The input GeoDataFrame.
        output_path (str): The path to save the Excel file.
    """
    log = Logger('Export Excel')
    log.info(f"Exporting data to Excel at {output_path}")

    baked_gdf, baked_headers = _FIELD_META.bake_units(gdf)
    baked_gdf.columns = baked_headers

    # excel version
    with pd.ExcelWriter(output_path) as writer:
        baked_gdf.to_excel(writer, sheet_name="data")
        _FIELD_META.field_meta.to_excel(writer, sheet_name="metadata")


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
    log.title('rs-rpt-rivers-need-space')

    try:
        make_report_orchestrator(
            args.report_name,
            output_path,
            args.path_to_shape,
            args.csv,
            args.include_pdf,
            args.unit_system
        )

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
