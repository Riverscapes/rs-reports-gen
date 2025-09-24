# System imports
import argparse
import logging
import os
import sys
import shutil
from datetime import datetime
from importlib import resources
# 3rd party imports
import geopandas as gpd
import pandas as pd
import pint
from shapely import wkt

from jinja2 import Template

from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from util.athena import get_s3_file, run_aoi_athena_query
from util.pdf.create_pdf import make_pdf_from_html
from util.plotly.export_figure import export_figure
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

    def render_report(fig_mode, suffix=""):
        figure_exports = {}
        for (name, fig) in figures.items():
            figure_exports[name] = export_figure(
                fig, figure_dir, name, mode=fig_mode, include_plotlyjs=False, report_dir=report_dir
            )
        templates_pkg = resources.files(__package__).joinpath('templates')
        template = Template(templates_pkg.joinpath('template.html').read_text(encoding='utf-8'))
        css = templates_pkg.joinpath('report.css').read_text(encoding='utf-8')
        style_tag = f"<style>{css}</style>"
        now = datetime.now()
        html = template.render(
            report={
                'head': style_tag,
                'title': report_name,
                'date': now.strftime('%B %d, %Y - %I:%M%p'),
                'ReportType': "Rivers Need Space"
            },
            report_name=report_name,
            figures=figure_exports,
            kpis=statistics(gdf),
            tables=tables
        )
        out_path = os.path.join(report_dir, f"report{suffix}.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"Report written to {out_path}")
        return out_path

    if mode == "both":
        interactive_path = render_report("interactive", "")
        static_path = render_report("png", "_static")
        return {"interactive": interactive_path, "static": static_path}
    elif mode == "static":
        return render_report("png", "_static")
    else:
        return render_report("interactive", "")


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


def get_metadata() -> pd.DataFrame:
    """
    Query Athena for column metadata from rme_table_column_defs and return as a DataFrame.

    Returns:
        pd.DataFrame - DataFrame of metadata

    Example:
        metadata_df = get_metadata_df()
    """
    log = Logger('Get metadata')
    log.info("Getting metadata from athena")
    from util.athena.athena import athena_query_get_parsed

    query = """
        SELECT table_name, name, type, friendly_name, unit, description
        FROM rme_table_column_defs
    """
    result = athena_query_get_parsed(S3_BUCKET, query)
    if result is not None:
        return pd.DataFrame(result)
    raise RuntimeError("Railed to retrieve metadata from Athena.")


def convert_gdf_units(gdf: gpd.GeoDataFrame, unit_system: str = "US"):
    """convert all measures according to unit system 
    does not work yet

    Args:
        gdf (gpd.GeoDataFrame): data_gpd
        unit_system (str, optional): Unit system. Defaults to "US".

    Returns:
        same data frame but with units converted
    """
    ureg.default_system = unit_system
    for col in gdf.columns:
        if hasattr(gdf[col], 'pint'):
            # convert each unit DOES NOT WORK this way
            # pint.to_unit(foot) etc. does work -- we'll need to know which units
            gdf[col] = gdf[col].pint.to_base_units()
    return gdf


def add_calculated_cols(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: add metadata for any added columns
    df['channel_length'] = df['rel_flow_length']*df['centerline_length']
    return df


def make_report_orchestrator(report_name: str, report_dir: str, path_to_shape: str,
                             existing_csv_path: str | None = None, include_pdf: bool = True):
    """ Orchestrates the report generation process:

    Args:
        report_name (str): The name of the report.
        report_dir (str): The directory where the report will be saved.
        path_to_shape (str): The path to the shapefile for the area of interest.
    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")
    # load shape as gdf
    aoi_gdf = gpd.read_file(path_to_shape)
    # get data first as csv
    safe_makedirs(os.path.join(report_dir, 'data'))
    csv_data_path = os.path.join(report_dir, 'data', 'data.csv')

    df_meta = get_metadata()
    df_meta.describe()

    if existing_csv_path:
        log.info(f"Using supplied csv file at {csv_data_path}")
        shutil.copyfile(existing_csv_path, csv_data_path)
    else:
        log.info(f"Querying athena for data for AOI")
        get_data_for_aoi(aoi_gdf, csv_data_path)
    data_gdf = load_gdf_from_csv(csv_data_path)
    data_gdf.meta.attach_metadata(df_meta)
    data_gdf = convert_gdf_units(data_gdf, 'US')
    data_gdf = add_calculated_cols(data_gdf)

    # excel version
    with pd.ExcelWriter(os.path.join(report_dir, 'data', 'data.xlsx')) as writer:
        data_gdf.to_excel(writer, sheet_name="data")
        df_meta.to_excel(writer, sheet_name="metadata")

    # make html report

    report_paths = make_report(data_gdf, aoi_gdf, report_dir, report_name, mode="both")
    html_path = report_paths["interactive"]
    static_path = report_paths["static"]
    log.info(f'Interactive HTML report built at {html_path}')
    log.info(f'Static HTML report built at {static_path}')

    if include_pdf:
        pdf_path = make_pdf_from_html(static_path)
        log.info(f'PDF report built from static at {pdf_path}')
    return


def main():
    """ Main function to parse arguments and generate the report
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=str)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=str)
    parser.add_argument('report_name', help='name for the report (usually description of the area selected)')
    parser.add_argument('--csv', help='Path to a local CSV of AOI data to use instead of querying Athena', type=str, default=None)
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

    # TODO add try /catch after testing
    make_report_orchestrator(args.report_name, output_path, args.path_to_shape, args.csv)
    log.info("all done")
    sys.exit(0)


def env_launch_params():
    """Default parameters for launching from the report launcher (Development env only).

    """

    # Get the base directory of the current file so that paths can be relative to it
    base_dir = os.path.dirname(__file__)
    return [
        "{env:DATA_ROOT}/rpt-rivers-need-space",
        # os.path.abspath(os.path.join(base_dir, "example/althouse_smaller_selection.geojson")),
        "{env:DATA_ROOT}/tmp/rock_cr_miss_247dgos.geojson",
        "Rock Cr 247",
        # "Althouse Creek 2",
        # "--csv",
        # "{env:DATA_ROOT}/tmp/althousecreek2.csv",
    ]


if __name__ == "__main__":
    main()
