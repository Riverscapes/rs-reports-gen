# System imports
import argparse
import logging
import os
from pathlib import Path
import sys
import shutil
import traceback
# 3rd party imports
import geopandas as gpd

from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from util import prepare_gdf_for_athena
from util.pandas import load_gdf_from_csv
from util.athena import get_data_for_aoi, get_field_metadata
from util.pdf import make_pdf_from_html
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.figures import (
    table_total_x_by_y,
    bar_group_x_by_y,
    bar_total_x_by_ybins,
    make_map_with_aoi,
    make_rs_area_by_featcode,
    prop_ag_dev,
    dens_road_rail,
    project_id_list,
    metric_cards,
)
# Local imports
from reports.rpt_rivers_need_space.dataprep import add_calculated_cols
from reports.rpt_rivers_need_space import __version__ as report_version
from reports.rpt_rivers_need_space.figures import statistics


def define_fields(unit_system: str = "SI"):
    """Set up the fields and units for this report"""
    _FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()
    _FIELD_META.field_meta = get_field_metadata(authority='data-exchange-scripts', authority_name='*', layer_id="raw_rme,rpt_rme")  # Set the field metadata for the report
    _FIELD_META.unit_system = unit_system  # Set the unit system for the report

    # Here's where we can set any preferred units that differ from the data unit
    _FIELD_META.set_display_unit('centerline_length', 'kilometer')

    return


def log_unit_status(df, label: str):
    """ Log Unit Status
    Used for debugging, not part of production report. 
    Args:
        df (DataFrame): _description_
        label (str): _description_
    """
    log = Logger('UnitStatus')
    pint_cols = [col for col in df.columns if "pint" in str(df[col].dtype)]
    log.info(f"[{label}] Pint columns: {pint_cols}")
    log.info(f"[{label}] dtypes: {df.dtypes.to_dict()}")
    if pint_cols:
        log.info(f"[{label}] Sample value from {pint_cols[0]}: {df[pint_cols[0]].iloc[0]}")


def make_report(gdf: gpd.GeoDataFrame, aoi_df: gpd.GeoDataFrame,
                report_dir: Path, report_name: str,
                include_static: bool = True,
                include_pdf: bool = True
                ):
    """
    Generates HTML report(s) in report_dir.
    Args:
        gdf (gpd.GeoDataFrame): The main data geodataframe for the report.
        aoi_df (gpd.GeoDataFrame): The area of interest geodataframe.
        report_dir (Path): The directory where the report will be saved.
        report_name (str): The name of the report.
        include_static (bool, optional): Whether to include a static version of the report. Defaults to True.
        include_pdf (bool, optional): Whether to include a PDF version of the report. Defaults to True.
    """
    log = Logger('make report')

    figures = {
        "map": make_map_with_aoi(gdf, aoi_df),
        "owner_bar": bar_group_x_by_y(gdf, 'segment_area', ['ownership_desc', 'fcode_desc']),
        "pie": make_rs_area_by_featcode(gdf),
        "low_lying_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['low_lying_ratio']),
        "prop_riparian_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['lf_riparian_prop']),
        "floodplain_access_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['fldpln_access']),
        "land_use_intensity_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['land_use_intens']),
        "prop_ag_dev": prop_ag_dev(gdf),
        "prop_ag_dev_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['lf_agriculture_prop', 'lf_developed_prop']),
        "dens_road_rail": dens_road_rail(gdf),
        "dens_road_rail_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['road_dens', 'rail_dens'], {"orientation": 'h'}),
    }
    tables = {
        "river_names": table_total_x_by_y(gdf, 'stream_length', ['stream_name']),
        "owners": table_total_x_by_y(gdf, 'stream_length', ['ownership', 'ownership_desc']),
    }
    appendices = {
        "project_ids": project_id_list(gdf),
    }
    figure_dir = report_dir / 'figures'
    safe_makedirs(str(figure_dir))

    report = RSReport(
        report_name=report_name,
        report_type="Rivers Need Space",
        report_dir=report_dir,
        figure_dir=figure_dir,
        report_version=report_version,
        body_template_path=os.path.join(os.path.dirname(__file__), 'templates', 'body.html'),
        css_paths=[os.path.join(os.path.dirname(__file__), 'templates', 'report.css')],
    )
    for (name, fig) in figures.items():
        log.debug(f"Adding figure: {name}")
        report.add_figure(name, fig)

    report.add_html_elements('tables', tables)
    report.add_html_elements('cards', metric_cards(statistics(gdf)))
    report.add_html_elements('appendices', appendices)

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


def make_report_orchestrator(report_name: str, report_dir: Path, path_to_shape: Path,
                             existing_csv_path: Path | None = None,
                             include_pdf: bool = True, unit_system: str = "SI"):
    """ Orchestrates the report generation process:

    Args:
        report_name (str): The name of the report.
        report_dir (Path): The directory where the report will be saved.
        path_to_shape (Path): The path to the shapefile for the area of interest.
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
    # make place for the data to go (as csv)
    safe_makedirs(str(report_dir / 'data'))
    csv_data_path = report_dir / 'data' / 'data.csv'

    if existing_csv_path:
        log.info(f"Using supplied csv file at {csv_data_path}")
        if existing_csv_path != csv_data_path:
            shutil.copyfile(existing_csv_path, csv_data_path)
    else:
        # use shape to query Athena
        query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
        if not simplification_results.success:
            raise ValueError("Unable to simplify input geometry sufficiently to insert into Athena query")
        if simplification_results.simplified:
            log.warning(
                f"""Input polygon was simplified using tolerance of {simplification_results.tolerance_m} metres for the purpose of intersecting with DGO geometries in the database.
                If you require a higher precision extract, please contact support@riverscapes.freshdesk.com.""")

        log.info("Querying athena for data for AOI")
        get_data_for_aoi(None, query_gdf, csv_data_path)

    data_gdf = load_gdf_from_csv(csv_data_path)
    # log_unit_status(data_gdf, "Loaded")
    data_gdf, _ = RSFieldMeta().apply_units(data_gdf)  # this is still a geodataframe but we will need to be more explicity about it for type checking
    # log_unit_status(data_gdf, "Applied units")

    data_gdf = add_calculated_cols(data_gdf)
    # log_unit_status(data_gdf, "added calculated columns")

    # Export the data to Excel
    RSGeoDataFrame(data_gdf).export_excel(os.path.join(report_dir, 'data', 'data.xlsx'))

    # make html report
    # log_unit_status(data_gdf, "after export to excel")

    # If we aren't including pdf we just make interactive report. No need for the static one
    make_report(data_gdf, aoi_gdf, report_dir, report_name,
                include_static=include_pdf,
                include_pdf=include_pdf
                )

    log.info(f"Report Path: {report_dir}")


def main():
    """ Main function to parse arguments and generate the report
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=Path)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=Path)
    parser.add_argument('report_name', help='name for the report (usually description of the area selected)')
    parser.add_argument('--include_pdf', help='Include a pdf version of the report', action='store_true', default=False)
    parser.add_argument('--unit_system', help='Unit system to use: SI or imperial', type=str, default='SI')
    parser.add_argument('--csv', help='Path to a local CSV of AOI data to use instead of querying Athena', type=str, default=None)
    # NOTE: IF WE CHANGE THESE VALUES PLEASE UPDATE ./launch.py

    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    output_path = Path(args.output_path)
    # new version of safe_makedirs will take a Path but for now all Paths are converted to string for this function
    safe_makedirs(str(output_path))

    log = Logger('Setup')
    log_path = output_path / 'report.log'
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-rivers-need-space')
    log.info(f"Output path: {output_path}")
    path_to_shape = Path(args.path_to_shape)
    log.info(f"AOI shape: {path_to_shape}")
    log.info(f"Report name: {args.report_name}")
    log.info(f"Report Version: {report_version}")
    if args.csv:
        csv_path = Path(args.csv)
        log.info(f"Using existing CSV: {csv_path}")
    else:
        log.info("No existing CSV provided, will query Athena")
        csv_path = None

    try:
        make_report_orchestrator(
            args.report_name,
            output_path,
            path_to_shape,
            csv_path,
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
