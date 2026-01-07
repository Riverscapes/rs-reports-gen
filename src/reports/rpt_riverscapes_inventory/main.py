# System imports
import argparse
import logging
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import sys
import shutil
import traceback
import psutil  # for checking locally
import pandas as pd
import geopandas as gpd
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from util import prepare_gdf_for_athena
from util.pandas import load_gdf_from_pq
from util.athena import athena_unload_to_dataframe, get_field_metadata
from util.color import DEFAULT_FCODE_COLOR_MAP

from util.athena import aoi_query_to_local_parquet
from util.pdf import make_pdf_from_html
from util.html import RSReport
from util.pandas import RSFieldMeta, RSGeoDataFrame
from util.figures import (
    get_bins_info,
    table_total_x_by_y,
    bar_group_x_by_y,
    bar_total_x_by_ybins,
    horizontal_bar_chart,
    make_aoi_outline_map,
    make_rs_area_by_featcode,
    prop_ag_dev,
    dens_road_rail,
    project_id_list,
    metric_cards,
)
from reports.rpt_riverscapes_inventory import __version__ as report_version
from reports.rpt_riverscapes_inventory.dataprep import add_calculated_rme_cols, get_nid_data, prepare_nid_display_table
from reports.rpt_riverscapes_inventory.figures import hypsometry_fig, statistics


def define_fields(unit_system: str = "SI"):
    """Set up the fields and units for this report"""
    _FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()
    _FIELD_META.field_meta = get_field_metadata(authority='data-exchange-scripts', authority_name='*', layer_id="raw_rme,rpt_rme,rs_context_huc10")
    _FIELD_META.unit_system = unit_system  # Set the unit system for the report

    # Here's where we can set any preferred units that differ from the data unit
    _FIELD_META.set_display_unit('centerline_length', 'kilometer')
    _FIELD_META.set_display_unit('segment_area', 'kilometer ** 2')

    return


def make_report(gdf: gpd.GeoDataFrame, huc_df: pd.DataFrame, aoi_df: gpd.GeoDataFrame,
                report_dir: Path, report_name: str,
                nid_gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(),
                include_static: bool = True,
                include_pdf: bool = True
                ):
    """
    Generates HTML report(s) in report_dir.
    Args:
        gdf (gpd.GeoDataFrame): The main data geodataframe for the report.
        huc_df (pd.DataFrame): The HUC data dataframe for the report.
        aoi_df (gpd.GeoDataFrame): The area of interest geodataframe.
        report_dir (Path): The directory where the report will be saved.
        report_name (str): The name of the report.
        include_static (bool, optional): Whether to include a static version of the report. Defaults to True.
        include_pdf (bool, optional): Whether to include a PDF version of the report. Defaults to True.
    """
    log = Logger('make report')

    # TODO: Check - beaver_dam_capacity only applies to perennieal - so may need to use filter gdf before building beaver_dam_capacity_bar
    # also can we make the units dams per km or dams per mile
    log.info(f"Generating report in {report_dir}")
    _edges, _labels, land_use_intens_bins_colours = get_bins_info('land_use_intens')
    figures = {
        "map": make_aoi_outline_map(aoi_df),
        "owner_bar": bar_group_x_by_y(gdf, 'segment_area', ['ownership_desc', 'fcode_desc']),
        "pie": make_rs_area_by_featcode(gdf),
        "low_lying_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['low_lying_ratio']),
        "prop_riparian_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['lf_riparian_prop']),
        "floodplain_access_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['fldpln_access']),
        "land_use_intensity_bar": bar_group_x_by_y(gdf, 'segment_area', ['land_use_intens_bins'], {'color': land_use_intens_bins_colours}),
        "prop_ag_dev": prop_ag_dev(gdf),
        "dens_road_rail": dens_road_rail(gdf),
        "hypsometry": hypsometry_fig(huc_df),
        "confinement_length_bar": bar_total_x_by_ybins(gdf, 'centerline_length', ['confinement_ratio', 'fcode_desc'], color_discrete_map=DEFAULT_FCODE_COLOR_MAP),
        "confinement_area_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['confinement_ratio', 'fcode_desc'], color_discrete_map=DEFAULT_FCODE_COLOR_MAP),
        "beaver_dam_capacity_historical_bar": horizontal_bar_chart(gdf, 'centerline_length', ['brat_hist_capacity']),
        "beaver_dam_capacity_current_bar": horizontal_bar_chart(gdf, 'centerline_length', ['brat_capacity']),
        "stream_order_bar": bar_group_x_by_y(gdf, 'centerline_length', ['stream_order']),
        "riparian_condition_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['riparian_condition']),
        "riparian_departure_bin_bar": bar_total_x_by_ybins(gdf, 'segment_area', ['riparian_veg_departure']),  # need to check these bins, also reverse them
        "riparian_departure_bin_bar2": bar_group_x_by_y(gdf, 'segment_area', ['riparian_veg_departure_bins'])
    }
    tables = {
        "river_names": table_total_x_by_y(gdf, 'centerline_length', ['stream_name']),
        "owners": table_total_x_by_y(gdf, 'centerline_length', ['ownership', 'ownership_desc']),
        "table_of_fcodes": table_total_x_by_y(gdf, 'centerline_length', ['fcode_desc'])
    }
    if not nid_gdf.empty:
        # Just top 100 for now to avoid huge tables in HTML
        nid_display_df = prepare_nid_display_table(nid_gdf)
        tables["nid_dams"] = nid_display_df.head(100).to_html(classes="table table-striped", index=False, escape=False)
    appendices = {
        "project_ids": project_id_list(gdf),
    }
    figure_dir = report_dir / 'figures'
    safe_makedirs(str(figure_dir))

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
    if not nid_gdf.empty:
        metric_data_for_key_indicators['total_dams'] = len(nid_gdf)
    report.add_html_elements('cards', metric_cards(metric_data_for_key_indicators))
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


def load_huc_data(hucs: list[str]) -> pd.DataFrame:
    """Queries rscontext_huc10 for all the huc10 watersheds that intersect the aoi
    * this could be a spatial query but we already have the huc12 from data_gdf so this is much faster
    * FUTURE ENHANCEMENT - take the aoi and join with huc geometries to produce some statistics about the amount of intersection between them
    * FUTURE ENHANCEMENT: check if we got data for all the hucs we were looking for
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


def make_report_orchestrator(report_name: str, report_dir: Path, path_to_shape: str,
                             include_pdf: bool = True, unit_system: str = "SI",
                             parquet_override: Path | None = None,
                             keep_parquet: bool = False,
                             disable_nid: bool = False,
                             ):
    """ Orchestrates the report generation process:

    Args:
        report_name (str): The name of the report.
        report_dir (Path): The directory where the report will be saved.
        path_to_shape (str): The path to the shapefile for the area of interest.
        existing_csv_path (Path | None, optional): Path to an existing CSV file to use instead of querying Athena. Defaults to None.
        include_pdf (bool, optional): Whether to generate a PDF version of the report. Defaults to True.
        unit_system (str, optional): The unit system to use ("SI" or "imperial"). Defaults to "SI".
        parquet_override (Path or None): for running multiple times in developement/test can supply path to previously downloaded data and skip athena query
        keep_parquet (bool, default False): keep parquet files, e.g. for debugging purposes

    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")

    # This is where all the initialization happens for fields and units
    define_fields(unit_system)  # ensure fields are defined

    aoi_gdf = gpd.read_file(path_to_shape)
    # make place for the data to go (as csv)
    safe_makedirs(str(report_dir / 'data'))

    # Start NID task in background
    nid_future = None
    executor = ThreadPoolExecutor(max_workers=2)
    if not disable_nid:
        log.info("Starting background NID query...")
        nid_future = executor.submit(get_nid_data, aoi_gdf)

    if parquet_override:
        parquet_data_source = Path(parquet_override)
        if not parquet_data_source.exists():
            raise FileNotFoundError(f"Parquet path '{parquet_data_source}' does not exist")
        log.info(f"Using supplied Parquet data files at {parquet_override}")
    else:
        parquet_data_source = report_dir / "pq"
        # use shape to query Athena
        query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
        if not simplification_results.success:
            raise ValueError("Unable to simplify input geometry sufficiently to insert into Athena query")
        if simplification_results.simplified:
            log.warning(
                f"""Input polygon was simplified using tolerance of {simplification_results.tolerance_m} metres for the purpose of intersecting with DGO geometries in the database.
                If you require a higher precision extract, please contact support@riverscapes.freshdesk.com.""")

        log.info("Querying Athena for data for AOI")

        fields_we_need = "level_path, seg_distance, centerline_length, segment_area, fcode, fcode_desc, longitude, latitude, ownership, ownership_desc, state, county, drainage_area, stream_name, stream_order, stream_length, huc12, rel_flow_length, channel_area, integrated_width, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, lf_agriculture_prop, lf_agriculture, lf_developed_prop, lf_developed, lf_riparian_prop, lf_riparian, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, develop, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent, confinement_ratio, brat_capacity,brat_hist_capacity, riparian_veg_departure, riparian_condition, rme_project_id, rme_project_name"
        query_str = f"SELECT {fields_we_need} FROM rpt_rme_pq WHERE {{prefilter_condition}} AND {{intersects_condition}}"

        aoi_query_to_local_parquet(
            query_str,
            geometry_field_expression='ST_GeomFromBinary(dgo_geom)',
            geom_bbox_field='dgo_geom_bbox',
            aoi_gdf=query_gdf,
            local_path=parquet_data_source
        )

    data_gdf = load_gdf_from_pq(parquet_data_source)
    data_gdf = add_calculated_rme_cols(data_gdf)
    data_gdf, _ = RSFieldMeta().apply_units(data_gdf)  # this is still a geodataframe but we will need to be more explicit about it for type checking

    unique_huc10 = data_gdf['huc12'].astype(str).str[:10].unique().tolist()
    huc_data_df = load_huc_data(unique_huc10)
    # print(huc_data_df)  # for DEBUG ONLY

    # Retrieve NID results
    nid_gdf = gpd.GeoDataFrame()
    if nid_future:
        try:
            nid_gdf = nid_future.result()
            log.info(f"NID background task finished. Found {len(nid_gdf)} dams.")
            if not nid_gdf.empty:
                nid_gdf.to_file(report_dir / 'data' / 'nid_dams.gpkg', driver='GPKG')
        except Exception as e:
            log.error(f"Error retrieving NID results: {e}")
        finally:
            executor.shutdown(wait=False)

    # Export the data to Excel
    # dumping all the raw data is not appropriate especially for very large areas
    RSGeoDataFrame(data_gdf).export_excel(report_dir / 'data' / 'data.xlsx')

    # make html report
    # If we aren't including pdf we just make interactive report. No need for the static one
    make_report(data_gdf, huc_data_df, aoi_gdf, report_dir, report_name,
                nid_gdf=nid_gdf,
                include_static=include_pdf,
                include_pdf=include_pdf
                )

    if not keep_parquet:
        try:
            if parquet_data_source.exists():
                shutil.rmtree(parquet_data_source)
                log.info(f"Deleted Parquet staging folder {parquet_data_source}")
        except Exception as cleanup_err:
            log.warning(f"Failed to delete Parquet folder {parquet_data_source}: {cleanup_err}")

    log.info(f"Report Path: {report_dir}")


def main():
    """ Main function to parse arguments and generate the report
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', help='Nonexistent folder to store the outputs (will be created)', type=Path)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=str)
    parser.add_argument('report_name', help='name for the report (usually description of the area selected)')
    parser.add_argument('--include_pdf', help='Include a pdf version of the report', action='store_true', default=False)
    parser.add_argument('--unit_system', help='Unit system to use: SI or imperial', type=str, default='SI')
    parser.add_argument(
        '--use-parquet',
        dest='parquet_path',
        type=Path,
        default=None,
        help='Use an existing Parquet file or directory instead of running the Athena AOI query'
    )
    parser.add_argument(
        '--keep-parquet',
        action='store_true',
        help='Keep the downloaded AOI Parquet files instead of deleting the pq folder'
    )
    parser.add_argument(
        '--no-nid',
        action='store_true',
        help='Disable fetching data from National Inventory of Dams'
    )
    # NOTE: IF WE CHANGE THESE VALUES PLEASE UPDATE ./launch.py

    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    output_path = Path(args.output_path)
    # new version of safe_makedirs will take a Path but for now all Paths are converted to string for this function
    safe_makedirs(str(output_path))

    log = Logger('Setup')
    log_path = output_path / 'report.log'
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-riverscapes-inventory')
    log.info(f"Output path: {output_path}")
    log.info(f"AOI shape: {args.path_to_shape}")
    log.info(f"Report name: {args.report_name}")
    log.info(f"Report Version: {report_version}")

    try:
        make_report_orchestrator(args.report_name,
                                 output_path,
                                 args.path_to_shape,
                                 args.include_pdf,
                                 args.unit_system,
                                 parquet_override=args.parquet_path,
                                 keep_parquet=args.keep_parquet,
                                 disable_nid=args.no_nid,)

        # While we work on performance, this is helpful
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().peak_wset / 1024 / 1024 if hasattr(process.memory_info(), 'peak_wset') else process.memory_info().rss / 1024 / 1024
        log.info(f"Peak memory usage: {mem_mb:.2f} MB\n")

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
