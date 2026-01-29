# Standard library
import argparse
import logging
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import sys
import shutil
import traceback
import psutil  # for checking locally
# 3rd party imports
import pandas as pd
import geopandas as gpd
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from util import prepare_gdf_for_athena
from util.athena import (
    aoi_query_to_local_parquet,
    get_field_metadata
)
from util.html import RSReport
from util.pandas import load_gdf_from_pq, pprint_df_meta
from util.pandas import RSFieldMeta
from util.pdf import make_pdf_from_html
from util.figures import (
    make_aoi_outline_map,
    project_id_list,
    metric_cards,
)
from reports.rpt_riverscapes_dynamics import __version__ as report_version
from reports.rpt_riverscapes_dynamics.figures import linechart, statistics, area_histogram

_FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()


def define_fields(unit_system: str = "SI"):
    """Set up the fields and units for this report"""
    raw_table_meta = get_field_metadata(
        authority='data-exchange-scripts',
        authority_name='rsdynamics_to_athena',
        layer_id=["rsdynamics", "rsdynamics_metrics"]
    )
    _FIELD_META.field_meta = raw_table_meta

    create_report_view_metadata()

    _FIELD_META.unit_system = unit_system  # Set the unit system for the report

    # Here's where we can set any preferred units that differ from the data unit
    # _FIELD_META.set_display_unit('centerline_length', 'kilometer')
    # _FIELD_META.set_display_unit('segment_area', 'kilometer ** 2')


def create_report_view_metadata():
    """Define the mapping from source tables to the report view and create the metadata references."""
    log = Logger('Add View Metadata')
    report_view_name = 'dynamics_report'

    # Define the Schema map: "DataFrame Column" -> "Source Table"
    vw_to_table_field_map = {
        # rsdynamics
        'huc': 'rsdynamics',
        'rd_project_id': 'rsdynamics',
        'centerline_length': 'rsdynamics',
        'segment_area': 'rsdynamics',

        # rsdynamics_metrics
        'dgo_id': 'rsdynamics_metrics',
        'landcover': 'rsdynamics_metrics',
        'epoch_length': 'rsdynamics_metrics',
        'epoch_name': 'rsdynamics_metrics',
        'confidence': 'rsdynamics_metrics',
        'area': 'rsdynamics_metrics',
        'areapc': 'rsdynamics_metrics',
        'width': 'rsdynamics_metrics',
        'widthpc': 'rsdynamics_metrics'
    }

    # Create the View Metadata
    # We duplicate the raw source metadata into our new 'dynamics_report' namespace.
    # This allows us to use layer_id='dynamics_report' later without worrying about collisions.
    for col, source_layer_id in vw_to_table_field_map.items():
        # Check if we've already defined this view field (idempotency for Singleton)
        if not _FIELD_META.get_field_meta(col, report_view_name):
            try:
                _FIELD_META.duplicate_meta(
                    orig_name=col,
                    orig_layer_id=source_layer_id,
                    new_name=col,
                    new_layer_id=report_view_name
                )
            except Exception as e:
                # Log warning but continue; allows report to run even if one field is missing metadata
                log.warning(f"Could not map metadata for view '{report_view_name}': {source_layer_id}.{col} -> {e}")


def make_report(gdf: gpd.GeoDataFrame, dynmetrics: pd.DataFrame, aoi_df: gpd.GeoDataFrame,
                report_dir: Path, report_name: str,
                include_static: bool = True,
                include_pdf: bool = True,
                error_message: str | None = None
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
        error_message: display to user *instead* of any figures
    """
    log = Logger('make report')

    log.info(f"Generating report in {report_dir}")

    figures = {}
    appendices = {}

    if error_message is None:
        figures.update({
            "map": make_aoi_outline_map(aoi_df),
            "area-histogram-30": area_histogram(dynmetrics),
            "area-line-5yr": linechart(dynmetrics, 'area'),
            "areapc-line-5yr": linechart(dynmetrics, 'areapc'),
            "width-line-5yr": linechart(dynmetrics, 'width'),
            "widthpc-line-5yr": linechart(dynmetrics, 'widthpc')
        })

        appendices = {
            "project_ids": project_id_list(gdf, id_col='rd_project_id', name_col='rs_project_name'),
        }

    figure_dir = report_dir / 'figures'
    safe_makedirs(str(figure_dir))

    report = RSReport(
        report_name=report_name,
        report_type="Riverscapes Dynamics",
        report_dir=report_dir,
        report_version=report_version,
        figure_dir=figure_dir,
        body_template_path=os.path.join(os.path.dirname(__file__), 'templates', 'body.html'),
        css_paths=[os.path.join(os.path.dirname(__file__), 'templates', 'report.css')],
    )

    if error_message:
        report.add_html_elements('error_message', {'text': error_message})
    else:
        for (name, fig) in figures.items():
            report.add_figure(name, fig)

        # calculate statistics and make cards
        all_stats = statistics(gdf)
        metrics_for_key_indicators = ['count_dgos', 'total_segment_area', 'total_centerline_length',  'integrated_valley_bottom_width']
        metric_data_for_key_indicators = {k: all_stats[k] for k in metrics_for_key_indicators if k in all_stats}

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


def get_report_data(aoi_gdf: gpd.GeoDataFrame, report_dir: Path, parquet_override: Path | None) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """Get the report data for AOI, either from athena using the AOI or from parquet files at parquet_override (does not use the AOI)
    """
    log = Logger('Get report data')
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

        # refactored to get all the data from the wide "rsydnamics' plus the long "rsdynamics_metrics"
        # in a single nested result (ARRAY_AGG)
        # -- Use ID from  project_id and 'r' for grouping
        # -- Optimization: Don't group by Geometry. Use ARBITRARY() to pass it through.
        # -- Collapse the 30 metric rows into one list of structs
        query_str = """
SELECT 
    r.rd_project_id, 
    r.dgo_id,
    ANY_VALUE(p.name) AS rs_project_name,  
    ANY_VALUE(r.huc) AS huc, 
    ANY_VALUE(r.dgo_geom) AS dgo_geom,
    ANY_VALUE(centerline_length) AS centerline_length,
    ANY_VALUE(segment_area) as segment_area,
    ARRAY_AGG(
        CAST(
            ROW(
                m.landcover,
                m.epoch_length,
                m.epoch_name,
                m.confidence,
                m.area,
                m.areapc,
                m.width,
                m.widthpc
            ) AS ROW(
                landcover VARCHAR,
                epoch_length VARCHAR,
                epoch_name VARCHAR,
                confidence VARCHAR,
                area DOUBLE,
                areapc DOUBLE,
                width DOUBLE,
                widthpc DOUBLE
            )
        )
    ) AS metrics_list
FROM rsdynamics r
JOIN rsdynamics_metrics m 
    ON (r.rd_project_id = m.rd_project_id AND r.dgo_id = m.dgo_id)
JOIN data_exchange_projects p
    ON (p.archived = FALSE AND p.project_type_id='rsdynamics' and r.rd_project_id = p.project_id)
WHERE {prefilter_condition} AND {intersects_condition}
GROUP BY 
    r.rd_project_id,
    r.dgo_id
"""

        aoi_query_to_local_parquet(
            query_str,
            geometry_field_expression='ST_GeomFromBinary(dgo_geom)',
            geom_bbox_field='dgo_geom_bbox',
            aoi_gdf=query_gdf,
            local_path=parquet_data_source
        )

    df_raw = load_gdf_from_pq(parquet_data_source, geometry_col=None)

    # Check for empty result before proceeding
    if df_raw.empty:
        return (gpd.GeoDataFrame(), pd.DataFrame())

    # RESTRUCTURE THE DATAFRAME into 2
    # 1. Convert to category and SET THE INDEX IMMEDIATELY
    # This establishes the "rd_project_id + dgo_id" as the unique identifier
    for col in ['rd_project_id', 'huc']:
        df_raw[col] = df_raw[col].astype('category')
    df_raw = df_raw.set_index(['rd_project_id', 'dgo_id'])

    # ---------------------------------------------------------
    # 2. CREATE GEO DATAFRAME (The 1 side of 1:N)
    # ---------------------------------------------------------
    # Since the index is already set, we just grab the columns we want.
    # We copy first to avoid SettingWithCopy warnings and decouple from df_raw
    df_geo_data = df_raw[['huc', 'dgo_geom', 'rs_project_name', 'centerline_length', 'segment_area']].copy()

    # Reset index to move 'rd_project_id' and 'dgo_id' back to columns
    # This makes them accessible for reports and plotting without duplicating storage in df_raw
    df_geo_data = df_geo_data.reset_index()

    # Convert to GeoDataFrame
    # 1. Fast Vectorized Conversion
    # This is much faster than .apply(wkb.loads)
    df_geo_data['dgo_geom'] = gpd.GeoSeries.from_wkb(df_geo_data['dgo_geom'])
    # although it is standard to name the geom column geometry, it isn't required, and since we deal with many geoms I like to keep it more specific

    # 2. "Cast" to GeoDataFrame (Overwrite the variable)
    #    This consumes the old DataFrame and returns the GeoDataFrame wrapper.
    #    Pandas is smart enough to pass the data by reference here (it's a view),
    #    so no deep copy of the data occurs.
    df_geo_data = gpd.GeoDataFrame(df_geo_data, geometry='dgo_geom')

    # Now df_geo_data has all the spatial powers (crs, plot, etc.)
    # df_geo_data.set_crs(epsg=4326, inplace=True)

    # A. Explode: Turn 1 row with a list of 30 items into 30 rows
    exploded_series = df_raw['metrics_list'].explode()

    # B. Normalize: Turn the dictionary/struct column into separate columns
    # AWS Wrangler usually returns ROW types as Python dictionaries
    df_metrics = pd.DataFrame(
        exploded_series.tolist(),
        index=exploded_series.index
    )
    return (df_geo_data, df_metrics)


def make_report_orchestrator(report_name: str, report_dir: Path, path_to_shape: str,
                             include_pdf: bool = True, unit_system: str = "SI",
                             parquet_override: Path | None = None,
                             keep_parquet: bool = False,
                             ):
    """ Orchestrates the report generation process:

    Args:
        report_name (str): The name of the report.
        report_dir (Path): The directory where the report will be saved.
        path_to_shape (str): The path to the shapefile for the area of interest.
        include_pdf (bool, optional): Whether to generate a PDF version of the report. Defaults to True.
        unit_system (str, optional): The unit system to use ("SI" or "imperial"). Defaults to "SI".
        parquet_override (Path or None): for running multiple times in developement/test can supply path to previously downloaded data and skip athena query
        keep_parquet (bool, default False): keep parquet files, e.g. for debugging purposes

    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")

    # load shape as gdf
    aoi_gdf = gpd.read_file(path_to_shape)
    # make place for the data to go (as csv)
    safe_makedirs(str(report_dir / 'data'))

    # Start tasks in background
    # 1. Metadata definition (Athena query)
    meta_future = None
    executor = ThreadPoolExecutor(max_workers=2)
    meta_future = executor.submit(define_fields, unit_system)

    # meanwhile, get report data
    gdf_dgo, df_metrics = get_report_data(aoi_gdf, report_dir, parquet_override)

    error_msg = None
    if gdf_dgo.empty:
        error_msg = "No data found for the selected Area of Interest."
        log.warning(error_msg)

    else:
        # We need to explicitly set the layer_id so that downstream figures can resolve metadata
        # any dataframes copied from these inherit the attr
        gdf_dgo.attrs['layer_id'] = 'dynamics_report'
        df_metrics.attrs['layer_id'] = 'dynamics_report'

        # Ensure metadata is loaded before applying units
        try:
            meta_future.result()
            log.info("Metadata loaded successfully.")
        except Exception as e:
            log.error(f"Failed to load field metadata: {e}")
            raise e

        df_metrics, _ = _FIELD_META.apply_units(df_metrics, 'dynamics_report')  # this is still a geodataframe but we will need to be more explicit about it for type checking

        # Convert categorical columns that plotting libraries expect to be categories
        # We do this AFTER apply_units because default metadata might cast them explicitly to string
        # TODO: change apply_units etc. to not screw up dataframes that have columns set as categories unless that is explicitly asked for
        for col in ['landcover', 'epoch_length', 'epoch_name', 'confidence']:
            if col in df_metrics.columns:
                df_metrics[col] = df_metrics[col].astype('category')

        pprint_df_meta(df_metrics, 'dynamics_report')  # for DEBUG ONLY
        # this is not going to work
        pprint_df_meta(gdf_dgo, 'dynamics_report')  # for DEBUG ONLY

        # Export the data to Excel
        # dumping all the raw data is not appropriate especially for very large areas
        # RSGeoDataFrame(data_gdf).export_excel(report_dir / 'data' / 'data.xlsx')

    # make html report
    # If we aren't including pdf we just make interactive report. No need for the static one

    make_report(gdf_dgo, df_metrics, aoi_gdf, report_dir, report_name,
                include_static=include_pdf,
                include_pdf=include_pdf,
                error_message=error_msg
                )

    if not keep_parquet and not parquet_override:
        try:
            parquet_data_source = report_dir / "pq"
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
                                 )

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
