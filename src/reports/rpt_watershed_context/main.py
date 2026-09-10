"""Main module for Watershed Context report"""

# System imports
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rsxml import Logger
from rsxml.util import safe_makedirs

from reports.rpt_watershed_context.version import report_version
from util import prepare_gdf_for_athena
from util.figures import metric_cards
from util.html import RSReport
from util.pandas import RSFieldMeta
from util.pdf import make_pdf_from_html


def define_fields(unit_system: str = 'SI') -> None:
    """Register field metadata and configure unit system for this report.

    Args:
        unit_system (str): The unit system to use ('SI' or 'Imperial').

    Returns:
        None
    """
    meta = RSFieldMeta()
    meta.unit_system = unit_system

    # Here's where we can set any preferred units that differ from the data unit


def make_report(df: pd.DataFrame, report_dir: Path, report_name: str, aoi_gdf: gpd.GeoDataFrame, include_static: bool = True, include_pdf: bool = True, unit_system: str = 'SI'):
    """Generate the Watershed Context report.

    Args:
        df (pd.DataFrame): The main data frame containing watershed data.
        report_dir (Path): The directory where the report will be saved.
        report_name (str): The name of the report file.
        aoi_gdf (gpd.GeoDataFrame): The area of interest as a GeoDataFrame.
        include_static (bool, optional): Whether to include static content. Defaults to True.
        include_pdf (bool, optional): Whether to generate a PDF version of the report. Defaults to True.
        unit_system (str, optional): The unit system to use ('SI' or 'Imperial'). Defaults to 'SI'.

    Note: define_fields() must be called before this function to configure units.
    """
    log = Logger('make report')

    log.info(f"Generating report in {report_dir}")

    figures = {}

    figure_dir = report_dir / "figures"
    safe_makedirs(str(figure_dir))

    tables = {}

    stats = {}

    report = RSReport(
        report_name="",
        report_subtitle=report_name,
        report_type="Watershed Context",
        report_dir=report_dir,
        report_version=report_version,
        body_template_path=Path(__file__).parent / 'templates' / 'body.html',
        css_paths=[Path(__file__).parent / 'templates' / 'report.css'],
    )
    for name, fig in figures.items():
        report.add_figure(name, fig)

    report.add_html_elements("tables", tables)
    report.add_html_elements("cards", metric_cards(stats))

    interactive_path = report.render(fig_mode="interactive", suffix="")
    static_path = None
    pdf_path = None
    if include_static:
        static_path = report.render(fig_mode="svg", suffix="_static")
        if include_pdf:
            pdf_path = make_pdf_from_html(static_path)
            log.info(f'PDF report built from static at {pdf_path}')

    log.title('Report Generation Complete')
    log.info(f'Interactive report available at {interactive_path}')
    if static_path:
        log.info(f'Static report available at {static_path}')
    if pdf_path:
        log.info(f'PDF report available at {pdf_path}')


def make_report_orchestrator(report_name: str, report_dir: Path, path_to_shape: str, include_pdf: bool = True, unit_system: str = 'SI', parquet_override: Path | None = None, keep_parquet: bool = False):
    """Orchestrates the report generation process

    Args:
        report_name (str): The name of the report.
        report_dir (Path): The directory where the report will be saved.
        path_to_shape (str): The path to the shapefile for the area of interest.
        include_pdf (bool, optional): Whether to generate a PDF version of the report. Defaults to True.
        unit_system (str, optional): The unit system to use ('SI' or 'Imperial'). Defaults to 'SI'.
        parquet_override (Path | None, optional): Path to an existing parquet file to use instead of generating a new one. Defaults to None.
        keep_parquet (bool, optional): Whether to keep the generated parquet file. Defaults to False.
    """
    log = Logger('Make report orchestrator')
    log.info("Report orchestration begun")

    define_fields(unit_system)

    aoi_gdf = gpd.read_file(path_to_shape)

    if parquet_override:
        parquet_data_source = Path(parquet_override)
        if not parquet_data_source.exists():
            raise FileNotFoundError(f"Parquet override file not found: {parquet_data_source}")
        log.info(f"Using parquet override file: {parquet_data_source}")
    else:
        parquet_data_source = report_dir / 'pq'
        # use shape to query Athena
        query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
        if not simplification_results.success:
            raise RuntimeError("Failed to simplify the geometry for Athena query.")
        if simplification_results.simplified:
            log.warning(
                f"""Input polygon was simplified using tolerance of {simplification_results.tolerance_m} metres for the purpose of intersecting with DGO geometries in the database.
                                If you require a higher precision extract, please contact support@riverscapes.freshdesk.com."""
            )

        log.info("Querying athena for data for AOI")
