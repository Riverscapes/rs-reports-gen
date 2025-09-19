# System imports
import argparse
import logging
import os
import tempfile
import sys
from datetime import datetime
from typing import Optional
from importlib import resources
# 3rd party imports
import geopandas as gpd
import pandas as pd
import weasyprint
from shapely import wkt

import plotly.graph_objects as go
import plotly.io as pio
from jinja2 import Template

from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from util.athena import get_s3_file, run_aoi_athena_query
# Local imports
from reports.rpt_rivers_need_space.figures import (make_map,
                                                   make_rs_area_by_owner,
                                                   make_rs_area_by_featcode,
                                                   make_map_with_aoi,
                                                   statistics,
                                                   table_of_river_names,
                                                   table_of_ownership,
                                                   )


S3_BUCKET = "riverscapes-athena"

# not specific to this report... can go in another file


def export_figure(fig: go.Figure, out_dir: str, name: str, mode: str, include_plotlyjs=False, report_dir=None) -> str:
    """export plotly figure html
    either interactive, or with path to static image created at out_dir
    either way returns html fragment
    """
    if mode == "interactive":
        # Enable mode bar for interactivity (zoom, pan, etc.)
        return pio.to_html(
            fig,
            include_plotlyjs=include_plotlyjs,
            full_html=False,
            config={"displayModeBar": True}
        )
    # will this work? make case insensitive
    elif mode in ('png', 'jpeg', 'svg', 'pdf', 'webp'):
        img_filename = f"{name}.{mode}"
        img_path = os.path.join(out_dir, img_filename)
        # requires kaleido (python packge) to be installed
        # and that requires Google Chrome to be installed - plotly_get_chrome or kaleido.get_chrome() or kaleido.get_chrome_sync()
        if report_dir:
            rel_path = os.path.relpath(img_path, start=report_dir)
        else:
            rel_path = img_filename
        fig.write_image(img_path)
        html_fragment = f'<img src="{rel_path}">'
        return html_fragment
    else:
        raise NotImplementedError  # is there a better error?


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
        "pie": make_rs_area_by_featcode(gdf)
    }
    tables = {
        "river_names": table_of_river_names(gdf),
        "owners": table_of_ownership(gdf)
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
    df['dgo_polygon_geom'] = df['dgo_geom_obj'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='dgo_polygon_geom', crs='EPSG:4326')
    # print(gdf)
    return gdf


def get_data_for_aoi(gdf: gpd.GeoDataFrame, output_path: str):
    """given aoi in gdf format (assume 4326), just get all the raw_rme (for now)
    returns: local path to the data csv file"""
    log = Logger('Run AOI query on Athena')
    # temporary approach -- later try using report-type specific CTAS and report-specific UNLOAD statement
    fields_str = "level_path, seg_distance, centerline_length, segment_area, fcode, fcode_desc, longitude, latitude, ownership, ownership_desc, state, county, drainage_area, stream_name, stream_order, stream_length, huc12, rel_flow_length, channel_area, integrated_width, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, lf_agriculture_prop, lf_agriculture, lf_developed_prop, lf_developed, lf_riparian_prop, lf_riparian, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, develop, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent"
    s3_csv_path = run_aoi_athena_query(gdf, S3_BUCKET, fields_str=fields_str, source_table="rpt_rme")
    if s3_csv_path is None:
        log.error("Didn't get a result from athena")
        raise NotImplementedError
    get_s3_file(s3_csv_path, output_path)
    return


def make_pdf_from_html(
    html_path: str,
    *,
    page_margin: str = "0.1in",
    zoom: float = 1.0,
    extra_styles: Optional[list[weasyprint.CSS]] = None,
) -> str:
    """Generate a PDF from an HTML file using WeasyPrint with layout controls.

    Args:
        html_path: Path to the source HTML document.
        page_margin: CSS margin value injected into the `@page` rule.
        zoom: Zoom factor passed to WeasyPrint's renderer (1.0 = 100%).

    Returns:
        Path to the generated PDF file.
    """
    pdf_path = os.path.splitext(html_path)[0] + ".pdf"
    margin_css = weasyprint.CSS(
        string=(
            "@page { margin: %s; } "
            "body { margin: 0 !important; padding: 0 !important; }"
        ) % page_margin,
        media_type="print",
    )

    stylesheets = [margin_css]

    if extra_styles:
        stylesheets.extend(extra_styles)

    weasyprint.HTML(filename=html_path, base_url=os.path.dirname(html_path)).write_pdf(
        pdf_path,
        stylesheets=stylesheets,
        zoom=zoom,
        presentational_hints=True,
    )
    return pdf_path


def make_report_orchestrator(report_name: str, report_dir: str, path_to_shape: str):
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
    get_data_for_aoi(aoi_gdf, csv_data_path)
    data_gdf = load_gdf_from_csv(csv_data_path)
    # make html report
    report_paths = make_report(data_gdf, aoi_gdf, report_dir, report_name, mode="both")
    html_path = report_paths["interactive"]
    static_path = report_paths["static"]
    log.info(f'Interactive HTML report built at {html_path}')
    log.info(f'Static HTML report built at {static_path}')
    # make pdf
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
    parser.add_argument('--csv', help='Path to a local CSV to use instead of querying Athena', type=str, default=None)
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    output_path = args.output_path
    # if want each iteration to be saved add datetimestamp to path
    # dt_str = datetime.now().strftime("%y%m%d_%H%M")
    # dt_str = ""
    safe_makedirs(output_path)

    log = Logger('Setup')
    log_path = os.path.join(output_path, 'rpt-gen.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-rivers-need-space')

    if args.csv:  # skip the generation of csv
        data_gdf = load_gdf_from_csv(args.csv)
        # load shape as gdf
        aoi_gdf = gpd.read_file(args.path_to_shape)
        # make html report
        report_paths = make_report(data_gdf, aoi_gdf, output_path, args.report_name, mode="both")
        html_path = report_paths["interactive"]
        static_path = report_paths["static"]
        log.info(f'Interactive HTML report built at {html_path}')
        log.info(f'Static HTML report built at {static_path}')
        # make pdf
        pdf_path = make_pdf_from_html(static_path)
        log.info(f'PDF report built from static at {pdf_path}')
        sys.exit(0)

    # TODO add try /catch after testing
    make_report_orchestrator(args.report_name, output_path, args.path_to_shape)
    log.info("all done")
    sys.exit(0)


def env_launch_params():
    """Default parameters for launching from the report launcher (Development env only).

    """

    # Get the base directory of the current file so that paths can be relative to it
    base_dir = os.path.dirname(__file__)
    return [
        "{env:DATA_ROOT}/rpt-rivers-need-space",
        os.path.abspath(os.path.join(base_dir, "example/althouse_smaller_selection.geojson")),
        # "{env:DATA_ROOT}/tmp/rock_cr_miss_247dgos.geojson",
        "Rock Cr 247",
        # "Althouse Creek 2",
        # "--csv",
        # "{env:DATA_ROOT}/tmp/rock247_data.csv",
    ]


if __name__ == "__main__":
    main()
