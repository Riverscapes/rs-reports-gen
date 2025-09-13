import argparse
import logging
import os
import geopandas as gpd
import pandas as pd
import tempfile
import sys
import weasyprint
from shapely import wkt
from datetime import datetime

import plotly.graph_objects as go
import plotly.io as pio
from jinja2 import Template

from figures import make_map, make_rs_area_by_owner
from rsxml import dotenv, Logger
from rsxml.util import safe_makedirs
from util_athena_query_aoi import run_aoi_athena_query # FIX should be in util
from util_athena import get_s3_file # FIX should be in util

S3_BUCKET = "riverscapes-athena"

# not specific to this report... can go in another file
def export_figure (fig: go.Figure, out_dir: str, name: str, mode: str, include_plotlyjs=False, report_dir=None):
    """export plotly figure html
    either interactive, or with path to static image
    """
    # what is the return signature? Can the mode signature list the options? 
    if mode == "interactive":
        # Enable mode bar for interactivity (zoom, pan, etc.)
        return pio.to_html(
            fig,
            include_plotlyjs=include_plotlyjs,
            full_html=False,
            config={"displayModeBar": True}
        )
    # will this work? make case insensitive 
    elif mode in ('png','jpeg','svg','pdf', 'webp'):
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
        raise NotImplementedError # is there a better error? 

def make_report(gdf: gpd.GeoDataFrame, report_dir, report_name, mode="interactive"):
    """
    Generates HTML report(s) in report_dir.
    mode: "interactive", "static", or "both"
    Returns path(s) to the generated html file(s).
    """
    log = Logger('make report')

    figures = {
        "map": make_map(gdf),
        "bar": make_rs_area_by_owner(gdf),
    }
    figure_dir = os.path.join(report_dir, 'figures')
    safe_makedirs(figure_dir)

    def render_report(fig_mode, suffix=""):
        figure_exports = {}
        for i, (name, fig) in enumerate(figures.items()):
            include_js = (i == 0) if fig_mode == "interactive" else False
            figure_exports[name] = export_figure(
                fig, figure_dir, name, mode=fig_mode, include_plotlyjs=include_js, report_dir=report_dir
            )
        with open('templates/p_template.html', encoding='utf8') as t:
            template = Template(t.read())
        with open('templates/report.css', encoding='utf8') as css_file:
            css = css_file.read()
        style_tag = f"<style>{css}</style"
        now = datetime.now()
        html = template.render(
            report={
                'head': style_tag,
                'title': report_name,
                'date': now.strftime('%B %d, %Y - %I:%M%p'),
                'ReportType': "Rivers Need Space"
            },
            report_name=report_name,
            figures=figure_exports
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
    df = pd.read_csv(csv_path)
    df.describe() # outputs some info for debugging
    df['dgo_polygon_geom'] = df['dgo_geom_obj'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='dgo_polygon_geom', crs='EPSG:4326')
    # print(gdf)
    return gdf

def get_data(gdf: gpd.GeoDataFrame) -> str:
    """given aoi in gdf format (assume 4326), just get all the raw_rme (for now)
    returns: local path to the data"""
    log = Logger ('Run AOI query on Athena')
    # temporary approach -- later try using report-type specific CTAS and report-specific UNLOAD statement
    fields_str = "level_path, seg_distance, centerline_length, segment_area, fcode, longitude, latitude, ownership, state, county, drainage_area, stream_name, stream_order, stream_length, huc12, rel_flow_length, channel_area, integrated_width, low_lying_ratio, elevated_ratio, floodplain_ratio, acres_vb_per_mile, hect_vb_per_km, channel_width, lf_agriculture_prop, lf_agriculture, lf_developed_prop, lf_developed, lf_riparian_prop, lf_riparian, ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, develop, road_len, road_dens, rail_len, rail_dens, land_use_intens, road_dist, rail_dist, div_dist, canal_dist, infra_dist, fldpln_access, access_fldpln_extent"
    s3_csv_path = run_aoi_athena_query(gdf, S3_BUCKET, fields_str=fields_str)
    if s3_csv_path is None:
        log.error("Didn't get a result from athena")
        raise NotImplementedError
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile:
        local_csv_path = tmpfile.name
        get_s3_file (s3_csv_path, local_csv_path)
    # local_csv_path = r"/tmp/tmphcfn8l6q.csv" # FOR TESTING ONLY 
    return local_csv_path

def make_pdf_from_html(html_path: str) -> str:
    """
    Generate a PDF from an HTML file using WeasyPrint.
    Returns the path to the generated PDF.
    """
    pdf_path = os.path.splitext(html_path)[0] + ".pdf"
    weasyprint.HTML(html_path).write_pdf(pdf_path)
    return pdf_path

def make_report_orchestrator (report_name: str, report_dir: str, path_to_shape: str):
    log = Logger ('Make report orchestrator')
    log.info("Report orchestration begun")
    # load shape as gdf
    aoi_gdf = gpd.read_file(path_to_shape)
    # get data first as csv
    csv_data_path = get_data(aoi_gdf)
    data_gdf = load_gdf_from_csv(csv_data_path)
    # make html report
    report_paths = make_report(data_gdf, report_dir, report_name, mode="both")
    html_path = report_paths["interactive"]
    static_path = report_paths["static"]
    log.info (f'Interactive HTML report built at {html_path}')
    log.info (f'Static HTML report built at {static_path}')
    # make pdf 
    pdf_path = make_pdf_from_html(static_path)
    log.info (f'PDF report built from static at {pdf_path}')
    return


def main():
    print("Hello from rs-rpt-rivers-need-space!")
    parser = argparse.ArgumentParser()
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('path_to_shape', help='path to the geojson that is the aoi to process', type=str)
    parser.add_argument('report_name', help='name for the report (usually description of the area selected)')
    parser.add_argument('--csv', help='Path to a local CSV to use instead of querying Athena', type=str, default=None)
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = args.working_folder
    # if want each iteration to be saved add datetimestamp to path
    dt_str = datetime.now().strftime("%y%m%d_%H%M") 
    # dt_str = ""
    report_dir = os.path.join(working_folder, dt_str, 'report')  # , 'outputs', 'riverscapes_metrics.gpkg')
    safe_makedirs(report_dir)

    log = Logger('Setup')
    log_path = os.path.join(report_dir, 'rpt-gen.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-rivers-need-space')

    if args.csv: # skip the generation of csv
        data_gdf = load_gdf_from_csv(args.csv)
        html_path = make_report(data_gdf, report_dir, args.report_name, mode="static")
        log.info (f'HTML report built at {html_path}')
        # make pdf 
        pdf_path = make_pdf_from_html(html_path)
        log.info (f'PDF report built at {pdf_path}')
        sys.exit(0)

    # TODO add try /catch after testing
    make_report_orchestrator (args.report_name, report_dir, args.path_to_shape)
    print("all done")
    sys.exit(0)

if __name__ == "__main__":
    main()
