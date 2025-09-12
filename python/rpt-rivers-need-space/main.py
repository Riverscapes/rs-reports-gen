import argparse
import logging
import os
import geopandas as gpd
import pandas as pd
import tempfile
import sys
from shapely import wkt
from datetime import datetime
from rsxml import dotenv, Logger
from rsxml.util import safe_makedirs
from util_athena_query_aoi import run_aoi_athena_query # FIX should be in util
from util_athena import get_s3_file # FIX should be in util

S3_BUCKET = "riverscapes-athena"

def make_report(gdf: gpd.GeoDataFrame, report_dir, report_name):
    """
    Generates a simple HTML report in report_dir containing:
    - The report_name as a title
    - A map of the polygons in dgo_polygon_geom using Plotly
    - A horizontal bar chart showing the sum of segment_area by ownership using Plotly
    """
    import plotly.express as px
    import plotly.graph_objects as go
    import plotly.io as pio
    import pandas as pd
    import geopandas as gpd
    from jinja2 import Template

    # Ensure geometry column is correct and reset index for mapping
    gdf = gdf.copy()
    gdf = gdf.reset_index(drop=True)
    gdf["id"] = gdf.index  # unique id for each row

    # 1. Create Plotly map (GeoJSON polygons)
    geojson = gdf.set_geometry("dgo_polygon_geom").__geo_interface__
    # Compute extent and center
    if gdf.empty:
        center = {"lat": 0, "lon": 0}
        zoom = 2
    else:
        bounds = gdf["dgo_polygon_geom"].total_bounds  # [minx, miny, maxx, maxy]
        center = {"lat": (bounds[1] + bounds[3]) / 2, "lon": (bounds[0] + bounds[2]) / 2}
        # Estimate zoom: smaller area = higher zoom
        # This is a rough heuristic for zoom based on longitude span
        lon_span = bounds[2] - bounds[0]
        if lon_span < 0.01:
            zoom = 14
        elif lon_span < 0.1:
            zoom = 12
        elif lon_span < 1:
            zoom = 10
        elif lon_span < 5:
            zoom = 8
        elif lon_span < 20:
            zoom = 6
        else:
            zoom = 4

    map_fig = px.choropleth_mapbox(
        gdf,
        geojson=geojson,
        locations="id",
        color="fcode",
        featureidkey="properties.id",
        center=center,
        mapbox_style="carto-positron",
        zoom=zoom,
        opacity=0.5,
        hover_name="fcode",
        hover_data={"segment_area": True, "ownership": True}
    )
    map_fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500)

    # 2. Create horizontal bar chart (sum of segment_area by ownership)
    chart_data = gdf.groupby('ownership', as_index=False)['segment_area'].sum()
    bar_fig = px.bar(
        chart_data,
        y="ownership",
        x="segment_area",
        orientation="h",
        title="Total Segment Area by Ownership",
        labels={"segment_area": "Total Segment Area", "ownership": "Ownership"},
        height=400
    )
    bar_fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})

    # 3. Export both figures to HTML divs
    # Enable mode bar for interactivity (zoom, pan, etc.)
    map_html = pio.to_html(map_fig, include_plotlyjs=True, full_html=False, config={"displayModeBar": True})
    bar_html = pio.to_html(bar_fig, include_plotlyjs=False, full_html=False, config={"displayModeBar": False})

    # 4. Combine into a single HTML page using Jinja2
    with open('templates/p_template.html', encoding='utf8') as t:
        template = Template(t.read())
    html = template.render(
        report_name=report_name,
        map_html=map_html,
        bar_html=bar_html
    )
    out_path = os.path.join(report_dir, "report.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Report written to {out_path}")

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
        raise NotImplementedError()
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile:
        local_csv_path = tmpfile.name
        get_s3_file (s3_csv_path, local_csv_path)
    # local_csv_path = r"/tmp/tmphcfn8l6q.csv" # FOR TESTING ONLY 
    return local_csv_path

def make_report_orchestrator (report_name: str, report_dir: str, path_to_shape: str):
    log = Logger ('Make report orchestrator')
    log.info("Report orchestration begun")
    # load shape as gdf
    aoi_gdf = gpd.read_file(path_to_shape)
    # get data first as csv
    csv_data_path = get_data(aoi_gdf)
    data_gdf = load_gdf_from_csv(csv_data_path)
    # make html report
    make_report(data_gdf, report_dir, report_name)
    # make pdf 
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
    # so I don't have to delete it every time I run a test, add datetimestamp
    dt_str = datetime.now().strftime("%y%m%d%H%M")
    report_dir = os.path.join(working_folder, dt_str, 'report')  # , 'outputs', 'riverscapes_metrics.gpkg')
    safe_makedirs(report_dir)

    log = Logger('Setup')
    log_path = os.path.join(report_dir, 'rpt-gen.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title('rs-rpt-rivers-need-space')

    if args.csv:
        data_gdf = load_gdf_from_csv(args.csv)
        make_report(data_gdf, report_dir, args.report_name)
        print("Report generated from CSV.")
        sys.exit(0)

    # TODO add try /catch after testing
    make_report_orchestrator (args.report_name, report_dir, args.path_to_shape)
    print("all done")
    sys.exit(0)

if __name__ == "__main__":
    main()
