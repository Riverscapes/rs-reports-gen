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
    - A map of the polygons in dgo_polygon_geom using Folium
    - A bar chart showing the sum of segment_area by ownership using Altair
    """
    import folium
    import altair as alt
    from branca.element import MacroElement
    from jinja2 import Template
    import json

    # 1. Create Folium map
    if gdf.empty:
        center = [0, 0]
    else:
        center = [gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()]
    m = folium.Map(location=center, zoom_start=8, tiles='cartodbpositron')
    folium.GeoJson(gdf[['dgo_polygon_geom', 'ownership']],
                   name="Polygons",
                   tooltip=folium.GeoJsonTooltip(fields=["ownership"]))\
        .add_to(m)

    # Extract only the map <div> and required scripts/styles from Folium
    folium_html = m.get_root().render()
    # Extract the map <div> (id starts with 'map_')
    import re
    map_div_match = re.search(r'(<div class="folium-map".*?</div>)', folium_html, re.DOTALL)
    map_div = map_div_match.group(1) if map_div_match else ''
    # Extract all <script> and <link> tags for Folium
    folium_scripts = '\n'.join(re.findall(r'(<script.*?</script>)', folium_html, re.DOTALL))
    folium_links = '\n'.join(re.findall(r'(<link.*?>)', folium_html, re.DOTALL))
    folium_styles = '\n'.join(re.findall(r'(<style.*?</style>)', folium_html, re.DOTALL))

    # 2. Create Altair horizontal bar chart (sum of segment_area by ownership)
    chart_data = gdf.groupby('ownership', as_index=False)['segment_area'].sum()
    chart = alt.Chart(chart_data).mark_bar().encode(
        y=alt.Y('ownership:N', title='Ownership'),
        x=alt.X('segment_area:Q', title='Total Segment Area'),
        tooltip=['ownership', 'segment_area']
    ).properties(
        width=600,
        height=400,
        title='Total Segment Area by Ownership'
    )
    # Get only the chart fragment (no <html>/<head>/<body>)
    chart_html = chart.to_html()

    # 3. Use Jinja2 template to combine fragments into a valid HTML doc
    template = Template("""
    <html>
    <head>
        <title>{{ report_name }}</title>
        <meta charset='utf-8'>
        <style>body { font-family: Arial, sans-serif; margin: 2em; }</style>
        {{ folium_links|safe }}
        {{ folium_styles|safe }}
    </head>
    <body>
        <h1>{{ report_name }}</h1>
        <h2>Map of Polygons</h2>
        {{ map_div|safe }}
        {{ folium_scripts|safe }}
        <h2>Segment Area by Ownership</h2>
        {{ chart_html|safe }}
    </body>
    </html>
    """)
    html = template.render(
        report_name=report_name,
        map_div=map_div,
        folium_links=folium_links,
        folium_styles=folium_styles,
        folium_scripts=folium_scripts,
        chart_html=chart_html
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
    print(gdf)
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
