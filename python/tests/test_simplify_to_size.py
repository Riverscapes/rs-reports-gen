import sys
import os
import geopandas as gpd
from util.rs_geo_helpers import simplify_to_size
import time

# Example GeoJSON or shapefile paths for testing
test_files = [
    '/mnt/c/nardata/work/rme_extraction/20250827-rkymtn/physio_rky_mtn_system_4326.geojson',
    '/mnt/c/nardata/work/rme_extraction/Price-riv/pricehuc10s.geojson',
    '/mnt/c/nardata/rslocal/rs_metric_engine/Mackobee-coul/project_bounds.geojson',
]

MAX_SIZE = 39_500

for path in test_files:
    if not os.path.exists(path):
        print(f"File not found: {path}")
        continue
    print(f"\nTesting: {path}")
    gdf = gpd.read_file(path)
    t0 = time.time()
    simplified_gdf, tolerance, succeeded = simplify_to_size(gdf, MAX_SIZE)
    t1 = time.time()
    geojson_str = simplified_gdf.to_json()
    size = len(geojson_str.encode('utf-8'))
    print(f"  Final size: {size} bytes")
    print(f"  Tolerance used: {tolerance} m")
    print(f"  Success: {succeeded}")
    print(f"  Time taken: {t1-t0:.3f} seconds")
