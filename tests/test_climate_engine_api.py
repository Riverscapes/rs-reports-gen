import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import MultiPolygon, Point, Polygon, MultiPoint

from util.climate_engine_connections import (
    CLIMATE_ENGINE_BASE_API_URL,
    extract_coordinates,
    query_climate_engine,
)

load_dotenv()


def test_extract_coordinates_point():
    gdf = gpd.GeoDataFrame(geometry=[Point(-121.61, 38.78)])
    result = extract_coordinates(gdf)
    expected_result = [[-121.61, 38.78]]
    print(f'Expected: {json.dumps(expected_result)}')
    print(f'Actual  : {json.dumps(result)}')
    assert json.dumps(result, sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def test_extract_coordinates_multipoint():
    # Single MultiPoint
    multipoint = MultiPoint([[-121.61, 38.78], [-122.71, 39.74]])
    gdf_multipoint = gpd.GeoDataFrame(geometry=[multipoint])
    result_multipoint = extract_coordinates(gdf_multipoint)
    expected_multipoint = [[-121.61, 38.78], [-122.71, 39.74]]
    print(f'Expected (MultiPoint): {json.dumps(expected_multipoint)}')
    print(f'Actual   (MultiPoint): {json.dumps(result_multipoint)}')
    assert json.dumps(result_multipoint, sort_keys=True) == json.dumps(expected_multipoint, sort_keys=True)


def test_extract_coordinates_multiple_points():
    # Multiple Points
    gdf_points = gpd.GeoDataFrame(geometry=[Point(-121.61, 38.78), Point(-122.71, 39.74)])
    result_points = extract_coordinates(gdf_points)
    expected_points = [[-121.61, 38.78], [-122.71, 39.74]]
    print(f'Expected (multiple Points): {json.dumps(expected_points)}')
    print(f'Actual   (multiple Points): {json.dumps(result_points)}')
    assert json.dumps(result_points, sort_keys=True) == json.dumps(expected_points, sort_keys=True)


def test_extract_coordinates_polygon():
    poly = Polygon(
        [
            (-121.61, 38.78),
            (-121.52, 38.78),
            (-121.52, 38.83),
            (-121.61, 38.83),
            (-121.61, 38.78),
        ]
    )
    gdf = gpd.GeoDataFrame(geometry=[poly])
    result = extract_coordinates(gdf)
    expected_result = [[[-121.61, 38.78], [-121.52, 38.78], [-121.52, 38.83], [-121.61, 38.83], [-121.61, 38.78]]]
    print(f'Expected: {json.dumps(expected_result)}')
    print(f'Actual  : {json.dumps(result)}')
    assert json.dumps(result, sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def test_extract_coordinates_multipolygon():
    mp = MultiPolygon(
        [
            [
                [
                    (-104.98, 40.72),
                    (-104.92, 40.77),
                    (-105.03, 40.80),
                    (-105.13, 40.70),
                    (-104.98, 40.72),
                ]
            ],
            [
                [
                    (-104.89, 40.78),
                    (-104.86, 40.726),
                    (-104.83276495279011, 40.76),
                    (-104.89, 40.78),
                ]
            ],
        ]
    )
    gdf = gpd.GeoDataFrame(geometry=[mp])
    result = extract_coordinates(gdf)
    expected_result = [
        [
            [
                [-104.98, 40.72],
                [-104.92, 40.77],
                [-105.03, 40.80],
                [-105.13, 40.70],
                [-104.98, 40.72],
            ]
        ],
        [
            [
                [-104.89, 40.78],
                [-104.86, 40.726],
                [-104.83276495279011, 40.76],
                [-104.89, 40.78],
            ]
        ],
    ]
    print(f'Expected: {json.dumps(expected_result)}')
    print(f'Actual  : {json.dumps(result)}')
    assert json.dumps(result, sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def test_extract_coordinates_multiple_polygons():
    poly1 = Polygon(
        [
            (-121.61, 38.78),
            (-121.52, 38.78),
            (-121.52, 38.83),
            (-121.61, 38.83),
            (-121.61, 38.78),
        ]
    )
    poly2 = Polygon(
        [
            (-122.61, 39.78),
            (-122.52, 39.78),
            (-122.52, 39.83),
            (-122.61, 39.83),
            (-122.61, 39.78),
        ]
    )
    gdf = gpd.GeoDataFrame(geometry=[poly1, poly2])
    result = extract_coordinates(gdf)
    expected_result = [
        [
            [-121.61, 38.78],
            [-121.52, 38.78],
            [-121.52, 38.83],
            [-121.61, 38.83],
            [-121.61, 38.78],
        ],
        [
            [-122.61, 39.78],
            [-122.52, 39.78],
            [-122.52, 39.83],
            [-122.61, 39.83],
            [-122.61, 39.78],
        ],
    ]
    print(f'Expected: {json.dumps(expected_result)}')
    print(f'Actual  : {json.dumps(result)}')
    assert json.dumps(result, sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def get_aoi_examples() -> list[Path]:
    """return paths to geojsons to test"""
    examples_folder = Path(__file__).parent.parent / "src" / "reports" / "rpt_riverscapes_inventory" / "example"
    files = list(examples_folder.glob("*.geojson"))
    return files


def get_area(gdf: gpd.GeoDataFrame) -> float:
    """return area in sq km"""
    # Area in km² (reproject to EPSG:5070 - NAD83 / Conus Albers)
    gdf_proj = gdf.to_crs(5070)
    km2_area = gdf_proj.geometry.area.sum() / 1e6  # m² to km²
    print(f"AOI area (km²): {km2_area:.2f}")
    return km2_area


def check_geometry(gdf: gpd.GeoDataFrame):
    """
    Report geometry types and check for common geometry errors.
    """
    geom_types = gdf.geometry.type.value_counts()
    print("Geometry types:")
    for t, count in geom_types.items():
        print(f"  {t}: {count}")
    # Check for common errors
    errors = []
    # Empty geometries
    empty_count = gdf.geometry.is_empty.sum()
    if empty_count > 0:
        errors.append(f"Empty geometries: {empty_count}")
    # Invalid geometries
    invalid_count = (~gdf.geometry.is_valid).sum()
    if invalid_count > 0:
        errors.append(f"Invalid geometries: {invalid_count}")
    # Non-polygon geometries
    non_poly_count = (~gdf.geometry.type.isin(["Polygon", "MultiPolygon"])).sum()
    if non_poly_count > 0:
        errors.append(f"Non-polygon geometries: {non_poly_count}")
    # Null geometries
    null_count = gdf.geometry.isna().sum()
    if null_count > 0:
        errors.append(f"Null geometries: {null_count}")
    if errors:
        print("Geometry errors:")
        for err in errors:
            print(f"  {err}")
    else:
        print("No common geometry errors detected.")
    coords = extract_coordinates(gdf)
    print(coords)


def check_geometries():
    """run check_geometry for a bunch of examples"""
    for geojsonfile in get_aoi_examples():
        print(f"\nTesting {geojsonfile} ============\n")
        gdf = gpd.read_file(geojsonfile)
        check_geometry(gdf)


def test_gets_results():
    """check that api call returns data"""
    url = f"{CLIMATE_ENGINE_BASE_API_URL}/timeseries/native/coordinates"
    # dataset options: https://docs.climateengine.org/docs/build/html/datasets.html
    dataset = "RAP_COVER"  # RAP Cover - 30m - Yearly https://support.climateengine.org/article/81-rap
    # this comes from https://api.climateengine.org/metadata/dataset_variables?dataset=RAP_COVER
    variables = ["AFG", "PFG", "SHR", "TRE", "BGR", "LTR"]

    params = {
        "user_email": "lorin@northarrowresearch.com",
        "area_reducer": "mean",
        "dataset": dataset,
        "variable": ",".join(variables),
        "compute_trends": "yes",
        "start_date": "2021-01-01",
        "end_date": "2025-12-31",
    }

    test_result_summary = []
    failed = 0
    n = 0
    for geojsonfile in get_aoi_examples():
        n += 1
        print(f"\n Testing {geojsonfile} ============ \n")
        aoi_gdf = gpd.read_file(geojsonfile)
        get_area(aoi_gdf)
        coords = extract_coordinates(aoi_gdf)
        try:
            params["coordinates"] = json.dumps(coords)
            results = query_climate_engine(url, params)  # this raises error - wrap in try catch
            # print(results)  # just when debugging
            df = pd.DataFrame(results[0]["Data"])
            assert len(df) > 0
            test_result_summary.append((geojsonfile, "PASS"))
        except Exception as e:
            test_result_summary.append((geojsonfile, str(e)))
            failed += 1
            print(f"FAILED: {e}")

    print("\nSummary of successes:")
    for f, msg in test_result_summary:
        print(f"{f}: {msg}")
    if failed > 0:
        assert False, f"{failed} failures out of {n} tested."
