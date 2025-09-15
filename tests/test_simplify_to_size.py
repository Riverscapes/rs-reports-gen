"""Unit tests for geometry simplification helpers."""

import math

import geopandas as gpd
from shapely.geometry import Polygon

from util import simplify_to_size


def _circle_polygon(radius: float, vertices: int = 200) -> Polygon:
    points = [
        (
            math.cos(theta) * radius,
            math.sin(theta) * radius,
        )
        for theta in [i * 2 * math.pi / vertices for i in range(vertices)]
    ]
    return Polygon(points)


def test_simplify_to_size_returns_original_when_below_threshold():
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

    simplified, tolerance, success = simplify_to_size(gdf, size_bytes=50_000)

    assert success is True
    assert tolerance == 0
    assert simplified.equals(gdf)


def test_simplify_to_size_reduces_complex_geometries():
    polygon = _circle_polygon(radius=1.0, vertices=2000)
    gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

    original_vertices = len(polygon.exterior.coords)
    simplified, tolerance, success = simplify_to_size(gdf, size_bytes=3_000, max_attempts=5)

    assert success is True
    assert tolerance > 0
    assert len(simplified.geometry.iloc[0].exterior.coords) < original_vertices
