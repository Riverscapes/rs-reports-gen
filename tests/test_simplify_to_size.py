"""Unit tests for geometry simplification helpers."""

import math

import geopandas as gpd
from shapely.geometry import Polygon

from util import simplify_gdf_to_size


def _circle_polygon(radius: float, vertices: int = 200) -> Polygon:
    """this is a quick way of generating a polygon with lots of vertices within a small space"""
    points = [
        (
            math.cos(theta) * radius,
            math.sin(theta) * radius,
        )
        for theta in [i * 2 * math.pi / vertices for i in range(vertices)]
    ]
    return Polygon(points)


def test_simplify_to_size_returns_original_when_below_threshold():
    """simplify to size should not make any changes if original is already below the supplied threshold"""
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

    simplified, metadata = simplify_gdf_to_size(gdf, size_bytes=50_000)

    assert metadata.success is True
    assert metadata.tolerance_m == 0
    assert simplified.equals(gdf)


def test_simplify_to_size_reduces_complex_geometries():
    """just check that we can simplify and it is because goes down in number of vertices"""
    polygon = _circle_polygon(radius=1.0, vertices=2000)
    gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

    original_vertices = len(polygon.exterior.coords)
    simplified, metadata = simplify_gdf_to_size(gdf, size_bytes=3_000, start_tolerance_m=6.0, max_attempts=6)

    assert metadata.success is True
    assert metadata.tolerance_m > 0
    simplified_geom = simplified.geometry.iloc[0]
    assert isinstance(simplified_geom, Polygon)
    assert len(list(simplified_geom.exterior.coords)) < original_vertices
