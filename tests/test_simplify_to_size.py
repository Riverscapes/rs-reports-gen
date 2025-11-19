"""Unit tests for geometry simplification helpers."""

import math

from _pytest.monkeypatch import MonkeyPatch
import geopandas as gpd
from shapely.geometry import Polygon

import util.rs_geo_helpers as rs_geo_helpers
from util import prepare_gdf_for_athena, simplify_to_size


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

    simplified, tolerance, success = simplify_to_size(gdf, size_bytes=50_000)

    assert success is True
    assert tolerance == 0
    assert simplified.equals(gdf)


def test_simplify_to_size_reduces_complex_geometries():
    """just check that we can simplify and it is because goes down in number of vertices"""
    polygon = _circle_polygon(radius=1.0, vertices=2000)
    gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

    original_vertices = len(polygon.exterior.coords)
    simplified, tolerance, success = simplify_to_size(gdf, size_bytes=3_000, start_tolerance_m=6.0, max_attempts=6)

    assert success is True
    assert tolerance > 0
    simplified_geom = simplified.geometry.iloc[0]
    assert isinstance(simplified_geom, Polygon)
    assert len(list(simplified_geom.exterior.coords)) < original_vertices


def test_prepare_gdf_for_athena_reports_metadata(monkeypatch: MonkeyPatch):
    """Ensure prepare_gdf_for_athena propagates simplify metadata when geometry shrinks."""
    polygon = Polygon([(0, 0), (3, 0), (3, 3), (0, 3)])
    gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")
    replacement = gpd.GeoDataFrame(geometry=[polygon.buffer(-0.1)], crs="EPSG:4326")

    def _fake_simplify_to_size(_gdf, size_bytes, max_attempts):  # pragma: no cover - simple stub
        assert size_bytes == 261_000
        assert max_attempts == 5
        return replacement, 25.0, True

    monkeypatch.setattr(rs_geo_helpers, "simplify_to_size", _fake_simplify_to_size)

    prepared, metadata = prepare_gdf_for_athena(gdf)

    assert prepared.equals(replacement)
    assert metadata["tolerance_m"] == 25.0
    assert metadata["simplified"] is True
    assert metadata["success"] is True
    assert metadata["final_size_bytes"] == len(prepared.to_json().encode("utf-8"))


def test_prepare_gdf_for_athena_passes_through_without_simplification(monkeypatch: MonkeyPatch):
    """Verify prepare_gdf_for_athena leaves small AOIs untouched and flags no simplification."""
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

    def _identity_simplify(_gdf, _size_bytes, _max_attempts):  # pragma: no cover - simple stub
        return gdf, 0.0, True

    monkeypatch.setattr(rs_geo_helpers, "simplify_to_size", _identity_simplify)

    prepared, metadata = prepare_gdf_for_athena(gdf, size_bytes=100, max_attempts=2)

    assert prepared.equals(gdf)
    assert metadata["simplified"] is False
    assert metadata["tolerance_m"] == 0.0
