"""Tests for prepare_aoi_query — verifying CTE generation, placeholder substitution, and size-budget logic.

Copilot-generated module.
"""

import geopandas as gpd
from shapely.geometry import Polygon

from util.athena.athena import prepare_aoi_query


def _simple_gdf() -> gpd.GeoDataFrame:
    """A small polygon AOI for testing — tiny WKB footprint."""
    polygon = Polygon([(-109, 31), (-109, 33), (-105, 33), (-105, 31), (-109, 31)])
    return gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")


# ---------- CTE generation ----------


def test_cte_prepended():
    """prepare_aoi_query always prepends a WITH input_geom CTE."""
    query = "SELECT * FROM input_geom, my_table WHERE {prefilter_condition} AND {intersects_condition}"
    result = prepare_aoi_query(query, "ST_GeomFromBinary(geom)", "geom_bbox", _simple_gdf())

    assert result.startswith("WITH input_geom AS (SELECT ")
    assert " AS geom) " in result


def test_geometry_hex_appears_once():
    """The AOI geometry hex string appears exactly once — in the CTE."""
    query = "SELECT * FROM input_geom, t WHERE {prefilter_condition} AND {intersects_condition}"
    result = prepare_aoi_query(query, "ST_GeomFromBinary(geom)", "geom_bbox", _simple_gdf())

    aoi_marker = "ST_GeomFromBinary(from_hex('"
    count = result.count(aoi_marker)
    assert count == 1, f"Expected AOI geometry hex to appear exactly 1 time, got {count}"


def test_intersects_uses_input_geom_alias():
    """ST_Intersects references input_geom.geom (the CTE alias), not the raw hex."""
    query = "SELECT * FROM input_geom, t WHERE {prefilter_condition} AND {intersects_condition}"
    result = prepare_aoi_query(query, "ST_GeomFromBinary(geom)", "geom_bbox", _simple_gdf())

    assert "ST_Intersects(ST_GeomFromBinary(geom), input_geom.geom)" in result


# ---------- Placeholder substitution ----------


def test_placeholders_filled():
    """Both {prefilter_condition} and {intersects_condition} are replaced."""
    query = "SELECT * FROM input_geom, my_table WHERE {prefilter_condition} AND {intersects_condition}"
    result = prepare_aoi_query(query, "ST_GeomFromBinary(geom)", "geom_bbox", _simple_gdf())

    assert "{prefilter_condition}" not in result
    assert "{intersects_condition}" not in result
    assert "ST_Intersects" in result
    assert "geom_bbox.xmax" in result


def test_input_geom_available_in_select():
    """Callers can reference input_geom.geom in their SELECT for calculations like percent_intersection."""
    query = "SELECT 100 * (ST_AREA(ST_INTERSECTION(h.geom, input_geom.geom)) / ST_AREA(h.geom)) AS pct FROM input_geom, my_table h WHERE {prefilter_condition} AND {intersects_condition}"
    result = prepare_aoi_query(query, "h.geom", "bbox", _simple_gdf())

    # The CTE defines input_geom.geom, so the SELECT reference should survive
    assert "ST_INTERSECTION(h.geom, input_geom.geom)" in result
    # And ST_Intersects uses the alias too
    assert "ST_Intersects(h.geom, input_geom.geom)" in result


# ---------- Size-budget logic ----------

ATHENA_MAX_BYTES = 262_144


def test_final_query_within_athena_limit():
    """The produced query must fit within Athena's 262 KB limit."""
    query = "SELECT * FROM input_geom, t WHERE {prefilter_condition} AND {intersects_condition}"
    result = prepare_aoi_query(query, "ST_GeomFromBinary(geom)", "geom_bbox", _simple_gdf())
    assert len(result) <= ATHENA_MAX_BYTES


def test_budget_with_large_template():
    """Even with a large template, a small polygon still fits."""
    padding = "x" * 200_000
    query = f"SELECT '{padding}' FROM input_geom, t WHERE {{prefilter_condition}} AND {{intersects_condition}}"
    result = prepare_aoi_query(query, "geom", "bbox", _simple_gdf())
    assert len(result) <= ATHENA_MAX_BYTES


# ---------- Output correctness ----------


def test_intersects_uses_geom_field_expression():
    """ST_Intersects wraps the caller's geometry field expression."""
    query = "SELECT * FROM input_geom, t WHERE {prefilter_condition} AND {intersects_condition}"
    result = prepare_aoi_query(query, "ST_GeomFromBinary(my_custom_geom)", "my_bbox", _simple_gdf())

    assert "ST_Intersects(ST_GeomFromBinary(my_custom_geom), input_geom.geom)" in result


def test_prefilter_uses_bbox_field():
    """The bounding-box prefilter references the caller's bbox field name."""
    query = "SELECT * FROM input_geom, t WHERE {prefilter_condition} AND {intersects_condition}"
    result = prepare_aoi_query(query, "geom", "my_bbox_field", _simple_gdf())

    assert "my_bbox_field.xmax" in result
    assert "my_bbox_field.xmin" in result
    assert "my_bbox_field.ymax" in result
    assert "my_bbox_field.ymin" in result


def test_bbox_values_cover_polygon():
    """Prefilter bounds should cover the input polygon extent."""
    gdf = _simple_gdf()
    query = "SELECT * FROM input_geom, t WHERE {prefilter_condition} AND {intersects_condition}"
    result = prepare_aoi_query(query, "geom", "bb", gdf)

    # Our polygon spans -109 to -105 lon, 31 to 33 lat
    assert "-109" in result
    assert "-105" in result
    assert "31" in result
    assert "33" in result
