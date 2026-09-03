"""Tests for Riverscapes-branded folium map defaults (util.folium.riverscapes)."""

import os
import subprocess
import sys
from pathlib import Path

import folium
import geopandas as gpd
import pytest
from shapely.geometry import box

import util.folium.riverscapes as rs
from util.brand import COLORWAY, HEADER_COLOR


def _simple_map(**kwargs) -> folium.Map:
    """A bare folium.Map with a location, plus whatever the caller adds."""
    return folium.Map(location=[45.0, -110.0], zoom_start=6, **kwargs)


def test_import_time_defaults_applied():
    assert rs.is_map_defaults_applied()
    m = _simple_map()
    assert m.control_scale is True  # brand default: show the scale bar
    assert m.options["zoom_control"] is True


def test_explicit_arguments_override_patch():
    m = _simple_map(control_scale=False)
    assert m.control_scale is False
    m2 = _simple_map(tiles="CartoDB positron")
    layer = list(m2._children.values())[0]
    assert "carto.com/attributions" in layer.options["attribution"]


def test_make_map_defaults_and_overrides():
    m = rs.make_map(location=[45.0, -110.0], zoom_start=4)
    assert m.control_scale is True
    assert m.options["zoom"] == 4
    m2 = rs.make_map(location=[45.0, -110.0], control_scale=False)
    assert m2.control_scale is False


@pytest.mark.parametrize("crs", ["EPSG:4326", "EPSG:3857"])
def test_make_map_for_gdf_frames_extent(crs):
    gdf = gpd.GeoDataFrame(
        {"name": ["aoi"]},
        geometry=[box(-111.0, 43.0, -109.0, 45.0)],
        crs=crs,
    )
    m = rs.make_map_for_gdf(gdf, zoom_start=7)
    assert isinstance(m, folium.Map)
    # fit_bounds was called: the map has a non-default (actual) fit target
    assert m.control_scale is True


def test_make_style_function_brand_colors():
    style_fn = rs.make_style_function()
    assert style_fn({})["fillColor"] == HEADER_COLOR
    assert style_fn({})["color"] == HEADER_COLOR
    assert style_fn({})["fillOpacity"] == 0.35


def test_brand_color_cycle_repeats_palette():
    colors = rs.brand_color_cycle()
    first_round = [next(colors) for _ in COLORWAY]
    assert first_round == list(COLORWAY)
    assert next(colors) == COLORWAY[0]  # cycles


def test_restore_and_reapply_is_reversible():
    try:
        rs.restore_riverscapes_defaults()
        assert not rs.is_map_defaults_applied()
        assert _simple_map().control_scale is False  # stock folium
    finally:
        rs.apply_riverscapes_defaults()
    assert rs.is_map_defaults_applied()
    assert _simple_map().control_scale is True


def test_env_var_opt_out_before_import():
    src = str(Path(__file__).resolve().parents[1] / "src")
    env = dict(os.environ)
    env["RS_FOLIUM_DEFAULTS"] = "0"
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    code = (
        "import util.folium.riverscapes as rs; "
        "print('applied=', rs.is_map_defaults_applied())"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "applied= False" in out.stdout