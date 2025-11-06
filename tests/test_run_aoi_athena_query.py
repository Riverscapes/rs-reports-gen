"""Tests for run_aoi_athena_query geometry handling."""

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

import util.athena.athena as athena


@pytest.fixture(name="simple_gdf")
def _simple_gdf() -> gpd.GeoDataFrame:
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    return gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")


def test_run_aoi_athena_query_requires_prepared_aoi(monkeypatch, simple_gdf):
    monkeypatch.setattr(
        athena,
        "athena_select_to_dict",
        lambda query, s3_output, max_wait=600: [{"record_count": 0}],
    )
    monkeypatch.setattr(
        athena,
        "get_aoi_geom_sql_expression",
        lambda gdf, max_size_bytes=261000: None,
    )

    with pytest.raises(ValueError, match="AOI geometry exceeds the maximum supported size"):
        athena.run_aoi_athena_query(simple_gdf)
