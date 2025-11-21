"""Tests for run_aoi_athena_query geometry handling."""

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

import util.athena.athena as athena


@pytest.fixture(name="simple_gdf")
def _simple_gdf() -> gpd.GeoDataFrame:
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    return gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")


@pytest.fixture(name="stream_names_query")
def _stream_names_query():
    return """
    select stream_name, round(sum(centerline_length),0) as total_riverscape_length, max(stream_order) as max_stream_order
    from raw_rme_pq2
    where substr(huc12,1,10) = '1802015702'
    and stream_name is not null
    group by stream_name
    """


def test_run_aoi_athena_query_requires_prepared_aoi(monkeypatch, simple_gdf):
    """verifies that run_aoi_athena_query raises specific error if get_aoi_geom_sql_expression returns None
    not that useful of a test really
    """

    # Patch athena_select_to_dict to always return a dummy record count
    monkeypatch.setattr(
        athena,
        "athena_select_to_dict",
        lambda query, s3_output, max_wait=600: [{"record_count": 0}],
    )
    # Patch get_aoi_geom_sql_expression to simulate AOI geometry too large (returns None)
    monkeypatch.setattr(
        athena,
        "get_aoi_geom_sql_expression",
        lambda gdf, max_size_bytes=261000: None,
    )

    with pytest.raises(ValueError, match="AOI geometry exceeds the maximum supported size"):
        athena.run_aoi_athena_query(simple_gdf)
