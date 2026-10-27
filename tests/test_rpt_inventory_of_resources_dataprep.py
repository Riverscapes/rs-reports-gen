"""Tests for Inventory of Resources report data preparation.

Created 2026-08-13.
Created by copilot.
"""

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from reports.rpt_inventory_of_resources import dataprep as inventory_dataprep
from reports.rpt_inventory_of_resources.dataprep import build_metric_cards, build_report_summaries


def test_inventory_summaries_and_cards_use_stream_length() -> None:
    """Summary and metric-card outputs should reflect stream-length totals and counts."""
    data = pd.DataFrame(
        {
            "ownership": ["BLM", "Private", "BLM"],
            "fcode": [46006, 33600, 55800],
            "stream_name": ["Pine Creek", "Not specified", "Pine Creek"],
            "stream_length": [100.0, 50.0, 25.0],
            "watershed_id": ["0101", "0101", "0102"],
            "drainage_area": [4.0, 6.0, 8.0],
            "lf_riparian": [10.0, 20.0, 30.0],
            "waterbody_extent": [2.0, 3.0, 4.0],
            "segment_area": [7.0, 8.0, 9.0],
            "lf_agriculture": [4.0, 5.0, 6.0],
            "lf_developed": [1.0, 0.0, 2.0],
        }
    )

    summaries = build_report_summaries(data)
    cards = build_metric_cards(data)

    inventory_summary = summaries["inventory"]
    assert inventory_summary.iloc[0]["Resource Category"] == "Stream Network"
    assert inventory_summary.iloc[0]["Total Inventory"] == 175.0
    assert inventory_summary.iloc[0]["Total BLM"] == 125.0
    assert cards["stream_length"]["value"] == "175 m"
    assert cards["watersheds"]["value"] == "2"
    assert cards["stream_names"]["value"] == "1"


@pytest.mark.parametrize(
    ("table_name", "expected_nwi_source"),
    [
        ("riparian", "ext_rpt.us_fws_nwi_riparian"),
        ("wetlands", "ext_rpt.us_fws_nwi_wetlands"),
    ],
)
def test_get_nwi_data_uses_enriched_sma_source(monkeypatch, table_name: str, expected_nwi_source: str) -> None:
    """NWI query uses enriched SMA source with AOI prefilter applied only to NWI rows."""
    aoi_gdf = gpd.GeoDataFrame(
        geometry=[Polygon([(-123.54, 42.05), (-123.54, 42.07), (-123.52, 42.07), (-123.52, 42.05), (-123.54, 42.05)])],
        crs="EPSG:4326",
    )
    captured: dict[str, object] = {}

    def _fake_aoi_query_to_dataframe(querystr: str, **kwargs):
        captured["query"] = querystr
        captured["kwargs"] = kwargs
        return pd.DataFrame()

    monkeypatch.setattr(inventory_dataprep, "aoi_query_to_dataframe", _fake_aoi_query_to_dataframe)

    result = inventory_dataprep.get_nwi_data(aoi_gdf, table=table_name)

    assert result.empty
    assert "area" in result.columns

    query = str(captured["query"])
    kwargs = dict(captured["kwargs"])
    assert "sma_prefilter AS (" not in query
    assert "LEFT JOIN ext_rpt.us_blm_sma_ownership_enriched sma" in query
    assert "LEFT JOIN lu_blm_ownership" not in query
    assert "COALESCE(sma.admin_agency_description, sma.admin_dept_description) AS ownership_desc" in query
    assert f"FROM input_geom, {expected_nwi_source} nwi" in query
    assert kwargs["geom_bbox_columns"] == ("nwi.bbox_xmin", "nwi.bbox_ymin", "nwi.bbox_xmax", "nwi.bbox_ymax")
