"""Tests for HUC10 AOI overlap metrics.

Created 2026-11-19. Created by copilot.
"""

import geopandas as gpd
import pytest
from shapely.geometry import box

from reports.rpt_igo_project.rawrme_to_igos_project import _add_huc10_aoi_overlap_metrics


def test_add_huc10_aoi_overlap_metrics_calculates_overlap_and_percentages() -> None:
    """Computes overlap area and both requested coverage percentages."""
    huc10_gdf = gpd.GeoDataFrame(
        {"huc10": ["h1", "h2"]},
        geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1)],
        crs="EPSG:4326",
    )
    aoi_gdf = gpd.GeoDataFrame(
        geometry=[box(0, 0, 1.5, 1)],
        crs="EPSG:4326",
    )

    result = _add_huc10_aoi_overlap_metrics(huc10_gdf, aoi_gdf)

    assert result.loc[0, "aoi_overlap_area_m2"] > result.loc[1, "aoi_overlap_area_m2"] > 0
    assert result.loc[0, "pct_huc10_covered_by_aoi"] == pytest.approx(100.0, abs=0.3)
    assert result.loc[1, "pct_huc10_covered_by_aoi"] == pytest.approx(50.0, abs=0.3)
    assert result.loc[0, "pct_aoi_covered_by_huc10"] == pytest.approx(66.6667, abs=0.3)
    assert result.loc[1, "pct_aoi_covered_by_huc10"] == pytest.approx(33.3333, abs=0.3)
