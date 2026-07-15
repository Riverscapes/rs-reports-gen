"""Tests for PBR Explorer export parsing and GeoPackage outputs.

Created 2026-07-15.
Created by copilot.
"""

from __future__ import annotations

import sqlite3

import geopandas as gpd
import pandas as pd

from reports.rpt_pbr_explorer.dataprep import (
    build_project_extents_gdf,
    normalize_affiliates_table,
    parse_dates_to_columns,
    parse_project_data,
)
from reports.rpt_pbr_explorer.main import (
    PBR_EXPORT_GPKG_FILENAME,
    PBR_EXPORT_TABLE_AFFILIATES,
    export_data_gpkg,
)


def test_parse_dates_to_columns_keeps_latest_value() -> None:
    """Date pivot should keep the latest value when duplicates exist for one enum."""
    df = pd.DataFrame(
        [
            {
                "id": "p1",
                "dates": [
                    {"name": "PROPOSED", "date": "2023-01-01T00:00:00Z"},
                    {"name": "PROPOSED", "date": "2024-01-01T00:00:00Z"},
                    {"name": "IMPLEMENTED", "date": "2025-03-01T00:00:00Z"},
                ],
            }
        ]
    )

    result = parse_dates_to_columns(df)

    assert "dates" not in result.columns
    assert str(result.loc[0, "date_PROPOSED"]).startswith("2024-01-01T00:00:00")
    assert str(result.loc[0, "date_IMPLEMENTED"]).startswith("2025-03-01T00:00:00")


def test_normalize_affiliates_table_expands_roles() -> None:
    """Affiliate normalization should emit one row per project x affiliate x role."""
    df = pd.DataFrame(
        [
            {
                "id": "p1",
                "orgAffiliates": [
                    {
                        "id": "oa1",
                        "organization": {"id": "org1", "name": "Org One"},
                        "roles": ["PROJECT_OWNER", "PARTNER"],
                    }
                ],
                "pbrAffiliates": [
                    {
                        "name": "Local Group",
                        "url": "https://example.org",
                        "roles": ["PARTNER"],
                    }
                ],
            }
        ]
    )

    result = normalize_affiliates_table(df)

    assert len(result) == 3
    assert set(result["affiliate_source_type"]) == {"ORG", "PBR"}
    assert set(result["role"]) == {"PROJECT_OWNER", "PARTNER"}


def test_build_project_extents_gdf_from_featurecollection() -> None:
    """Extent parser should convert FeatureCollection payloads into geometries."""
    df = pd.DataFrame(
        [
            {
                "id": "p1",
                "extent": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [-120.0, 45.0],
                                        [-119.9, 45.0],
                                        [-119.9, 45.1],
                                        [-120.0, 45.1],
                                        [-120.0, 45.0],
                                    ]
                                ],
                            },
                        }
                    ],
                },
            }
        ]
    )

    result = build_project_extents_gdf(df)

    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 1
    assert result.crs is not None and result.crs.to_epsg() == 4326
    assert result.iloc[0]["project_id"] == "p1"


def test_export_data_gpkg_writes_layers_and_affiliates_table(tmp_path) -> None:
    """GeoPackage export should write projects, extents, and normalized affiliates."""
    data_df = pd.DataFrame(
        [
            {
                "id": "p1",
                "name": "Project One",
                "location.latitude": 45.05,
                "location.longitude": -119.95,
                "lengthKm": 2.5,
                "dates": [{"name": "PROPOSED", "date": "2024-01-01T00:00:00Z"}],
                "orgAffiliates": [
                    {
                        "id": "oa1",
                        "organization": {"id": "org1", "name": "Org One"},
                        "roles": ["PROJECT_OWNER"],
                    }
                ],
                "pbrAffiliates": [
                    {
                        "name": "Local Group",
                        "url": "https://example.org",
                        "roles": ["PARTNER"],
                    }
                ],
                "extent": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [-120.0, 45.0],
                                        [-119.9, 45.0],
                                        [-119.9, 45.1],
                                        [-120.0, 45.1],
                                        [-120.0, 45.0],
                                    ]
                                ],
                            },
                        }
                    ],
                },
            }
        ]
    )
    data_df.attrs["layer_id"] = "pbr_projects"

    gpkg_path = export_data_gpkg(data_df, tmp_path)

    assert gpkg_path == tmp_path / "data" / PBR_EXPORT_GPKG_FILENAME
    assert gpkg_path.exists()

    projects = gpd.read_file(gpkg_path, layer="projects")
    extents = gpd.read_file(gpkg_path, layer="project_extents")
    assert len(projects) == 1
    assert len(extents) == 1
    assert not any(col.startswith("extent") for col in projects.columns)

    with sqlite3.connect(gpkg_path) as conn:
        affiliate_count = conn.execute(f"SELECT COUNT(*) FROM {PBR_EXPORT_TABLE_AFFILIATES}").fetchone()[0]
        gpkg_contents_row = conn.execute(
            "SELECT data_type FROM gpkg_contents WHERE table_name = ?",
            (PBR_EXPORT_TABLE_AFFILIATES,),
        ).fetchone()
    assert affiliate_count == 2
    assert gpkg_contents_row is not None
    assert gpkg_contents_row[0] == "attributes"
    assert (tmp_path / "column_metadata.csv").exists() or (tmp_path / "data" / "column_metadata.csv").exists()


def test_export_data_gpkg_writes_extents_from_parsed_project_json(tmp_path) -> None:
    """Extents should still export when project rows were flattened by json_normalize."""
    projects_json = [
        {
            "id": "p_ext_1",
            "name": "Project With Extent",
            "location": {"latitude": 37.6087, "longitude": -105.3009},
            "lengthKm": 1.0,
            "actions": [{"action": "STRUCTURES_BUILT", "value": 3}],
            "dates": [{"name": "IMPLEMENTED", "date": "2023-07-05"}],
            "orgAffiliates": [],
            "pbrAffiliates": [],
            "extent": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [-105.3157089642, 37.6424454676],
                                        [-105.3069828681, 37.6391582396],
                                        [-105.3042933179, 37.6269058444],
                                        [-105.3157089642, 37.6424454676],
                                    ]
                                ]
                            ],
                        },
                    }
                ],
            },
        }
    ]

    parsed_df = parse_project_data(projects_json)
    gpkg_path = export_data_gpkg(parsed_df, tmp_path)

    extents = gpd.read_file(gpkg_path, layer="project_extents")
    assert len(extents) == 1
    assert str(extents.iloc[0]["project_id"]) == "p_ext_1"
