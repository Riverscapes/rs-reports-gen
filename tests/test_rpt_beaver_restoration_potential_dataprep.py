"""Tests for Beaver Restoration Potential data preparation.

Created 2026-07-14.
Created by copilot.
"""

import pandas as pd

from reports.rpt_beaver_restoration_potential.dataprep import summarize_beaver_potential, summarize_by_level_path, summarize_by_watershed


def test_summarize_by_level_path_groups_rows_and_computes_dam_capacity() -> None:
    """Level-path summaries should aggregate dam counts and capacity.

    Created by copilot.
    """
    df = pd.DataFrame(
        [
            {"level_path": "100", "stream_name": "Alpha Creek", "dam_ct": 2, "brat_capacity": 1.5, "centerline_length": 10.0},
            {"level_path": "100", "stream_name": "Alpha Creek", "dam_ct": 1, "brat_capacity": 2.0, "centerline_length": 5.0},
            {"level_path": "200", "stream_name": "Beta Creek", "dam_ct": 4, "brat_capacity": 0.5, "centerline_length": 8.0},
            {"level_path": "300", "stream_name": "Gamma Creek", "dam_ct": 0, "brat_capacity": 3.6, "centerline_length": 6.0},
        ]
    )

    result = summarize_by_level_path(df)

    expected = pd.DataFrame(
        [
            {"level_path": "100", "stream_name": "Alpha Creek", "dam_ct": 3, "dam_capacity": 25, "percent_capacity": "12.0%"},
            {"level_path": "200", "stream_name": "Beta Creek", "dam_ct": 4, "dam_capacity": 4, "percent_capacity": "100.0%"},
        ]
    )

    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
    assert pd.api.types.is_integer_dtype(result["dam_capacity"])
    assert result["dam_capacity"].tolist() == sorted(result["dam_capacity"].tolist(), reverse=True)
    assert result.attrs["layer_id"] == "rpt_beaver_restoration_potential"
    assert result.attrs["total_field"] == "dam_capacity"


def test_summarize_beaver_potential_includes_level_path_summary() -> None:
    """Top-level beaver summaries should expose the level-path table.

    Created by copilot.
    """
    df = pd.DataFrame(
        [
            {
                "level_path": "100",
                "huc10": "1701010101",
                "stream_name": "Alpha Creek",
                "dam_ct": 2,
                "brat_capacity": 1.5,
                "centerline_length": 10.0,
                "segment_area": 3.0,
                "brat_opportunity": "Encourage Beaver Expansion/Colonization",
                "brat_limitation": "None",
                "brat_risk": "Low",
            }
        ]
    )

    summaries = summarize_beaver_potential(df)

    assert "level_paths" in summaries
    assert list(summaries["level_paths"].columns) == ["level_path", "stream_name", "dam_ct", "dam_capacity", "percent_capacity"]
    assert summaries["level_paths"].loc[0, "dam_capacity"] == 15
    assert summaries["level_paths"].loc[0, "percent_capacity"] == "13.3%"


def test_summarize_by_level_path_handles_pint_backed_capacity_inputs() -> None:
    """Level-path summary should support pint-backed capacity and length columns."""
    df = pd.DataFrame(
        [
            {"level_path": "100", "stream_name": "Alpha Creek", "dam_ct": 2, "brat_capacity": 1.5, "centerline_length": 10.0},
            {"level_path": "200", "stream_name": "Beta Creek", "dam_ct": 1, "brat_capacity": 0.5, "centerline_length": 8.0},
        ]
    )
    df["brat_capacity"] = df["brat_capacity"].astype("pint[count / kilometer]")
    df["centerline_length"] = df["centerline_length"].astype("pint[kilometer]")

    result = summarize_by_level_path(df)

    assert result["dam_capacity"].tolist() == [15, 4]
    assert result["percent_capacity"].tolist() == ["13.3%", "25.0%"]
    assert pd.api.types.is_integer_dtype(result["dam_capacity"])


def test_summarize_by_watershed_groups_rows_and_computes_dam_capacity() -> None:
    """Watershed summaries should aggregate dam counts and capacity by HUC10."""
    df = pd.DataFrame(
        [
            {"huc10": "1701010101", "stream_name": "Alpha Creek", "dam_ct": 2, "brat_capacity": 1.5, "centerline_length": 10.0},
            {"huc10": "1701010101", "stream_name": "Alpha Creek", "dam_ct": 1, "brat_capacity": 2.0, "centerline_length": 5.0},
            {"huc10": "1701010102", "stream_name": "Beta Creek", "dam_ct": 4, "brat_capacity": 0.5, "centerline_length": 8.0},
            {"huc10": "1701010103", "stream_name": "Gamma Creek", "dam_ct": 0, "brat_capacity": 3.6, "centerline_length": 6.0},
        ]
    )

    result = summarize_by_watershed(df)

    expected = pd.DataFrame(
        [
            {"huc10": "1701010101", "dam_ct": 3, "dam_capacity": 25, "percent_capacity": "12.0%"},
            {"huc10": "1701010102", "dam_ct": 4, "dam_capacity": 4, "percent_capacity": "100.0%"},
        ]
    )

    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
    assert "stream_name" not in result.columns
    assert pd.api.types.is_integer_dtype(result["dam_capacity"])
    assert result["dam_capacity"].tolist() == sorted(result["dam_capacity"].tolist(), reverse=True)
