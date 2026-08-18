"""Tests for reusable Top-N rollup summary helper.

Created 2026-08-17.
Created by copilot.
"""

from __future__ import annotations

import pandas as pd

from util.summary import summarize_top_n_rollups


def test_summarize_top_n_rollups_includes_unnamed_and_grand_total() -> None:
    """Top-N rollups should include named, unnamed, and total values correctly.

    Created by copilot.
    """
    full_df = pd.DataFrame(
        {
            "stream_name": ["Alpha", "Beta", "Gamma", "-unnamed-", None],
            "level_path_count": [10, 6, 4, 3, 2],
            "total_riverscape_length": [100.0, 70.0, 50.0, 20.0, 10.0],
            "total_channel_length": [95.0, 65.0, 45.0, 18.0, 9.0],
        }
    )

    named_df = full_df.loc[[0, 1, 2]].copy()

    result = summarize_top_n_rollups(
        full_df,
        named_df,
        name_field="stream_name",
        metric_fields=["level_path_count", "total_riverscape_length", "total_channel_length"],
        top_n=2,
        sort_by=["level_path_count", "total_riverscape_length", "stream_name"],
        ascending=[False, False, True],
        include_unnamed_row=True,
    )

    assert result["stream_name"].tolist() == [
        "Top 2 Total",
        "All Other Named Values",
        "Unnamed Systems",
        "Grand Total (Named + Unnamed)",
    ]

    top_2 = result.iloc[0]
    all_others = result.iloc[1]
    unnamed = result.iloc[2]
    grand_total = result.iloc[3]

    assert top_2["level_path_count"] == 16
    assert top_2["total_riverscape_length"] == 170.0
    assert top_2["total_channel_length"] == 160.0

    assert all_others["level_path_count"] == 4
    assert all_others["total_riverscape_length"] == 50.0
    assert all_others["total_channel_length"] == 45.0

    assert unnamed["level_path_count"] == 5
    assert unnamed["total_riverscape_length"] == 30.0
    assert unnamed["total_channel_length"] == 27.0

    assert grand_total["level_path_count"] == 25
    assert grand_total["total_riverscape_length"] == 250.0
    assert grand_total["total_channel_length"] == 232.0


def test_summarize_top_n_rollups_handles_fewer_rows_than_top_n() -> None:
    """Top-N label should reflect available named rows when fewer than N.

    Created by copilot.
    """
    full_df = pd.DataFrame(
        {
            "stream_name": ["Alpha", "Beta", "-unnamed-"],
            "level_path_count": [5, 3, 2],
            "total_riverscape_length": [10.0, 7.0, 4.0],
        }
    )
    named_df = full_df.loc[[0, 1]].copy()

    result = summarize_top_n_rollups(
        full_df,
        named_df,
        name_field="stream_name",
        metric_fields=["level_path_count", "total_riverscape_length"],
        top_n=10,
        sort_by=["level_path_count", "total_riverscape_length", "stream_name"],
        ascending=[False, False, True],
    )

    assert result.iloc[0]["stream_name"] == "Top 2 Total"
    assert result.iloc[1]["level_path_count"] == 0
    assert result.iloc[2]["level_path_count"] == 2
    assert result.iloc[3]["level_path_count"] == 10
