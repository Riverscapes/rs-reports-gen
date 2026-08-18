"""Tests for full-export ranking columns in stream names report.

Created 2026-08-17.
Created by copilot.
"""

from __future__ import annotations

import pandas as pd

from reports.rpt_stream_names.main import add_full_export_rank_columns


def test_add_full_export_rank_columns_ranks_named_only() -> None:
    """Assign rank columns to named rows and leave unnamed rows unranked.

    Created by copilot.
    """
    df = pd.DataFrame(
        {
            "stream_name": ["Alpha", "Beta", "Gamma", "-unnamed-", ""],
            "level_path_count": [5, 5, 3, 9, 1],
            "total_riverscape_length": [100.0, 120.0, 150.0, 999.0, 10.0],
            "total_channel_length": [90.0, 110.0, 140.0, 900.0, 8.0],
        }
    )

    ranked = add_full_export_rank_columns(df)

    beta = ranked.loc[ranked["stream_name"] == "Beta"].iloc[0]
    alpha = ranked.loc[ranked["stream_name"] == "Alpha"].iloc[0]
    gamma = ranked.loc[ranked["stream_name"] == "Gamma"].iloc[0]
    unnamed = ranked.loc[ranked["stream_name"] == "-unnamed-"].iloc[0]
    blank = ranked.loc[ranked["stream_name"] == ""].iloc[0]

    assert int(beta["rank_by_path_count"]) == 1
    assert int(alpha["rank_by_path_count"]) == 2
    assert int(gamma["rank_by_path_count"]) == 3

    assert int(gamma["rank_by_riverscape_length"]) == 1
    assert int(beta["rank_by_riverscape_length"]) == 2
    assert int(alpha["rank_by_riverscape_length"]) == 3

    assert pd.isna(unnamed["rank_by_path_count"])
    assert pd.isna(unnamed["rank_by_riverscape_length"])
    assert pd.isna(blank["rank_by_path_count"])
    assert pd.isna(blank["rank_by_riverscape_length"])
