"""Tests for Inventory of Resources report data preparation.

Created 2026-08-13.
Created by copilot.
"""

import pandas as pd

from reports.rpt_inventory_of_resources.dataprep import build_metric_cards, build_report_summaries, normalize_inventory_data


def test_inventory_summaries_and_cards_use_stream_length() -> None:
    """Ownership and feature summaries retain all records and sum stream length."""
    data = normalize_inventory_data(
        pd.DataFrame(
            {
                "ownership_desc": ["Federal", "Private", "Federal"],
                "fcode_desc": ["Perennial Stream", "Canal", "Perennial Stream"],
                "stream_name": ["Pine Creek", None, "Pine Creek"],
                "stream_order": [2, 1, 3],
                "stream_length": [100.0, 50.0, 25.0],
                "watershed_id": ["0101", "0101", "0102"],
                "drainage_area": [4.0, 6.0, 8.0],
                "waterbody_type": ["None", "Canal", "None"],
            }
        )
    )

    summaries = build_report_summaries(data)
    cards = build_metric_cards(data)

    assert summaries["ownership"].iloc[0].to_dict() == {"ownership_desc": "Federal", "stream_length": 125.0, "record_count": 2}
    assert summaries["feature_type"].iloc[0]["stream_length"] == 125.0
    assert cards["stream_length"]["value"] == "175 m"
    assert cards["watersheds"]["value"] == "2"
    assert cards["stream_names"]["value"] == "1"
