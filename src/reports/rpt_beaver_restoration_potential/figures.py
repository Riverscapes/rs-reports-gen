"""Figure builders for the Beaver Restoration Potential report.

Created 2026-07-07.
Created by copilot.
"""

import pandas as pd
import plotly.graph_objects as go
from rsxml import Logger

from util.figures import bar_from_summary

SUMMARY_TOTAL_FIELD = "segment_area"


def build_beaver_figures(summary_tables: dict[str, pd.DataFrame]) -> dict[str, go.Figure]:
    """Build the baseline figures for the Beaver Restoration Potential stub report.
    TODO: titles should come from metadata
    """
    log = Logger("BeaverFigures")
    figures = {
        "capacity_by_length": bar_from_summary(
            summary_tables.get("capacity", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_capacity",
            show_legend=False,
            height=420,
        ),
        "opportunity_by_length": bar_from_summary(
            summary_tables.get("opportunity", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_opportunity",
            show_legend=False,
            height=420,
        ),
        "limitation_by_length": bar_from_summary(
            summary_tables.get("limitation", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_limitation",
            show_legend=False,
            height=420,
        ),
        "risk_by_length": bar_from_summary(
            summary_tables.get("risk", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_risk",
            show_legend=False,
            height=420,
        ),
    }
    log.info(f"Built {len(figures)} figures for Beaver Restoration Potential")
    return figures
