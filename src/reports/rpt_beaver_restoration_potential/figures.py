"""Figure builders for the Beaver Restoration Potential report.

Created 2026-07-07.
Created by copilot.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from rsxml import Logger


def _build_placeholder(title: str) -> go.Figure:
    """Return an empty figure with a friendly no-data annotation."""
    fig = go.Figure()
    fig.add_annotation(
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        text="No rows available for this chart.",
        showarrow=False,
    )
    fig.update_layout(
        title=title,
        height=380,
        xaxis_visible=False,
        yaxis_visible=False,
    )
    return fig


def _build_summary_bar(summary_df: pd.DataFrame, *, title: str, x_axis_title: str) -> go.Figure:
    """Build a bar chart from a summary table with centerline-length totals."""
    if summary_df.empty:
        return _build_placeholder(title)

    fig = px.bar(
        summary_df,
        x="group",
        y="total_centerline_length",
        color="group",
        hover_data=["segment_count"],
    )
    fig.update_layout(
        title=title,
        height=420,
        showlegend=False,
        xaxis_title=x_axis_title,
        yaxis_title="Total Centerline Length",
    )
    return fig


def build_beaver_figures(summary_tables: dict[str, pd.DataFrame]) -> dict[str, go.Figure]:
    """Build the baseline figures for the Beaver Restoration Potential stub report."""
    log = Logger("BeaverFigures")
    figures = {
        "capacity_by_length": _build_summary_bar(
            summary_tables.get("capacity", pd.DataFrame()),
            title="Centerline Length by Current BRAT Capacity",
            x_axis_title="Current BRAT Capacity",
        ),
        "opportunity_by_length": _build_summary_bar(
            summary_tables.get("opportunity", pd.DataFrame()),
            title="Centerline Length by BRAT Opportunity",
            x_axis_title="BRAT Opportunity",
        ),
        "limitation_by_length": _build_summary_bar(
            summary_tables.get("limitation", pd.DataFrame()),
            title="Centerline Length by BRAT Limitation",
            x_axis_title="BRAT Limitation",
        ),
        "risk_by_length": _build_summary_bar(
            summary_tables.get("risk", pd.DataFrame()),
            title="Centerline Length by BRAT Risk",
            x_axis_title="BRAT Risk",
        ),
    }
    log.info(f"Built {len(figures)} figures for Beaver Restoration Potential")
    return figures
