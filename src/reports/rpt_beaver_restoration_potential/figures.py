"""Figure builders for the Beaver Restoration Potential report.

Created 2026-07-07.
Created by copilot.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from rsxml import Logger

from util.pandas import RSFieldMeta

SUMMARY_TOTAL_FIELD = "segment_area"


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


def _build_summary_bar(summary_df: pd.DataFrame, *, fallback_group_field: str) -> go.Figure:
    """Build a bar chart from a summary table using metadata-aware labels and units."""
    meta = RSFieldMeta()
    layer_id = summary_df.attrs.get("layer_id") if hasattr(summary_df, "attrs") else None
    total_field = summary_df.attrs.get("total_field", SUMMARY_TOTAL_FIELD) if hasattr(summary_df, "attrs") else SUMMARY_TOTAL_FIELD
    group_field = summary_df.attrs.get("group_field", fallback_group_field) if hasattr(summary_df, "attrs") else fallback_group_field

    total_label = meta.get_headers_dict(pd.DataFrame(columns=[total_field]), layer_id=layer_id).get(
        total_field,
        meta.get_friendly_name(total_field, layer_id=layer_id),
    )
    group_label = meta.get_friendly_name(group_field, layer_id=layer_id)
    title = f"Total {total_label} by {group_label}"

    if summary_df.empty:
        return _build_placeholder(title)

    baked_summary_df, _ = meta.bake_units(summary_df)
    label_lookup = meta.get_headers_dict(summary_df, layer_id=layer_id)

    fig = px.bar(
        baked_summary_df,
        x="group",
        y=total_field,
        color="group",
        hover_data=["segment_count"],
        labels=label_lookup,
    )
    fig.update_layout(
        title=title,
        height=420,
        showlegend=False,
        xaxis_title=group_label,
        yaxis_title=total_label,
    )
    return fig


def build_beaver_figures(summary_tables: dict[str, pd.DataFrame]) -> dict[str, go.Figure]:
    """Build the baseline figures for the Beaver Restoration Potential stub report.
    TODO: titles should come from metadata
    """
    log = Logger("BeaverFigures")
    figures = {
        "capacity_by_length": _build_summary_bar(
            summary_tables.get("capacity", pd.DataFrame()),
            fallback_group_field="brat_capacity",
        ),
        "opportunity_by_length": _build_summary_bar(
            summary_tables.get("opportunity", pd.DataFrame()),
            fallback_group_field="brat_opportunity",
        ),
        "limitation_by_length": _build_summary_bar(
            summary_tables.get("limitation", pd.DataFrame()),
            fallback_group_field="brat_limitation",
        ),
        "risk_by_length": _build_summary_bar(
            summary_tables.get("risk", pd.DataFrame()),
            fallback_group_field="brat_risk",
        ),
    }
    log.info(f"Built {len(figures)} figures for Beaver Restoration Potential")
    return figures
