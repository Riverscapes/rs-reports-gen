"""Figure builders for the IGO project summary report.

Created 2026-08-18.
Created by copilot.
"""

from __future__ import annotations

import geopandas as gpd
import plotly.graph_objects as go

from util.figures import make_aoi_outline_map

PERENNIAL_COLOR = "#0d5ea8"
NON_PERENNIAL_COLOR = "#62a83d"


def _metric_value_format(value: float, unit: str) -> str:
    """Format chart values consistently by metric unit.

    Created 2026-08-18.
    Created by copilot.
    """
    if unit == "count":
        return f"{value:,.0f}"
    return f"{value:,.2f}"


def flow_breakdown_chart(metric_name: str, metric_data: dict[str, float | str]) -> go.Figure:
    """Create a single stacked horizontal permanence breakdown bar chart.

    Created 2026-08-18.
    Created by copilot.
    """
    perennial = float(metric_data.get("perennial", 0.0))
    non_perennial = float(metric_data.get("non_perennial", 0.0))
    total = float(metric_data.get("total", 0.0))
    unit = str(metric_data.get("unit", ""))

    if total > 0:
        per_pct = (perennial / total) * 100.0
        non_pct = (non_perennial / total) * 100.0
    else:
        per_pct = float(metric_data.get("perennial_pct", 0.0))
        non_pct = float(metric_data.get("non_perennial_pct", 0.0))

    pct_sum = per_pct + non_pct
    if pct_sum > 0:
        scale = 100.0 / pct_sum
        per_pct *= scale
        non_pct *= scale

    fig = go.Figure()
    per_value_label = _metric_value_format(perennial, unit)
    non_value_label = _metric_value_format(non_perennial, unit)
    unit_suffix = f" {unit}" if unit and unit != "count" else ""

    fig.add_trace(
        go.Bar(
            y=["Riverscape"],
            x=[per_pct],
            orientation="h",
            name="Perennial",
            marker_color=PERENNIAL_COLOR,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Bar(
            y=["Riverscape"],
            x=[non_pct],
            orientation="h",
            name="Non-perennial",
            marker_color=NON_PERENNIAL_COLOR,
            hoverinfo="skip",
        )
    )

    fig.add_annotation(
        x=per_pct / 2,
        y=0.92,
        xref="x",
        yref="paper",
        text=f"Perennial<br>{per_value_label}{unit_suffix}<br>{per_pct:.0f}%",
        showarrow=False,
        xanchor="center",
        yanchor="bottom",
        align="center",
        font={"size": 12},
    )
    fig.add_annotation(
        x=per_pct + (non_pct / 2),
        y=0.92,
        xref="x",
        yref="paper",
        text=f"Non-perennial<br>{non_value_label}{unit_suffix}<br>{non_pct:.0f}%",
        showarrow=False,
        xanchor="center",
        yanchor="bottom",
        align="center",
        font={"size": 12},
    )

    fig.update_layout(
        title=f"{metric_name}",
        barmode="stack",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis_title="",
        yaxis_title="",
        showlegend=False,
        margin={"r": 10, "t": 85, "l": 0, "b": 0},
        height=280,
    )

    fig.update_xaxes(
        range=[0, 100],
        showticklabels=False,
        ticks="",
        showgrid=False,
        zeroline=False,
        title=None,
    )
    fig.update_yaxes(
        showticklabels=False,
        ticks="",
        showgrid=False,
        zeroline=False,
        title=None,
    )

    return fig


def build_igo_figures(
    aoi_gdf: gpd.GeoDataFrame,
    flow_metrics: dict[str, dict[str, float | str]],
) -> dict[str, go.Figure]:
    """Build all report figures for the IGO summary report.

    Created 2026-08-18.
    Created by copilot.
    """
    return {
        "map": make_aoi_outline_map(aoi_gdf),
        "flow_length": flow_breakdown_chart("Riverscape Length", flow_metrics["length"]),
        "flow_area": flow_breakdown_chart("Riverscape Area", flow_metrics["area"]),
        "flow_segments": flow_breakdown_chart("Riverscape Segments", flow_metrics["segments"]),
    }
