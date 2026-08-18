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

    per_pct = float(metric_data.get("perennial_pct", 0.0))
    non_pct = float(metric_data.get("non_perennial_pct", 0.0))

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=["Riverscape"],
            x=[perennial],
            orientation="h",
            name="Perennial",
            marker_color=PERENNIAL_COLOR,
            text=[f"{per_pct:.1f}%"],
            textposition="inside",
            hovertemplate="Perennial<br>Value: %{x:,.2f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            y=["Riverscape"],
            x=[non_perennial],
            orientation="h",
            name="Non-perennial",
            marker_color=NON_PERENNIAL_COLOR,
            text=[f"{non_pct:.1f}%"],
            textposition="inside",
            hovertemplate="Non-perennial<br>Value: %{x:,.2f}<extra></extra>",
        )
    )

    total_label = _metric_value_format(total, unit)
    total_text = f"Total: {total_label} {unit}".strip()

    fig.add_annotation(
        x=total,
        y="Riverscape",
        text=total_text,
        showarrow=False,
        xanchor="left",
        xshift=8,
        font={"size": 13},
    )

    x_axis_title = f"{metric_name} ({unit})" if unit and unit != "count" else metric_name
    fig.update_layout(
        title=f"Flow Permanence Breakdown: {metric_name}",
        barmode="stack",
        xaxis_title=x_axis_title,
        yaxis_title="",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        margin={"r": 60, "t": 55, "l": 0, "b": 0},
        height=280,
    )

    if unit == "count":
        fig.update_xaxes(tickformat=",")
    else:
        fig.update_xaxes(tickformat=",.2f")

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
