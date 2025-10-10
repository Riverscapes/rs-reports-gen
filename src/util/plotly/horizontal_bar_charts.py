#!/usr/bin/env python3
import re
from typing import Optional, Dict

import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Regex to validate hex color codes
HEX_RE = re.compile(r"^#(?:[0-9a-fA-F]{3}){1,2}$")


def make_rs_horizontal_bar(
    gdf: gpd.GeoDataFrame,
    group_col: str,
    value_col: str,
    title: str = "Horizontal Bar Chart",
    show_percent: bool = True,
    color_map: Optional[Dict[str, str]] = None,
    width: int = 720,
    height: int = 480,
    major_dtick: float = 5000,   # <-- fixed default; user can override
    decimals: int = 0,
) -> go.Figure:
    """
    Create a horizontal bar chart aggregated by `group_col`.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Must contain `group_col` and `value_col`.
    group_col : str
        Categorical column used for y-axis labels.
    value_col : str
        Numeric column to sum for x-axis values.
    title : str, default "Horizontal Bar Chart"
        Figure title text.
    show_percent : bool, default True
        If True, displays each bar's percent of total at the bar end.
    color_map : dict[str, str] | None, default None
        Maps category -> hex color (unused keys ignored).
    width, height : int, defaults 720x480
        Figure dimensions in pixels.
    major_dtick : float, default 5000
        Major x-axis tick step. Minor gridlines will be at half this value.
    decimals : int, default 0
        Decimal places for x-axis tick labels (thousands separators kept).

    Returns
    -------
    plotly.graph_objects.Figure
        Configured horizontal bar chart.
    """
    if width <= 0 or height <= 0:
        raise ValueError("width and height must be positive integers")
    if major_dtick <= 0:
        raise ValueError("major_dtick must be > 0")

    # Aggregate
    df = (
        gdf[[group_col, value_col]]
        .dropna(subset=[group_col, value_col])
        .groupby(group_col, as_index=False)[value_col]
        .sum()
        .sort_values(value_col, ascending=False)
        .reset_index(drop=True)
    )

    # Percent of total for labels
    total = df[value_col].sum()
    df["__pct_text__"] = (
        ((df[value_col] / total) * 100).round(0).astype(int).astype(str) + "%"
        if total
        else "0%"
    )

    # Colors
    color_discrete_map = None
    if color_map:
        for k, v in color_map.items():
            if not isinstance(v, str) or not HEX_RE.match(v):
                raise ValueError(f"Invalid hex color for '{k}': {v}")
        present = set(df[group_col].astype(str))
        color_discrete_map = {k: v for k, v in color_map.items() if k in present}

    # Build figure
    fig = px.bar(
        df,
        x=value_col,
        y=group_col,
        orientation="h",
        title=title,
        color=group_col,
        color_discrete_map=color_discrete_map,
        width=width,
        height=height,
        text="__pct_text__" if show_percent else None,
    )

    if show_percent:
        fig.update_traces(textposition="outside", cliponaxis=False)

    # x-axis formatting with thousands separators and custom steps
    tickformat = f",.{decimals}f"
    fig.update_layout(
        xaxis=dict(
            title=None,
            showgrid=True,
            gridcolor="rgba(0,0,0,0.25)",
            gridwidth=1,
            tickformat=tickformat,
            dtick=major_dtick,
            minor=dict(
                showgrid=True,
                gridcolor="rgba(0,0,0,0.12)",
                gridwidth=1,
                dtick=major_dtick / 2.0,
            ),
            rangemode="tozero",
            zeroline=False,
        ),
        yaxis=dict(title=None, showgrid=False, automargin=True),
        margin=dict(l=10, r=10, t=40, b=10),
        showlegend=False,
        bargap=0.2,
        paper_bgcolor="white",
        plot_bgcolor="white",
    )

    return fig


# --- Example usage ---
if __name__ == "__main__":
    # Example 1
    data1 = {
        "ownership": ["BLM", "Private", "State", "Unknown"],
        "area_acres": [27000, 35000, 5000, 100],
    }
    gdf1 = gpd.GeoDataFrame(pd.DataFrame(data1))
    COLORS1 = {"BLM": "#f1e3a6", "Private": "#1f77b4", "State": "#2ca02c", "Unknown": "#999999"}

    fig1 = make_rs_horizontal_bar(
        gdf1,
        group_col="ownership",
        value_col="area_acres",
        title="Watershed Ownership Breakdown",
        show_percent=True,
        color_map=COLORS1,
        width=800,
        height=480,
        decimals=0,
        major_dtick=5000,
    )
    fig1.write_html("ownership_horizontal_bar.html", full_html=True)

    # Example 2
    data2 = {
        "class": [
            "Herbaceous - shrub-steppe",
            "Herbaceous - grassland",
            "Open tree canopy",
            "Shrubland",
        ],
        "acres": [2750, 2050, 210, 35],
    }
    gdf2 = gpd.GeoDataFrame(pd.DataFrame(data2))
    COLORS2 = {
        "Herbaceous - shrub-steppe": "#b4946b",
        "Herbaceous - grassland": "#f4da73",
        "Open tree canopy": "#b8e986",
        "Shrubland": "#8c6239",
    }

    fig2 = make_rs_horizontal_bar(
        gdf2,
        group_col="class",
        value_col="acres",
        title="Land Use Type (Non-BLM)",
        show_percent=True,
        color_map=COLORS2,
        width=800,
        height=480,
        decimals=0,
        major_dtick=500,
    )
    fig2.write_html("landuse_horizontal_bar.html", full_html=True)
    print("✅ Saved ownership_horizontal_bar.html and landuse_horizontal_bar.html")
