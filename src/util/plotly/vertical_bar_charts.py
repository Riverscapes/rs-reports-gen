#!/usr/bin/env python3
import re
from typing import Optional, Dict

import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Regex to validate hex color codes
HEX_RE = re.compile(r"^#(?:[0-9a-fA-F]{3}){1,2}$")


def make_rs_vertical_bar(
    gdf: gpd.GeoDataFrame,
    group_col: str,
    value_col: str,
    title: str = "Vertical Bar Chart",
    show_percent: bool = True,
    color_map: Optional[Dict[str, str]] = None,
    width: int = 720,
    height: int = 480,
    decimals: int = 0,             # y-axis tick label decimals
    label_angle: int = -30,        # <-- fixed default; user can override
    keep_input_order: bool = False,
    labels: Optional[Dict[str, str]] = None,     # <-- NEW
    hover_decimals: Optional[int] = None,        # <-- NEW
) -> go.Figure:
    """
    Create a vertical bar chart aggregated by `group_col`.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Must contain `group_col` and `value_col`.
    group_col : str
        Categorical column used for x-axis categories.
    value_col : str
        Numeric column to sum for bar heights (y-axis).
    title : str, default "Vertical Bar Chart"
        Figure title text.
    show_percent : bool, default True
        If True, displays each bar's percent of total above the bar.
    color_map : dict[str, str] | None, default None
        Maps category -> hex color (unused keys ignored).
    width, height : int, defaults 720x480
        Figure dimensions in pixels.
    decimals : int, default 0
        Decimal places for y-axis tick labels (thousands separators kept).
    label_angle : int, default -30
        Rotation angle (degrees) for x-axis category labels. Use 0, -30, -45, etc.

    Returns
    -------
    plotly.graph_objects.Figure
        Configured vertical bar chart.
    """
    if width <= 0 or height <= 0:
        raise ValueError("width and height must be positive integers")

    df = (
        gdf[[group_col, value_col]]
        .dropna(subset=[group_col, value_col])
        .groupby(group_col, as_index=False)[value_col]
        .sum()
        .reset_index(drop=True)
    )

    # Preserve input or categorical order if requested
    if keep_input_order and isinstance(gdf[group_col].dtype, pd.CategoricalDtype):
        cat_order = list(gdf[group_col].cat.categories)
        df[group_col] = pd.Categorical(df[group_col], categories=cat_order, ordered=True)

    total = df[value_col].sum()
    df["__pct_text__"] = (
        ((df[value_col] / total) * 100).round(0).astype(int).astype(str) + "%"
        if total
        else "0%"
    )

    # Colors (optional)
    color_discrete_map = None
    if color_map:
        for k, v in color_map.items():
            if not isinstance(v, str):
                raise ValueError(f"Invalid color format for '{k}': {v}")

            # Accept both #hex and rgb() formats
            is_hex = bool(HEX_RE.match(v))
            is_rgb = v.strip().lower().startswith("rgb(")
            if not (is_hex or is_rgb):
                raise ValueError(
                    f"Invalid color string for '{k}': {v} "
                    "(must be '#hex' or 'rgb(...)')"
                )

    present = set(df[group_col].astype(str))
    color_discrete_map = {k: v for k, v in color_map.items() if k in present}

    # Figure
    fig = px.bar(
        df,
        x=group_col,
        y=value_col,
        title=title,
        color=group_col,
        color_discrete_map=color_discrete_map,  # None => Plotly defaults
        width=width,
        height=height,
        text="__pct_text__" if show_percent else None,  # per-bar label
        labels=labels,                              # <-- pass labels through

    )

    # If the incoming data has an ordered categorical, or caller wants to keep order,
    # enforce it on the axis so Plotly doesn't re-sort categories.
    if keep_input_order:
        # Use the current df order for x categories
        cat_order = list(df[group_col].astype(str))
        fig.update_xaxes(categoryorder="array", categoryarray=cat_order)

    if show_percent:
        fig.update_traces(textposition="outside", cliponaxis=False)

    # y-axis: thousands formatting, major+minor gridlines behind bars
    tickformat = f",.{decimals}f"
    fig.update_layout(
        yaxis=dict(
            title=None,
            tickformat=tickformat,
            showgrid=True,
            gridcolor="white",            # white grid lines
            gridwidth=1,
            minor=dict(
                showgrid=False,
            ),
            rangemode="tozero",
            zeroline=False,
            showticklabels=True,                     # make sure ticks are shown
            tickfont=dict(size=12, color="black"),   # readable ticks
        ),
        xaxis=dict(title=None, tickangle=int(label_angle)),
        margin=dict(l=60, r=10, t=40, b=10),         # extra left margin so ticks never clip
        showlegend=True,                             # show legend like your left chart
        legend=dict(
            traceorder="normal",     # <--- ensures ascending order (matches x-axis)
        ),
        bargap=0.2,
        paper_bgcolor="white",            # full figure background stays white
        plot_bgcolor="rgb(240, 245, 250)",  # light gray plot area (the key)
    )

    x_label = (labels or {}).get(group_col, group_col)
    y_label = (labels or {}).get(value_col, value_col)
    y_dec = hover_decimals if hover_decimals is not None else decimals
    fig.update_traces(
        hovertemplate=f"{x_label}=%{{x}}<br>{y_label}=%{{y:,.{y_dec}f}}<extra></extra>"
    )

    return fig


# ---- Example usage ----
if __name__ == "__main__":
    data = {
        "ownership": ["BLM", "Private", "State", "Unknown"],
        "area_acres": [27000, 35000, 5000, 100],
    }
    gdf = gpd.GeoDataFrame(pd.DataFrame(data))
    COLORS = {"BLM": "#f1e3a6", "Private": "#1f77b4", "State": "#2ca02c", "Unknown": "#999999"}

    fig = make_rs_vertical_bar(
        gdf,
        group_col="ownership",
        value_col="area_acres",
        title="Watershed Ownership Breakdown (Vertical)",
        show_percent=True,
        color_map=COLORS,
        width=900,
        height=520,
        label_angle=-30,   # override with e.g., 0 or -45
    )
    fig.write_html("ownership_vertical_bar.html", full_html=True)
    print("✅ Saved ownership_vertical_bar.html")
