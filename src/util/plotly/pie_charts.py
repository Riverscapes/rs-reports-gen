#!/usr/bin/env python3
import re
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional, Dict

# Regex to validate hex color codes
HEX_RE = re.compile(r"^#(?:[0-9a-fA-F]{3}){1,2}$")


def make_rs_area_by_owner_pie(
    gdf: gpd.GeoDataFrame,
    title: str = "Total Riverscape Area (units) by Ownership",
    hole: float = 0.3,
    show_percent: bool = True,
    color_map: Optional[Dict[str, str]] = None,
    width: int = 640,
    height: int = 420,
) -> go.Figure:
    """Create a (donut) pie chart of total segment area by ownership.

    Args:
        gdf: GeoDataFrame with 'ownership' and 'segment_area' columns.
        title: Chart title.
        hole: Donut hole size from 0 (full pie) to <1 (donut).
        show_percent: If True, show percent labels; else label+value.
        color_map: Dict mapping ownership -> hex color (e.g., {"Public":"#1f77b4"}).
        width: Figure width in pixels (default 640).
        height: Figure height in pixels (default 420).

    Returns:
        Plotly go.Figure.
    """
    if width <= 0 or height <= 0:
        raise ValueError("width and height must be positive integers")

    chart_data = gdf.groupby('ownership', as_index=False)['segment_area'].sum()

    # Validate colors if provided
    color_discrete_map = None
    if color_map:
        for k, v in color_map.items():
            if not isinstance(v, str) or not HEX_RE.match(v):
                raise ValueError(f"Invalid hex color for '{k}': {v}")
        # Only pass colors for categories we actually have (extra keys are ignored)
        present = set(chart_data['ownership'].astype(str))
        color_discrete_map = {k: v for k, v in color_map.items() if k in present}

    pie_fig = px.pie(
        chart_data,
        names="ownership",
        values="segment_area",
        title=title,
        hole=hole,
        color="ownership",
        color_discrete_map=color_discrete_map,  # None => Plotly defaults
        width=width,
        height=height,
    )

    textinfo = "percent+label" if show_percent else "label+value"
    pie_fig.update_traces(
        textinfo=textinfo,
        hovertemplate="%{label}<br>Area: %{value}<br>%{percent}"
    )
    pie_fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return pie_fig


if __name__ == "__main__":
    import pandas as pd
    from shapely.geometry import Polygon

    data = {
        "ownership": ["Intermittent", "Perennial", "Canal", "Intermittent", "Perennial"],
        "segment_area": [10, 30, 15, 20, 35],
        "geometry": [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
            Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
            Polygon([(3, 3), (4, 3), (4, 4), (3, 4)]),
            Polygon([(4, 4), (5, 4), (5, 5), (4, 5)]),
        ],
    }
    input_gdf = gpd.GeoDataFrame(pd.DataFrame(data), geometry="geometry")

    COLORS = {
        "Perennial": "#1f77b4",
        "Intermittent": "#ff7f0e",
        "Canal":  "#2ca02c",
        # Extra keys are fine; unused are ignored
        "Conserved": "#d62728",
    }

    fig = make_rs_area_by_owner_pie(
        input_gdf,
        title="Ownership Share of Riverscape Area",
        hole=0.3,
        show_percent=True,
        color_map=COLORS,
        width=720,
        height=480,
    )
    out = "ownership_pie_chart.html"
    fig.write_html(out, full_html=True)
    print(f"✅ Chart saved to {out}. Open it in your browser to view.")
