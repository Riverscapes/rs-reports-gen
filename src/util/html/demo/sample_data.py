"""Deterministic sample content for the DEMO style-guide report.

Everything here mirrors the data shapes that real reports pass to
``RSReport``: plotly figures, HTML table fragments, metric-card dicts and
highlight-card dicts. Nothing touches Athena or network data.
"""

import pandas as pd
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------


def make_ownership_pie() -> go.Figure:
    labels = ["Private", "Public - State", "Public - Federal", "Tribal"]
    values = [61.4, 22.8, 12.1, 3.7]
    return go.Figure(data=[go.Pie(labels=labels, values=values)])


def make_flow_type_bar() -> go.Figure:
    categories = ["Perennial", "Intermittent", "Ephemeral"]
    areas = [542.3, 201.8, 38.2]
    return go.Figure(data=[go.Bar(x=categories, y=areas)])


def make_longitudinal_line() -> go.Figure:
    xs = list(range(0, 21, 2))
    latest = [2.1, 2.4, 3.3, 4.7, 5.1, 5.8, 6.2, 6.9, 7.4, 8.1, 8.3]
    earliest = [1.9, 2.2, 2.9, 4.1, 4.6, 5.2, 5.7, 6.4, 6.9, 7.6, 7.9]
    fig = go.Figure(
        data=[
            go.Scatter(x=xs, y=earliest, name="Earliest epoch"),
            go.Scatter(x=xs, y=latest, name="Latest epoch"),
        ]
    )
    return fig


def sample_figures() -> dict[str, go.Figure]:
    """Figures keyed the way real reports key them."""
    return {
        "ownership_pie": make_ownership_pie(),
        "flow_type_bar": make_flow_type_bar(),
        "longitudinal_line": make_longitudinal_line(),
    }


# ---------------------------------------------------------------------------
# Tables (HTML fragments, as produced by DataFrame.to_html)
# ---------------------------------------------------------------------------


def _simple_table() -> str:
    df = pd.DataFrame(
        {  # noqa: E501 - formatted column names are intentional
            "Ownership": ["Private", "Public - State", "Public - Federal", "Tribal"],
            "Area (km²)": [61.4, 22.8, 12.1, 3.7],
            "Percent": [61.4, 22.8, 12.1, 3.7],
        }
    )
    return df.to_html(index=False)


def _footer_table() -> str:
    df = pd.DataFrame(
        {
            "Flow Type": ["Perennial", "Intermittent", "Ephemeral"],
            "Area (km²)": [542.3, 201.8, 38.2],
            "Percent": [69.4, 25.8, 4.8],
        }
    )
    try:
        from util.pandas.RSGeoDataFrame import RSGeoDataFrame

        rsdf = RSGeoDataFrame(df)
        rsdf.set_footer(
            pd.DataFrame(
                {
                    "Flow Type": ["Total"],
                    "Area (km²)": [round(df["Area (km²)"].sum(), 1)],
                    "Percent": [100.0],
                }
            )
        )
        return rsdf.to_html(index=False, escape=False)
    except Exception:  # pragma: no cover - fall back to plain pandas
        return df.to_html(index=False)


def sample_tables() -> dict[str, str]:
    return {
        "ownership": _simple_table(),
        "with_footer": _footer_table(),
    }


# ---------------------------------------------------------------------------
# Metric cards & highlight cards (same dict shapes the macros consume)
# ---------------------------------------------------------------------------


def sample_metric_cards() -> dict[str, dict[str, str]]:
    return {
        "total_area": {
            "title": "Total Riverscape Area",
            "value": "42.0 km²",
            "details": "Sum of DGO polygon areas",
        },
        "total_centerline": {
            "title": "Total Centerline Length",
            "value": "40.5 km",
            "details": "Valley bottom centerline",
        },
        "valley_width": {
            "title": "Integrated Valley Bottom Width",
            "value": "516 m",
            "details": "",
        },
        "channel_length": {
            "title": "Total Channel Length",
            "value": "72.6 km",
            "details": "All channels within the valley bottom",
        },
        "stem_length": {
            "title": "Main Stem Length",
            "value": "38.1 km",
            "details": "",
        },
        "fragmentation": {
            "title": "Fragmentation Index",
            "value": "23.4%",
            "details": "Share of riverscape cut by roads or rail",
        },
    }


def sample_highlight_cards() -> list[dict]:
    return [
        {
            "theme": "blue",
            "icon": "water",
            "header": "Largest Riverscape",
            "primary_value": "Bear River",
            "secondary_stat": {"icon": "straighten", "text": "14.2 km reached"},
            "footer": {"metric": "9.7 km²", "label": "largest single riverscape"},
        },
        {
            "theme": "green",
            "icon": "eco",
            "header": "Healthiest Reach",
            "primary_value": "North Fork",
            "secondary_stat": {"icon": "trending_up", "text": "92% intact"},
            "footer": {"metric": "8%", "label": "of all riverscape area"},
        },
        {
            "theme": "teal",
            "icon": "warning",
            "header": "Most Fragmented Reach",
            "primary_value": "South Runn",
            "secondary_stat": {"icon": "straighten", "text": "2.1 km reached"},
            "footer": {"metric": "0.3 km²", "label": "smallest riverscape"},
        },
    ]
