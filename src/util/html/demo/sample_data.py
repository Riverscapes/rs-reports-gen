"""Deterministic sample content for the DEMO style-guide report.

Everything here mirrors the data shapes that real reports pass to
``RSReport``: plotly figures (including the shared AOI map from
``util.figures.make_map_with_aoi``), HTML table fragments, metric-card dicts
and highlight-card dicts. Nothing touches Athena or network data.
"""

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
from shapely.geometry import Polygon

from util.pandas import RSFieldMeta

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


# ---------------------------------------------------------------------------
# Maps
# ---------------------------------------------------------------------------


def _sample_aoi_map_data() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Tiny deterministic DGO + AOI polygons in WGS84 (lon/lat).

    Mirrors the shape rivers_need_space passes to ``make_map_with_aoi``:
    a GeoDataFrame with a geometry column named ``dgo_polygon_geom`` plus
    ``fcode_desc`` / ``ownership_desc`` / ``segment_area``, and an AOI
    GeoDataFrame with a ``geometry`` column.
    """
    dgo = gpd.GeoDataFrame(
        {
            "fcode_desc": [
                "Perennial Stream / River",
                "Intermittent Stream / River",
                "Perennial Stream / River",
                "Artificial Path",
            ],
            "ownership_desc": ["State", "Private", "Private", "State"],
            "segment_area": [12.4, 31.7, 6.2, 8.9],
        },
        geometry=[
            Polygon([(-106.95, 40.41), (-106.88, 40.415), (-106.86, 40.37), (-106.93, 40.36), (-106.95, 40.41)]),
            Polygon([(-106.92, 40.39), (-106.86, 40.39), (-106.84, 40.355), (-106.9, 40.35), (-106.92, 40.39)]),
            Polygon([(-106.91, 40.395), (-106.87, 40.395), (-106.87, 40.375), (-106.91, 40.375), (-106.91, 40.395)]),
            Polygon([(-106.96, 40.38), (-106.9, 40.385), (-106.88, 40.36), (-106.94, 40.355), (-106.96, 40.38)]),
        ],
        crs="EPSG:4326",
    ).rename_geometry("dgo_polygon_geom")

    aoi = gpd.GeoDataFrame(
        {
            "geometry": [
                Polygon(
                    [
                        (-106.98, 40.42),
                        (-106.87, 40.43),
                        (-106.83, 40.365),
                        (-106.95, 40.34),
                        (-107.0, 40.38),
                        (-106.98, 40.42),
                    ]
                )
            ]
        },
        crs="EPSG:4326",
    )
    return dgo, aoi


def _register_sample_field_meta():
    """Register minimal field metadata so ``make_map_with_aoi`` can bake units.

    Real reports load this from Athena (``define_fields()``); the demo uses a
    tiny stand-in so it stays network-free.
    """
    meta = RSFieldMeta()
    meta.add_field_meta(name="fcode_desc", friendly_name="Flow Type", dtype="TEXT")
    meta.add_field_meta(name="ownership_desc", friendly_name="Ownership", dtype="TEXT")
    meta.add_field_meta(name="segment_area", friendly_name="Segment Area", dtype="REAL")


def make_sample_aoi_map() -> go.Figure:
    """Build the DEMO AOI map with the shared `Rivers Need Space` helper.

    Uses ``util.figures.make_map_with_aoi`` — the exact function the
    rivers_need_space report uses — so the demo map is branded (Riverscapes
    Plotly template) and exports to interactive HTML *and* static SVG/PNG
    through the normal ``RSReport.add_figure`` pipeline.

    Field metadata is registered for the sample columns and then restored, so
    this function never pollutes the shared ``RSFieldMeta`` Borg singleton
    (which real reports own during their run).
    """
    from util.figures import make_map_with_aoi

    meta = RSFieldMeta()
    saved_field_meta = meta._field_meta
    saved_unit_system = meta._unit_system
    try:
        _register_sample_field_meta()
        dgo, aoi = _sample_aoi_map_data()
        return make_map_with_aoi(dgo, aoi)
    finally:
        # Restore whatever the caller had loaded, leaving no trace behind.
        meta._field_meta = saved_field_meta
        meta._unit_system = saved_unit_system


def sample_figures() -> dict[str, go.Figure]:
    """Figures keyed the way real reports key them."""
    return {
        "ownership_pie": make_ownership_pie(),
        "flow_type_bar": make_flow_type_bar(),
        "longitudinal_line": make_longitudinal_line(),
        "aoi_map": make_sample_aoi_map(),
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
