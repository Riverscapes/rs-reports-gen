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

from util.html.progress import (
    ProgressCard,
    ProgressGroup,
    ProgressRow,
    render_progress_card,
    render_progress_rows,
)
from util.html.table import render_table
from util.html.widgets import (
    Badge,
    Callout,
    Citation,
    Meter,
    Step,
    Term,
    render_badges,
    render_callout,
    render_citation_block,
    render_citations,
    render_flag_banner,
    render_gauge,
    render_glossary,
    render_key_figure,
    render_meter,
    render_steps,
)
from util.pandas import RSFieldMeta
from util.plotly.histograms import Threshold, make_histogram_with_thresholds

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


def make_gradient_histogram() -> go.Figure:
    """DGO gradient distribution with anadromous + step-pool thresholds."""
    values = [
        0.1, 0.2, 0.4, 0.6, 0.8, 0.9, 1.2, 1.5, 1.7, 1.9, 2.2, 2.4, 2.8,
        3.1, 3.4, 3.7, 4.1, 4.6, 5.2, 5.8, 6.3, 7.0, 7.6, 8.4, 9.1, 10.2,
        11.5, 12.4,
    ]
    return make_histogram_with_thresholds(
        values,
        thresholds=[
            Threshold(4.0, "Anadromous cutoff", color="#1b9e6b"),
            Threshold(10.0, "Step-pool transition", color="#e8840c", dashed=True),
        ],
        title="Reach gradient across DGOs",
        x_title="Gradient (%)",
        y_title="DGOs",
        bin_size=1.5,
    )


def sample_figures() -> dict[str, go.Figure]:
    """Figures keyed the way real reports key them."""
    return {
        "ownership_pie": make_ownership_pie(),
        "flow_type_bar": make_flow_type_bar(),
        "longitudinal_line": make_longitudinal_line(),
        "aoi_map": make_sample_aoi_map(),
        "gradient_histogram": make_gradient_histogram(),
    }



# ---------------------------------------------------------------------------
# Tables (rendered through the shared render_table / render_data_table macro)
# ---------------------------------------------------------------------------


def _simple_table() -> str:
    df = pd.DataFrame(
        {  # noqa: E501 - formatted column names are intentional
            "Ownership": ["Private", "Public - State", "Public - Federal", "Tribal"],
            "Area (km²)": [61.4, 22.8, 12.1, 3.7],
            "Percent": [61.4, 22.8, 12.1, 3.7],
        }
    )
    return render_table(df, caption="Ownership summary (sample data)", use_friendly=False, include_units=False)


def _footer_table() -> str:
    df = pd.DataFrame(
        {
            "Flow Type": ["Perennial", "Intermittent", "Ephemeral"],
            "Area (km²)": [542.3, 201.8, 38.2],
            "Percent": [69.4, 25.8, 4.8],
        }
    )
    totals = pd.DataFrame(
        {
            "Flow Type": ["Total"],
            "Area (km²)": [round(df["Area (km²)"].sum(), 1)],
            "Percent": [100.0],
        }
    )
    return render_table(
        df,
        caption="Flow type summary with footer totals (sample data)",
        footer=totals,
        use_friendly=False,
        include_units=False,
    )


def sample_tables() -> dict[str, str]:
    return {
        "ownership": _simple_table(),
        "with_footer": _footer_table(),
    }


# ---------------------------------------------------------------------------
# Percentage bars & extended metric cards (util/html/progress.py)
# ---------------------------------------------------------------------------


def sample_progress_groups() -> dict[str, str]:
    """Grouped percentage-bar lists, keyed for ``| safe`` injection."""
    return {
        # A primary row plus two subcomponent rows in one group.
        "stream_miles": render_progress_rows(
            [
                ProgressRow("555", "MI", pct=68, color="blue"),
                ProgressRow("85", "mi", pct=68, color="#5a92e5"),
                ProgressRow("470", "mi", pct=68, color="#4d565e"),
            ]
        ),
        # A single-value group (no subdividers) renders just as cleanly.
        "area_acres": render_progress_rows(
            [
                ProgressRow("950", "AC", pct=62, color="green"),
            ]
        ),
        # details-second line + explicit percentage.
        "structures": render_progress_rows(
            [
                ProgressRow("29", "QTY", pct=45, color="indigo", details="130 BLM ACRES"),
            ]
        ),
        # A raw CSS color exercises the inline-override path above ("#5a92e5"
    # sibling row); an unknown named color would do the same.
        "fragmentation": render_progress_rows(
            [
                ProgressRow("535", "AC", pct=15, color="#e63247"),
            ]
        ),
    }


def sample_progress_cards() -> dict[str, str]:
    """Extended metric cards (title + total + one bar per group)."""
    cards = [
        ProgressCard(
            "Reservoirs",
            total="12 count (180 ac)",
            groups=[
                ProgressGroup("BLM", "4 / 60 AC", numerator=60, denominator=180, color="indigo"),
                ProgressGroup("NON-BLM", "8 / 120 AC", numerator=120, denominator=180, color="gray"),
            ],
        ),
        ProgressCard(
            "Natural Lakes",
            total="5 count (65 ac)",
            groups=[
                ProgressGroup("BLM", "3 / 40 AC", numerator=40, denominator=65, color="indigo"),
                ProgressGroup("NON-BLM", "2 / 25 AC", numerator=25, denominator=65, color="gray"),
            ],
        ),
        ProgressCard(
            "Ponds",
            total="34 count (45 ac)",
            groups=[
                ProgressGroup("BLM", "22 / 30 AC", numerator=30, denominator=45, color="indigo"),
                ProgressGroup("NON-BLM", "12 / 15 AC", numerator=15, denominator=45, color="gray"),
                ProgressGroup("OTHER", "2 / 5 AC", numerator=2, denominator=5, color="green"),
            ],
        ),
    ]
    return {card.title.lower(): render_progress_card(card) for card in cards}


# ---------------------------------------------------------------------------
# Metric cards & highlight cards (same dict shapes the macros consume)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Widgets (util/html/widgets.py)
# ---------------------------------------------------------------------------


def sample_widgets() -> dict[str, str]:
    """One fragment per widget kind, keyed for ``| safe`` injection."""
    callouts = [
        Callout(
            "Ephemeral flows are seasonally estimated; the drying cycle on these "
            "reaches can exceed 30 days.",
            kind="note",
            title="Method note",
        ),
        Callout(
            "Three reaches lacked NHD flow-permanence classifiers; percentages "
            "were interpolated from adjacent DGOs.",
            kind="warning",
            title="Data gap",
        ),
        Callout(
            "Post-restoration survey completed September 2026; valley-bottom "
            "mapping is field verified.",
            kind="success",
            title="Field verified",
        ),
        Callout(
            "The NID dams query returned no rows; this section was rendered "
            "with an empty table.",
            kind="error",
            title="Query failed",
        ),
    ]
    return {
        "badges": render_badges(
            [
                Badge("Perennial", color="blue"),
                Badge("Intermittent", color="orange"),
                Badge("Ephemeral", color="red"),
                Badge("BLM", color="indigo"),
                Badge("NON-BLM", color="gray"),
                Badge("TMDL Listed", color="violet"),
            ]
        ),
        "steps": render_steps(
            [
                Step("Query", "done"),
                Step("Bin", "done"),
                Step("Classify", "active"),
                Step("Score", "pending"),
                Step("Report", "pending"),
            ]
        ),
        "callouts": "".join(render_callout(c) for c in callouts),
        "meter": render_meter(
            Meter(
                "Flood stage (share of bankfull)",
                value=0.62,
                minimum=0,
                maximum=1,
                low=0.3,
                high=0.9,
                optimum=0.9,
                unit="Q/Qbf",
            )
        ),
        "gauge": render_gauge("Sinuosity percentile", pct=78, color="green")
        + render_gauge("Fragmentation index", pct=23, color="orange") 
        + render_gauge("Connectivity index", pct=45, color="blue")
        + render_gauge("Full", pct=100, color="green")
        + render_gauge("Empty", pct=0, color="red"),
        "glossary": render_glossary(
            [
                Term("DGO", "Discrete geomorphic output — the smallest reach unit in a riverscape."),
                Term("HUC", "Hydrologic unit code; USGS watershed hierarchy (HUC-8 → HUC-12)."),
                Term("TMDL", "Total maximum daily load; the Clean Water Act pollutant budget."),
                Term("VBET", "Valley bottom extraction tool; delineates the valley bottom from DEMs."),
                Term("BRAT", "Beaver restoration assessment tool; scores beaver dam capacity."),
                Term("Q/Qbf", "Discharge normalized by bankfull flow; the flood-stage ratio."),
            ]
        ),
        "citations": render_citations(
            [
                Citation(
                    "National Hydrography Dataset (NHDPlus HR)",
                    url="https://www.usgs.gov/national-hydrography/national-hydrography-dataset",
                    url_label="usgs.gov/nhd",
                ),
                Citation(
                    "National Inventory of Dams",
                    url="https://nid.sec.usace.army.mil",
                    url_label="nid.sec.usace.army.mil",
                    icon="dam",
                ),
                Citation(
                    "BLM Assessment, Inventory & Monitoring (AIM)",
                    url="https://doi.org/10.5066/P9FFRG2X",
                    url_label="doi.org/10.5066/P9FFRG2X",
                    icon="link",
                ),
            ]
        ),
        "flag_banners": "".join(
            [
                render_flag_banner("Draft — not for distribution", "draft"),
                " ",
                render_flag_banner("Field verified", "verified"),
                " ",
                render_flag_banner("Estimated", "estimated"),
            ]
        ),
        "key_figure": render_key_figure(
            "23.4% of the riverscape is fragmented by roads or rail; "
            "restoration here returns the largest connectivity gain."
        ),
        "citation_block": render_citation_block(
            "Riverscapes Consortium (2026). DEMO Style Guide, Riverscapes "
            "Report Generator v0.1. https://reports.riverscapes.net"
        ),
    }


def sample_metric_cards() -> dict[str, dict[str, str]]:
    return {
        "total_area": {
            "title": "Total Riverscape Area",
            "value": "42.0 km²",
            "value_full_format": "42.0 square kilometer",
            "details": "Sum of DGO polygon areas",
        },
        "total_centerline": {
            "title": "Total Centerline Length",
            "value": "40.5 km",
            "value_full_format": "40.5 kilometer",
            "details": "Valley bottom centerline",
        },
        "valley_width": {
            "title": "Integrated Valley Bottom Width",
            "value": "516 m",
            "value_full_format": "516 meter",
            "details": "",
        },
        "channel_length": {
            "title": "Total Channel Length",
            "value": "72.6 km",
            "value_full_format": "72.6 kilometer",
            "details": "All channels within the valley bottom",
        },
        "stem_length": {
            "title": "Main Stem Length",
            "value": "38.1 km",
            "value_full_format": "38.1 kilometer",
            "details": "",
        },
        "fragmentation": {
            "title": "Fragmentation Index",
            "value": "23.4%",
            "value_full_format": "23.4 percent",
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
