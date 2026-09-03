"""Tests for the Riverscapes-branded Plotly template (util.plotly.riverscapes)."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

from util.brand import (
    ACCENT_COLOR,
    BACKGROUND,
    BODY_FONT_FAMILY,
    COLORWAY,
    HEADER_COLOR,
    HEADER_FONT_FAMILY,
    METRIC_NUMBER,
    MUTED_BORDER,
    TEXT_COLOR,
)
from util.plotly.riverscapes import TEMPLATE_NAME, apply_riverscapes_theme


def _brand_template():
    """The registered template's layout, with defaults materialized."""
    return pio.templates[TEMPLATE_NAME].layout


def test_template_registered_and_default():
    assert TEMPLATE_NAME in pio.templates
    assert pio.templates.default == TEMPLATE_NAME


def test_template_layout_uses_brand_tokens():
    layout = _brand_template()
    assert layout.paper_bgcolor == BACKGROUND
    assert layout.plot_bgcolor == BACKGROUND
    assert layout.font.family == BODY_FONT_FAMILY
    assert layout.font.color == TEXT_COLOR
    assert layout.title.font.family == HEADER_FONT_FAMILY
    assert layout.title.font.color == HEADER_COLOR
    assert tuple(layout.colorway) == COLORWAY
    # grids hang off --muted-border; ticks off --metric-number
    assert layout.xaxis.gridcolor == MUTED_BORDER
    assert layout.yaxis.gridcolor == MUTED_BORDER
    assert layout.xaxis.tickfont.color == METRIC_NUMBER
    assert layout.hoverlabel.bgcolor == ACCENT_COLOR
    # slim, report-friendly margins (not the fat 60px Plotly default)
    assert layout.margin.t >= 40
    assert layout.margin.l <= 12


def test_go_figure_attached_brand_template():
    fig = go.Figure(go.Bar(x=["a", "b"], y=[1, 2]))
    tpl = fig.layout.template.layout
    assert tpl.font.family == BODY_FONT_FAMILY
    assert tpl.paper_bgcolor == BACKGROUND
    assert tuple(tpl.colorway) == COLORWAY


def test_express_chart_gets_brand_colorway():
    df = pd.DataFrame({"x": ["a", "b", "c"], "y": [1, 2, 3]})
    bar = px.bar(df, x="x", y="y")
    assert bar.data[0].marker.color == COLORWAY[0]
    line = px.line(df, x="x", y="y")
    assert line.data[0].line.color == COLORWAY[0]


def test_json_roundtrip_preserves_brand():
    fig = px.bar(pd.DataFrame({"x": ["a"], "y": [1]}), x="x", y="y")
    raw = fig.to_json()
    assert COLORWAY[0] in raw  # brand navy baked into the serialized figure
    assert BODY_FONT_FAMILY.split(",")[0].strip('"') in raw
    cloned = pio.from_json(raw)
    assert cloned.layout.template.layout.font.family == BODY_FONT_FAMILY
    assert cloned.layout.template.layout.paper_bgcolor == BACKGROUND
    assert tuple(cloned.layout.template.layout.colorway) == COLORWAY


def test_update_layout_overrides_template_but_brand_layer_survives():
    fig = go.Figure(go.Bar(x=["a"], y=[1]))
    fig.update_layout(margin={"l": 99}, paper_bgcolor="#ff00ff")
    # Explicit overrides win...
    assert fig.layout.margin.l == 99
    assert fig.layout.paper_bgcolor == "#ff00ff"
    # ...while unset values still come from the brand template.
    assert fig.layout.template.layout.font.family == BODY_FONT_FAMILY


def test_escape_hatch_explicit_template():
    fig = go.Figure(go.Bar(x=["a"], y=[1]), layout=go.Layout(template="plotly"))
    assert fig.layout.template.layout.colorway[0] != COLORWAY[0]


def test_apply_riverscapes_theme_restores_brand():
    fig = go.Figure(go.Bar(x=["a"], y=[1]), layout=go.Layout(template="plotly"))
    apply_riverscapes_theme(fig)
    tpl = fig.layout.template.layout
    assert tuple(tpl.colorway) == COLORWAY
    assert tpl.font.family == BODY_FONT_FAMILY
    assert tpl.paper_bgcolor == BACKGROUND