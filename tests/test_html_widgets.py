"""Tests for the report widgets (util/html/widgets.py + widgets.css)."""

from pathlib import Path

from util.html.demo.build_demo import build_demo
from util.html.widgets import (
    COLOR_THEMES,
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
from util.plotly.histograms import Threshold, make_histogram_with_thresholds


def _markup(html: str) -> str:
    """Strip the inlined widgets.css from a fragment for assertions."""
    return html.split("</style>", 1)[1]


# ---------------------------------------------------------------------------
# Badges
# ---------------------------------------------------------------------------


def test_badges_render_theme_classes_and_raw_colors():
    html = render_badges(
        [
            Badge("Perennial", color="blue"),
            Badge("Ephemeral", color="#cf596b"),
            Badge("No color"),
        ]
    )
    markup = _markup(html)
    assert 'class="rs-badge badge-blue"' in markup
    assert 'style="--badge-color: #cf596b"' in markup
    assert "No color" in markup
    assert markup.count('<span class="rs-badge') == 3


def test_badges_drop_blank_items_and_escape():
    html = render_badges([Badge("<b>x</b>"), {"label": ""}])
    markup = _markup(html)
    assert "&lt;b&gt;x&lt;/b&gt;" in markup
    assert markup.count('<span class="rs-badge') == 1


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def test_steps_render_numbered_pills_with_statuses():
    html = render_steps([Step("Query", "done"), Step("Bin", "active"), Step("Score", "pending")])
    markup = _markup(html)
    assert 'class="rs-steps"' in markup
    assert '<li class="done">Query</li>' in markup
    assert '<li class="active">Bin</li>' in markup
    assert '<li class="pending">Score</li>' in markup


def test_steps_unknown_status_falls_back_to_pending():
    html = render_steps([Step("X", "banana")])
    assert '<li class="pending">X</li>' in _markup(html)


# ---------------------------------------------------------------------------
# Callouts
# ---------------------------------------------------------------------------


def test_callout_kinds_and_titles():
    html = render_callout(Callout("Body text", kind="warning", title="Data gap"))
    markup = _markup(html)
    assert 'class="rs-callout rs-callout--warning"' in markup
    assert 'class="rs-callout__title">Data gap<' in markup
    assert "Body text" in markup
    assert "warning" in markup  # material icon


def test_callout_unknown_kind_falls_back_to_note():
    html = render_callout({"content": "x", "kind": "purple"})
    assert "rs-callout--note" in _markup(html)


# ---------------------------------------------------------------------------
# Meter
# ---------------------------------------------------------------------------


def test_meter_renders_threshold_attrs():
    html = render_meter(Meter("Flood stage", value=0.62, low=0.3, high=0.9, optimum=0.9, unit="Q/Qbf"))
    markup = _markup(html)
    assert "<meter" in markup
    assert 'value="0.62"' in markup
    assert 'low="0.3"' in markup
    assert 'high="0.9"' in markup
    assert 'optimum="0.9"' in markup
    assert "Q/Qbf" in markup


# ---------------------------------------------------------------------------
# Gauge
# ---------------------------------------------------------------------------


def test_gauge_pct_and_theme():
    html = render_gauge("Sinuosity percentile", pct=78, color="green")
    markup = _markup(html)
    assert 'class="rs-gauge gauge-green"' in markup
    assert "78%" in markup
    assert "Sinuosity percentile" in markup
    assert "rs-gauge__svg" in markup


def test_gauge_maps_value_onto_min_max():
    html = render_gauge("x", value=50, minimum=0, maximum=200)
    assert "25%" in _markup(html)


def test_gauge_raw_color_inline():
    html = render_gauge("x", pct=50, color="#123456")
    assert 'style="--gauge-color: #123456"' in _markup(html)


# ---------------------------------------------------------------------------
# Glossary
# ---------------------------------------------------------------------------


def test_glossary_renders_dt_dd_grid():
    html = render_glossary([Term("DGO", "Discrete geomorphic output"), Term("HUC", "Hydrologic unit code")])
    markup = _markup(html)
    assert 'class="rs-glossary"' in markup
    assert "<dt>DGO</dt>" in markup
    assert "<dd>Discrete geomorphic output</dd>" in markup
    assert markup.count("rs-glossary__item") == 2


# ---------------------------------------------------------------------------
# Citations
# ---------------------------------------------------------------------------


def test_citations_render_title_url_and_icon():
    html = render_citations(
        [
            Citation("NHD", url="https://usgs.gov/nhd", url_label="usgs.gov/nhd"),
            Citation("BLM AIM", url="https://doi.org/10.5066/P9FFRG2X", icon="link"),
        ]
    )
    markup = _markup(html)
    assert 'class="rs-citations"' in markup
    assert "NHD" in markup
    assert 'href="https://usgs.gov/nhd"' in markup
    assert "usgs.gov/nhd" in markup
    assert "link" in markup  # icon override


# ---------------------------------------------------------------------------
# Flag banners
# ---------------------------------------------------------------------------


def test_flag_banner_kinds():
    for kind in ("draft", "verified", "estimated", "warning"):
        html = render_flag_banner("X", kind)
        assert f"rs-flag--{kind}" in _markup(html)


def test_flag_banner_unknown_kind_falls_back_to_draft():
    assert "rs-flag--draft" in _markup(render_flag_banner("X", "gold"))


# ---------------------------------------------------------------------------
# Key-figure box
# ---------------------------------------------------------------------------


def test_key_figure_renders_label_and_text():
    html = render_key_figure("23.4% fragmented.", label="Bottom line")
    markup = _markup(html)
    assert 'class="rs-key-figure"' in markup
    assert "BOTTOM LINE" in markup or "Bottom line" in markup
    assert "23.4% fragmented." in markup


# ---------------------------------------------------------------------------
# Copyable citation block
# ---------------------------------------------------------------------------


def test_citation_block_renders_text_and_copy_button():
    html = render_citation_block("Cite me (2026).")
    markup = _markup(html)
    assert 'class="rs-cite-block"' in markup
    assert "Cite me (2026)." in markup
    assert "rs-cite-block__copy" in markup
    assert "pdf-hide" in markup
    assert "onclick=" in markup


# ---------------------------------------------------------------------------
# Histogram with thresholds
# ---------------------------------------------------------------------------


def test_histogram_with_thresholds_builds_vlines():
    fig = make_histogram_with_thresholds(
        [1, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        thresholds=[Threshold(4.0, "Cutoff", color="#1b9e6b"), {"value": 8.0, "label": "Upper", "dashed": True}],
        x_title="Gradient (%)",
        bin_size=2,
    )
    assert fig.data[0].type == "histogram"
    shapes = fig.layout.shapes
    assert len(shapes) == 2
    assert shapes[0].x0 == 4.0 and shapes[0].line.color == "#1b9e6b"
    assert shapes[1].line.dash == "dash"
    assert fig.layout.xaxis.title.text == "Gradient (%)"


def test_histogram_raw_threshold_values():
    fig = make_histogram_with_thresholds([1, 2, 3], thresholds=[2.5])
    assert len(fig.layout.shapes) == 1
    assert fig.layout.shapes[0].x0 == 2.5


# ---------------------------------------------------------------------------
# Color-theme parity
# ---------------------------------------------------------------------------


def test_color_themes_are_documented_in_widgets_css():
    css = (Path(__file__).parent.parent / "src/util/html/templates/widgets.css").read_text()
    for theme in COLOR_THEMES:
        assert f".badge-{theme}" in css
        assert f".gauge-{theme}" in css


# ---------------------------------------------------------------------------
# DEMO integration
# ---------------------------------------------------------------------------


def test_demo_renders_widgets_section(tmp_path):
    outputs = build_demo(tmp_path, html_only=True)
    html = Path(outputs[0]).read_text(encoding="utf-8")

    assert 'id="widgets"' in html
    assert "<h2>Widgets</h2>" in html
    assert 'href="#widgets"' in html

    # Each widget family shows up in the rendered demo.
    for marker in (
        'class="rs-badges"',
        'class="rs-steps"',
        "rs-callout--warning",
        "<meter",
        "rs-gauge__svg",
        'class="rs-glossary"',
        'class="rs-citations"',
        "rs-flag--draft",
        'class="rs-key-figure"',
        "rs-cite-block__copy",
    ):
        assert marker in html, marker

    # The histogram with thresholds is a normal plotly figure embed.
    assert "gradient_histogram" in html
    assert "Anadromous cutoff" in html
