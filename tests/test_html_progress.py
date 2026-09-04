"""Tests for the progress-bar components (util/html/progress.py + macros)."""

from pathlib import Path

from util.html.demo.build_demo import build_demo
from util.html.progress import (
    COLOR_THEMES,
    ProgressCard,
    ProgressGroup,
    ProgressRow,
    render_progress_card,
    render_progress_cards,
    render_progress_rows,
)

# ---------------------------------------------------------------------------
# render_progress_rows
# ---------------------------------------------------------------------------


def test_progress_row_renders_bar_and_percent():
    html = render_progress_rows([ProgressRow("555", "MI", pct=68, color="blue")])
    assert '<ul class="rs-progress-group">' in html
    assert 'class="progress-blue"' in html
    assert 'value="68" max="100"' in html
    assert "555" in html
    assert "MI" in html
    assert "68%" in html
    assert 'aria-label="555 MI: 68%"' in html


def test_progress_row_computes_pct_from_numerator_denominator():
    html = render_progress_rows([ProgressRow("22 / 30 AC", color="indigo", numerator=30, denominator=45)])
    assert "66.7%" in html
    assert 'value="66.6667" max="100"' in html


def test_progress_row_pct_is_clamped_and_missing_pct_defaults_to_zero():
    html = render_progress_rows(
        [
            ProgressRow("A", pct=140),
            ProgressRow("B", pct=-12),
            ProgressRow("C"),
        ]
    )
    assert 'value="100" max="100"' in html
    assert 'value="0" max="100"' in html


def test_progress_row_raw_css_color_emits_inline_override():
    html = render_progress_rows([ProgressRow("535", "AC", pct=15, color="#e63247")])
    markup = html.split("</style>")[1]  # drop the inlined progress.css
    # Raw colors are not promoted to a theme class; both bridge vars are set.
    assert "progress-blue" not in markup and "progress-indigo" not in markup
    assert 'style="--progress-color: #e63247; --pico-progress-color: #e63247"' in markup


def test_progress_row_named_theme_uses_class_not_inline_style():
    html = render_progress_rows([ProgressRow("x", pct=1, color="slate")])
    markup = html.split("</style>")[1]
    assert 'class="progress-slate"' in markup
    assert "style=" not in markup


def test_progress_row_details_and_unstyled_row():
    html = render_progress_rows([ProgressRow("29", "QTY", pct=45, color="indigo", details="130 BLM ACRES")])
    assert 'class="rs-progress-row__details">130 BLM ACRES<' in html
    # A row without a color omits the class and inline style.
    plain = render_progress_rows([ProgressRow("x", pct=50)])
    assert "<progress class=\"\"" in plain
    assert "style=" not in plain


def test_progress_snippet_escapes_html_and_drops_blank_items():
    html = render_progress_rows([{"value": "<b>x</b>", "pct": 50}, {"value": "", "label": None}])
    markup = html.split("</style>")[1]
    assert "&lt;b&gt;x&lt;/b&gt;" in markup
    assert markup.count('class="rs-progress-row"') == 1  # blank item dropped


# ---------------------------------------------------------------------------
# render_progress_card
# ---------------------------------------------------------------------------


def test_progress_card_renders_title_total_and_groups():
    html = render_progress_card(
        ProgressCard(
            "Reservoirs",
            total="12 count (180 ac)",
            groups=[
                ProgressGroup("BLM", "4 / 60 AC", numerator=60, denominator=180, color="indigo"),
                ProgressGroup("NON-BLM", "8 / 120 AC", numerator=120, denominator=180, color="gray"),
            ],
        )
    )
    assert 'class="rs-progress-card"' in html
    markup = html.split("</style>")[1]
    assert "<h3>Reservoirs</h3>" in markup
    assert "12 count (180 ac)" in markup
    assert "BLM" in markup and "NON-BLM" in markup
    assert "33.3%" in markup and "66.7%" in markup
    assert markup.count("<progress") == 2


def test_progress_card_missing_total_and_empty_groups():
    html = render_progress_card({"title": "Bare", "groups": [ProgressGroup("A", "1", pct=10), {"label": None, "value": None}]})
    markup = html.split("</style>")[1]
    assert "rs-progress-card__total" not in markup
    assert markup.count("<progress") == 1  # blank group dropped


def test_render_progress_cards_concatenates_fragments():
    cards = [
        ProgressCard("One", groups=[ProgressGroup("A", "1", pct=10)]),
        ProgressCard("Two", groups=[ProgressGroup("B", "2", pet=20)] if False else [ProgressGroup("B", "2", pct=20)]),
        ProgressCard("Three", groups=[ProgressGroup("C", "3", pct=30)]),
    ]
    html = render_progress_cards(cards)
    assert html.count('class="rs-progress-card"') == 3


def test_color_themes_are_documented_in_the_css():
    """Every named theme in COLOR_THEMES has a matching class in progress.css."""
    css = (Path(__file__).parent.parent / "src/util/html/templates/progress.css").read_text()
    for theme in COLOR_THEMES:
        assert f".progress-{theme}" in css


# ---------------------------------------------------------------------------
# DEMO style-guide integration
# ---------------------------------------------------------------------------


def test_demo_renders_progress_section(tmp_path):
    outputs = build_demo(tmp_path, html_only=True)
    html = Path(outputs[0]).read_text(encoding="utf-8")

    # Section exists and is TOC-visible (id + h2 auto-extracted).
    assert 'id="progress-bars"' in html
    assert "<h2>Percentage Bars &amp; Progress Cards</h2>" in html
    assert 'href="#progress-bars"' in html

    # Grouped bars and cards both render, built on the native <progress> element.
    assert 'class="rs-progress-group"' in html
    assert 'class="rs-progress-card"' in html
    assert "progress-indigo" in html and "progress-gray" in html
    # Raw CSS color rows use the inline custom-property override.
    assert "--progress-color: #e63247" in html
    assert "--progress-color: #5a92e5" in html
