"""Riverscapes brand tokens for charts and maps.

This is the single Python source of truth for the Riverscapes report brand,
mirroring the design tokens in ``src/util/html/templates/base.css`` and the
Power BI "Riverscapes" theme in ``src/util/pbi_model.py``. Plotly and folium
theming modules consume these so every chart, map, and the HTML shell stay in
lockstep.

.. note::

    If you change a color here, update the matching ``--*`` custom property in
    ``base.css`` and the ``dataColors`` in ``pbi_model.py`` so the three don't
    drift.
"""

# ── Core palette (from base.css) ─────────────────────────────────────────────
#: --header-color / --accent-color / --border-bg
HEADER_COLOR = "#003166"
#: alias for the primary brand navy used on accents and interactive states
ACCENT_COLOR = HEADER_COLOR
#: --text-color
TEXT_COLOR = "#222222"
#: --muted-bg (subtle section backgrounds)
MUTED_BG = "#f1f4f9"
#: --muted-border (rules/grid lines)
MUTED_BORDER = "#d5deeb"
#: --metric-number (secondary/supporting text, axis ticks)
METRIC_NUMBER = "#4a5764"
#: --metric-description
METRIC_DESCRIPTION = "#6e7889"
#: --table-bg
TABLE_BG = "#f5f9ff"
#: --chart-bg
CHART_BG = "#e5ecf6"
#: error/destructive red
ERROR_RED = "#c75146"
#: plot paper/background colour (charts sit on white cards in the shell)
BACKGROUND = "#ffffff"

# ── Data-series palette (PBI Riverscapes theme ``dataColors``) ───────────────
# Derived from the core anchors with enough contrast and hue variety to read
# clearly on white / light-grey backgrounds.
COLORWAY: tuple[str, ...] = (
    "#003166",  # deep navy — primary brand
    "#2171a8",  # river blue
    "#5ba4cf",  # sky / shallow water
    "#4a7c59",  # riparian green
    "#d97b2b",  # warm amber / sediment
    "#a05436",  # brown earth
    "#4a5764",  # slate (--metric-number)
    "#c75146",  # accent red
)

# ── Typography (from base.css) ───────────────────────────────────────────────
#: --header-font-family (reports' headings; used for chart titles)
HEADER_FONT_FAMILY = '"JetBrains Mono", "Courier New", Courier, monospace'
#: --body-font-family (body copy; used for all other chart text)
BODY_FONT_FAMILY = '"Roboto", Arial, sans-serif'