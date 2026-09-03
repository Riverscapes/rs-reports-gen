"""Riverscapes-branded Plotly template.

Registers ``pio.templates["riverscapes"]`` — brand colors (from
:mod:`util.brand`, which mirrors ``base.css`` / the PBI Riverscapes theme),
Karla (display) / Roboto type, soft grid lines, white background, and report-friendly
margins — and makes it the default template for every new figure.

Importing this module (or anything under :mod:`util.plotly`, which imports it)
is enough: the import-time side effect registers the template once and sets
``pio.templates.default``, so **every figure everywhere picks it up for free**
— including figures created by ``plotly.express`` and by ``go.Figure`` with
explicitly colored traces, and both interactive HTML and kaleido/static exports.

Overriding / opting out (reports stay in control):

* Explicit layout attributes always beat template values, e.g.
  ``fig.update_layout(margin={"l": 0, "t": 60, "b": 0})`` or
  ``fig.update_layout(paper_bgcolor="#f1f4f9")``.
* To drop the brand styling for one figure, pass an explicit template:
  ``go.Figure(data, layout=go.Layout(template="plotly"))`` or
  ``fig.update_layout(template="plotly")``.
* To get brand styling back after a report has mutated a figure, call
  :func:`apply_riverscapes_theme`.
"""

from __future__ import annotations

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

#: Name of the registered template. Reports can reference it directly:
#: ``fig.update_layout(template=util.plotly.riverscapes.TEMPLATE_NAME)``.
TEMPLATE_NAME = "riverscapes"

#: Bare-minimum trace defaults that keep the brand look when traces are created
#: with :class:`plotly.graph_objects` (no marker outlines).
_TEMPLATE_DATA: dict = {
    "scatter": [{"marker": {"line": {"width": 0}}}],
    "bar": [{"marker": {"line": {"width": 0}}}],
}


def _build_template() -> dict:
    """Compose the ``riverscapes`` template from brand tokens."""
    axis_style: dict = {
        "gridcolor": MUTED_BORDER,
        "zerolinecolor": MUTED_BORDER,
        "linecolor": MUTED_BORDER,
        "automargin": True,
        "tickfont": {"color": METRIC_NUMBER},
        "title": {"font": {"color": METRIC_NUMBER}},
    }
    return {
        "layout": {
            "colorway": COLORWAY,
            "paper_bgcolor": BACKGROUND,
            "plot_bgcolor": BACKGROUND,
            "font": {"family": BODY_FONT_FAMILY, "color": TEXT_COLOR, "size": 13},
            "title": {
                "font": {"family": HEADER_FONT_FAMILY, "color": HEADER_COLOR, "size": 16},
            },
            # Slim margins so charts fit the branded shell (the old default
            # 60px-all-round margins waste card space). Reports can override.
            "margin": {"l": 8, "r": 8, "t": 46, "b": 8},
            "xaxis": axis_style,
            "yaxis": axis_style,
            "legend": {
                "bgcolor": "rgba(255, 255, 255, 0.85)",
                "bordercolor": MUTED_BORDER,
                "borderwidth": 1,
                "font": {"color": METRIC_NUMBER},
            },
            "hoverlabel": {"bgcolor": ACCENT_COLOR, "font": {"color": BACKGROUND}},
            "modebar": {"activecolor": ACCENT_COLOR},
        },
        "data": _TEMPLATE_DATA,
    }


def register_riverscapes_template() -> go.layout.Template:
    """Register (idempotently) the ``riverscapes`` template and return it."""
    pio.templates[TEMPLATE_NAME] = _build_template()
    return pio.templates[TEMPLATE_NAME]


def set_riverscapes_default() -> None:
    """Make ``riverscapes`` the default template for all new figures."""
    register_riverscapes_template()
    pio.templates.default = TEMPLATE_NAME


def apply_riverscapes_theme(fig: go.Figure, *, set_default: bool = False) -> go.Figure:
    """Re-assert the Riverscapes template on an existing figure.

    Handy when a report has mutated a figure (or switched templates) and wants
    the brand styling back. Explicit layout attributes already set on ``fig``
    still win, because template values are a lower-priority layer.

    Args:
        fig: Figure to re-theme (modified in place and returned).
        set_default: Also make ``riverscapes`` the default for future figures.

    Returns:
        The same figure, with the brand template attached.
    """
    register_riverscapes_template()
    fig.update_layout(template=TEMPLATE_NAME)
    if set_default:
        pio.templates.default = TEMPLATE_NAME
    return fig


# ── One-shot, import-time setup ──────────────────────────────────────────────
# Every module that builds charts imports this package, so registering here
# means new figures everywhere pick up the brand template for free.
register_riverscapes_template()
pio.templates.default = TEMPLATE_NAME