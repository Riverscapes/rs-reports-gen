"""Histogram helpers with analytic threshold lines.

``which``: a branded ``go.Histogram`` plus vertical threshold lines (TMDL
limits, return-period events, anadromous cutoffs…), so reports get the same
figure whether they embed interactive (HTML) or static (SVG/PNG).

Threshold entries can be raw numbers, or dicts/dataclasses with ``value``,
``label``, and ``color``. Colors fall back to the Riverscapes accent.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

import plotly.graph_objects as go

from util.brand import ACCENT_COLOR


class Threshold:
    """One vertical threshold line.

    Args:
        value: X position of the line.
        label: Optional annotation text (placed at the top of the plot).
        color: Line color (default the Riverscapes accent navy).
        dashed: True for a dashed line (default False).
    """

    def __init__(
        self,
        value: float,
        label: str | None = None,
        color: str | None = None,
        dashed: bool = False,
    ):
        self.value = float(value)
        self.label = label
        self.color = color or ACCENT_COLOR
        self.dashed = dashed


def _normalize_threshold(item: Threshold | dict[str, Any] | float | int) -> Threshold:
    if isinstance(item, Threshold):
        return item
    if is_dataclass(item) or isinstance(item, dict):
        data = asdict(item) if is_dataclass(item) else dict(item)
        return Threshold(
            data.get("value"),
            data.get("label"),
            data.get("color"),
            bool(data.get("dashed", False)),
        )
    return Threshold(float(item))


def make_histogram_with_thresholds(
    values: list[float],
    thresholds: list[Threshold | dict[str, Any] | float] | None = None,
    *,
    title: str | None = None,
    x_title: str | None = None,
    y_title: str | None = None,
    nbins: int | None = None,
    bin_size: float | None = None,
) -> go.Figure:
    """Build a branded histogram with optional vertical threshold lines.

    Args:
        values: Raw values binned by plotly.
        thresholds: Each entry is a raw x position or a
            ``{"value": x, "label": "…", "color": "#hex", "dashed": True}`` dict
            (or a :class:`Threshold` instance).
        title: Optional figure title.
        x_title: Optional x-axis label (units go here, e.g. ``"Gradient (%)"``).
        y_title: Optional y-axis label (defaults to ``"Count"`` when set).
        nbins: Optional fixed bin count.
        bin_size: Optional fixed bin width (takes precedence over ``nbins``).

    Returns:
        A ``go.Figure`` ready for ``report.add_figure(name, fig)``.
    """
    hist_specs: dict[str, Any] = {"x": list(values), "name": "Distribution"}
    if bin_size is not None:
        hist_specs["xbins"] = {"size": float(bin_size)}
    elif nbins:
        hist_specs["nbinsx"] = int(nbins)

    fig = go.Figure(data=[go.Histogram(**hist_specs)])

    for item in thresholds or []:
        t = _normalize_threshold(item)
        fig.add_vline(
            x=t.value,
            line_color=t.color,
            line_width=2,
            line_dash="dash" if t.dashed else None,
            annotation_text=t.label or "",
            annotation_position="top",
            annotation_font_color=t.color,
            annotation_font_size=11,
        )

    fig.update_layout(
        title=title,
        xaxis_title=x_title,
        yaxis_title=y_title,
        showlegend=False,
    )
    return fig
