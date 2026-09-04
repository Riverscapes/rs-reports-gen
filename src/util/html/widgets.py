"""Small reusable report widgets: badges, steps, callouts, meter, gauge,
glossary, citations, flag banners, key-figure box, and a copyable citation block.

Mirrors ``util/html/progress.py``: each helper normalizes a small model
(dataclass or plain dict) and returns a self-contained HTML fragment whose own
``<style>`` tag carries ``templates/widgets.css``. Inject fragments into a
report body with ``{{ widgets['key'] | safe }}``.

All browser-visible text is HTML-escaped. The copyable citation block ships a
tiny inline ``onclick`` handler (no external JS dependency).

Example:
    >>> from util.html.widgets import Badge, Step, render_badges, render_steps
    >>> render_badges([Badge("Perennial", color="green"), Badge("Ephemeral")])
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from html import escape
from typing import Any

_CSS_PATH = __file__.replace("widgets.py", "templates/widgets.css")
_WIDGETS_CSS: str | None = None

# Named color themes, shared with the progress components so report code can
# mix badges, bars and cards in one palette.
COLOR_THEMES = (
    "blue",
    "blue-light",
    "indigo",
    "green",
    "orange",
    "red",
    "slate",
    "gray",
    "violet",
)

#: Callout kinds and their Material icon defaults.
CALLOUT_KINDS = {
    "note": "info",
    "info": "info",
    "success": "check_circle",
    "warning": "warning",
    "error": "error",
}

# ---- fragment helpers -----------------------------------------------------


def _style() -> str:
    """Emit one `<style>` tag with the widgets stylesheet."""
    global _WIDGETS_CSS
    if _WIDGETS_CSS is None:
        with open(_CSS_PATH, encoding="utf-8") as f:
            _WIDGETS_CSS = f.read()
    return f"<style>{_WIDGETS_CSS}</style>"


def _themed(color: Any, prefix: str) -> tuple[str | None, str | None]:
    """Resolve a color to either a theme class (e.g. ``badge-blue``) or an
    inline custom-property override (``--badge-color: #hex``)."""
    if not color:
        return None, None
    c = str(color)
    if c in COLOR_THEMES:
        return f"{prefix}-{c}", None
    safe = c.replace('"', "").replace(";", "")
    return None, f"--{prefix}-color: {safe}"


def _attrs(cls: str | None, style: str | None) -> str:
    """Build the tag attribute string for a fragment root element."""
    parts = []
    if cls:
        parts.append(f'class="{cls.strip()}"')
    if style:
        parts.append(f'style="{style}"')
    return (" " + " ".join(parts)) if parts else ""


# ---- badges / pills -------------------------------------------------------


@dataclass
class Badge:
    """A categorical pill chip.

    Args:
        label: Short text, e.g. ``"Perennial"``.
        color: Named theme (blue, blue-light, indigo, green, orange, red,
            slate, gray, violet) or any CSS color (emitted inline).
        icon: Optional Material icon name.
    """

    label: str
    color: str | None = None
    icon: str | None = None


def render_badges(items: list[Badge | dict[str, Any]]) -> str:
    """Render a row of categorical pill chips."""
    spans = []
    for item in items or []:
        data = asdict(item) if is_dataclass(item) else dict(item)
        if not data.get("label"):
            continue
        cls, style = _themed(data.get("color"), "badge")
        icon = data.get("icon")
        icon_html = f'<span class="material-icons" aria-hidden="true">{escape(str(icon))}</span>' if icon else ""
        spans.append(f'<span{_attrs(f"rs-badge {cls or ""}".strip(), style)}>{icon_html}{escape(str(data["label"]))}</span>')
    return f'{_style()}<div class="rs-badges">{"".join(spans)}</div>'


# ---- steps / pipeline indicator -------------------------------------------


@dataclass
class Step:
    """One pipeline step.

    Args:
        title: Step label, e.g. ``"Bin"``.
        status: ``done`` | ``active`` | ``pending``.
    """

    title: str
    status: str = "pending"


def render_steps(items: list[Step | dict[str, Any]]) -> str:
    """Render a numbered horizontal step/pipeline indicator."""
    lis = []
    for item in items or []:
        data = asdict(item) if is_dataclass(item) else dict(item)
        if not data.get("title"):
            continue
        status = str(data.get("status") or "pending")
        if status not in ("done", "active", "pending"):
            status = "pending"
        lis.append(f'<li class="{status}">{escape(str(data["title"]))}</li>')
    return f'{_style()}<ol class="rs-steps">{"".join(lis)}</ol>'


# ---- callouts / admonitions -----------------------------------------------


@dataclass
class Callout:
    """A note/success/warning/error/info admonition.

    Args:
        content: Body text.
        kind: One of ``note``, ``info``, ``success``, ``warning``, ``error``.
        title: Optional bold heading.
        icon: Optional Material icon override.
    """

    content: str
    kind: str = "note"
    title: str | None = None
    icon: str | None = None


def render_callout(item: Callout | dict[str, Any]) -> str:
    """Render one callout/admonition block."""
    data = asdict(item) if is_dataclass(item) else dict(item)
    kind = str(data.get("kind") or "note")
    if kind not in CALLOUT_KINDS:
        kind = "note"
    icon = data.get("icon") or CALLOUT_KINDS[kind]
    title = data.get("title")
    content = data.get("content") or ""
    return (
        f'{_style()}<div class="rs-callout rs-callout--{kind}">'
        f'<span class="material-icons" aria-hidden="true">{escape(str(icon))}</span>'
        f"<div>"
        + (f'<p class="rs-callout__title">{escape(str(title))}</p>' if title else "")
        + f"<p>{escape(str(content))}</p></div></div>"
    )


# ---- meter (flood-stage / threshold bands) --------------------------------


@dataclass
class Meter:
    """Native ``<meter>`` with low/high/optimum zones (green/yellow/red coloring).

    Args:
        label: Left label text.
        value: Current value.
        minimum: Scale minimum (default 0).
        maximum: Scale maximum (default 1; match the ``value`` scale).
        low: Below-low = "less good" bound.
        high: Above-high bound; pair with ``optimum``.
        optimum: e.g. 0.9 → values above ``high`` render green.
        unit: Optional unit suffix appended to the value label.
    """

    label: str
    value: float
    minimum: float = 0
    maximum: float = 1
    low: float | None = None
    high: float | None = None
    optimum: float | None = None
    unit: str | None = None


def render_meter(item: Meter | dict[str, Any]) -> str:
    """Render one labeled meter with threshold zones."""
    data = asdict(item) if is_dataclass(item) else dict(item)
    label = data.get("label") or ""
    unit = data.get("unit")
    value = data.get("value")
    attrs = f'value="{value}" min="{data.get("minimum", 0)}" max="{data.get("maximum", 1)}"'
    for key in ("low", "high", "optimum"):
        if data.get(key) is not None:
            attrs += f' {key}="{data[key]}"'
    value_text = escape(str(value))
    if unit:
        value_text += f" <small>{escape(str(unit))}</small>"
    return (
        f'{_style()}<div class="rs-meter"><div class="rs-meter__meta">'
        f'<span class="rs-meter__label">{escape(str(label))}</span>'
        f'<span class="rs-meter__value">{value_text}</span></div>'
        f"<meter {attrs}>{value_text}</meter></div>"
    )


# ---- gauge / dial ----------------------------------------------------------


def render_gauge(
    label: str,
    pct: float | None = None,
    value: float | None = None,
    minimum: float = 0,
    maximum: float = 100,
    color: str | None = None,
    value_text: str | None = None,
) -> str:
    """Render a semicircle dial as inline SVG — no JS, prints cleanly.

    Args:
        label: Caption below the dial, e.g. ``"Sinuosity percentile"``.
        pct: Position on the dial, 0–100. Takes precedence over ``value``.
        value: Raw value mapped onto ``minimum``..``maximum``.
        minimum: Scale min (default 0).
        maximum: Scale max (default 100).
        color: Named theme or CSS color for the arc + needle.
        value_text: Big text over the pivot; defaults to the rounded percent.
    """
    import math

    if pct is None and value is not None and maximum != minimum:
        pct = 100.0 * (float(value) - minimum) / (maximum - minimum)
    pct = max(0.0, min(100.0, float(pct or 0)))
    shown = value_text or (f"{round(pct, 1):g}%" if pct else "0%")

    # Semicircle donut gauge. A single semicircle path with pathLength="100"
    # makes pct → stroke length trivial and guarantees the fill and track share
    # exactly the same geometry (no separate arc-endpoint math to drift apart).
    # The semicircle spans left (20,96) → right (180,96), centered on (100,96).
    semicircle = "M 20 96 A 80 80 0 0 1 180 96"

    # Needle endpoint on the arc: 0% → angle π (left), 100% → angle 0 (right).
    # SVG y points down, so a point at angle θ above the baseline is
    # (cx + r·cosθ, cy − r·sinθ).
    cx, cy, r = 100, 96, 80
    angle = math.radians(180 - pct * 1.8)
    nx = cx + (r - 16) * math.cos(angle)
    ny = cy - (r - 16) * math.sin(angle)

    cls, style = _themed(color, "gauge")
    # The value text lives inside the SVG viewBox (centered above the pivot),
    # so it scales with the dial and needs no overlay positioning. The fill is
    # the same path as the track with a stroke-dasharray that reveals `pct`%
    # of it — this is what keeps fill and track perfectly concentric.
    return (
        f'{_style()}<div{_attrs(f"rs-gauge {cls or ""}".strip(), style)}>'
        f'<svg viewBox="0 0 200 110" class="rs-gauge__svg" role="img" aria-label="{escape(f"{label}: {shown}")}">'
        f'<path d="{semicircle}" pathLength="100" fill="none" class="rs-gauge__track"/>'
        f'<path d="{semicircle}" pathLength="100" fill="none" class="rs-gauge__fill"'
        f' stroke-dasharray="{round(pct, 2):g} 100"/>'
        f'<line x1="{cx}" y1="{cy}" x2="{nx:.2f}" y2="{ny:.2f}" class="rs-gauge__needle"/>'
        f'<circle cx="{cx}" cy="{cy}" r="5" class="rs-gauge__pivot"/>'
        f'<text x="{cx}" y="62" text-anchor="middle" class="rs-gauge__value">{escape(shown)}</text></svg>'
        f'<div class="rs-gauge__label">{escape(str(label))}</div></div>'
    )


# ---- glossary / definitions ------------------------------------------------


@dataclass
class Term:
    """One glossary entry.

    Args:
        term: Left-hand term, e.g. ``"DGO"``.
        definition: Right-hand definition.
    """

    term: str
    definition: str


def render_glossary(items: list[Term | dict[str, Any]]) -> str:
    """Render a definition list in an auto-fitting grid."""
    rows = []
    for item in items or []:
        data = asdict(item) if is_dataclass(item) else dict(item)
        if not data.get("term"):
            continue
        rows.append(
            f'<div class="rs-glossary__item"><dt>{escape(str(data["term"]))}</dt>'
            f'<dd>{escape(str(data.get("definition") or ""))}</dd></div>'
        )
    return f'{_style()}<dl class="rs-glossary">{"".join(rows)}</dl>'


# ---- citation chips / DOI cards --------------------------------------------


@dataclass
class Citation:
    """One attribution card.

    Args:
        title: Source name, e.g. ``"National Hydrography Dataset"``.
        url: Optional link (DOI, dataset page).
        url_label: Optional link text; defaults to ``url`` minus the protocol.
        icon: Material icon name (default ``source``).
    """

    title: str
    url: str | None = None
    url_label: str | None = None
    icon: str | None = None


def render_citations(items: list[Citation | dict[str, Any]]) -> str:
    """Render a row of citation/DOI cards."""
    cards = []
    for item in items or []:
        data = asdict(item) if is_dataclass(item) else dict(item)
        if not data.get("title"):
            continue
        url = data.get("url")
        icon = data.get("icon") or "source"
        url_html = ""
        if url:
            label = data.get("url_label") or str(url).removeprefix("https://").removeprefix("http://")
            url_html = f'<a href="{escape(str(url), quote=True)}" target="_blank" rel="noopener">{escape(str(label))}</a>'
        cards.append(
            f'<div class="rs-citation"><span class="material-icons" aria-hidden="true">{escape(str(icon))}</span>'
            f'<div><div class="rs-citation__title">{escape(str(data["title"]))}</div>{url_html}</div></div>'
        )
    return f'{_style()}<div class="rs-citations">{"".join(cards)}</div>'


# ---- flag banner (DRAFT / VERIFIED / ESTIMATED) ------------------------------

FLAG_KINDS = {"draft", "verified", "estimated", "warning"}


def render_flag_banner(text: str, kind: str = "draft") -> str:
    """Render a small ribbon banner, e.g. ``DRAFT — not for distribution``."""
    k = str(kind or "draft")
    if k not in FLAG_KINDS:
        k = "draft"
    return f'{_style()}<span class="rs-flag rs-flag--{k}">{escape(str(text))}</span>'


# ---- key-figure box ---------------------------------------------------------


def render_key_figure(content: str, label: str = "Bottom line") -> str:
    """Render the full-width "bottom line" callout."""
    return (
        f'{_style()}<div class="rs-key-figure"><span class="rs-key-figure__label">{escape(str(label))}</span>'
        f'<p class="rs-key-figure__text">{escape(str(content))}</p></div>'
    )


# ---- copyable citation block -------------------------------------------------

_CITE_COPY_JS = """
var el=document.createElement('textarea');
"""
# A minimal inline handler: writes the block's text to the clipboard with a
# "Copied" flash. Falls back to an "Open to select" hint if the API is blocked.
_CITE_ONCLICK = (
    "var t=this.closest('.rs-cite-block').querySelector('.rs-cite-block__text').innerText;"
    "this.disabled=true;"
    "var done=function(b){b.textContent='Copied';setTimeout(function(){b.textContent=b.dataset.label;b.disabled=false;},1400);};"
    "if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(t).then(done.bind(null,this))}"
    "else{this.textContent='Select text';}"
)


def render_citation_block(text: str, button_label: str = "Copy") -> str:
    """Render a citable-text block with a hover-copy button.

    Mirrors the copy-button UX from ``render_table()``; the button is a real
    one-liner inline handler (no external script needed).
    """
    return (
        f'{_style()}<div class="rs-cite-block"><p class="rs-cite-block__text">{escape(str(text))}</p>'
        f'<button type="button" class="rs-cite-block__copy pdf-hide" data-label="{escape(str(button_label))}"'
        f' onclick="{_CITE_ONCLICK}">{escape(str(button_label))}</button></div>'
    )
