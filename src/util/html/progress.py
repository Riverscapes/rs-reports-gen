"""Colorized percentage-bar components built on Pico's ``<progress>`` element.

Pico CSS styles the pure-HTML ``<progress>`` element with no JavaScript
(``https://picocss.com/docs/progress``); these components hand it branded
colors and grouped layouts. Mirroring ``util/html/table.py``, the helpers here
normalize a small model (dataclass or plain dict — either works) and call the
shared Jinja macros (``templates/macros.html``), so all markup stays in one
place and every report renders identically.

Two components:

* :func:`render_progress_rows` — a stacked group of percentage bars; each row
  is value+unit on the left, the bar, and the percent on the right (or a
  ``right_label`` override when the caller supplies one). Groups
  stack with hairline dividers, so pass one group per call and inject several
  fragments in sequence.
* :func:`render_progress_card` — an "extended metric card": a title and total
  headline, with one bar per comparison group (e.g. BLM vs NON-BLM), laid out
  in an auto-fitting grid.

Colors may be a named theme (``blue``, ``blue-light``, ``indigo``, ``green``,
``orange``, ``red``, ``slate``, ``gray``, ``violet`` — see
``templates/progress.css``) or any CSS color string (``"#1f6feb"``,
``"tomato"``), which is emitted as an inline custom-property override.

Percentages are either given explicitly (``pct``) or computed from a
``numerator``/``denominator`` pair, so report code can hand over the numbers
it already has. Values are clamped to the 0–100 range.

Example:
    >>> from util.html.progress import ProgressGroup, ProgressCard
    >>> card = ProgressCard(
    ...     "Reservoirs",
    ...     total="12 count (180 ac)",
    ...     groups=[
    ...         ProgressGroup("BLM", "4 / 60 AC", numerator=60, denominator=180, color="indigo"),
    ...         ProgressGroup("NON-BLM", "8 / 120 AC", numerator=120, denominator=180, color="gray"),
    ...     ],
    ... )
    >>> html = render_progress_card(card)
"""

import jinja2

from dataclasses import asdict, dataclass, field, is_dataclass
from html import escape
from pathlib import Path
from typing import Any

_TEMPLATES_DIR = Path(__file__).parent / "templates"

# The shared Jinja environment used to call the progress macros (same caching
# pattern as util/html/table.py).
_ENV: jinja2.Environment | None = None

#: Named color themes (must match the ``progress-<name>`` classes in
#: ``templates/progress.css``). Anything not on this list is treated as a raw
#: CSS color and emitted as an inline custom-property override.
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


def _get_env() -> jinja2.Environment:
    global _ENV
    if _ENV is None:
        _ENV = jinja2.Environment(loader=jinja2.FileSystemLoader(str(_TEMPLATES_DIR)), autoescape=False)
    return _ENV


# ---------------------------------------------------------------------------
# Public model
# ---------------------------------------------------------------------------


@dataclass
class ProgressRow:
    """One labeled percentage bar: value+unit, bar, percent.

    Args:
        value: Pre-formatted display value, e.g. ``"555"`` or ``"4 / 60 AC"``.
        unit: Optional small unit suffix, e.g. ``"MI"``.
        pct: Explicit percentage, 0–100. Takes precedence over the
            ``numerator``/``denominator`` pair.
        numerator: Numerator used to compute ``pct`` when ``pct`` is omitted.
        denominator: Denominator; ``pct = numerator / denominator * 100``.
        color: Named theme (see :data:`COLOR_THEMES`) or a raw CSS color.
        details: Optional second line under the value (muted, e.g.
            ``"130 BLM ACRES"``).
        right_label: Optional right-hand label shown instead of the percent,
            e.g. ``"HIGH"`` or ``"< 1%"``. The bar still encodes the numeric
            percentage; only the displayed text changes.
    """

    value: str
    unit: str | None = None
    pct: float | None = None
    numerator: float | None = None
    denominator: float | None = None
    color: str | None = None
    details: str | None = None
    right_label: str | None = None


@dataclass
class ProgressGroup:
    """One comparison bar inside an extended metric card.

    Args:
        label: Group name, e.g. ``"BLM"``.
        value: Pre-formatted display value, e.g. ``"4 / 60 AC"``.
        pct: Explicit percentage, 0–100. Takes precedence over the
            ``numerator``/``denominator`` pair.
        numerator: Numerator used to compute ``pct`` when ``pct`` is omitted.
        denominator: Denominator; ``pct = numerator / denominator * 100``.
        color: Named theme (see :data:`COLOR_THEMES`) or a raw CSS color.
    """

    label: str
    value: str
    pct: float | None = None
    numerator: float | None = None
    denominator: float | None = None
    color: str | None = None


@dataclass
class ProgressCard:
    """Extended metric card: a headline total plus one bar per group.

    Args:
        title: Card title, e.g. ``"Reservoirs"``.
        groups: The comparison bars (usually a small list such as BLM vs
            NON-BLM). Empty groups are skipped.
        total: Optional headline total, e.g. ``"12 count (180 ac)"``.
    """

    title: str
    groups: list[ProgressGroup] = field(default_factory=list)
    total: str | None = None


# ---------------------------------------------------------------------------
# Normalization (dataclass or dict → the exact dict each macro consumes)
# ---------------------------------------------------------------------------


def _resolve_pct(entry: dict[str, Any]) -> float:
    """Resolve the bar percentage. Explicit ``pct`` wins; otherwise compute
    from ``numerator``/``denominator``; else 0. Result is clamped to 0–100."""
    pct = entry.get("pct")
    if pct is None:
        numerator = entry.get("numerator")
        denominator = entry.get("denominator")
        if numerator is not None and denominator:
            pct = 100 * float(numerator) / float(denominator)
    if pct is None:
        pct = 0.0
    return max(0.0, min(100.0, float(pct)))


def _pct_text(pct: float) -> str:
    """Display percentage: up to 1 decimal, integral values drop the decimal."""
    return f"{round(pct, 1):g}%"


def _pct_html(pct: float) -> str:
    """The row's percent display: number with a small ``%`` suffix."""
    text = _pct_text(pct)
    return f"{text[:-1]}<small>%</small>"


def _resolve_color(color: Any) -> tuple[str | None, str | None]:
    """Map ``color`` to either a theme class (``progress-blue``) or an inline
    custom-property override (``--progress-color: #hex``)."""
    if not color:
        return None, None
    color = str(color)
    if color in COLOR_THEMES:
        return f"progress-{color}", None
    # Raw CSS color: feed both the shared token and Pico's own variable so the
    # bar colors regardless of which one the stylesheet reads.
    safe = color.replace('"', "").replace(";", "")
    inline = f"--progress-color: {safe}; --pico-progress-color: {safe}"
    return None, inline


def _normalize_row(row: ProgressRow | ProgressGroup | dict[str, Any]) -> dict[str, Any]:
    data = asdict(row) if is_dataclass(row) else dict(row)
    pct = _resolve_pct(data)
    color_class, color_style = _resolve_color(data.get("color"))
    value = str(data.get("value") or "")
    unit = data.get("unit")
    label = data.get("label")
    details = data.get("details")
    right_label_raw = data.get("right_label")
    right_label = str(right_label_raw) if right_label_raw else None
    aria_text = value + (f" {unit}" if unit else "")
    return {
        # Rows use 'value'/'unit'; groups use 'label'/'value'. Either is fine.
        "label": escape(str(label)) if label else None,
        "value": escape(value),
        "unit": escape(str(unit)) if unit else None,
        "details": escape(str(details)) if details else None,
        "right_label": escape(right_label) if right_label else None,
        "pct": f"{round(pct, 4):g}",
        "pct_text": _pct_text(pct),
        "pct_html": _pct_html(pct),
        "color_class": color_class,
        "color_style": color_style,
        "aria": f"{aria_text}: {escape(right_label) if right_label else _pct_text(pct)}",
    }


def _normalize_progress_model(items: list[ProgressRow | ProgressGroup | dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalizer for a list of rows/groups; blanks (empty value *and* empty
    label) are dropped so optional groups vanish cleanly."""
    normalized = []
    for item in items or []:
        data = asdict(item) if is_dataclass(item) else dict(item)
        if not data.get("value") and not data.get("label"):
            continue
        normalized.append(_normalize_row(item))
    return normalized


def _normalize_card(card: ProgressCard | dict[str, Any]) -> dict[str, Any]:
    data = asdict(card) if is_dataclass(card) else dict(card)
    return {
        "title": escape(str(data.get("title") or "")),
        "total": escape(str(data["total"])) if data.get("total") else None,
        "groups": _normalize_progress_model(data.get("groups") or []),
    }


# ---------------------------------------------------------------------------
# Public renderers
# ---------------------------------------------------------------------------


def render_progress_rows(rows: list[ProgressRow | dict[str, Any]]) -> str:
    """Render one group of labeled percentage bars.

    The returned fragment is self-contained (its macro embeds
    ``progress.css``) and safe to inject with ``{{ ... | safe }}``.
    """
    macro = _get_env().get_template("macros.html").module.render_progress_rows
    return macro(_normalize_progress_model(rows or []))


def render_progress_card(card: ProgressCard | dict[str, Any]) -> str:
    """Render one extended metric card (title + total + one bar per group)."""
    macro = _get_env().get_template("macros.html").module.render_progress_card
    return macro(_normalize_card(card))


def render_progress_cards(cards: list[ProgressCard | dict[str, Any]]) -> str:
    """Render several extended metric cards back-to-back."""
    macro = _get_env().get_template("macros.html").module.render_progress_card
    return "".join(macro(_normalize_card(card)) for card in cards or [])
