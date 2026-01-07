"""Demonstrate how `preferred_format` values on RSFieldMeta rows influence
metric output without having to run pytest.

Run this module directly (``python -m util.examples.metric_format_demo``) to see
sample metric card rows along with a few `format_scalar` variations.
"""
from __future__ import annotations

import pint

from util.figures import metric_cards
from util.pandas import RSFieldMeta

ureg = pint.UnitRegistry()

_DEMO_FIELDS = [
    {
        "name": "wetted_length",
        "friendly_name": "Wetted Valley Length",
        "data_unit": "kilometer",
        "preferred_format": "{:,.1f}",
        "description": "Named placeholder with thousands separator",
    },
    {
        "name": "bankfull_width",
        "friendly_name": "Mean Bankfull Width",
        "data_unit": "meter",
        "preferred_format": None,
        "description": "No preferred format so decimals parameter takes over",
    },
    {
        "name": "riparian_cover",
        "friendly_name": "Riparian Vegetation Cover",
        "data_unit": "percent",
        "preferred_format": "{value:.1f}",
        "description": "Format magnitude before Pint appends the % unit",
    },
    {
        "name": "project_count",
        "friendly_name": "Restoration Projects",
        "data_unit": None,
        "preferred_format": "{value:.0f} projects",
        "description": "Inject static text directly through the format string",
    },
]


def _snapshot(meta: RSFieldMeta):
    """Return a deep copy of the current metadata for safe restoration."""
    frame = meta.field_meta
    return frame.copy(deep=True) if frame is not None else None


def _restore(meta: RSFieldMeta, snapshot):
    meta._field_meta = snapshot  # noqa: SLF001 - lab context utility script


def _seed_demo_metadata(meta: RSFieldMeta) -> None:
    for field in _DEMO_FIELDS:
        meta.add_field_meta(**field)


def _build_demo_metrics() -> dict[str, pint.Quantity | int]:
    return {
        "wetted_length": 248.745 * ureg.kilometer,
        "bankfull_width": 28.64 * ureg.meter,
        "riparian_cover": 87.3 * ureg.percent,
        "project_count": 19,
    }


def _print_metric_cards(cards: dict[str, dict[str, str]]) -> None:
    print("Metric cards with preferred_format guidance:\n")
    for card in cards.values():
        details = f" — {card['details']}" if card.get("details") else ""
        print(f"- {card['title']:<30} -> {card['value']}{details}")


def _show_scalar_controls(meta: RSFieldMeta, metrics: dict[str, pint.Quantity | int]) -> None:
    sample = metrics["bankfull_width"]
    print("\nformat_scalar fallback and overrides for bankfull_width:\n")
    print(f"default decimals (0): {meta.format_scalar('bankfull_width', sample)}")
    print(f"decimals=1:          {meta.format_scalar('bankfull_width', sample, decimals=1)}")
    no_units = meta.format_scalar("bankfull_width", sample, decimals=1, include_units=False)
    print(f"decimals=1, no units: {no_units}")


def demonstrate_metric_formatting() -> None:
    meta = RSFieldMeta()
    snapshot = _snapshot(meta)
    try:
        meta.clear()
        _seed_demo_metadata(meta)
        metrics = _build_demo_metrics()
        cards = metric_cards(metrics)
        _print_metric_cards(cards)
        _show_scalar_controls(meta, metrics)
    finally:
        _restore(meta, snapshot)


if __name__ == "__main__":
    demonstrate_metric_formatting()
