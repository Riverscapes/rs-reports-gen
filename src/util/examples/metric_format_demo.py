"""Demonstrate and document `preferred_format` options for RSFieldMeta.

Run this module directly (``python -m util.examples.metric_format_demo``) to generate:
1. A Markdown table of format string examples (Reference).
2. A demonstration of metric cards mechanism (Contextual usage).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pint

from util.figures import metric_cards
from util.pandas import RSFieldMeta

ureg = pint.UnitRegistry()


@dataclass
class FormatDemoCase:
    """A single format string scenarios for documentation."""

    fmt: str | None
    description: str
    examples: list[Any]


_FORMAT_CASES = [
    FormatDemoCase(
        fmt="{:,.1f}",
        description="Fixed-point with 1 decimal digit and thousands separator",
        examples=[
            248.745 * ureg.kilometer,
            1234567.89 * ureg.meter,
        ],
    ),
    FormatDemoCase(
        fmt="{:.2f}",
        description="Standard fixed-point, 2 decimals",
        examples=[
            3.14159 * ureg.meter,
            0.009 * ureg.kilometer,
            pint.Quantity(12.321, 'count/km**2'),
        ],
    ),
    FormatDemoCase(
        fmt="{:.3g}",
        description="General format (3 significant digits, drops trailing zeros)",
        examples=[
            12345 * ureg.meter,
            0.00012345 * ureg.meter,
        ],
    ),
    FormatDemoCase(
        fmt="{value:.0f} projects",
        description="Integer with injected static text suffix",
        examples=[
            19,
            42,
            pint.Quantity(1322, "count")
        ],
    ),
    FormatDemoCase(
        fmt="{value:.1f}",
        description="Explicit `value` placeholder (same as `{:.1f}`)",
        examples=[
            87.31 * ureg.percent,
            12.55 * ureg.degree,
        ],
    ),
    FormatDemoCase(
        fmt=None,
        description="No format (Uses `format_scalar` defaults, e.g. `decimals=0`)",
        examples=[
            28.64 * ureg.meter,
            100.123 * ureg.foot,
        ],
    ),
    FormatDemoCase(
        fmt="{:.3~#P}",
        description="Pint specific: Compact notation (auto-scaling units)",
        examples=[
            12000000 * ureg.meter ** 2,
            0.005 * ureg.kilometer,
        ],
    ),
    FormatDemoCase(
        fmt="{:.3~P}",
        description="Pint specific: Pretty notation w/o autoscale",
        examples=[
            12000000 * ureg.meter ** 2,
            0.005 * ureg.kilometer,
            # default simplification of units
            pint.Quantity(295.7, "km") / pint.Quantity(536.1, "km**2"),
            # forcing a compound unit
            (pint.Quantity(395.7, "km") / pint.Quantity(736.1, "km**2")).to("km/km**2"),
        ],
    ),
]


_METRIC_CARD_FIELDS = [
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


def _seed_example_case(meta: RSFieldMeta, fmt: str | None, idx: int) -> str:
    """Register a temporary field for the table generator."""
    name = f"demo_field_{idx}"
    meta.add_field_meta(
        name=name,
        friendly_name=f"Demo Field {idx}",
        preferred_format=fmt,
    )
    return name


def _print_markdown_table(meta: RSFieldMeta) -> None:
    print("\n## Format String Reference Table\n")
    headers = [
        "Format String",
        "Interpretation",
        "Example Quantity",
        "Output (No Units)",
        "Output (With Units)",
    ]
    print(f"| {' | '.join(headers)} |")
    print(f"| {' | '.join(['---'] * len(headers))} |")

    for idx, case in enumerate(_FORMAT_CASES):
        field_name = _seed_example_case(meta, case.fmt, idx)
        fmt_display = f"`{case.fmt}`" if case.fmt is not None else "*None*"

        for val in case.examples:
            # We use defaults for format_scalar to show base behavior
            out_no_units = meta.format_scalar(field_name, val, include_units=False)
            out_with_units = meta.format_scalar(field_name, val, include_units=True)

            # Format the quantity for display in the table
            val_display = f"`{val}`"

            row = [
                fmt_display,
                case.description,
                val_display,
                f"`{out_no_units}`",
                f"`{out_with_units}`",
            ]
            print(f"| {' | '.join(row)} |")


def _seed_demo_metadata(meta: RSFieldMeta) -> None:
    for field in _METRIC_CARD_FIELDS:
        meta.add_field_meta(**field)


def _build_demo_metrics() -> dict[str, pint.Quantity | int]:
    return {
        "wetted_length": 248.745 * ureg.kilometer,
        "bankfull_width": 28.64 * ureg.meter,
        "riparian_cover": 87.3 * ureg.percent,
        "project_count": 19,
    }


def _print_metric_cards(cards: dict[str, dict[str, str]]) -> None:
    print("\n## Metric Cards Demo\n")
    print("Demonstrates how `metric_cards` uses `preferred_format` from metadata:\n")
    for card in cards.values():
        details = f" — {card['details']}" if card.get("details") else ""
        print(f"- **{card['title']}**: {card['value']}{details}")


def _show_scalar_controls(meta: RSFieldMeta, metrics: dict[str, pint.Quantity | int]) -> None:
    sample = metrics["bankfull_width"]
    print("\n### Overriding Defaults (format_scalar)\n")
    print("When `preferred_format` is None, `format_scalar` arguments control output:\n")

    def _code_line(code, result):
        print(f"- `{code}` -> **{result}**")

    _code_line("format_scalar(..., decimals=0)", meta.format_scalar('bankfull_width', sample))
    _code_line("format_scalar(..., decimals=1)", meta.format_scalar('bankfull_width', sample, decimals=1))

    no_units = meta.format_scalar("bankfull_width", sample, decimals=1, include_units=False)
    _code_line("format_scalar(..., include_units=False)", no_units)


def demonstrate_all() -> None:
    meta = RSFieldMeta()
    snapshot = _snapshot(meta)
    try:
        meta.clear()

        # 1. Generate the Reference Table
        _print_markdown_table(meta)

        # 2. Run the Card Demo
        # Clear again to have clean state for the cards demo
        meta.clear()
        _seed_demo_metadata(meta)
        metrics = _build_demo_metrics()
        cards = metric_cards(metrics)
        _print_metric_cards(cards)
        _show_scalar_controls(meta, metrics)

    finally:
        _restore(meta, snapshot)


if __name__ == "__main__":
    demonstrate_all()
