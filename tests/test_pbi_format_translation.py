"""Tests for the Python → Power BI format-string translation.

Copilot-generated module.
"""

import pytest

from util.pbi_format import python_format_to_pbi


# ── Mapped formats ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "python_fmt, expected_pbi",
    [
        # Standard brace-wrapped formats
        ("{:,.0f}", "#,##0"),
        ("{:,.1f}", "#,##0.0"),
        ("{:,.2f}", "#,##0.00"),
        ("{:.0f}", "0"),
        ("{:.1f}", "0.0"),
        ("{:.2f}", "0.00"),
        ("{:.1%}", "0.0%"),
        ("{:.3g}", "0.###"),
        # {value:...} variant (used in metric_format_demo / metric cards)
        ("{value:.1f}", "0.0"),
        ("{value:.0f}", "0"),
        ("{value:,.0f}", "#,##0"),
        # Bare spec without braces (metadatabase convention)
        (",.0f", "#,##0"),
        (".2f", "0.00"),
        (".1%", "0.0%"),
    ],
)
def test_known_formats(python_fmt: str, expected_pbi: str):
    assert python_format_to_pbi(python_fmt) == expected_pbi


# ── Edge cases returning None ───────────────────────────────────────────


@pytest.mark.parametrize(
    "python_fmt",
    [
        "",
        None,
    ],
)
def test_empty_returns_none(python_fmt):
    assert python_format_to_pbi(python_fmt) is None


# ── Unmapped formats warn and return None ───────────────────────────────


def test_unmapped_brace_format_returns_none():
    """Pint-specific format specs are not mappable to PBI."""
    assert python_format_to_pbi("{:.3~#P}") is None


def test_unmapped_bare_spec_returns_none():
    assert python_format_to_pbi("unknown_spec") is None


def test_format_with_suffix_text_returns_none():
    """Formats with injected text (e.g. '{value:.0f} projects') don't match."""
    assert python_format_to_pbi("{value:.0f} projects") is None


# ── Whitespace tolerance ────────────────────────────────────────────────


def test_leading_trailing_whitespace():
    assert python_format_to_pbi("  {:,.0f}  ") == "#,##0"
