"""Translate Python preferred_format strings to Power BI Custom Format Strings.

This module provides a hand-curated lookup covering the Python format
strings actually used in the Riverscapes metadata system.  Unknown
formats log a warning so they surface during development rather than
silently producing wrong output.

Copilot-generated module.
"""

from __future__ import annotations

import re

from rsxml import Logger

log = Logger("PBI Format")

# ── Hand-curated mapping ────────────────────────────────────────────────
# Keys are the *spec* portion extracted from the Python format string
# (the part after the colon inside ``{:…}``).  The values are the
# corresponding Power BI Custom Format Strings.
#
# Maintenance: when a new ``preferred_format`` appears in the metadata
# registry, add an entry here and a test case in
# ``tests/test_pbi_format_translation.py``.

_SPEC_TO_PBI: dict[str, str] = {
    ",.0f": "#,##0",
    ",.1f": "#,##0.0",
    ",.2f": "#,##0.00",
    ".0f": "0",
    ".1f": "0.0",
    ".2f": "0.00",
    ".1%": "0.0%",
    ".3g": "0.###",
}

# Regex that matches the standard ``{:spec}`` or ``{value:spec}`` forms
# we actually use. Captures the *spec* group.
_FMT_RE = re.compile(r"^\{(?:value)?:([^}]+)\}$")


def python_format_to_pbi(preferred_format: str) -> str | None:
    """Convert a Python *preferred_format* string to a PBI Custom Format String.

    Args:
        preferred_format: The format string stored in RSFieldMeta
            (e.g. ``"{:,.0f}"``).  Empty / None values return ``None``.

    Returns:
        The Power BI format string, or ``None`` if the input is empty or
        could not be mapped (a warning is logged for unmapped non-empty
        formats).
    """
    if not preferred_format:
        return None

    fmt = preferred_format.strip()

    m = _FMT_RE.match(fmt)
    if m:
        spec = m.group(1)
        pbi = _SPEC_TO_PBI.get(spec)
        if pbi is not None:
            return pbi
        log.warning(f"Unmapped Python format spec '{spec}' (from '{fmt}') — no PBI equivalent defined")
        return None

    # Bare spec without braces (e.g. from Athena metadatabase: ",.0f")
    pbi = _SPEC_TO_PBI.get(fmt)
    if pbi is not None:
        return pbi

    log.warning(f"Unrecognised preferred_format '{fmt}' — no PBI equivalent defined")
    return None
