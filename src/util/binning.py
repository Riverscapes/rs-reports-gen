"""Utilities for loading shared bin definitions.

Created 2026-07-09.
Created by copilot.
"""

from __future__ import annotations

import json
from pathlib import Path


def get_bins_info(key: str) -> tuple[list[float], list[str], list[str]]:
    """Load bin edges, labels, and colours from util/bins.json.

    Args:
        key: bins.json key to load.

    Returns:
        Tuple of (edges, labels, colours).
    """
    bins_path = Path(__file__).with_name("bins.json")
    with bins_path.open(encoding="utf-8") as f:
        bins_dict = json.load(f)

    info = bins_dict[key]
    edges = info["edges"]
    legend = info["legend"]
    labels = [item[1] for item in legend]
    colours = [item[0] for item in legend]
    return edges, labels, colours
