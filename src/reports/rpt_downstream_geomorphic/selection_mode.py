"""Selection mode enum for downstream geomorphic report.

Created 2026-07-15.
Created by copilot.
"""

from enum import Enum


class SelectionMode(Enum):
    """Supported spatial selection modes for data queries and report labeling."""

    WHOLE = "whole"
    UP = "up"
    DOWN = "down"

    @property
    def label(self) -> str:
        """Human-readable label for report templates and logs."""
        labels = {
            SelectionMode.WHOLE: "Whole Level Path",
            SelectionMode.UP: "Upstream",
            SelectionMode.DOWN: "Downstream",
        }
        return labels[self]
