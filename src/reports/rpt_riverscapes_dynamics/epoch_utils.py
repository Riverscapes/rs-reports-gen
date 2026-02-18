"""Epoch metadata helpers for Riverscapes Dynamics.

Typestring: module
100% Copilot-authored.
"""

from __future__ import annotations

import pandas as pd


def prepare_epoch_metadata(df_metrics: pd.DataFrame) -> pd.DataFrame:
    """Add derived epoch metadata columns to a metrics dataframe.

    Typestring: function
    100% Copilot-authored.

    Args:
        df_metrics: Metrics dataframe with an ``epoch_name`` column in start_end format.

    Returns:
        A copy of the dataframe with ``epoch_start``, ``epoch_end``, and ``epoch_label`` columns
        plus an ``epoch_lookup`` table stored under ``df.attrs``.
    """
    if "epoch_name" not in df_metrics.columns:
        raise KeyError("Metrics dataframe must include an 'epoch_name' column before deriving metadata.")

    attrs = dict(getattr(df_metrics, "attrs", {}))
    epoch_df = df_metrics.copy()

    epoch_parts = epoch_df["epoch_name"].astype(str).str.split("_", n=1, expand=True)
    epoch_df["epoch_start"] = pd.to_numeric(epoch_parts[0], errors="coerce")
    epoch_df["epoch_end"] = pd.to_numeric(epoch_parts[1], errors="coerce")
    epoch_df = epoch_df[epoch_df["epoch_end"].notna()].copy()
    epoch_df["epoch_label"] = epoch_df["epoch_name"].astype(str).str.replace("_", "-", regex=False)

    epoch_lookup = _build_epoch_lookup(epoch_df)
    attrs["epoch_lookup"] = epoch_lookup
    epoch_df.attrs = attrs
    return epoch_df


def get_epoch_lookup(df: pd.DataFrame, *, prefer_stored: bool = True) -> pd.DataFrame:
    """Return a sorted lookup table describing the epochs present in ``df``.

    Typestring: function
    100% Copilot-authored.

    Args:
        df: Dataframe containing epoch metadata columns.
        prefer_stored: When ``True`` (default), use the cached lookup stored on ``df.attrs`` if available.

    Returns:
        Dataframe with the unique epochs plus ``epoch_i`` ordering.
    """
    if prefer_stored:
        lookup = getattr(df, "attrs", {}).get("epoch_lookup")
        if lookup is not None:
            return lookup

    required_cols = {"epoch_name", "epoch_start", "epoch_end", "epoch_label"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        missing = ", ".join(sorted(missing_cols))
        raise KeyError(f"Dataframe is missing required epoch columns: {missing}")

    return _build_epoch_lookup(df)


def _build_epoch_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """Construct and order the epoch lookup table from ``df``.

    Typestring: function
    100% Copilot-authored.
    """
    epoch_lut = (
        df[["epoch_name", "epoch_start", "epoch_end", "epoch_label"]]
        .drop_duplicates()
        .sort_values(["epoch_end", "epoch_start", "epoch_name"])
        .reset_index(drop=True)
    )
    epoch_lut["epoch_i"] = range(len(epoch_lut))
    return epoch_lut
