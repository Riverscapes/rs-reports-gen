"""Reusable summary-table helpers for report data preparation.

Created 2026-07-09.
Created by copilot.
"""

from __future__ import annotations

import pandas as pd
import pint_pandas

from util.binning import get_bins_info

DEFAULT_UNKNOWN_LABEL = "Unknown"
DEFAULT_OUT_OF_RANGE_LABEL = "Out of Range / Unknown"
DEFAULT_COUNT_FIELD = "segment_count"


def _validate_metric_series(series: pd.Series, metric_field: str) -> pd.Series:
    """Validate and normalize a metric series for sum/count aggregation.

    Expects metric values to already be numeric or pint-typed. This keeps
    responsibilities clear: callers perform parsing/cleanup upstream.
    """
    if isinstance(series.dtype, pint_pandas.PintType):
        return series[series.notna()]

    if not pd.api.types.is_numeric_dtype(series):
        raise TypeError(f"Expected numeric or pint dtype for metric '{metric_field}', got {series.dtype}")

    return series.dropna()


def summarize_metric_by_group(
    df: pd.DataFrame,
    group_field: str,
    metric_field: str,
    *,
    count_field: str = DEFAULT_COUNT_FIELD,
    unknown_label: str = DEFAULT_UNKNOWN_LABEL,
    layer_id: str | None = None,
) -> pd.DataFrame:
    """Summarize a metric by a categorical group field.

    Ordering semantics:
    - ordered categorical input -> preserve category order
    - otherwise -> stable alphabetical order
    """
    if group_field not in df.columns or metric_field not in df.columns:
        return pd.DataFrame(columns=["group", metric_field, count_field])

    summary_df = df[[group_field, metric_field]].copy()
    group_series = summary_df[group_field]

    if pd.api.types.is_categorical_dtype(group_series):
        categories = [str(c) for c in group_series.cat.categories]
        summary_df["group"] = group_series.astype(object).where(group_series.notna(), unknown_label).astype(str)
        group_order = categories + ([unknown_label] if unknown_label not in categories else [])
    else:
        summary_df["group"] = group_series.fillna(unknown_label).astype(str)
        group_order = sorted(summary_df["group"].unique().tolist())

    summary_df[metric_field] = _validate_metric_series(summary_df[metric_field], metric_field)
    summary_df = summary_df.dropna(subset=[metric_field])

    result = summary_df.groupby("group", as_index=False, observed=False).agg(
        **{
            metric_field: (metric_field, "sum"),
            count_field: (metric_field, "size"),
        }
    )

    result["group"] = pd.Categorical(result["group"], categories=group_order, ordered=True)
    result = result.sort_values("group").reset_index(drop=True)
    result["group"] = result["group"].astype(str)

    if layer_id:
        result.attrs["layer_id"] = layer_id
    result.attrs["group_field"] = group_field
    result.attrs["total_field"] = metric_field
    result.attrs["group_order"] = group_order
    return result


def summarize_metric_by_binned_numeric(
    df: pd.DataFrame,
    value_field: str,
    bin_lookup: str,
    metric_field: str,
    *,
    count_field: str = DEFAULT_COUNT_FIELD,
    out_of_range_label: str = DEFAULT_OUT_OF_RANGE_LABEL,
    layer_id: str | None = None,
) -> pd.DataFrame:
    """Summarize a metric by bins.json-defined bins for a numeric value field.

    Ordering semantics follow bins.json label order, with out-of-range bucket last.
    """
    if value_field not in df.columns or metric_field not in df.columns:
        return pd.DataFrame(columns=["group", metric_field, count_field])

    summary_df = df[[value_field, metric_field]].copy()

    value_series = summary_df[value_field]
    if isinstance(value_series.dtype, pint_pandas.PintType):
        value_series = value_series.pint.magnitude

    if not pd.api.types.is_numeric_dtype(value_series):
        raise TypeError(f"Expected numeric or pint dtype for value field '{value_field}', got {summary_df[value_field].dtype}")

    summary_df[value_field] = value_series
    summary_df[metric_field] = _validate_metric_series(summary_df[metric_field], metric_field)
    summary_df = summary_df.dropna(subset=[value_field, metric_field])

    edges, labels, _colours = get_bins_info(bin_lookup)
    bins = pd.cut(summary_df[value_field], bins=edges, labels=labels, include_lowest=True)
    summary_df["group"] = bins.astype(object).where(bins.notna(), out_of_range_label).astype(str)

    result = summary_df.groupby("group", as_index=False, observed=False).agg(
        **{
            metric_field: (metric_field, "sum"),
            count_field: (metric_field, "size"),
        }
    )

    group_order = list(labels)
    if out_of_range_label in result["group"].values and out_of_range_label not in group_order:
        group_order.append(out_of_range_label)

    result["group"] = pd.Categorical(result["group"], categories=group_order, ordered=True)
    result = result.sort_values("group").reset_index(drop=True)
    result["group"] = result["group"].astype(str)

    if layer_id:
        result.attrs["layer_id"] = layer_id
    result.attrs["group_field"] = value_field
    result.attrs["total_field"] = metric_field
    result.attrs["group_order"] = group_order
    return result
