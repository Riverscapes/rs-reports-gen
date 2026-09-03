"""Canonical DataFrame → styled HTML table renderer.

Replaces the scattered ad-hoc ``df.to_html()`` calls so that every table in
every report renders identically through the shared ``render_data_table``
Jinja macro (``templates/macros.html``) and the shared ``table.css``.

Why not ``df.to_html()``? Pandas emits generic markup that each report then
re-styles independently. :func:`render_table` instead prepares a small,
well-defined model — friendly headers (with units), per-column alignment
classes, caption, footer rows — and hands it to the macro. The macro owns the
markup, so a change to report styling lands in one place.

Key features
    * Caption support — a ``<caption>`` rendered above the table.
    * Numeric alignment — numeric columns are right-aligned (both headers and
      cells, via the ``numeric`` class); text/bool/datetime stay left.
    * Footer support — a footer DataFrame renders in ``<tfoot>``: visibly a
      totals block, and repeated on every PDF page thanks to the base.css
      print rules.
    * Copy button — a floating button, visible only on hover, that copies the
      table as CSV to the clipboard (plus a styled HTML variant for rich
      pastes).
    * Metadata-aware — when ``RSFieldMeta`` has units/friendly names
      registered, values are converted and formatted exactly like
      ``RSGeoDataFrame.to_html``.
"""

from collections.abc import Callable
from html import escape as _escape_html
from pathlib import Path
from typing import Any

import jinja2
import pandas as pd
import pint  # noqa: F401  # pylint: disable=unused-import
from rsxml import Logger

from util.pandas.RSFieldMeta import RSFieldMeta

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_LOG = Logger("util.html.table")

# The shared Jinja environment used to call the render_data_table macro.
# Built lazily and cached: identical markup everywhere, zero per-call overhead.
_ENV: jinja2.Environment | None = None


def _get_env() -> jinja2.Environment:
    global _ENV
    if _ENV is None:
        _ENV = jinja2.Environment(loader=jinja2.FileSystemLoader(str(_TEMPLATES_DIR)), autoescape=False)
    return _ENV


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def render_table(
    df: pd.DataFrame,
    caption: str | None = None,
    footer: pd.DataFrame | list[list[str]] | None = None,
    *,
    include_units: bool = True,
    use_friendly: bool = True,
    include_columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    layer_id: str | None = None,
    table_id: str | None = None,
    escape: bool = True,
    copy_button: bool = True,
    na_rep: str = "-",
    empty_message: str | None = None,
) -> str:
    """Render a DataFrame as a branded, styled HTML table fragment.

    The returned fragment is self-contained (its own ``<style>`` via the
    macro) and safe to inject with ``{{ tables['name'] | safe }}`` in a report
    body template.

    Args:
        df: The DataFrame to render. ``RSGeoDataFrame`` works too. The index
            is not rendered (mirrors ``to_html(index=False)``).
        caption: Optional caption text, displayed above the table.
        footer: Optional footer rows. Pass a DataFrame with the same columns
            (missing columns are filled with ``NA``, matching
            ``RSGeoDataFrame.set_footer`` semantics) for values formatted with
            the exact same rules as the body; or a list of pre-formatted
            row-lists of strings. Renders in ``<tfoot>``.
        include_units: Convert values to display units (SI/imperial) from
            field metadata and append unit text to headers. Requires metadata
            registered in ``RSFieldMeta``; if none is registered the frame is
            rendered units-less instead of crashing.
        use_friendly: Use metadata friendly names for headers. Without
            metadata, column names are title-ized; pass ``False`` to keep the
            raw column names verbatim.
        include_columns: If given, render only these columns (in this order).
        exclude_columns: Drop these columns from the output.
        layer_id: Optional metadata layer/disambiguation context.
        table_id: Optional id applied to the table's wrapper div.
        escape: Escape cell text, headers, and caption (default True). Set to
            False if you pass pre-rendered HTML strings.
        copy_button: Show the hover-revealed "copy as CSV" button.
        na_rep: Replacement for missing values (default ``"-"``).
        empty_message: Text shown when the DataFrame is empty. Defaults to
            ``"No data available."``.

    Returns:
        str: An HTML fragment: ``<style>…</style><div class="rs-table-wrap">…``
    """
    if df is None or len(df) == 0:
        return _render_empty(empty_message, escape)

    columns_meta, rows, footer_rows = prepare_table_data(
        df,
        footer=footer,
        include_units=include_units,
        use_friendly=use_friendly,
        include_columns=include_columns,
        exclude_columns=exclude_columns,
        layer_id=layer_id,
        escape=escape,
        na_rep=na_rep,
    )

    return _render_data_table_macro()(
        columns=columns_meta,
        rows=rows,
        caption=_escape_html(caption, quote=False) if (caption and escape) else caption,
        footer=footer_rows,
        table_id=_escape_html(table_id, quote=True) if table_id else None,
        copy_button=copy_button,
    )


def prepare_table_data(
    df: pd.DataFrame,
    *,
    footer: pd.DataFrame | list[list[str]] | None = None,
    include_units: bool = True,
    use_friendly: bool = True,
    include_columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    layer_id: str | None = None,
    escape: bool = True,
    na_rep: str = "-",
) -> tuple[list[dict[str, str]], list[list[str]], list[list[str]] | None]:
    """Build the model consumed by the ``render_data_table`` Jinja macro.

    Useful when a body template wants to call the macro directly:

        columns, rows, footer_rows = prepare_table_data(df, footer=footer_df)
        # body.html:
        #   {% from 'macros.html' import render_data_table %}
        #   {{ render_data_table(columns, rows, caption='…', footer=footer_rows) }}

    Args:
        df: See :func:`render_table`.
        footer: See :func:`render_table`.
        include_units: See :func:`render_table`.
        use_friendly: See :func:`render_table`.
        include_columns: See :func:`render_table`.
        exclude_columns: See :func:`render_table`.
        layer_id: See :func:`render_table`.
        escape: When True, cell text and headers are HTML-escaped so the macro
            can output them verbatim.
        na_rep: Replacement for missing values.

    Returns:
        tuple: ``(columns, rows, footer_rows)`` where ``columns`` is a list of
        ``{'label': str, 'classes': str}`` dicts, ``rows`` is a list of
        row cell-string lists, and ``footer_rows`` is the same shape or None.
    """
    meta = RSFieldMeta()

    display_df = df.copy()

    # Column selection: include first (and order), then exclude.
    if include_columns is not None:
        existing = [col for col in include_columns if col in display_df.columns]
        display_df = display_df.loc[:, existing]
    if exclude_columns is not None:
        display_df = display_df.drop(columns=exclude_columns, errors="ignore")

    # Units + friendly headers (identical machinery to RSGeoDataFrame.to_html).
    applied_units: dict[str, Any] = {}
    if include_units:
        try:
            display_df, applied_units = meta.apply_units(display_df, layer_id=layer_id)
        except RuntimeError:
            _LOG.warning("No field metadata registered; rendering table without units.")
            applied_units = {}

    headers = meta.get_headers(display_df, include_units=include_units, layer_id=layer_id) if use_friendly else list(display_df.columns)

    # Merge footer rows into the frame *before* formatting so they go through
    # the exact same converters/formatting as the body (mirrors
    # RSGeoDataFrame.to_html). Pre-formatted row-lists skip this step.
    footer_df = _align_footer(footer, df_columns=list(display_df.columns), dtypes=display_df.dtypes)
    preformatted_footer: list[list[str]] | None = None
    if footer_df is None and isinstance(footer, (list, tuple)):
        preformatted_footer = [[str(c) for c in row] for row in footer]

    if footer_df is not None and not footer_df.empty:
        display_all = pd.concat([display_df, footer_df], ignore_index=True)
        footer_start = len(display_df)
    else:
        display_all = display_df
        footer_start = len(display_df)  # sentinel: no footer rows

    # ------------------------------------------------------------------ #
    # Per-column classification + formatters (same classes/semantics as
    # RSGeoDataFrame.to_html so base.css styling matches exactly).
    # ------------------------------------------------------------------ #
    def _to_magnitude(val):
        return val.magnitude if hasattr(val, "magnitude") else val

    def _format_datetime(val):
        if pd.isna(val):
            return na_rep
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d %H:%M:%S")
        return str(val)

    def _format_boolean(val):
        if pd.isna(val):
            return na_rep
        if isinstance(val, str):
            lowered = val.strip().lower()
            if lowered in {"true", "t", "yes", "y", "1"}:
                return "Yes"
            if lowered in {"false", "f", "no", "n", "0"}:
                return "No"
            return val
        return "Yes" if bool(val) else "No"

    def _format_text(val):
        if pd.isna(val):
            return na_rep
        if isinstance(val, str):
            return val.strip()
        return str(val)

    column_classes: dict[str, str] = {}
    formatters: dict[str, Callable[[Any], str]] = {}

    for column in list(display_all.columns):
        class_tokens: list[str] = []

        if include_units:
            unit_obj = applied_units.get(column)
            if isinstance(unit_obj, pint.Unit):
                dim_name = RSFieldMeta.get_dimensionality_name(unit_obj)
                if dim_name:
                    class_tokens.append(dim_name)

        # Always convert to magnitudes for dtype checks (Pint dtypes hide the
        # real numeric kind).
        col_magnitude = display_all[column].apply(_to_magnitude)

        is_integer_type = pd.api.types.is_integer_dtype(col_magnitude)
        is_decimal_type = pd.api.types.is_float_dtype(col_magnitude)
        is_bool_type = pd.api.types.is_bool_dtype(col_magnitude)
        is_datetime_type = pd.api.types.is_datetime64_any_dtype(col_magnitude)
        is_all_nan = col_magnitude.isna().all()

        if is_all_nan:
            formatters[column] = lambda x: na_rep
        elif is_bool_type:
            class_tokens.append("boolean")
            formatters[column] = _format_boolean
        elif is_datetime_type:
            class_tokens.append("datetime")
            formatters[column] = _format_datetime
        elif is_integer_type or is_decimal_type:
            class_tokens.append("numeric")
            display_all[column] = col_magnitude  # drop Pint wrappers for formatting

            def _get_scalar_formatter(col_name: str, decimals: int) -> Callable[[Any], str]:
                return lambda x: meta.format_scalar(col_name, x, layer_id=layer_id, include_units=False, decimals=decimals)

            if is_integer_type:
                class_tokens.append("integer")
                formatters[column] = _get_scalar_formatter(column, 0)
            else:
                class_tokens.append("decimal")
                formatters[column] = _get_scalar_formatter(column, 2)
        else:
            class_tokens.append("text")
            formatters[column] = _format_text

        column_classes[column] = " ".join(dict.fromkeys(class_tokens)) if class_tokens else ""

    # Format every cell.
    formatted_rows: list[list[str]] = []
    for row_idx in range(len(display_all)):
        row_vals = display_all.iloc[row_idx]
        formatted_rows.append([formatters.get(col, _format_text)(row_vals[col]) for col in display_all.columns])

    body_rows = formatted_rows[:footer_start]
    footer_rows_out = formatted_rows[footer_start:] if footer_start < len(formatted_rows) else None
    if footer_rows_out is not None and len(footer_rows_out) == 0:
        footer_rows_out = None
    if footer_rows_out is None and preformatted_footer is not None:
        footer_rows_out = preformatted_footer

    # Escape now so the macro can emit cells verbatim (macro env has no autoescape).
    def _esc(value: str) -> str:
        return _escape_html(value, quote=False) if escape else value

    columns_meta = [{"label": _esc(label), "classes": (column_classes.get(col, "") or "")} for col, label in zip(display_all.columns, headers, strict=True)]
    rows_out = [[_esc(cell) for cell in row] for row in body_rows]
    if footer_rows_out is not None:
        footer_rows_out = [[_esc(cell) for cell in row] for row in footer_rows_out]

    return columns_meta, rows_out, footer_rows_out


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _align_footer(footer: Any, *, df_columns: list[str], dtypes: pd.Series) -> pd.DataFrame | None:
    """Normalize a footer DataFrame to the display frame's column order/dtypes.

    Mirrors ``RSGeoDataFrame`` footer semantics: missing columns are filled
    with ``NA``, extra columns dropped, and Pint columns are cast so the merged
    frame keeps its numeric dtype for classification/formatting.
    """
    if footer is None or isinstance(footer, (list, tuple)):
        return None
    footer_df = footer.copy()

    # Drop columns that are not in the display frame.
    footer_df = footer_df[[c for c in footer_df.columns if c in df_columns]]
    # Add missing columns as NA.
    for col in df_columns:
        if col not in footer_df.columns:
            footer_df[col] = pd.NA
    footer_df = footer_df[list(df_columns)]  # reorder to match

    # Cast Pint columns so concat keeps a homogeneous numeric dtype.
    for col in footer_df.columns:
        main_dtype = dtypes.get(col)
        if main_dtype is None:
            continue
        if "pint" in str(main_dtype):
            try:
                footer_df[col] = pd.to_numeric(footer_df[col]).astype(main_dtype)
            except Exception:  # noqa: BLE001 - leave as-is; classification falls back to text
                _LOG.warning(f"Could not cast footer column '{col}' to {main_dtype}; rendering as-is.")
    return footer_df


def _render_data_table_macro() -> Callable[..., str]:
    """Return the shared ``render_data_table`` macro as a plain callable."""
    return _get_env().get_template("macros.html").module.render_data_table


def _render_empty(empty_message: str | None, escape: bool) -> str:
    """Render the empty-table fallback (with the table stylesheet included)."""
    msg = _escape_html(empty_message) if empty_message else "No data available."
    css = _get_env().get_template("table.css").render()
    return f"<style>{css}</style>\n<div class=\"rs-table-wrap\"><p class=\"table-empty\">{msg}</p></div>"
