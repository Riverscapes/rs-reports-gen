#!/usr/bin/env python3
"""
Overview
--------
- `df_to_basic_html` wraps `pandas.DataFrame.to_html`, exposing commonly useful
  parameters while keeping defaults aligned with a clean, unstyled table.
- Three example outputs:
    1) basic.html                -> minimal usage
    2) intermediate_params.html  -> a few practical parameters
    3) advanced_params.html      -> many parameters showcased

Function Parameters (mapped to pandas.to_html unless noted)
-----------------------------------------------------------
df : pandas.DataFrame
    The DataFrame to render.

table_class : str | list[str], default "rs-reports-data"
    One or more CSS classes to add to <table>.

index : bool, default False
    Include the DataFrame index as the first column.

float_format : callable | None, default None
    Function applied to *all* floats not covered by `formatters`. Example:
    `float_format=lambda x: f"{x:,.2f}"`

formatters : dict[str, callable] | None, default None
    Per-column formatters; overrides `float_format` for specified columns.
    Example: `{"Percent": lambda x: f"{x:.1f}%"}`

columns : Sequence | None, default None
    Subset/reorder columns to include.

classes : Sequence[str] | None
    Extra classes to add (in addition to "dataframe").

table_id : str | None
    Adds id="..." to <table>.
"""

from __future__ import annotations
import pandas as pd


def df_to_basic_html(
    df: pd.DataFrame,
    *,
    table_class: str | list[str] = "rs-reports-data",
    index: bool = False,
    float_format=None,
    formatters: dict | None = None,
    columns: list | None = None,
    # below map straight to to_html:
    classes: list[str] | None = None,
    table_id: str | None = None,
) -> str:
    """
    Render a DataFrame to minimal/unstyled HTML, exposing many useful to_html parameters.
    See module docstring for detailed parameter explanations.
    """
    # Normalize classes: merge `table_class` and `classes` into one list
    merged_classes: list[str] = []
    if isinstance(table_class, str) and table_class:
        merged_classes.append(table_class)
    elif isinstance(table_class, (list, tuple)):
        merged_classes.extend([c for c in table_class if c])

    if classes:
        merged_classes.extend([c for c in classes if c])

    html = df.to_html(
        columns=columns,
        index=index,
        formatters=formatters,
        float_format=float_format,
        sparsify=None,              # let pandas decide default behavior
        border=0,
        classes=merged_classes if merged_classes else None,
        table_id=table_id,
    )

    return html


def write_html_table(df: pd.DataFrame, path: str, **html_kwargs) -> None:
    """
    Render HTML from a DataFrame via df_to_basic_html and write it to disk.

    Parameters
    ----------
    df : pandas.DataFrame
    path : str
        Destination .html path.
    **html_kwargs :
        Forwarded to df_to_basic_html (e.g., index=False, table_id='x', etc.)
    """
    html_table = df_to_basic_html(df, **html_kwargs)
    encoding = html_kwargs.get("encoding", "utf-8") or "utf-8"
    with open(path, "w", encoding=encoding) as f:
        f.write(html_table)
    print(f"Wrote {path}")


def _example_basic():
    """Example 1: minimal/basic usage (clean defaults)."""
    data = {
        "Name": ["Alice", "Bob", "Charlie", "David"],
        "Age": [25, 30, 35, 40],
        "City": ["New York", "Los Angeles", "Chicago", "Houston"],
    }
    df = pd.DataFrame(data)
    write_html_table(
        df,
        "basic.html",
        table_class="rs-reports-data",
        index=False,
    )


def _example_intermediate():
    """Example 2: a few useful parameters—subset columns, per-column formatting, table_id."""
    df = pd.DataFrame(
        {
            "River": ["Willow Creek", "Silver Fork", "Bear River", "Maple Run", "Dry Gulch"],
            "Length_km": [12.4, 25.7, 42.3, 9.8, 3.6],
            "Ownership": ["Public", "Private", "Public", "Unmarked", "Unmarked"]
        }
    )

    formatters = {
        "Length_km": lambda x: f"{x:,.2f}",
    }

    write_html_table(
        df,
        "intermediate_params.html",
        columns=["River", "Ownership", "Length_km"],
        formatters=formatters,
        table_id="rivers-table",   # add id attribute to the table
        index=False,
    )


def _example_advanced():
    """Example 3: many parameters—index on, strip class, etc."""
    df = pd.DataFrame(
        {
            "Station": ["A", "B", "C", "D", "E"],
            "Flow": [4_200.5, 7_900.25, 15_600.0, 2_100.0, 700.75],
            "Pct": [0.18, 0.24, 0.09, 0.31, 0.45],
            "Notes": ["ok", "check", None, "ok", "ok"],
        }
    ).set_index("Station")  # show off index rendering

    formatters = {
        "Pct": lambda x: f"{x:.1%}",   # convert 0.18 -> 18.0%
    }

    write_html_table(
        df,
        "advanced_params.html",
        index=True,
        formatters=formatters,
        float_format=lambda x: f"{x:,.1f}",  # fallback for other floats
        classes=["zebra"],           # additional classes if you want multiple
        table_class="rs-reports-data",
    )


def main() -> None:
    _example_basic()
    _example_intermediate()
    _example_advanced()


if __name__ == "__main__":
    main()
