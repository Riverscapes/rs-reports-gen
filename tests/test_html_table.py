"""Tests for the canonical DataFrame → styled HTML table renderer (util/html/table.py)."""

import pandas as pd

from util.html.table import prepare_table_data, render_table

# ---------------------------------------------------------------------------
# Basic structure
# ---------------------------------------------------------------------------


def _df():
    return pd.DataFrame(
        {
            "Name": ["Private", "State"],
            "Value": [61.4, 22.8],
            "Count": [12, 4],
        }
    )


def test_render_table_emits_shared_markup():
    html = render_table(_df(), use_friendly=False, include_units=False)
    assert '<table class="dataframe rs-table">' in html
    assert "<thead>" in html
    assert "<tbody>" in html
    # Header row + 2 data rows, no footer.
    body = html.split("<tbody>")[1].split("</tbody>")[0]
    assert body.count("<tr>") == 2
    # No index column.
    assert "<th></th>" not in html


def test_render_table_has_no_caption_by_default():
    html = render_table(_df(), use_friendly=False, include_units=False)
    assert "<caption>" not in html


def test_caption_supported_and_escaped():
    html = render_table(_df(), caption="Summary <script>alert(1)</script>", use_friendly=False, include_units=False)
    assert "<caption>Summary &lt;script&gt;alert(1)&lt;/script&gt;</caption>" in html
    assert "<script>alert(1)" not in html


def test_table_id_applied_to_wrapper():
    html = render_table(_df(), table_id="my-table", use_friendly=False, include_units=False)
    assert 'id="my-table"' in html


# ---------------------------------------------------------------------------
# Numeric alignment
# ---------------------------------------------------------------------------


def test_numeric_columns_are_right_aligned():
    html = render_table(_df(), use_friendly=False, include_units=False)
    # Cells
    assert 'class="numeric decimal"' in html
    assert 'class="numeric integer"' in html
    # Headers above numeric columns align with their cells.
    assert '<th scope="col" class="numeric decimal">Value</th>' in html
    assert '<th scope="col" class="numeric integer">Count</th>' in html
    # Text columns stay left-aligned.
    assert '<th scope="col" class="text">Name</th>' in html


def test_numeric_values_are_formatted():
    html = render_table(_df(), use_friendly=False, include_units=False)
    # Decimal default 2 places; integer default 0 places.
    assert "61.40" in html
    assert "22.80" in html
    assert ">12<" in html


# ---------------------------------------------------------------------------
# Footer support
# ---------------------------------------------------------------------------


def test_footer_dataframe_renders_in_tfoot():
    df = pd.DataFrame({"Metric": ["A", "B"], "Count": [1, 2]})
    footer = pd.DataFrame({"Metric": ["Total"], "Count": [3]})
    html = render_table(df, footer=footer, use_friendly=False, include_units=False)
    # Keep the assertions on the table markup, not the inlined stylesheet.
    table_html = html.split('<table class="dataframe rs-table">')[1]
    assert "<tfoot>" in table_html
    footer_html = table_html.split("<tfoot>")[1].split("</tfoot>")[0]
    assert "Total" in footer_html
    assert ">3<" in footer_html
    # Footer rows carry both the footer class and column classes.
    assert 'class="footer numeric integer"' in footer_html
    # Body rows stay out of the tfoot.
    assert footer_html.count("<tr>") == 1
    assert table_html.split("<tbody>")[1].split("</tbody>")[0].count("<tr>") == 2


def test_footer_preformatted_rows_supported():
    df = pd.DataFrame({"A": ["x"], "B": [1]})
    html = render_table(df, footer=[["Total", "2"]], use_friendly=False, include_units=False)
    assert "<tfoot>" in html
    assert "Total" in html


def test_footer_missing_columns_filled():
    df = pd.DataFrame({"A": ["x"], "B": [1]})
    footer = pd.DataFrame({"A": ["Total"]})  # B is missing -> NA -> na_rep
    html = render_table(df, footer=footer, use_friendly=False, include_units=False)
    table_html = html.split('<table class="dataframe rs-table">')[1]
    footer_html = table_html.split("<tfoot>")[1].split("</tfoot>")[0]
    assert "Total" in footer_html
    assert "-" in footer_html


# ---------------------------------------------------------------------------
# Column selection / headers
# ---------------------------------------------------------------------------


def test_include_and_exclude_columns():
    html = render_table(_df(), include_columns=["Count", "Name"], use_friendly=False, include_units=False)
    assert "<th scope=\"col\" class=\"numeric integer\">Count</th>" in html
    assert "<th scope=\"col\" class=\"text\">Name</th>" in html
    assert "Value" not in html

    html = render_table(_df(), exclude_columns=["Value"], use_friendly=False, include_units=False)
    assert "Value" not in html


def test_metadata_friendly_headers_with_units():
    """RSGeoDataFrame-style metadata: friendly names + units in headers."""
    from util.pandas.RSFieldMeta import RSFieldMeta

    meta = RSFieldMeta()
    # RSFieldMeta is a Borg singleton; restore state so other tests are unaffected.
    saved_field_meta = meta._field_meta
    saved_unit_system = meta._unit_system
    try:
        meta.add_field_meta(name="segment_area", friendly_name="Riverscape Area", data_unit="meter**2", dtype="REAL", layer_id="test-layer")

        df = pd.DataFrame({"segment_area": [12.4, 31.7]})
        html = render_table(df, layer_id="test-layer")
        # Friendly name + unit suffix; values formatted (2 dp).
        assert "Riverscape Area (m²)" in html
        assert "12.40" in html
        assert 'class="area numeric decimal"' in html
    finally:
        meta._field_meta = saved_field_meta
        meta._unit_system = saved_unit_system


# ---------------------------------------------------------------------------
# Escape / copy button / empty
# ---------------------------------------------------------------------------


def test_text_cells_are_escaped():
    df = pd.DataFrame({"Note": ["<b>bold</b>", "a & b"]})
    html = render_table(df, use_friendly=False, include_units=False)
    assert "&lt;b&gt;bold&lt;/b&gt;" in html
    assert "a &amp; b" in html
    assert "<b>bold</b>" not in html


def test_copy_button_and_hover_reveal_css():
    html = render_table(_df(), use_friendly=False, include_units=False)
    # Button present, floated over the table wrapper.
    assert 'class="rs-table-copy pdf-hide"' in html
    assert "title=\"Copy table as CSV\"" in html
    assert "content_copy" in html
    # Hidden until the table is hovered.
    assert "opacity: 0" in html
    assert ".rs-table-wrap:hover .rs-table-copy" in html
    # Copy wiring present (ClipboardItem API + legacy fallback).
    assert "collectCsv" in html or "navigator.clipboard" in html
    assert "execCommand" in html


def test_copy_button_can_be_disabled():
    html = render_table(_df(), copy_button=False, use_friendly=False, include_units=False)
    # No button markup and no copy script (the sheets stylesheet still ships).
    assert 'class="rs-table-copy pdf-hide"' not in html
    assert 'rs-table-copy-script' not in html
    assert "execCommand" not in html


def test_empty_dataframe_renders_message():
    html = render_table(pd.DataFrame({"A": pd.Series(dtype="float64")}), use_friendly=False, include_units=False)
    assert "table-empty" in html
    assert "No data available." in html
    html = render_table(
        pd.DataFrame({"A": pd.Series(dtype="float64")}),
        empty_message="No rows matched the query.",
        use_friendly=False,
        include_units=False,
    )
    assert "No rows matched the query." in html


# ---------------------------------------------------------------------------
# Macro can be driven directly from a template context
# ---------------------------------------------------------------------------


def test_prepare_table_data_feeds_macro_directly():
    columns, rows, footer_rows = prepare_table_data(_df(), footer=None, use_friendly=False, include_units=False)
    assert [c["label"] for c in columns] == ["Name", "Value", "Count"]
    assert columns[1]["classes"] == "numeric decimal"
    assert len(rows) == 2
    assert footer_rows is None

    from util.html.table import _render_data_table_macro

    out = _render_data_table_macro()(columns=columns, rows=rows, caption="Direct", footer=footer_rows, copy_button=False)
    assert '<table class="dataframe rs-table">' in out
    assert "<caption>Direct</caption>" in out
    assert 'class="rs-table-copy pdf-hide"' not in out


def test_na_rep_customisable():
    df = pd.DataFrame({"A": pd.Series(["x", None], dtype=object)})
    html = render_table(df, use_friendly=False, include_units=False, na_rep="")
    assert ">x<" in html
    assert '"><' in html  # the NA cell renders empty instead of "-"
    html = render_table(df, use_friendly=False, include_units=False, na_rep="n/a")
    assert "n/a" in html


def test_none_df_renders_empty_message():
    html = render_table(None, use_friendly=False, include_units=False)
    assert "No data available." in html
