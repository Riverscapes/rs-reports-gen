"""Regression tests for the report-level migration from ``df.to_html()`` to
``util.html.table.render_table``.

Every report table should now render through the shared macro: same branded
markup (``dataframe rs-table``), numeric alignment, hover copy button, and
footer rows in ``<tfoot>``. These tests pin the *entry points* each report
calls, so a future regression back to ad-hoc ``to_html`` is caught here.
"""

import re

import pandas as pd
import pint

from util.html.table import render_table
from util.pandas import RSFieldMeta

ureg = pint.get_application_registry()


def _table_html(out: str) -> str:
    """Return just the <table> tag (skip the inlined stylesheet fragment)."""
    return out.split('<table class="dataframe rs-table">')[1].split("</table>")[0]


def _squash(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"</tr>", " |", html))


# ---------------------------------------------------------------------------
# util.figures.table_total_x_by_y (shared by rivers_need_space + inventory)
# ---------------------------------------------------------------------------


def test_table_total_x_by_y_renders_styled_table_with_footer():
    from util.figures import table_total_x_by_y

    saved_meta, saved_sys = _snapshot_meta()
    try:
        df = pd.DataFrame(
            {
                "ownership": ["Private", "Private", "State", "State"],
                "ownership_desc": ["A", "A", "B", "B"],
                "centerline_length": [10.0, 20.0, 30.0, 40.0],
            }
        )
        out = table_total_x_by_y(df, "centerline_length", ["ownership", "ownership_desc"], with_footer=True)

        assert 'class="dataframe rs-table"' in out
        assert "rs-table-copy" in out
        tbl = _table_html(out)
        assert "<tfoot>" in tbl
        assert 'class="footer numeric decimal"' in tbl  # total row right-aligned
        assert "Total" in tbl
        assert 'class="numeric decimal" class=' not in tbl  # no class= duplication
    finally:
        _restore_meta(saved_meta, saved_sys)


def test_table_total_x_by_y_without_footer_has_no_tfoot():
    from util.figures import table_total_x_by_y

    saved_meta, saved_sys = _snapshot_meta()
    try:
        df = pd.DataFrame(
            {
                "ownership": ["Private", "State"],
                "centerline_length": [10.0, 20.0],
            }
        )
        out = table_total_x_by_y(df, "centerline_length", ["ownership"], with_footer=False)
        tbl = _table_html(out)
        assert "<tfoot>" not in tbl
        assert "<thead>" in tbl and "<tbody>" in tbl
    finally:
        _restore_meta(saved_meta, saved_sys)


def test_bar_total_x_by_ybins_can_hide_legend():
    from util.figures import bar_total_x_by_ybins

    meta = RSFieldMeta()
    meta.clear()
    meta.add_field_meta(name="low_lying_ratio", data_unit="percent", dtype="REAL")
    meta.add_field_meta(name="segment_area", data_unit="meter ** 2", dtype="REAL")

    df = pd.DataFrame(
        {
            "low_lying_ratio": [0.1, 0.4, 0.7, 0.9],
            "segment_area": [10.0, 20.0, 30.0, 40.0],
        }
    )
    fig = bar_total_x_by_ybins(df, "segment_area", ["low_lying_ratio"], show_legend=False)
    assert fig.layout.showlegend is False


def test_table_total_x_by_y_uses_large_value_unit_override():
    from util.figures import table_total_x_by_y

    saved_meta, saved_sys = _snapshot_meta()
    try:
        meta = RSFieldMeta()
        meta.clear()
        meta.add_field_meta(name="centerline_length", data_unit="meter", display_unit="kilometer", dtype="REAL")
        df = pd.DataFrame(
            {
                "ownership": ["Private", "State", "Private", "State"],
                "centerline_length": [12000.0, 5000.0, 8000.0, 15000.0],
            }
        )
        out = table_total_x_by_y(df, "centerline_length", ["ownership"], with_footer=True)
        tbl = _table_html(out)
        assert "(km)" in tbl
    finally:
        _restore_meta(saved_meta, saved_sys)


# ---------------------------------------------------------------------------
# rpt_watershed_summary: hydrography / waterbodies / ownership tables
# ---------------------------------------------------------------------------


def test_watershed_ownership_table_uses_render_table():
    from reports.rpt_watershed_summary.figures import ownership_summary_table

    out = ownership_summary_table(pd.DataFrame({"Owner": ["Private"], "Pct": [61.4]}))
    assert 'class="dataframe rs-table"' in out
    assert "<tfoot>" not in _table_html(out)


def _snapshot_meta() -> tuple[object, object]:
    """Snapshot RSFieldMeta (Borg singleton) state.

    add_field_meta mutates the shared ``_field_meta`` DataFrame IN PLACE when
    it already exists, so a plain reference snapshot aliases the live frame and
    restoring becomes a no-op. Copy so the snapshot is a distinct object.
    """
    meta = RSFieldMeta()
    saved_meta = meta._field_meta.copy() if meta._field_meta is not None else None
    saved_sys = meta._unit_system
    return saved_meta, saved_sys


def _restore_meta(saved_meta: object, saved_sys: object) -> None:
    meta = RSFieldMeta()
    meta._field_meta = saved_meta
    meta._unit_system = saved_sys


def _register_watershed_meta() -> tuple[object, object]:
    saved_meta, saved_sys = _snapshot_meta()
    meta = RSFieldMeta()
    meta.clear()  # hermetic baseline: register against a fresh empty frame
    meta.add_field_meta(name="flowline_length_category", dtype="TEXT")
    meta.add_field_meta(name="stream_length", data_unit="mile", dtype="REAL")
    meta.add_field_meta(name="level_path_count", dtype="INT")
    return saved_meta, saved_sys


def test_watershed_hydrography_table_tfoot_and_footnote(monkeypatch):
    import reports.rpt_watershed_summary.figures as wf

    saved_meta, saved_sys = _register_watershed_meta()
    try:
        body = pd.DataFrame(
            {
                "flowline_length_category": ["Intermittent", "Perennial"],
                "stream_length": pd.Series([12.34, 45.67], dtype="pint[mile]"),
                "level_path_count": [4, 9],
            }
        )
        footer = pd.DataFrame(
            {
                "flowline_length_category": ["Total Stream Length"],
                "stream_length": pd.Series([58.0], dtype="pint[mile]"),
                "level_path_count": [13],
            }
        )
        monkeypatch.setattr(wf, "create_hydrography_summary_table", lambda df: (body, footer, "Sample footnote."))

        out = wf.hydrography_table(body)
        assert "Sample footnote." in out and "table-footnote" in out
        tbl = _table_html(out)
        assert "Total Stream Length" in tbl
        assert "<tfoot>" in tbl
        # Units were applied: miles converted to km in the header.
        assert "(km)" in _table_html(out)
    finally:
        _restore_meta(saved_meta, saved_sys)


def test_watershed_waterbody_summary_table_tfoot(monkeypatch):
    import reports.rpt_watershed_summary.figures as wf

    saved_meta, saved_sys = _register_watershed_meta()
    try:
        body = pd.DataFrame(
            {
                "flowline_length_category": ["Reservoir"],
                "stream_length": pd.Series([10.0], dtype="pint[mile]"),
                "level_path_count": [1],
            }
        )
        footer = pd.DataFrame(
            {
                "flowline_length_category": ["Total"],
                "stream_length": pd.Series([10.0], dtype="pint[mile]"),
                "level_path_count": [1],
            }
        )
        monkeypatch.setattr(wf, "create_waterbody_summary_table", lambda df: (body, footer))

        out = wf.waterbody_summary_table(body)
        assert "<tfoot>" in _table_html(out)
        assert "Total" in _table_html(out)
    finally:
        _restore_meta(saved_meta, saved_sys)


# ---------------------------------------------------------------------------
# rpt_igo_project: source projects table keeps its HTML link column
# ---------------------------------------------------------------------------


def _igo_style_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "HUC10 code": ["1704020901"],
            "Project name": ["Test Project"],
            "Data Exchange": ['<a href="https://reports.riverscapes.net" target="_blank" rel="noopener"><span class="material-icons" aria-hidden="true">open_in_new</span> View</a>'],
        }
    )


def test_igo_source_projects_table_keeps_link_and_verbatim_headers():
    # The exact call shape rpt_igo_project/main.py now uses.
    out = render_table(
        _igo_style_df(),
        escape=False,
        use_friendly=False,
        include_units=False,
        empty_message="No contributing source projects were found in the AOI dataset.",
    )
    tbl = _table_html(out)
    # Pre-rendered anchor survives unescaped.
    assert '<a href="https://reports.riverscapes.net"' in tbl
    assert "open_in_new" in tbl
    # Manually renamed display headers stay verbatim (no title-casing).
    assert "<th scope=\"col\" class=\"text\">HUC10 code</th>" in tbl
    assert "class=\"text\">Data Exchange</th>" in tbl


def test_igo_source_projects_empty_message():
    out = render_table(
        _igo_style_df().iloc[0:0],
        escape=False,
        use_friendly=False,
        include_units=False,
        empty_message="No contributing source projects were found in the AOI dataset.",
    )
    assert "table-empty" in out
    assert "No contributing source projects were found in the AOI dataset." in out


# ---------------------------------------------------------------------------
# rpt_stream_names: top-N tables render rollups as tfoot rows
# ---------------------------------------------------------------------------


def test_stream_names_tables_render_via_shared_macro():
    from reports.rpt_stream_names.main import build_top_names_by_path_count_table, define_fields

    saved_meta, saved_sys = _snapshot_meta()
    try:
        meta = RSFieldMeta()
        meta.clear()  # hermetic baseline for define_fields
        define_fields("SI")
        full_df = pd.DataFrame(
            {
                "stream_name": ["Bear", "Bear", "Fork", "Fork", "<Unnamed>", "<Unnamed>"],
                "level_path_count": [1, 1, 1, 1, 1, 1],
                "total_riverscape_length": [10.0, 5.0, 3.0, 7.0, 4.0, 1.0],
                "total_channel_length": [20.0, 10.0, 6.0, 14.0, 8.0, 2.0],
            }
        )
        named_df = full_df[full_df["stream_name"] != "<Unnamed>"]

        out = build_top_names_by_path_count_table(full_df, named_df, top_n=2)
        assert 'class="dataframe rs-table"' in out
        assert "rs-table-copy" in out
        tbl = _table_html(out)
        assert "<tfoot>" in tbl
        # Multiple rollup rows (Top N + All Other + Unnamed + grand total).
        assert tbl.count("<tr>") >= 4
        # Friendly names + unit headers from define_fields.
        assert "Distinct Systems" in tbl
        assert "Riverscape Length (km)" in tbl
    finally:
        _restore_meta(saved_meta, saved_sys)


def test_no_report_module_calls_df_to_html():
    """Scan report sources: the ad-hoc renderer must not be imported anymore."""
    import pathlib

    reports_dir = pathlib.Path("src/reports")
    offenders = []
    for py in reports_dir.rglob("*.py"):
        if "__pycache__" in str(py):
            continue
        text = py.read_text(encoding="utf-8")
        if ".to_html(" in text:
            offenders.append(str(py))
    assert not offenders, f"reports still call .to_html() directly: {offenders}"


def test_util_figures_no_df_to_html():
    import pathlib

    text = pathlib.Path("src/util/figures.py").read_text(encoding="utf-8")
    assert ".to_html(" not in text
    assert "render_table" in text
