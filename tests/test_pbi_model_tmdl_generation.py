"""Regression tests for PBIP TMDL generation edge cases.

Copilot-generated module.
"""

from util.pbi_model import ColumnDef, _generate_column_tmdl, _quote_name


def test_quote_name_escapes_apostrophe() -> None:
    """TMDL identifiers with apostrophes should be escaped safely."""
    assert _quote_name("Owner's Name") == "'Owner''s Name'"


def test_generate_column_tmdl_quotes_source_column_with_spaces() -> None:
    """sourceColumn should be quoted when raw source names include spaces."""
    col = ColumnDef(
        table_name="vegetation_cover",
        column_name="Annual Forb and Grass Cover",
        friendly_name="Annual Forb And Grass Cover",
        dtype="FLOAT",
    )
    col.pbi_name = "Annual Forb And Grass Cover"
    col.pbi_data_type = "double"
    col.lineage_tag = "test-lineage"
    col.summarize_by = "sum"

    lines = _generate_column_tmdl(col)

    assert "\t\tsourceColumn: 'Annual Forb and Grass Cover'" in lines
