import pandas as pd
import pytest

from util.pandas.RSFieldMeta import RSFieldMeta, ureg


@pytest.fixture
def fresh_meta():
    """Provide an RSFieldMeta instance with isolated shared state for each test."""
    RSFieldMeta._shared_state.clear()
    meta = RSFieldMeta()
    meta.clear()
    return meta


def set_basic_meta(meta: RSFieldMeta, rows):
    """Helper to assign metadata rows to the shared RSFieldMeta instance."""
    meta_df = pd.DataFrame(rows)
    meta.field_meta = meta_df


def test_duplicate_meta_requires_metadata(fresh_meta):
    """ Make sure we reject attempts to duplicate metadata when none exists
    """
    with pytest.raises(RuntimeError):
        fresh_meta.duplicate_meta("width", "width_copy")


def test_duplicate_meta_requires_existing_original(fresh_meta):
    """ Make sure we reject attempts to duplicate metadata for a column that doesn't exist
    """
    set_basic_meta(
        fresh_meta,
        [
            {
                "table_name": "tbl",
                "name": "width",
                "friendly_name": "Width",
                "data_unit": "meter",
                "display_unit": "meter",
                "dtype": "FLOAT",
                "no_convert": False,
                "description": "Channel width",
            }
        ],
    )

    with pytest.raises(ValueError, match="Original column 'depth'"):
        fresh_meta.duplicate_meta("depth", "depth_copy")


def test_duplicate_meta_rejects_existing_target(fresh_meta):
    """ Make sure we reject attempts to duplicate metadata to a column that already exists"""
    set_basic_meta(
        fresh_meta,
        [
            {
                "table_name": "tbl",
                "name": "width",
                "friendly_name": "Width",
                "data_unit": "meter",
                "display_unit": "meter",
                "dtype": "FLOAT",
                "no_convert": False,
                "description": "Channel width",
            },
            {
                "table_name": "tbl",
                "name": "width_copy",
                "friendly_name": "Width Copy",
                "data_unit": "meter",
                "display_unit": "meter",
                "dtype": "FLOAT",
                "no_convert": False,
                "description": "Another width column",
            },
        ],
    )

    with pytest.raises(ValueError, match="New column 'width_copy'"):
        fresh_meta.duplicate_meta("width", "width_copy")


def test_duplicate_meta_clones_and_overrides_fields(fresh_meta):
    """Test that duplicating metadata works and that overrides are applied correctly"""
    set_basic_meta(
        fresh_meta,
        [
            {
                "table_name": "tbl",
                "name": "area",
                "friendly_name": "Area",
                "data_unit": "meter ** 2",
                "display_unit": "meter ** 2",
                "dtype": "FLOAT",
                "no_convert": False,
                "description": "Surface area",
            }
        ],
    )

    new_row = fresh_meta.duplicate_meta(
        "area",
        "area_sqmi",
        new_friendly="Area (sq mi)",
        new_description="Surface area converted to square miles",
        new_data_unit="mile ** 2",
        new_display_unit="acre",
        new_dtype="DECIMAL",
        new_no_convert=True,
    )

    original_row = fresh_meta.get_field_meta("area")

    assert new_row.name == "area_sqmi"
    assert "area_sqmi" in fresh_meta.field_meta.index
    assert new_row.friendly_name == "Area (sq mi)"
    assert new_row.description == "Surface area converted to square miles"
    assert new_row.data_unit == ureg.Unit("mile ** 2")
    assert new_row.display_unit == ureg.Unit("acre")
    assert new_row.dtype == "DECIMAL"
    assert bool(new_row.no_convert) is True
    assert original_row.friendly_name == "Area"
    assert original_row.data_unit == ureg.Unit("meter ** 2")
