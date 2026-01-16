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


def set_basic_meta(meta, rows):
    """Helper to assign metadata rows to the shared RSFieldMeta instance."""
    meta_df = pd.DataFrame(rows)
    meta.field_meta = meta_df


def test_apply_units_requires_metadata(fresh_meta):
    """ This test ensures that apply_units raises an error when no metadata is set
    """
    df = pd.DataFrame({"height": [1, 2, 3]})

    with pytest.raises(RuntimeError):
        fresh_meta.apply_units(df)


def test_apply_units_converts_to_preferred_system(fresh_meta):
    """ This test ensures that apply_units converts units according to the preferred system"""
    set_basic_meta(
        fresh_meta,
        [
            {
                "layer_id": "tbl",
                "name": "length",
                "friendly_name": "Length",
                "data_unit": "meter",
                "display_unit": "",
                "dtype": "FLOAT",
                "no_convert": False,
            }
        ],
    )
    fresh_meta.unit_system = "imperial"
    source = pd.DataFrame({"length": [1.0, 2.0]})

    applied, applied_units = fresh_meta.apply_units(source)

    assert applied is not source
    assert applied_units["length"] == ureg.Unit("foot")
    assert [x.magnitude for x in applied["length"]] == pytest.approx([3.28084, 6.56168])
    assert source["length"].tolist() == [1.0, 2.0]


def test_apply_units_dtypes_correct(fresh_meta):
    """ This test ensures that apply_units respects the no_convert flag and display_unit"""
    set_basic_meta(
        fresh_meta,
        [
            {
                "layer_id": "tbl",
                "name": "distance",
                "friendly_name": "Distance",
                "data_unit": "meter",
                "display_unit": "kilometer",
                "dtype": "FLOAT",
                "no_convert": True,
            },
            {
                "layer_id": "tbl",
                "name": "distance_converted",
                "friendly_name": "Distance Converted",
                "data_unit": "meter",
                "display_unit": "kilometer",
                "dtype": "FLOAT",
                "no_convert": False,
            }
        ],
    )
    source_df = pd.DataFrame({
        "distance": [1000.0, 2000.0],
        "distance_converted": [1000.0, 2000.0],
        "no_unit": [1.0, 2.0]
    })
    # Now swap the unit system
    fresh_meta.unit_system = "imperial"

    applied_df, applied_units = fresh_meta.apply_units(source_df)

    # Make sure the source is unaltered
    assert str(source_df["distance"].dtype) == "float64"
    assert str(source_df["distance_converted"].dtype) == "float64"
    assert str(source_df["no_unit"].dtype) == "float64"

    # Check the applied_units to make sure we did something
    assert applied_units["distance"] == ureg.Unit("kilometer")
    assert applied_units["distance_converted"] == ureg.Unit("mile")

    # Also check the dtype of the column and make sure it's a Pint type
    assert str(applied_df["distance"].dtype) == "pint[kilometer][Float64]"
    assert str(applied_df["distance_converted"].dtype) == "pint[mile][Float64]"
    assert str(applied_df["no_unit"].dtype) == "float64"


def test_apply_units_respects_no_convert_and_display_unit(fresh_meta):
    """ This test ensures that apply_units respects the no_convert flag and display_unit"""
    set_basic_meta(
        fresh_meta,
        [
            {
                "layer_id": "tbl",
                "name": "distance",
                "friendly_name": "Distance",
                "data_unit": "meter",
                "display_unit": "kilometer",
                "dtype": "FLOAT",
                "no_convert": True,
            }
        ],
    )
    fresh_meta.unit_system = "imperial"
    source = pd.DataFrame({"distance": [1000.0, 2000.0]})

    applied, applied_units = fresh_meta.apply_units(source)

    assert applied_units["distance"] == ureg.Unit("kilometer")
    # Also check the dtype of the column and make sure it's a Pint type
    assert str(source["distance"].dtype) == "float64"
    assert str(applied["distance"].dtype) == "pint[kilometer][Float64]"
    assert [float(x.magnitude) for x in applied["distance"].tolist()] == pytest.approx([1.0, 2.0])
