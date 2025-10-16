import pandas as pd
import pytest
import pint  # noqa: F401  # pylint: disable=unused-import

from util.pandas.RSFieldMeta import RSFieldMeta, ureg

ureg = pint.get_application_registry()


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


def test_apply_units_custom_units(fresh_meta):
    """ This test ensures that apply_units respects custom units in the metadata"""
    set_basic_meta(
        fresh_meta,
        [
            {
                "name": "length",
                "friendly_name": "Length",
                "data_unit": "meter",
                "dtype": "FLOAT",
            },
            {
                "name": "length_sum",
                "friendly_name": "Length Sum",
                "data_unit": "kilometer",
                "dtype": "FLOAT",
            }
        ],
    )

    source = pd.DataFrame({"length": [1000.0, 2000.0]})
    # Now we add a second column and convert it to a different unit
    # use pint to do the conversion
    # x is a pint object here so we can use that to convert the unit
    source["length_sum"] = source["length"].apply(lambda x: (x * ureg.meter).to("kilometer").magnitude)
    applied, applied_units = fresh_meta.apply_units(source)

    assert applied is not source
    assert applied_units["length"] == ureg.Unit("meter")
    assert applied_units["length_sum"] == ureg.Unit("kilometer")

    fresh_meta.unit_system = "SI"
