import pandas as pd
import pytest
import pint  # noqa: F401  # pylint: disable=unused-import

from util.pandas.RSFieldMeta import RSFieldMeta, PREFERRED_UNIT_DEFAULTS, ureg

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


def test_preferred_unit_for_defaults_si_length(fresh_meta):
    """Preferred unit should respect SI defaults for simple dimensions."""
    result = fresh_meta.preferred_unit_for(ureg.meter)

    assert result == ureg.Unit("meter")


def test_preferred_unit_for_defaults_imperial_area(fresh_meta):
    """Preferred unit should respect imperial defaults for compound dimensions."""
    fresh_meta.unit_system = "imperial"

    result = fresh_meta.preferred_unit_for(ureg.meter ** 2)

    assert result == ureg.Unit("acres")


def test_preferred_unit_for_dimensionless_returns_none(fresh_meta):
    """Preferred unit should be None when dimensionality cannot be resolved."""
    result = fresh_meta.preferred_unit_for(ureg.dimensionless)

    assert result is None


def test_preferred_unit_for_unknown_dimension_returns_none(fresh_meta):
    """Preferred unit should be None when the dimension is not in the mapping."""
    current_unit = ureg.ampere  # electrical current is not in default map

    result = fresh_meta.preferred_unit_for(current_unit)

    assert result is None


def test_preferred_unit_for_respects_custom_mapping(fresh_meta):
    """Preferred unit should use overrides provided via preferred_units setter."""
    custom_mapping = {system: mapping.copy() for system, mapping in PREFERRED_UNIT_DEFAULTS.items()}
    custom_mapping["SI"]["length"] = "kilometer"
    custom_mapping["imperial"]["length"] = "mile"
    fresh_meta.preferred_units = custom_mapping

    result = fresh_meta.preferred_unit_for(ureg.meter)

    assert result == ureg.Unit("kilometer")


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
    assert applied_units["length_sum"] == ureg.Unit("meter")

    fresh_meta.unit_system = "SI"
