import pandas as pd
import pint  # noqa: F401  # pylint: disable=unused-import
import pytest

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
    """This test ensures that apply_units respects custom units in the metadata"""
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
            },
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


def test_get_friendly_name_falls_back_when_metadata_name_is_null(fresh_meta):
    """Null friendly names should fall back to a title-cased column name."""
    set_basic_meta(
        fresh_meta,
        [
            {
                "layer_id": "tbl",
                "name": "dam_ct",
                "friendly_name": None,
                "dtype": "INTEGER",
            }
        ],
    )

    assert fresh_meta.get_friendly_name("dam_ct", layer_id="tbl") == "Dam Ct"


# ---------------------------------------------------------------------------
# Count-compound unit conversion tests (Created by copilot)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "data_unit,expected_imperial,expected_si",
    [
        # X / count  (density-like: dams per km, avg length per reach, etc.)
        ("kilometer / count", "mile / count", "kilometer / count"),
        ("meter / count", "foot / count", "meter / count"),
        ("millimeter / count", "inch / count", "millimeter / count"),
        # count / X  (frequency-like: dams per km)
        ("count / kilometer", "count / mile", "count / kilometer"),
        ("count / meter", "count / foot", "count / meter"),
    ],
)
def test_get_system_units_count_compound_imperial(fresh_meta, data_unit, expected_imperial, expected_si):
    """Count-compound units are converted by swapping the non-count dimension."""
    fresh_meta.unit_system = "imperial"
    result = fresh_meta.get_system_units(ureg.Unit(data_unit))
    assert result == ureg.Unit(expected_imperial), f"Expected {expected_imperial!r}, got {result!r}"

    fresh_meta.unit_system = "SI"
    result_si = fresh_meta.get_system_units(ureg.Unit(data_unit))
    assert result_si == ureg.Unit(expected_si), f"Expected SI passthrough {expected_si!r}, got {result_si!r}"


def test_get_field_unit_count_compound_respects_unit_system(fresh_meta):
    """get_field_unit returns the correct count-compound unit based on the active unit system."""
    set_basic_meta(
        fresh_meta,
        [
            {
                "name": "dam_density",
                "friendly_name": "Dam Density",
                "data_unit": "count / kilometer",
                "dtype": "FLOAT",
            },
            {
                "name": "avg_dam_length",
                "friendly_name": "Avg Dam Length",
                "data_unit": "meter / count",
                "dtype": "FLOAT",
            },
        ],
    )

    fresh_meta.unit_system = "imperial"
    assert fresh_meta.get_field_unit("dam_density") == ureg.Unit("count / mile")
    assert fresh_meta.get_field_unit("avg_dam_length") == ureg.Unit("foot / count")

    fresh_meta.unit_system = "SI"
    assert fresh_meta.get_field_unit("dam_density") == ureg.Unit("count / kilometer")
    assert fresh_meta.get_field_unit("avg_dam_length") == ureg.Unit("meter / count")
