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


UNIT_ALIAS = {
    "m": "meter",
    "km": "kilometer",
    "feet": "foot",
    "foot": "foot",
    "miles": "mile",
    "mile": "mile",
    "": None,
    None: None,
}

# Truth table derived expectations for preferred units.
TRUTH_TABLE = [
    # no_convert=False scenarios
    ("CONVERT", "m", "", "m", "feet"),
    ("CONVERT", "km", "", "km", "miles"),
    ("CONVERT", "feet", "", "m", "feet"),
    ("CONVERT", "miles", "", "m", "feet"),
    ("CONVERT", "m", "km", "km", "miles"),
    ("CONVERT", "km", "km", "km", "miles"),
    ("CONVERT", "feet", "km", "km", "miles"),
    ("CONVERT", "miles", "km", "km", "miles"),
    # no_convert=True scenarios
    ("NO_CONVERT", "m", "", "m", "m"),
    ("NO_CONVERT", "km", "", "km", "km"),
    ("NO_CONVERT", "feet", "", "feet", "feet"),
    ("NO_CONVERT", "miles", "", "miles", "miles"),
    ("NO_CONVERT", "m", "km", "m", "m"),
    ("NO_CONVERT", "km", "km", "m", "m"),
    ("NO_CONVERT", "feet", "km", "m", "m"),
    ("NO_CONVERT", "miles", "km", "m", "m"),
]


@pytest.mark.parametrize(
    "scenario,data_unit_literal,display_unit_literal,expected_si_literal,expected_imperial_literal",
    TRUTH_TABLE,
)
def test_preferred_unit_truth_table(
    fresh_meta, scenario, data_unit_literal, display_unit_literal, expected_si_literal, expected_imperial_literal
):
    """
    Ensure preferred_unit_for aligns with the provided truth table for common length scenarios.
    The table captures both conversion-permitted (no_convert=false) and conversion-prohibited (no_convert=true) cases.
    """
    unit_literal = display_unit_literal or data_unit_literal
    canonical_unit = UNIT_ALIAS[unit_literal]
    expected_si = UNIT_ALIAS[expected_si_literal]
    expected_imperial = UNIT_ALIAS[expected_imperial_literal]

    if canonical_unit is None:
        pytest.skip(f"No canonical unit defined for scenario {scenario} ({data_unit_literal}/{display_unit_literal}).")

    unit_obj = ureg.Unit(canonical_unit)

    # Now call RSFieldMeta.preferred_unit_for() to see if we end up with the right value
    pref_unit = fresh_meta.preferred_unit_for(unit_obj)

    fresh_meta.unit_system = 'SI'

    print(f"Preferred unit for {unit_obj} is {pref_unit}")
    assert (pref_unit == ureg.Unit(expected_si)), f"Failed scenario {scenario} with unit {unit_obj}"

    # Now switch over and test the imperial side
    fresh_meta.unit_system = 'imperial'

    pref_unit = fresh_meta.preferred_unit_for(unit_obj)

    assert (pref_unit == ureg.Unit(expected_imperial)), f"Failed scenario {scenario} with unit {unit_obj}"
