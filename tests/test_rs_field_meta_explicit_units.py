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


def test_get_system_unit_value_converts_imperial_to_si(fresh_meta):
    """Foot should resolve to the explicit SI unit when the system is SI."""
    quantity = 1 * ureg.foot

    result = fresh_meta.get_system_unit_value(quantity)

    # The result should be approximately 0.3048 meters
    assert result.units == ureg.meter
    assert result.magnitude == pytest.approx(0.3048)


def test_get_system_unit_value_converts_si_to_imperial(fresh_meta):
    """Meter should resolve to the explicit imperial unit when the system is imperial."""
    fresh_meta.unit_system = "imperial"
    quantity = 1 * ureg.meter

    result = fresh_meta.get_system_unit_value(quantity)

    # The result should be approximately 3.28084 feet
    assert result.units == ureg.foot
    assert result.magnitude == pytest.approx(3.28084)


def test_get_system_unit_value_no_conversion_necessary(fresh_meta):
    """Meter should resolve to the explicit imperial unit when the system is imperial."""
    quantity = 1 * ureg.meter

    result = fresh_meta.get_system_unit_value(quantity)

    # The result should be approximately 1.0 feet
    assert result.units == ureg.meter
    assert result.magnitude == pytest.approx(1.0)

    fresh_meta.unit_system = "imperial"
    quantity2 = 1 * ureg.foot
    result2 = fresh_meta.get_system_unit_value(quantity2)

    assert result2.units == ureg.foot
    assert result2.magnitude == pytest.approx(1.0)


def test_get_system_unit_value_returns_original_when_missing_mapping(fresh_meta):
    """Units without a mapping should be returned unchanged."""
    quantity = 1 * ureg.tablespoon

    result = fresh_meta.get_system_unit_value(quantity)

    # It should spit back the original quantity unchanged
    assert result.units == ureg.tablespoon
    assert result.magnitude == pytest.approx(1.0)
