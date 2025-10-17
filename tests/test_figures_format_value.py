import pint
import pytest
from util.figures import format_value

ureg = pint.UnitRegistry()


def test_float_rounding():

    assert format_value(123.4567, 2) == "123.46"
    assert format_value(123.4567, 0) == "123"
    assert format_value(-987.654, 1) == "-987.7"


def test_int():
    assert format_value(42, 0) == "42"
    assert format_value(42, 2) == "42.00"


def test_pint_quantity_with_unit():
    val = 123.456 * ureg.kilometer
    result = format_value(val, 1)
    assert result == "123.5 km"
    val = 41913 * ureg.meter
    assert format_value(val, 0) == "41,913 m"


# this option is not implemented
# def test_pint_quantity_without_unit():
#     val = 123.456 * ureg.kilometer
#     result = format_value(val, 1, show_unit=False)
#     assert result == "123.5"


def test_large_number_thousands_separator():
    assert format_value(12345.678, 1) == "12,345.7"


def test_text_format():
    assert format_value("Apples") == "Apples"
