import pytest
from reports.rpt_watershed_summary.main import parse_hucs


def test_parse_hucs_single_10():
    """single 10 digit uses equality ? or maybe IN is just fine and easier"""
    assert parse_hucs("1234567890", 'HUC10') == "HUC10 IN ('1234567890')"


def test_parse_hucs_multiple_10s():
    """multiple huc10s using IN operator """
    assert parse_hucs("1234567890,0987654321", 'HUC10') == "HUC10 IN ('1234567890','0987654321')"


def test_parse_hucs_with_spaces():
    """spaces in input are okay - should be ignored"""
    assert parse_hucs("1234567890, 0987654321, 2244668800", 'HUC10') == "HUC10 IN ('1234567890','0987654321','2244668800')"
    assert parse_hucs(" 1234567890 , 0987654321 ", 'HUC10') == "HUC10 IN ('1234567890','0987654321')"


def test_parse_bigger_hucs():
    """you can use collections of huc8s or huc6s, but not both combined"""
    assert parse_hucs('12345678,87654321', 'huc10', 10) == "substr(huc10,1,8) IN ('12345678','87654321')"
    assert parse_hucs('123456,654321', 'huc10', 10) == "substr(huc10,1,6) IN ('123456','654321')"
    with pytest.raises(NotImplementedError):
        assert parse_hucs('123456, 654321, 22446688')


def test_parse_hucs_invalid():
    """should raise error with invalid entries"""
    with pytest.raises(ValueError):
        parse_hucs("")
    with pytest.raises(ValueError):
        parse_hucs("notanumber")
    with pytest.raises(ValueError):
        parse_hucs("123456789012345", "HUC10")  # too long

    with pytest.raises(ValueError):
        parse_hucs("12345abcde", "HUC10")  # contains non-numeric
