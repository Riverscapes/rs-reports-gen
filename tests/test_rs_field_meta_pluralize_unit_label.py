import pint
import pytest

from util.pandas.RSFieldMeta import RSFieldMeta, ureg


@pytest.mark.parametrize(
    "unit_label,magnitude,expected",
    [
        # singular examples
        ("mile", 1, "mile"),
        ("inch", 1.0, "inch"),
        ("foot", 1, "foot"),
        # Pint Unit input examples
        (ureg.Unit("mile"), 1, "mile"),
        (ureg.Unit("mile"), 2, "miles"),
        (ureg.Unit("foot"), 2, "feet"),
        (ureg.Unit("mile ** 2"), 1, "square mile"),
        (ureg.Unit("mile ** 2"), 2, "square miles"),
        (ureg.Unit("yard ** 3"), 1, "cubic yard"),
        (ureg.Unit("yard ** 3"), 2, "cubic yards"),
        # normalized string input examples
        ("  mile  ", 2, "miles"),
        ("square miles", 2, "square miles"),
        # irregular plural examples
        ("foot", 2, "feet"),
        ("inch", 3, "inches"),
        # allowlisted plural examples
        ("mile", 2, "miles"),
        ("meter", 0, "meters"),
        ("meter", -2, "meters"),
        ("kilometer", 2, "kilometers"),
        ("millimeter", 2, "millimeters"),
        ("yard", 2, "yards"),
        ("acre", 2, "acres"),
        ("hectare", 2, "hectares"),
        ("degree", 2, "degrees"),
        ("count", 2, "counts"),
        # non-allowlisted labels are unchanged
        ("percent", 2, "percent"),
        ("dimensionless", 2, "dimensionless"),
        ("mile²", 2, "square miles"),
        ("acre·foot", 2, "acre·foot"),
        # defensive fallback examples
        ("km", "not-a-number", "km"),
        ("", 2, ""),
    ],
)
def test_pluralize_unit_label_examples(unit_label: str | pint.Unit, magnitude: object, expected: str):
    """Accept Pint Units or strings and pluralize only allowlisted canonical labels."""
    assert RSFieldMeta.pluralize_unit_label(unit_label, magnitude) == expected
