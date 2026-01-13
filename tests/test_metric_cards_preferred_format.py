"""test metric_cards output
    see also util.examples.metric_format_demo
"""
import pint

from util.figures import metric_cards
from util.pandas import RSFieldMeta

ureg = pint.UnitRegistry()


def test_metric_cards_uses_preferred_format():
    meta = RSFieldMeta()
    original_meta = meta.field_meta.copy(deep=True) if meta.field_meta is not None else None
    try:
        meta.clear()
        meta.add_field_meta(
            name='test_metric',
            friendly_name='Test Metric',
            data_unit='meter',
            preferred_format="{:.2f}"
        )
        cards = metric_cards({'test_metric': 1234.567 * ureg.meter})
        assert cards['test_metric']['value'] == '1234.57 m'
    finally:
        meta._field_meta = original_meta


def test_format_scalar_default_units():
    meta = RSFieldMeta()
    original_meta = meta.field_meta.copy(deep=True) if meta.field_meta is not None else None
    try:
        meta.clear()
        meta.add_field_meta(
            name='length_metric',
            friendly_name='Length Metric',
            data_unit='meter')
        result = meta.format_scalar('length_metric', 9876.543 * ureg.meter, decimals=1)
        assert result == '9,876.5 m'
    finally:
        meta._field_meta = original_meta
