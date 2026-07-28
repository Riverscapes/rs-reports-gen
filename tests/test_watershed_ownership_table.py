import pandas as pd

from reports.rpt_watershed_summary.figures import ownership_summary_table


def test_ownership_summary_table_includes_percent_of_total_area() -> None:
    df = pd.DataFrame(
        {
            "ownership_desc": ["BLM", "USFS"],
            "sum_ownership_area": [100.0, 100.0],
        }
    )

    html = ownership_summary_table(df)

    assert "Percent of Total Area" in html
    assert "50.00%" in html
    assert "50.00%" in html
