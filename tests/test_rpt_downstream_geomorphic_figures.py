"""Minimum safety tests for downstream geomorphic profile figures.

Copilot-generated module.
"""

import pandas as pd

from reports.rpt_downstream_geomorphic.figures import build_profile_figures


def _sample_profile_df() -> pd.DataFrame:
    """Build a compact profile dataframe for two level paths.

    Copilot-generated function.
    """
    rows: list[dict[str, object]] = []
    for level_path, stream_name in (("1001", "River A"), ("1002", "River B")):
        for seg_distance in (0.0, 1.0, 2.0):
            rows.append(
                {
                    "level_path": level_path,
                    "seg_distance": seg_distance,
                    "stream_name": stream_name,
                    "elevation": 1000.0 - (seg_distance * 5.0),
                    "drainage_area": 10.0 + (seg_distance * 2.0),
                    "channel_width": 8.0 + seg_distance,
                    "confinement_ratio": 1.1 + (seg_distance * 0.1),
                    "prim_channel_gradient": 0.02 - (seg_distance * 0.002),
                    "planform_sinuosity": 1.2 + (seg_distance * 0.05),
                    "floodplain_ratio": 0.3 + (seg_distance * 0.02),
                    "fldpln_access": 0.6 - (seg_distance * 0.03),
                }
            )
    return pd.DataFrame(rows)


def test_build_profile_figures_generates_expected_keys() -> None:
    """Figure keys/count should remain stable for two level paths.

    Copilot-generated function.
    """
    df = _sample_profile_df()

    figures = build_profile_figures(df)

    expected_keys = {
        "profile_1001",
        "channel_1001",
        "gradient_1001",
        "floodplain_1001",
        "profile_1002",
        "channel_1002",
        "gradient_1002",
        "floodplain_1002",
    }

    assert set(figures.keys()) == expected_keys
    assert len(figures) == len(expected_keys)
