"""Tests for rme_common_dataprep bin/colour utilities.

Copilot-generated test module.
"""

import pandas as pd
import pytest

from util.rme.rme_common_dataprep import apply_all_bins, color_to_hex


# ── color_to_hex ──────────────────────────────────────────────────────


class TestColorToHex:
    def test_hex_passthrough(self):
        assert color_to_hex("#ff0000") == "#ff0000"

    def test_hex_short(self):
        assert color_to_hex("#f00") == "#ff0000"

    def test_rgb(self):
        assert color_to_hex("rgb(247, 252, 245)") == "#f7fcf5"

    def test_rgb_black(self):
        assert color_to_hex("rgb(0, 0, 0)") == "#000000"

    def test_hsl_red(self):
        # hsl(0, 100%, 50%) → pure red
        result = color_to_hex("hsl(0, 100%, 50%)")
        assert result == "#ff0000"

    def test_hsl_blue(self):
        # hsl(216, 100%, 45%)
        result = color_to_hex("hsl(216, 100%, 45%)")
        # Should be a dark blue – just check it's a valid hex
        assert result.startswith("#")
        assert len(result) == 7

    def test_unrecognized_passthrough(self):
        assert color_to_hex("magenta") == "magenta"


# ── apply_all_bins ────────────────────────────────────────────────────


class TestApplyAllBins:
    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        """DataFrame with a few columns that have entries in bins.json."""
        return pd.DataFrame(
            {
                "low_lying_ratio": [0.01, 0.03, 0.12, 0.60, 0.90],
                "lf_riparian_prop": [0.0, 0.02, 0.10, 0.40, 0.80],
                "confinement_ratio": [0.05, 0.3, 0.7, 0.9, 0.95],
                "unrelated_col": [1, 2, 3, 4, 5],
            }
        )

    def test_adds_bin_and_color_columns(self, sample_df):
        result = apply_all_bins(sample_df)
        for col in ("low_lying_ratio", "lf_riparian_prop", "confinement_ratio"):
            assert f"{col}_bin" in result.columns
            assert f"{col}_color" in result.columns
            assert f"{col}_bin_sort" in result.columns

    def test_does_not_touch_unrelated_columns(self, sample_df):
        result = apply_all_bins(sample_df)
        assert "unrelated_col_bin" not in result.columns
        assert "unrelated_col_bin_sort" not in result.columns

    def test_color_values_are_hex(self, sample_df):
        result = apply_all_bins(sample_df)
        for val in result["low_lying_ratio_color"].dropna():
            assert val.startswith("#"), f"Expected hex, got {val}"
            assert len(val) == 7

    def test_custom_mapping(self, sample_df):
        custom = {"low_lying_ratio": "low_lying_ratio"}
        result = apply_all_bins(sample_df, column_to_bin_key=custom)
        assert "low_lying_ratio_bin" in result.columns
        assert "low_lying_ratio_bin_sort" in result.columns
        # Should NOT have processed confinement_ratio since it wasn't in custom map
        assert "confinement_ratio_bin" not in result.columns
        assert "confinement_ratio_bin_sort" not in result.columns

    def test_row_count_unchanged(self, sample_df):
        result = apply_all_bins(sample_df)
        assert len(result) == len(sample_df)

    def test_sort_order_is_sequential(self, sample_df):
        result = apply_all_bins(sample_df)
        sort_vals = result["low_lying_ratio_bin_sort"].dropna().unique()
        # All sort values should be non-negative integers
        for v in sort_vals:
            assert v >= 0
            assert v == int(v)

    def test_sort_order_matches_label_order(self):
        """Rows in the same bin should share the same sort value,
        and lower bins should have lower sort values."""
        df = pd.DataFrame({"low_lying_ratio": [0.01, 0.03, 0.80]})
        result = apply_all_bins(df, {"low_lying_ratio": "low_lying_ratio"})
        sorts = result["low_lying_ratio_bin_sort"].tolist()
        # 0.01 is in the first bin (sort 0), 0.03 is in the second (sort 1),
        # 0.80 is in a higher bin
        assert sorts[0] < sorts[1] < sorts[2]
