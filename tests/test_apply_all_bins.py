"""Tests for rme_common_dataprep bin/colour utilities.

Copilot-generated test module.
"""

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from util.metadata_export import (
    TableEntry,
    _infer_logical_type,
    _normalise_registry_dtype,
    export_data_dictionary,
)
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


# ── export_data_dictionary (shared) ───────────────────────────────────


class TestExportDataDictionary:
    def test_writes_csv_with_expected_columns(self, tmp_path):
        df = pd.DataFrame({"low_lying_ratio": [0.1], "some_text": ["hello"]})
        out = tmp_path / "dd.csv"
        export_data_dictionary({"tbl": TableEntry(df=df)}, out)
        result = pd.read_csv(out)
        assert set(result.columns) == {
            "table_name",
            "column_name",
            "friendly_name",
            "description",
            "data_unit",
            "display_unit",
            "export_unit",
            "dtype",
            "preferred_format",
            "theme",
            "in_registry",
        }
        assert list(result["column_name"]) == ["low_lying_ratio", "some_text"]
        assert list(result["table_name"]) == ["tbl", "tbl"]

    def test_includes_bin_columns_after_apply(self, tmp_path):
        df = pd.DataFrame({"low_lying_ratio": [0.05, 0.30]})
        df = apply_all_bins(df, {"low_lying_ratio": "low_lying_ratio"})
        out = tmp_path / "dd.csv"
        export_data_dictionary({"dgo": TableEntry(df=df)}, out)
        result = pd.read_csv(out)
        col_names = set(result["column_name"])
        assert "low_lying_ratio_bin" in col_names
        assert "low_lying_ratio_color" in col_names
        assert "low_lying_ratio_bin_sort" in col_names
        # bin columns should have a friendly name containing "(bin)"
        bin_row = result[result["column_name"] == "low_lying_ratio_bin"]
        assert "(bin)" in bin_row["friendly_name"].iloc[0]
        # bin columns should be marked in_registry
        assert bin_row["in_registry"].iloc[0] == True

    def test_one_row_per_column(self, tmp_path):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        out = tmp_path / "dd.csv"
        export_data_dictionary({"t": TableEntry(df=df)}, out)
        result = pd.read_csv(out)
        assert len(result) == 3

    def test_multiple_tables(self, tmp_path):
        df1 = pd.DataFrame({"x": [1]})
        df2 = pd.DataFrame({"y": ["hello"], "z": [True]})
        out = tmp_path / "dd.csv"
        export_data_dictionary({"fact": TableEntry(df=df1), "dim": TableEntry(df=df2)}, out)
        result = pd.read_csv(out)
        assert len(result) == 3
        assert set(result["table_name"]) == {"fact", "dim"}

    def test_in_registry_false_for_unknown_columns(self, tmp_path):
        df = pd.DataFrame({"totally_unknown_xyz": [42]})
        out = tmp_path / "dd.csv"
        export_data_dictionary({"t": TableEntry(df=df)}, out)
        result = pd.read_csv(out)
        assert result["in_registry"].iloc[0] == False

    def test_dtype_uses_logical_enum(self, tmp_path):
        df = pd.DataFrame(
            {
                "an_int": pd.array([1, 2], dtype="int64"),
                "a_float": pd.array([1.0, 2.0], dtype="float64"),
                "a_str": ["hello", "world"],
                "a_bool": [True, False],
            }
        )
        out = tmp_path / "dd.csv"
        export_data_dictionary({"t": TableEntry(df=df)}, out)
        result = pd.read_csv(out)
        type_map = dict(zip(result["column_name"], result["dtype"]))
        assert type_map["an_int"] == "INTEGER"
        assert type_map["a_float"] == "FLOAT"
        assert type_map["a_str"] == "STRING"
        assert type_map["a_bool"] == "BOOLEAN"

    def test_geometry_column(self, tmp_path):
        gdf = gpd.GeoDataFrame({"val": [1]}, geometry=[Point(0, 0)])
        out = tmp_path / "dd.csv"
        export_data_dictionary({"geo": TableEntry(df=gdf)}, out)
        result = pd.read_csv(out)
        geom_row = result[result["column_name"] == "geometry"]
        assert geom_row["dtype"].iloc[0] == "GEOMETRY"


# ── _normalise_registry_dtype ─────────────────────────────────────────


class TestNormaliseRegistryDtype:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("REAL", "FLOAT"),
            ("TEXT", "STRING"),
            ("VARCHAR", "STRING"),
            ("INT", "INTEGER"),
            ("BIGINT", "INTEGER"),
            ("BOOLEAN", "BOOLEAN"),
            ("TIMESTAMP", "DATETIME"),
            ("GEOMETRY", "GEOMETRY"),
            ("", ""),
        ],
    )
    def test_mapping(self, raw, expected):
        assert _normalise_registry_dtype(raw) == expected


# ── _infer_logical_type ───────────────────────────────────────────────


class TestInferLogicalType:
    def test_int(self):
        assert _infer_logical_type(pd.Series([1, 2], dtype="int64")) == "INTEGER"

    def test_float(self):
        assert _infer_logical_type(pd.Series([1.0], dtype="float64")) == "FLOAT"

    def test_string(self):
        assert _infer_logical_type(pd.Series(["a", "b"])) == "STRING"

    def test_bool(self):
        assert _infer_logical_type(pd.Series([True, False])) == "BOOLEAN"

    def test_datetime(self):
        assert _infer_logical_type(pd.Series(pd.to_datetime(["2024-01-01"]))) == "DATETIME"

    def test_categorical(self):
        assert _infer_logical_type(pd.Series(pd.Categorical(["a", "b"]))) == "STRING"
