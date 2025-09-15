"""Unit tests for `est_rows_for_csv_file`."""

import csv
from pathlib import Path

import pytest

from util import est_rows_for_csv_file


@pytest.fixture
def write_csv(tmp_path: Path):
    def _write_csv(filename: str, rows: list[tuple[str, ...]]):
        path = tmp_path / filename
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["col1", "col2"])
            for row in rows:
                writer.writerow(row)
        return path

    return _write_csv


def test_est_rows_small_files_are_counted_exactly(write_csv):
    rows = [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")]
    csv_path = write_csv("small.csv", rows)

    assert est_rows_for_csv_file(str(csv_path)) == len(rows)


def test_est_rows_large_file_is_estimated_reasonably(write_csv):
    row = ("value", "1234567890")
    rows = [row] * 250_000  # Ensure file exceeds the sampling threshold (3 MB)
    csv_path = write_csv("large.csv", rows)

    estimate = est_rows_for_csv_file(str(csv_path))
    assert abs(estimate - len(rows)) < len(rows) * 0.05
