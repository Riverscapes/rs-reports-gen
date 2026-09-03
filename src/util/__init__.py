"""Utility helpers shared across rs_reports.

Importing this package also registers the Riverscapes brand Plotly template and
sets it as the default (see util.plotly.riverscapes).
"""

from .attains_assessment import query_attains_assessments
from .csvhelper import est_rows_for_csv_file
from .math_functions import round_down, round_up
from .plotly import riverscapes as _riverscapes_brand  # noqa: F401  # side effect: brand template as plotly default
from .report_entrypoint import (
    build_common_launch_args,
    build_output_path,
    build_report_parser,
    derive_report_name,
    init_report_logging,
    parse_report_args,
    report_main_wrapper,
)
from .rs_geo_helpers import (
    get_bounds_from_gdf,
    prepare_gdf_for_athena,
    simplify_gdf,
    simplify_gdf_to_size,
)

__all__ = [
    "build_common_launch_args",
    "build_output_path",
    "build_report_parser",
    "derive_report_name",
    "est_rows_for_csv_file",
    "get_bounds_from_gdf",
    "init_report_logging",
    "parse_report_args",
    "prepare_gdf_for_athena",
    "query_attains_assessments",
    "report_main_wrapper",
    "round_down",
    "round_up",
    "simplify_gdf",
    "simplify_gdf_to_size",
]
