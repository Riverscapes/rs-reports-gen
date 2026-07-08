"""Interactive launcher for Beaver Restoration Potential.

Created 2026-07-07.
Created by copilot.
"""

from util.prompt import get_env_or_confirm, get_include_pdf, get_unit_system
from util.report_entrypoint import (
    build_common_launch_args,
    derive_report_name,
    prompt_geojson,
    prompt_parquet,
    require_data_root,
)


def main() -> list[str] | None:
    """Collect launch arguments for reports.rpt_beaver_restoration_potential.main."""
    data_root = require_data_root()

    geojson_file = prompt_geojson(env_var="BRP_AOI_GEOJSON")

    unit_system = get_unit_system()
    if unit_system is None:
        return None

    include_pdf = get_include_pdf()
    if include_pdf is None:
        return None

    parquet_path = prompt_parquet(env_var="BRP_PARQUET_PATH")
    keep_parquet = get_env_or_confirm(
        env_var="BRP_KEEP_PARQUET",
        message="Keep downloaded Parquet files after processing?",
        default=bool(parquet_path),
    )
    if keep_parquet is None:
        return None

    report_name = derive_report_name(
        geojson_file=geojson_file,
        suffix="Beaver Restoration Potential",
        env_var="BRP_REPORT_NAME",
    )

    return build_common_launch_args(
        data_root=data_root,
        report_slug="rpt-beaver-restoration-potential",
        report_name=report_name,
        geojson_file=geojson_file,
        unit_system=unit_system,
        include_pdf=include_pdf,
        parquet_path=parquet_path,
        keep_parquet=keep_parquet,
    )
