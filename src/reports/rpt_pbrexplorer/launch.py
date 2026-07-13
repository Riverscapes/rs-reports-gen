"""Interactive launcher for PBR Explorer

Created 2026-07-13.
Copied from the rpt_beaver_restoration_potential version
"""

from util.prompt import get_env_or_confirm, get_include_pdf, get_unit_system
from util.report_entrypoint import (
    build_common_launch_args,
    derive_report_name,
    prompt_geojson,
    prompt_raw_cache_dir,
    require_data_root,
)


def main() -> list[str] | None:
    """Collect launch arguments for reports.rpt_pbrexplorer.main."""
    data_root = require_data_root()

    geojson_file = prompt_geojson(env_var="PBR_AOI_GEOJSON")

    unit_system = get_unit_system()
    if unit_system is None:
        return None

    include_pdf = get_include_pdf()
    if include_pdf is None:
        return None

    raw_cache_path = prompt_raw_cache_dir(
        env_var="PBR_RAW_CACHE_PATH",
        prompt_message="Optional: path to cached page JSON directory (leave blank to query PBR API)",
    )
    keep_raw = get_env_or_confirm(
        env_var="PBR_KEEP_RAW",
        message="Keep downloaded raw files after processing?",
        default=bool(raw_cache_path),
    )
    if keep_raw is None:
        return None

    report_name = derive_report_name(
        geojson_file=geojson_file,
        suffix="pbrex",
        env_var="PBR_REPORT_NAME",
    )

    args = build_common_launch_args(
        data_root=data_root,
        report_slug="rpt-pbrexplorer",
        report_name=report_name,
        geojson_file=geojson_file,
        unit_system=unit_system,
        include_pdf=include_pdf,
        parquet_path="",
        keep_parquet=False,
    )
    if raw_cache_path:
        args += ["--use-raw-cache", raw_cache_path]
    if keep_raw:
        args.append("--keep-raw")
    return args
