import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import get_include_pdf, get_unit_system, is_truthy


def main() -> list[str] | None:
    """The purpose of this function is to return an array of arguments that will satisfy the
    main() function in the report

    NOTE: YOU CAN BYPASS ALL THESE QUESTIONS BY SETTING ENVIRONMENT VARIABLES

    Environment variables that can be set:
        For all reports:
            DATA_ROOT - Path to the outputs folder. A subfolder rpt-rivers-need-space will be created if it does not exist (REQUIRED)
            UNIT_SYSTEM - unit system to use: "SI" or "imperial" (optional, default is "SI")
            INCLUDE_PDF - whether to include a PDF version of the report (optional, default is True)

        Report-specific variables:
            RDYN_AOI_GEOJSON - path to the input geojson file for rpt-rivers-need-space (optional)
            RDYN_REPORT_NAME - name for the report (optional)
            RDYN_PARQUET_PATH - path to an existing Athena UNLOAD Parquet folder/file (optional)
            RDYN_KEEP_PARQUET - set to '1' or 'true' to retain downloaded Parquet files (optional)

    """

    if not os.environ.get("DATA_ROOT"):
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))
    data_root = os.environ.get("DATA_ROOT")

    # IF we have everything we need from environment variables then we can skip the prompts
    env_aoi_geojson = os.environ.get("RDYN_AOI_GEOJSON")
    if env_aoi_geojson:
        geojson_file = Path(env_aoi_geojson)
        if not geojson_file.exists():
            raise RuntimeError(colored(f"\nThe RDYN_AOI_GEOJSON environment variable is set to '{os.environ.get('RDYN_AOI_GEOJSON')}' but that file does not exist. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        # If it's not set we need to ask for it. We choose from a list of preset shapes in the code example folder
        base_dir = Path(__file__).parent
        example_dir = base_dir / "example"
        choices = sorted(p.name for p in example_dir.glob("*.geojson")) if example_dir.exists() and example_dir.is_dir() else []
        if not choices:
            raise RuntimeError(colored(f"\nNo example geojson files found in {example_dir}. Check this folder or set RDYN_AOI_GEOJSON instead.\n", "red"))
        selected = questionary.select(
            message="Select a geojson file to use as the AOI",
            choices=choices,
        ).ask()
        if selected is None:
            print("\nNo geojson file selected. Exiting.\n")
            return None
        geojson_file = (example_dir / selected).resolve()

    # ── Unit system ───────────────────────────────────────────────────
    unit_system = get_unit_system()
    if unit_system is None:
        return None

    report_name = os.environ.get("RDYN_REPORT_NAME")
    if not report_name:
        report_name = geojson_file.stem.replace(' ', '_') + " - Riverscapes Dynamics"

    # Ask for whether or not to include PDF. Default to NO
    include_pdf = get_include_pdf()
    if include_pdf is None:
        return None

    parquet_path = os.environ.get("RDYN_PARQUET_PATH")
    if parquet_path and not Path(parquet_path).exists():
        raise RuntimeError(f"\nRDYN_PARQUET_PATH is set to '{parquet_path}' but that path does not exist. Please fix or unset the variable.\n")

    if not parquet_path:
        parquet_prompt = questionary.text(
            message='Optional: path to the Parquet folder or file to use for results (leave blank to query Athena)',
            default="",
        ).ask()
        if parquet_prompt is None:
            return None
        parquet_path = parquet_prompt.strip().strip('"').strip("'")

    keep_parquet_env = os.environ.get("RDYN_KEEP_PARQUET")
    if keep_parquet_env is not None:
        keep_parquet = is_truthy(keep_parquet_env)
    elif parquet_path:
        # if we were supplied Parquet let's assume we want to keep it
        keep_parquet = True
    else:
        keep_parquet = questionary.confirm(
            message='Keep downloaded Parquet files after processing?',
            default=False,
        )
        if keep_parquet is None:
            return None

    # ── Build args ────────────────────────────────────────────────────
    output_dir = Path(data_root) / "rpt-riverscapes-dynamics" / report_name.replace(" ", "_")
    args = [
        output_dir,
        geojson_file,
        report_name,
        "--unit_system",
        unit_system,
    ]
    if include_pdf:
        args.append("--include_pdf")

    if parquet_path:
        args.append("--use-parquet")
        args.append(parquet_path)

    if keep_parquet:
        args.append("--keep-parquet")

    return args
