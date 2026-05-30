import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import get_env_or_confirm, get_include_pdf, get_unit_system


def main() -> list[str] | None:
    """The purpose of this function is to return an array of arguments that will satisfy the
    main() function in the report

    NOTE: YOU CAN BYPASS ALL THESE QUESTIONS BY SETTING ENVIRONMENT VARIABLES

    Environment variables that can be set:
        For all reports:
            DATA_ROOT - Path to the outputs folder. A subfolder rpt-riverscapes-inventory will be created if it does not exist (REQUIRED)
            UNIT_SYSTEM - unit system to use: "SI" or "imperial" (optional, default is "SI")
            INCLUDE_PDF - if 1 or true, adds --include-pdf FLAG TO include static html and PDF versions of the report (default is False)

        Report-specific variables:
            RSI_AOI_GEOJSON - path to the input geojson file for rpt-riverscapes-inventory (optional)
            RSI_REPORT_NAME - name for the report (optional)
            RSI_PARQUET_PATH - path to an existing Athena UNLOAD Parquet folder/file (optional)
            RSI_KEEP_PARQUET - set to '1' or 'true' to retain downloaded Parquet files (optional)
            RSI_NO_NID - set to '1' or 'true' to disable fetching NID data (optional)

    """

    data_root = os.environ.get("DATA_ROOT")
    if not data_root:
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))

    # IF we have everything we need from environment variables then we can skip the prompts
    env_aoi_geojson = os.environ.get("RSI_AOI_GEOJSON")
    if env_aoi_geojson:
        geojson_file = Path(env_aoi_geojson)
        if not geojson_file.exists():
            raise RuntimeError(colored(f"\nThe RSI_AOI_GEOJSON environment variable is set to '{os.environ.get('RSI_AOI_GEOJSON')}' but that file does not exist. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        # If it's not set we need to ask for it. We choose from a list of preset shapes in the code example folder
        base_dir = Path(__file__).parent
        example_dir = base_dir / "example"
        choices = sorted(p.name for p in example_dir.glob("*.geojson")) if example_dir.exists() and example_dir.is_dir() else []
        if not choices:
            raise RuntimeError(colored(f"\nNo example geojson files found in {example_dir}. Check this folder or set RSI_AOI_GEOJSON instead.\n", "red"))
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
    if not unit_system:
        return None

    if os.environ.get("RSI_REPORT_NAME"):
        report_name = os.environ.get("RSI_REPORT_NAME")
    else:
        report_name = geojson_file.stem.replace(' ', '_') + " - Riverscapes Inventory"

    # Ask for whether or not to include PDF. Default to NO
    include_pdf = get_include_pdf()
    if include_pdf is None:
        return None

    parquet_path = os.environ.get("RSI_PARQUET_PATH")
    if parquet_path and not Path(parquet_path).exists():
        raise RuntimeError(f"\nRSI_PARQUET_PATH is set to '{parquet_path}' but that path does not exist. Please fix or unset the variable.\n")

    if not parquet_path:
        parquet_prompt = questionary.text(
            message='Optional: path to the Parquet folder or file to use for results (leave blank to query Athena)',
            default="",
        ).ask()
        if parquet_prompt is None:
            return
        parquet_path = parquet_prompt.strip().strip('"').strip("'")

    keep_parquet_default = bool(parquet_path)
    keep_parquet = get_env_or_confirm(env_var="RSI_KEEP_PARQUET", message='Keep downloaded Parquet files after processing?', default=keep_parquet_default)
    if keep_parquet is None:
        return None

    # Ask to fetch NID data (default Yes)
    no_nid = get_env_or_confirm(env_var="RSI_NO_NID", message="Fetch data from National Inventory of Dams? (Default is Yes)", default=True)
    if no_nid is None:
        print("\nNo NID option selected. Exiting.\n")
        return None

    # ── Build args ────────────────────────────────────────────────────
    output_dir = Path(data_root) / "rpt-riverscapes-inventory" / report_name.replace(" ", "_")
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

    if no_nid:
        args.append("--no-nid")

    return args
