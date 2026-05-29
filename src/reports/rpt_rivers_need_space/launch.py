import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import is_truthy


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
            RNS_AOI_GEOJSON - path to the input geojson file for rpt-rivers-need-space (optional)
            RNS_REPORT_NAME - name for the report (optional)
            RNS_PARQUET_PATH = path to an existing Athena UNLOAD Parquet folder/file (optional)
            RNS_KEEP_PARQUET = set to '1' or 'true' to retain downloaded Parquet files (optional)

    """

    data_root = os.environ.get("DATA_ROOT")
    if not data_root:
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))
    data_root = Path(data_root)

    # IF we have everything we need from environment variables then we can skip the prompts
    # ── AOI geojson ───────────────────────────────────────────────────
    rns_aoi_geojson = os.environ.get("RNS_AOI_GEOJSON")
    if rns_aoi_geojson:
        geojson_file = Path(rns_aoi_geojson)
        if not geojson_file.exists():
            raise RuntimeError(colored(f"\nThe RNS_AOI_GEOJSON environment variable is set to '{rns_aoi_geojson}' but that file does not exist. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        # If it's not set we need to ask for it. We choose from a list of preset shapes in the code example folder
        example_dir = Path(__file__).parent / "example"
        choices = sorted(p.name for p in example_dir.glob("*.geojson")) if example_dir.exists() and example_dir.is_dir() else []
        selected_geojson = questionary.select(
            message="Select a geojson file to use as the AOI",
            choices=choices,
        ).ask()
        if selected_geojson is None:
            print("\nNo geojson file selected. Exiting.\n")
            exit(0)
        geojson_file = example_dir / selected_geojson.resolve()

    # ── Optional Parquet override ─────────────────────────────────────
    parquet_path = os.environ.get("RNS_PARQUET_PATH")
    if parquet_path and not Path(parquet_path).exists():
        raise RuntimeError(f"\nRNS_PARQUET_PATH is set to '{parquet_path}' but that path does not exist. Please fix or unset the variable.\n")

    if not parquet_path:
        parquet_prompt = questionary.text(
            message='Optional: path to the Parquet folder or file to use for results (leave blank to query Athena)',
            default="",
        ).ask()
        if parquet_prompt is None:
            print("\nCancelled. Exiting.\n")
            return None
        parquet_path = parquet_prompt.strip().strip('"').strip("'")

    # ── Unit system ───────────────────────────────────────────────────
    unit_env = os.environ.get("UNIT_SYSTEM")
    if unit_env:
        if unit_env not in ("SI", "imperial"):
            raise RuntimeError(colored(f"\nUNIT_SYSTEM must be 'SI' or 'imperial', got '{unit_env}'.\n", "red"))
        unit_system = unit_env
    else:
        unit_system = questionary.select("Select a unit system:", choices=["SI", "imperial"], default="SI").ask()
        if unit_system is None:
            print("\nNo unit system selected. Exiting.\n")
            return None

    # ── Report name ───────────────────────────────────────────────────
    report_name = os.environ.get("RNS_REPORT_NAME")
    if not report_name:
        report_name = geojson_file.stem.replace(' ', '_') + " - Rivers Need Space"

    # ── Include PDF ───────────────────────────────────────────────────
    # Ask for whether or not to include PDF. Default to NO
    include_pdf_env = os.environ.get("INCLUDE_PDF", None)
    if include_pdf_env is not None:
        include_pdf = is_truthy(include_pdf_env)
    else:
        include_pdf = questionary.confirm(message="Include a PDF version of the report?", default=False).ask()
        if include_pdf is None:
            print("\nCancelled. Exiting.\n")
            return None

    # ── Keep parquet ──────────────────────────────────────────────────
    keep_parquet_env = os.environ.get("RSI_KEEP_PARQUET")
    if keep_parquet_env is not None:
        keep_parquet = is_truthy(keep_parquet_env)
    elif parquet_path:
        # if we were supplied Parquet let's assume we want to keep it
        keep_parquet = True
    else:
        keep_parquet = questionary.confirm(
            message='Keep downloaded Parquet files after processing?',
            default=False,
        ).ask()

        if keep_parquet is None:
            print("\nCancelled. Exiting.\n")
            return

    # ── Build args ────────────────────────────────────────────────────
    args = [
        Path(data_root / "rpt-rivers-need-space" / report_name.replace(" ", "_")),
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
