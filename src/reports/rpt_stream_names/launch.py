import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import get_include_pdf


def main() -> list[str] | None:
    """The purpose of this function is to return an array of arguments that will satisfy the
    main() function in the report

    NOTE: YOU CAN BYPASS ALL THESE QUESTIONS BY SETTING ENVIRONMENT VARIABLES

    Environment variables that can be set:
        For all reports:
            DATA_ROOT - Path to the outputs folder. A subfolder rpt-rivers-need-space will be created if it does not exist (REQUIRED)
            UNIT_SYSTEM - unit system to use: "SI" or "imperial" (optional, default is "SI") NOT USED IN THIS REPORT
            INCLUDE_PDF - whether to include a PDF version of the report (optional, default is True)

        Report-specific variables:
            RSN_AOI_GEOJSON - path to the input geojson file for rpt-rivers-need-space (optional)
            RSI_REPORT_NAME - name for the report (optional)
            RSI_CSV - optional path to a CSV file to use instead of querying Athena (optional)


    """

    data_root = os.environ.get("DATA_ROOT")
    if not data_root:
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))

    # IF we have everything we need from environment variables then we can skip the prompts
    env_aoi_geojson = os.environ.get("RSN_AOI_GEOJSON")
    if env_aoi_geojson:
        geojson_file = Path(env_aoi_geojson)
        if not geojson_file.exists():
            raise RuntimeError(colored(f"\nThe RSN_AOI_GEOJSON environment variable is set to '{env_aoi_geojson}' but that file does not exist. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        # If it's not set we need to ask for it. We choose from a list of preset shapes in the example folder
        base_dir = os.path.dirname(__file__)
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

    env_csv_file = os.environ.get("RSI_CSV")
    if env_csv_file:
        csv_file = Path(env_csv_file)
        if not csv_file.exists():
            raise RuntimeError(colored(f"\nThe RSI_CSV environment variable is set to '{env_csv_file}' but that file does not exist. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        # No CSV file provided. Ask for an optional csv path
        csv_file = questionary.text(
            message="Optional: Enter a path to a CSV file to use for results (leave blank to query Athena)",
            default="",
        ).ask()
        # Strip leading/trailing quotes if present
        if csv_file is not None:
            csv_file = csv_file.strip().strip('"').strip("'")

    report_name = os.environ.get("RSI_REPORT_NAME")
    if not report_name:
        report_name = geojson_file.stem.replace(' ', '_') + " - Riverscapes Stream Names"

    # Ask for whether or not to include PDF. Default to NO
    include_pdf = get_include_pdf()
    if include_pdf is None:
        return None

    output_dir = Path(data_root) / "rpt-riverscapes-stream-names", report_name.replace(" ", "_"))
    args = [
        output_dir,
        geojson_file,
        report_name,
    ]
    if include_pdf:
        args.append("--include_pdf")
    if csv_file:
        args.append("--csv")
        args.append(csv_file)

    return args
