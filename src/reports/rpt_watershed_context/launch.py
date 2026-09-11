import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import get_include_pdf, get_unit_system

EXAMPLE_DIR = Path(__file__).resolve().parent / "examples"


def main():
    """The purpose of this function is to return an array of arguments that will satisfy the
    main() function in the report

    NOTE: YOU CAN BYPASS ALL THESE QUESTIONS BY SETTING ENVIRONMENT VARIABLES

    Environment variables that can be set:
        For all reports:
            DATA_ROOT - Path to the outputs folder. A subfolder rpt-rivers-need-space will be created if it does not exist (REQUIRED)
            UNIT_SYSTEM - unit system to use: "SI" or "imperial" (optional, default is "SI")
            INCLUDE_PDF - whether to include a PDF version of the report (optional, default is True)

        Report-specific variables:
            WS_AOI_GEOJSON - path to the input polygon file (geojson/shapefile) defining the AOI
            WS_REPORT_NAME - name for the report (optional)

    """

    data_root = os.environ.get("DATA_ROOT")
    if not data_root:
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))

    # IF we have everything we need from environment variables then we can skip the prompts

    # ── Unit system ───────────────────────────────────────────────────
    unit_system = get_unit_system()
    if unit_system is None:
        return None

    # Ask for whether or not to include PDF. Default to NO
    include_pdf = get_include_pdf()

    # ── AOI polygon ───────────────────────────────────────────────────
    aoi_env = os.environ.get("WS_AOI_GEOJSON")
    if aoi_env:
        aoi_path = Path(aoi_env)
        if not aoi_path.exists():
            raise RuntimeError(colored(f"\nThe WS_AOI_GEOJSON environment variable is set to '{aoi_env}' but that file does not exist. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        choices = sorted(p.name for p in EXAMPLE_DIR.glob("*.geojson")) if EXAMPLE_DIR.exists() and EXAMPLE_DIR.is_dir() else []
        selected_file = questionary.select("Select a geojson file to use as the AOI:", choices=choices).ask()
        if selected_file is None:
            print("\nNo geojson file selected. Exiting.\n")
            return None
        aoi_path = (EXAMPLE_DIR / selected_file).resolve()

    # ── Report name ───────────────────────────────────────────────────
    report_name = os.environ.get("WS_REPORT_NAME")
    if not report_name:
        report_name = aoi_path.stem.replace(' ', '_')

    # Create a clean, combined folder name for the report output
    report_folder_name = f"{report_name[:50].replace(' ', '_')}_{unit_system}"

    args = [
        Path(data_root) / "rpt-watershed-context" / report_folder_name,
        aoi_path,
        report_name,
        "--unit_system",
        unit_system,
    ]
    if include_pdf:
        args.append("--include_pdf")

    return args
