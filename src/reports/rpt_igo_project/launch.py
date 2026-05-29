"""Interactive launcher for the IGO Scraper report.

Verifies environment variables, and prompts for AOI, etc.
"""

import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import is_truthy

EXAMPLE_DIR = Path(__file__).resolve().parent / "example"


def main() -> list[str] | None:
    """The purpose of this function is to return an array of arguments that will satisfy the
      main() function in the `rpt_igo_project` report

    NOTE: YOU CAN BYPASS ALL THESE QUESTIONS BY SETTING ENVIRONMENT VARIABLES

    Environment variables that *must* be set:
        SPATIALITE_PATH - path to the mod_spatialite library (REQUIRED)
        DATA_ROOT - Path to the outputs folder. A subfolder rpt-igo-project will be created if it does not exist (REQUIRED)

    Environment variables that may be set, bypassing prompts:
        IGO_AOI_GEOJSON - path to the input geojson file for rpt-igo-project (optional)
        IGO_REPORT_NAME - name for the report (optional)
        IGO_PARQUET_PATH - path to an existing Athena UNLOAD Parquet folder/file (optional)
        IGO_KEEP_PARQUET - set to '1' or 'true' to retain downloaded Parquet files (optional)

    """
    # ── DATA_ROOT (required) ──────────────────────────────────────────
    data_root = os.environ.get("DATA_ROOT")
    if not data_root:
        raise RuntimeError(
            colored(
                "\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n",
                "red",
            )
        )

    # ── SPATIALITE_PATH (required) ──────────────────────────────────────────
    spatialite_path = os.environ.get("SPATIALITE_PATH")
    if not spatialite_path:
        raise RuntimeError(
            "\nSPATIALITE_PATH environment variable is not set. Please set it in your .env file\n\n  e.g. (on Mac) SPATIALITE_PATH=/opt/homebrew/lib/mod_spatialite.8.dylib \n (on PC) SPATIALITE_PATH=C:\\OSGeo4W\\bin\\mod_spatialite.dll"
        )

    # ── AOI geojson ───────────────────────────────────────────────────
    # IF we have everything we need from environment variables then we can skip the prompts
    aoi_env = os.environ.get("IGO_AOI_GEOJSON")
    if aoi_env:
        geojson_file = Path(aoi_env)
        if not geojson_file.exists():
            raise RuntimeError(colored(f"\nThe IGO_AOI_GEOJSON environment variable is set to '{aoi_env}' but that file does not exist. Please fix or unset the variable to choose manually.\n", "red"))
    else:
        # If not set we need to ask for it. We choose from a list of preset shapes in the example folder
        choices = sorted(p.name for p in EXAMPLE_DIR.glob("*.geojson")) if EXAMPLE_DIR.exists() and EXAMPLE_DIR.is_dir() else []
        selected_file = questionary.select("Select a geojson file to use as the AOI:", choices=choices).ask()
        if selected_file is None:
            print("\nNo geojson file selected. Exiting.\n")
            return None
        geojson_file = (EXAMPLE_DIR / selected_file).resolve()

    # ── Report name ───────────────────────────────────────────────────
    report_name = os.environ.get("IGO_REPORT_NAME")
    if not report_name:
        report_name = geojson_file.stem.replace(' ', '_') + " - IGO Scrape"

    # ── Optional Parquet override ─────────────────────────────────────
    parquet_path = os.environ.get("IGO_PARQUET_PATH")
    if parquet_path and not Path(parquet_path).exists():
        raise RuntimeError(colored(f"\nIGO_PARQUET_PATH is set to '{parquet_path}' but that path does not exist. Please fix or unset the variable for interactive prompt.\n", "red"))
    if not parquet_path:
        parquet_prompt = questionary.text(
            message='Optional: path to notthe Parquet folder or file to use for results (leave blank to query Athena)',
            default="",
        ).ask()
        if parquet_prompt is None:
            print("\nCancelled. Exiting.\n")
            return None

        parquet_path = parquet_prompt.strip().strip('"').strip("'")

    # ── Keep parquet ──────────────────────────────────────────────────
    keep_parquet_env = os.environ.get("IGO_KEEP_PARQUET")
    if keep_parquet_env is not None:
        keep_parquet = is_truthy(keep_parquet_env)
    else:
        keep_parquet = questionary.confirm(message='Keep downloaded Parquet files after processing?', default=False).ask()
        if keep_parquet is None:
            print("\nCancelled. Exiting.\n")
            return None

    # The final argument array we pass back
    args = [
        spatialite_path,
        os.path.join(data_root, "rpt-igo-project", report_name.replace(" ", "_")),
        geojson_file,
        report_name,
    ]

    if parquet_path:
        args.append("--use-parquet")
        args.append(parquet_path)

    if keep_parquet:
        args.append("--keep-parquet")

    return args
