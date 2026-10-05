import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import get_include_pdf
from util.report_entrypoint import prompt_geojson


def normalize_guessed_name(raw_guess: str | None) -> str:
    """Normalize optional user guess input for CLI forwarding.

    Treats blank and single-dot placeholder values as empty.

    Args:
        raw_guess (str | None): Raw prompt input.

    Returns:
        str: Cleaned guess or empty string.

    Created by copilot.
    """
    if raw_guess is None:
        return ""

    cleaned = raw_guess.strip().strip('"').strip("'").strip()
    if cleaned in {"", "."}:
        return ""

    return cleaned


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

    geojson_file = prompt_geojson(env_var="RSN_AOI_GEOJSON")

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
    report_name = os.environ.get("RSI_REPORT_NAME")
    if not report_name:
        report_name = geojson_file.stem.replace(' ', '_')

    # ── Stream name Guess──────────────────────────────────────────────
    guessed_name = normalize_guessed_name(questionary.text(message="What name do you think is most common in this area?", default="").ask())

    # ── Include PDF ───────────────────────────────────────────────────
    # Ask for whether or not to include PDF. Default to NO
    include_pdf = get_include_pdf()
    if include_pdf is None:
        return None

    output_dir = Path(data_root) / "rpt-riverscapes-stream-names" / report_name.replace(" ", "_")
    args = [
        output_dir,
        geojson_file,
        report_name,
        "--unit_system",
        unit_system,
    ]
    if guessed_name:
        args.append("--guessed_name")
        args.append(guessed_name)
    if include_pdf:
        args.append("--include_pdf")
    if csv_file:
        args.append("--csv")
        args.append(csv_file)

    return args
