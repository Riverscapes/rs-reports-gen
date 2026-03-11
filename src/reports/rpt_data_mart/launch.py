"""Interactive launcher for the Data Mart export.

Prompts for AOI, unit system, and Parquet options, then returns
an argv-style list that satisfies ``rpt_data_mart.main.main()``.

Environment variables can bypass every prompt — see the docstring on ``main()``.

Copilot-generated module.
"""

import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import is_truthy

EXAMPLE_DIR = Path(__file__).resolve().parent / "example"


def main() -> list[str] | None:
    """Gather CLI arguments interactively (or from env vars) for ``rpt_data_mart``.

    Environment variables that skip prompts:
        DATA_ROOT         – root output folder (required)
        DM_AOI_GEOJSON    – path to an AOI geojson file
        DM_REPORT_NAME    – human-readable export name
        UNIT_SYSTEM       – "SI" or "imperial"
        DM_PARQUET_PATH   – reuse an existing Parquet folder/file
        DM_KEEP_PARQUET   – "1"/"true" to keep staging Parquet

    Returns:
        List of string arguments, or ``None`` if the user cancels.

    Copilot-generated function.
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

    # ── AOI geojson ───────────────────────────────────────────────────
    aoi_env = os.environ.get("DM_AOI_GEOJSON")
    if aoi_env:
        geojson_file = Path(aoi_env)
        if not geojson_file.exists():
            raise RuntimeError(colored(f"\nDM_AOI_GEOJSON is set to '{aoi_env}' but that file does not exist.\n", "red"))
    else:
        choices = sorted(p.name for p in EXAMPLE_DIR.glob("*.geojson")) if EXAMPLE_DIR.exists() else []
        if not choices:
            raise RuntimeError(colored(f"\nNo example geojson files found in {EXAMPLE_DIR}. Set DM_AOI_GEOJSON instead.\n", "red"))
        selected = questionary.select("Select a geojson file to use as the AOI:", choices=choices).ask()
        if selected is None:
            print("\nNo geojson file selected. Exiting.\n")
            return None
        geojson_file = (EXAMPLE_DIR / selected).resolve()

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
    report_name = os.environ.get("DM_REPORT_NAME")
    if not report_name:
        report_name = geojson_file.stem.replace(" ", "_") + " - Data Mart"

    # ── Optional Parquet override ─────────────────────────────────────
    parquet_path = os.environ.get("DM_PARQUET_PATH")
    if parquet_path and not Path(parquet_path).exists():
        raise RuntimeError(colored(f"\nDM_PARQUET_PATH is set to '{parquet_path}' but that path does not exist.\n", "red"))
    if not parquet_path:
        parquet_path = questionary.text(
            "Optional: path to Parquet folder/file (leave blank to query Athena):",
            default="",
        ).ask()
        if parquet_path is None:
            print("\nCancelled. Exiting.\n")
            return None
        parquet_path = parquet_path.strip().strip('"').strip("'")

    # ── Keep parquet ──────────────────────────────────────────────────
    keep_env = os.environ.get("DM_KEEP_PARQUET")
    if keep_env is not None:
        keep_parquet = is_truthy(keep_env)
    elif parquet_path:
        keep_parquet = True
    else:
        keep_parquet = questionary.confirm("Keep downloaded Parquet files after processing?", default=False).ask()
        if keep_parquet is None:
            print("\nCancelled. Exiting.\n")
            return None

    # ── Build argument list ───────────────────────────────────────────
    output_dir = os.path.join(data_root, "rpt-data-mart", report_name.replace(" ", "_"))

    args: list[str] = [
        output_dir,
        str(geojson_file),
        report_name,
        "--unit_system",
        unit_system,
    ]

    if parquet_path:
        args += ["--use-parquet", parquet_path]

    if keep_parquet:
        args.append("--keep-parquet")

    return args
