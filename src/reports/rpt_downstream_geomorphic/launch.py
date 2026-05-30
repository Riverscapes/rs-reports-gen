"""Interactive launcher for the Downstream Geomorphic report.

Prompts for AOI, unit system, and Parquet options, then returns an argv-style
list that satisfies ``rpt_downstream_geomorphic.main.main()``.

Designed to be called by ``scripts/report_launcher.py``.

Environment variables can bypass every prompt — see the docstring on ``main()``.

Copilot-generated module.
"""

import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import is_truthy, get_include_pdf, get_unit_system

EXAMPLE_DIR = Path(__file__).resolve().parent / "example"


def main() -> list[str] | None:
    """Gather CLI arguments interactively (or from env vars) for ``rpt_downstream_geomorphic``.

    Environment variables that skip prompts:
        DATA_ROOT              - root output folder (required)
        DG_REPORT_NAME         - human-readable report name (optional)
        UNIT_SYSTEM            - "SI" or "imperial" (optional, default "SI")
        INCLUDE_PDF            - "1"/"true" to include a PDF version (optional)
        DG_PARQUET_PATH        - reuse an existing Parquet folder/file (optional)
        DG_KEEP_PARQUET        - "1"/"true" to keep staging Parquet (optional)

    Returns:
        List of string arguments, or ``None`` if the user cancels.

    Copilot-generated function.
    """

    # ── DATA_ROOT (required) ──────────────────────────────────────────
    data_root = os.environ.get("DATA_ROOT")
    if not data_root:
        raise RuntimeError(colored("\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n", "red"))

    # ── Unit system ───────────────────────────────────────────────────
    unit_system = get_unit_system()
    if unit_system is None:
        return None

    # -- Mode TODO
    mode = 'whole-lp'
    # -- level path
    level_path = questionary.text(
        "Level path",
        default='55001200094822',
    ).ask()

    # ── Report name ───────────────────────────────────────────────────
    report_name = os.environ.get("DG_REPORT_NAME")
    if not report_name:
        report_name = str(mode) + "_" + level_path + " - Downstream Geomorphic"

    # ── Optional Parquet override ─────────────────────────────────────
    parquet_path = os.environ.get("DG_PARQUET_PATH")
    if parquet_path and not Path(parquet_path).exists():
        raise RuntimeError(colored(f"\nDG_PARQUET_PATH is set to '{parquet_path}' but that path does not exist.\n", "red"))
    if not parquet_path:
        parquet_path = questionary.text(
            "Optional: path to Parquet folder/file (leave blank to query Athena):",
            default="",
        ).ask()
        if parquet_path is None:
            print("\nCancelled. Exiting.\n")
            return None
        parquet_path = parquet_path.strip().strip('"').strip("'")

    # ── Include PDF ───────────────────────────────────────────────────
    include_pdf = get_include_pdf()
    if include_pdf is None:
        return None

    # ── Keep parquet ──────────────────────────────────────────────────
    keep_parquet_env = os.environ.get("DG_KEEP_PARQUET")
    if keep_parquet_env is not None:
        keep_parquet = is_truthy(keep_parquet_env)
    elif parquet_path:
        keep_parquet = True
    else:
        keep_parquet = questionary.confirm("Keep downloaded Parquet files after processing?", default=False).ask()
        if keep_parquet is None:
            print("\nCancelled. Exiting.\n")
            return None

    # ── Build args ────────────────────────────────────────────────────
    output_dir = os.path.join(data_root, "rpt-downstream-geomorphic", report_name.replace(" ", "_"))

    args: list[str] = [
        output_dir,
        level_path,
        report_name,
        "--unit_system",
        unit_system,
    ]
    if include_pdf:
        args.append("--include_pdf")
    if parquet_path:
        args += ["--use-parquet", parquet_path]
    if keep_parquet:
        args.append("--keep-parquet")

    return args
