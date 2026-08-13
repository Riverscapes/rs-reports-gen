"""Interactive launcher configuration for the Inventory of Resources report.

This module is called by ``scripts/report_launcher.py`` to gather the command
line arguments required by the report entry point.

Created 2026-08-13.
Created by copilot.
"""

import os
from pathlib import Path

import questionary
from termcolor import colored

from util.prompt import get_include_pdf


def _get_aoi_file() -> Path | None:
    """Return an AOI from the environment or the report example directory.

    Returns:
        The selected AOI path, or ``None`` when interactive selection is
        cancelled.

    Created by copilot.
    """
    configured_path = os.environ.get("IOR_AOI_GEOJSON")
    if configured_path:
        aoi_file = Path(configured_path)
        if not aoi_file.exists():
            raise RuntimeError(colored(f"\nIOR_AOI_GEOJSON is set to '{aoi_file}', but that file does not exist.\n", "red"))
        return aoi_file

    example_dir = Path(__file__).parent / "example"
    choices = sorted(path.name for path in example_dir.glob("*.geojson"))
    if not choices:
        raise RuntimeError(colored(f"\nNo example GeoJSON files found in {example_dir}. Set IOR_AOI_GEOJSON instead.\n", "red"))
    selected = questionary.select("Select a GeoJSON file to use as the AOI", choices=choices).ask()
    return (example_dir / selected).resolve() if selected else None


def main() -> list[str] | None:
    """Gather report arguments interactively or from environment variables.

    Environment variables:
        DATA_ROOT: Required root directory for generated outputs.
        IOR_AOI_GEOJSON: Optional AOI GeoJSON path.
        IOR_REPORT_NAME: Optional output/report name.
        IOR_CSV: Optional inventory CSV to use instead of Athena.
        INCLUDE_PDF: Set to ``1`` or ``true`` to generate static HTML and PDF.

    Returns:
        Arguments for ``reports.rpt_inventory_of_resources.main.main()``, or
        ``None`` when the user cancels setup.

    Created by copilot.
    """
    data_root = os.environ.get("DATA_ROOT")
    if not data_root:
        raise RuntimeError(
            colored(
                "\nDATA_ROOT environment variable is not set. Add it to .env, for example DATA_ROOT=/workspaces/rs-reports-gen/output\n",
                "red",
            )
        )

    aoi_file = _get_aoi_file()
    if aoi_file is None:
        return None

    csv_path = os.environ.get("IOR_CSV")
    if csv_path:
        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise RuntimeError(colored(f"\nIOR_CSV is set to '{csv_file}', but that file does not exist.\n", "red"))
    else:
        entered_path = questionary.text("Optional: inventory CSV to use instead of querying Athena", default="").ask()
        if entered_path is None:
            return None
        csv_file = Path(entered_path.strip().strip('"').strip("'")) if entered_path.strip() else None
        if csv_file and not csv_file.exists():
            raise RuntimeError(colored(f"\nThe supplied CSV file '{csv_file}' does not exist.\n", "red"))

    report_name = os.environ.get("IOR_REPORT_NAME") or f"{aoi_file.stem} - Inventory of Resources"
    include_pdf = get_include_pdf()
    if include_pdf is None:
        return None

    args = [
        str(Path(data_root) / "rpt-inventory-of-resources" / report_name.replace(" ", "_")),
        str(aoi_file),
        report_name,
    ]
    if csv_file:
        args.extend(["--csv", str(csv_file)])
    if include_pdf:
        args.append("--include-pdf")
    return args
