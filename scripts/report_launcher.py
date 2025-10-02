"""Interactive launcher for Riverscapes reports.

This script scans the local ``reports`` package for report modules
with a ``main.py`` entry point and lets the user choose which one to run. It
then executes the selected report module as if it were launched with
``python -m``. Any command-line arguments provided to this launcher are passed
through to the underlying report. If no arguments are provided, the launcher
will prompt for them interactively.
"""

from __future__ import annotations
from typing import Iterable
from pathlib import Path
from dataclasses import dataclass
from importlib import import_module
import sys
import shlex
import traceback
import logging
import inquirer
from termcolor import colored

BASE_PACKAGE = "reports"
SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
REPORTS_DIR = SRC_ROOT / "reports"


@dataclass
class ReportEntry:
    """Container describing a launchable report module."""
    name: str
    module_path: str
    display_name: str
    launch_path: str


def iter_reports(directory: Path) -> Iterable[ReportEntry]:
    """Yield all report packages that provide a ``main.py`` entry point AND a launch.py."""

    if not directory.exists():
        raise RuntimeError(f"Reports directory not found: {directory}")

    for child in sorted(directory.iterdir()):
        if not child.is_dir():
            continue
        if not (child / "__init__.py").exists():
            continue
        if not (child / "main.py").exists():
            continue
        if not (child / "launch.py").exists():
            continue

        module_path = f"{BASE_PACKAGE}.{child.name}.main"
        launch_path = f"{BASE_PACKAGE}.{child.name}.launch"
        base_name = child.name
        if base_name.startswith("rpt_"):
            base_name = base_name[4:]
        display = base_name.replace("_", " ").title()
        yield ReportEntry(name=child.name, module_path=module_path, display_name=display, launch_path=launch_path)


def choose_report() -> ReportEntry:
    """Prompt the user to pick a report, falling back to manual input."""
    reports = list(iter_reports(REPORTS_DIR))

    if not reports:
        print("No launchable reports were found.")
        return None

    # Track the canonical package name so inquirer can return a stable value.
    question_choices: list[tuple[str, ReportEntry]] = [
        ("📋 " + entry.display_name, entry) for entry in reports
    ]

    try:
        answer = inquirer.prompt([
            inquirer.List(
                "report",
                message="Select a report to launch",
                choices=question_choices,
            )
        ])
    except Exception:
        answer = None

    return answer.get("report")


def gather_arguments(entry: ReportEntry) -> list[str]:
    """Collect arguments to forward to the selected report."""

    if len(sys.argv) > 1:
        return sys.argv[1:]

    defaults: list[str] = []
    try:
        module = import_module(entry.launch_path)
        env_defaults = getattr(module, "main", None)
        if callable(env_defaults):
            defaults = [str(arg) for arg in env_defaults()]
    except Exception as exc:  # pragma: no cover - defensive safety net
        print(colored(exc, "red"))
        sys.exit(1)

    return defaults


def launch_report(entry: ReportEntry, extra_args: list[str]) -> int:
    """Execute the report module within this process and return its exit code."""

    module_path = entry.module_path
    src_path = str(SRC_ROOT)

    # Now run the main() function of the selected report module.
    sys.argv = [module_path] + extra_args
    sys.path.insert(0, src_path)

    module = import_module(entry.module_path)

    print(f"Launching report: {entry.display_name}")
    print(f"Module path: {entry.module_path}")
    print(f"Arguments: {' '.join(shlex.quote(arg) for arg in sys.argv)}")

    # Run the main() function from the imported module
    logging.getLogger("LOGGER").propagate = False

    if hasattr(module, "main") and callable(module.main):
        try:
            result = module.main()
            # If main returns an int, use it as exit code
            if isinstance(result, int):
                return result
            return 0
        except SystemExit as exc:
            # Handle sys.exit() calls in the report
            return exc.code if isinstance(exc.code, int) else 1
        except Exception as exc:
            print(colored(f"Error running report: {exc}", "red"))
            return 1
    else:
        print(colored(f"Module {entry.module_path} does not have a callable main() function.", "red"))
        return 1


def main() -> int:
    """ Main entry point for the report launcher.

    Returns:
        int: Exit code from the launched report, or 0 if no report was run.
    """

    selection = choose_report()
    if selection is None:
        print("No report selected. Exiting.")
        return 0

    extra_args = gather_arguments(selection)
    return launch_report(selection, extra_args)


if __name__ == "__main__":
    sys.exit(main())
