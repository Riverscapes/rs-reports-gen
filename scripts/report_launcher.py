"""Interactive launcher for Riverscapes reports.

This script scans the local ``reports`` package for report modules
with a ``main.py`` entry point and lets the user choose which one to run. It
then executes the selected report module as if it were launched with
``python -m``. Any command-line arguments provided to this launcher are passed
through to the underlying report. If no arguments are provided, the launcher
will prompt for them interactively.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from importlib import import_module
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import inquirer


BASE_PACKAGE = "reports"
SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
REPORTS_DIR = SRC_ROOT / "reports"


@dataclass
class ReportEntry:
    """Container describing a launchable report module."""

    name: str
    module_path: str
    display_name: str


def iter_reports(directory: Path) -> Iterable[ReportEntry]:
    """Yield all report packages that provide a ``main.py`` entry point."""

    if not directory.exists():
        raise RuntimeError(f"Reports directory not found: {directory}")

    for child in sorted(directory.iterdir()):
        if not child.is_dir():
            continue
        if not (child / "__init__.py").exists():
            continue
        if not (child / "main.py").exists():
            continue

        module_path = f"{BASE_PACKAGE}.{child.name}.main"
        base_name = child.name
        if base_name.startswith("rpt_"):
            base_name = base_name[4:]
        display = base_name.replace("_", " ").title()
        yield ReportEntry(name=child.name, module_path=module_path, display_name=display)


def choose_report(reports: list[ReportEntry]) -> ReportEntry | None:
    """Prompt the user to pick a report, falling back to manual input."""

    if not reports:
        print("No launchable reports were found.")
        return None

    # Track the canonical package name so inquirer can return a stable value.
    choice_lookup = {entry.name: entry for entry in reports}
    question_choices: list[tuple[str, str]] = [
        ("📋 " + entry.display_name, entry.name) for entry in reports
    ]

    question = [
        inquirer.List(
            "report",
            message="Select a report to launch",
            choices=question_choices,
        )
    ]

    try:
        answer = inquirer.prompt(question)
    except Exception:
        answer = None

    if isinstance(answer, dict):
        selection = answer.get("report")
        if isinstance(selection, ReportEntry):
            return selection
        # ``inquirer`` may return a Choice wrapper or None when cancelled.
        if selection is None:
            return None
        choice_value = getattr(selection, "value", None)
        if isinstance(choice_value, ReportEntry):
            return choice_value
        if isinstance(choice_value, str):
            resolved = choice_lookup.get(choice_value)
            if resolved is not None:
                return resolved
        if isinstance(selection, str):
            resolved = choice_lookup.get(selection)
            if resolved is not None:
                return resolved

    # Plain-text fallback when inquirer is unavailable or fails.
    print("Select a report to launch:")
    for idx, entry in enumerate(reports, start=1):
        print(f"  {idx}. {entry.display_name} ({entry.name})")
    print("  0. Cancel")

    while True:
        choice = input("Enter choice: ").strip()
        if choice in {"0", "q", "Q"}:
            return None
        if choice.isdigit():
            index = int(choice) - 1
            if 0 <= index < len(reports):
                return reports[index]
        print("Invalid selection, please try again.")


def gather_arguments(entry: ReportEntry) -> list[str]:
    """Collect arguments to forward to the selected report."""

    if len(sys.argv) > 1:
        return sys.argv[1:]

    defaults: list[str] = []
    try:
        module = import_module(entry.module_path)
        env_defaults = getattr(module, "env_launch_params", None)
        if callable(env_defaults):
            defaults = [str(arg) for arg in env_defaults()]
    except Exception as exc:  # pragma: no cover - defensive safety net
        print(f"Warning: unable to load env_launch_params from {entry.module_path}: {exc}")

    if not defaults:
        raw = input("Additional arguments for the report (press Enter to skip): ").strip()
        return shlex.split(raw) if raw else []

    print("Press Enter to accept the default value in brackets.")
    final_args: list[str] = []
    for idx, resolved in enumerate(defaults, start=1):
        prompt = f"Argument {idx} [{resolved}]: "
        response = input(prompt).strip()
        final_args.append(response or resolved)

    extra = input("Additional arguments (space separated, Enter to skip): ").strip()
    if extra:
        final_args.extend(shlex.split(extra))

    return final_args


def launch_report(entry: ReportEntry, extra_args: list[str]) -> int:
    """Execute the report module in a subprocess and return its exit code."""

    env = os.environ.copy()
    existing_path = env.get("PYTHONPATH")
    pythonpath = str(SRC_ROOT)
    if existing_path:
        pythonpath = os.pathsep.join([pythonpath, existing_path])
    env["PYTHONPATH"] = pythonpath

    cmd = [sys.executable, "-m", entry.module_path, *extra_args]
    return subprocess.call(cmd, env=env)


def main() -> int:
    """ Main entry point for the report launcher.

    Returns:
        int: Exit code from the launched report, or 0 if no report was run.
    """
    reports = list(iter_reports(REPORTS_DIR))
    selection = choose_report(reports)
    if selection is None:
        print("No report selected. Exiting.")
        return 0

    extra_args = gather_arguments(selection)
    return launch_report(selection, extra_args)


if __name__ == "__main__":
    sys.exit(main())
