"""Shared helpers for report CLI entry points and interactive launchers.

Reduces boilerplate across report ``main()`` and ``launch.main()`` functions
by extracting the patterns that are repeated in every report module.

Copilot-generated module.

Usage in a report ``main.py``::

    from util.report_entrypoint import build_report_parser, init_report_logging, report_main_wrapper

    def main():
        parser = build_report_parser()
        # add report-specific args here
        args, output_path = parse_report_args(parser)
        log = init_report_logging(output_path, "rpt-my-report")
        report_main_wrapper(log, lambda: my_orchestrator(args, output_path, log))

Usage in a report ``launch.py``::

    from util.report_entrypoint import (
        require_data_root, prompt_geojson, prompt_unit_system,
        prompt_include_pdf, prompt_parquet, prompt_keep_parquet,
        derive_report_name, build_output_path,
    )
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psutil
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

# ---------------------------------------------------------------------------
# main.py helpers
# ---------------------------------------------------------------------------


def build_report_parser(description: str = "Riverscapes report") -> argparse.ArgumentParser:
    """Create an ArgumentParser pre-loaded with the common positional and optional args.

    Common args added:
        output_path     (positional, Path)
        path_to_shape   (positional, str)
        report_name     (positional, str)
        --include_pdf   (flag)
        --unit_system   (str, default "SI")
        --use-parquet   (optional Path, dest='parquet_path')
        --keep-parquet  (flag)
        --debug         (flag)

    Reports that don't need all of these can simply ignore the extra namespace
    attributes, or call ``argparse.ArgumentParser()`` directly for unusual cases
    like ``rpt_watershed_summary`` (which takes ``huc_list`` instead of a shape).

    Copilot-generated function.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("output_path", help="Folder to write outputs (will be created)", type=Path)
    parser.add_argument("path_to_shape", help="Path to the GeoJSON AOI", type=str)
    parser.add_argument("report_name", help="Human-readable name for the report")
    parser.add_argument("--include_pdf", help="Include a PDF version of the report", action="store_true", default=False)
    parser.add_argument("--unit_system", help="Unit system: SI or imperial", type=str, default="SI")
    parser.add_argument(
        "--use-parquet",
        dest="parquet_path",
        type=Path,
        default=None,
        help="Reuse an existing Parquet directory instead of querying Athena",
    )
    parser.add_argument("--keep-parquet", action="store_true", help="Keep staging Parquet files")
    parser.add_argument("--debug", help="Extra logging and raise errors with traceback.")
    return parser


def parse_report_args(parser: argparse.ArgumentParser) -> tuple[argparse.Namespace, Path]:
    """Parse args via ``dotenv.parse_args_env``, create the output dir, return ``(args, output_path)``.

    Copilot-generated function.
    """
    args = dotenv.parse_args_env(parser)
    output_path = Path(args.output_path)
    safe_makedirs(str(output_path))
    return args, output_path


def init_report_logging(
    output_path: Path,
    title: str,
    *,
    log_filename: str = "report.log",
    log_level: int = logging.DEBUG,
) -> Logger:
    """Set up the standard rsxml Logger for a report run.

    Copilot-generated function.
    """
    log = Logger("Setup")
    log.setup(log_path=output_path / log_filename, log_level=log_level)
    log.title(title)
    return log


def report_main_wrapper(log: Logger, run: Callable[[], Any], *, debug: bool = False) -> None:
    """Run *run*, log peak memory, and call ``sys.exit`` on failure.

    This replaces the try/except + traceback + sys.exit boilerplate present
    in every report ``main()``.

    Copilot-generated function.
    """
    try:
        run()

        process = psutil.Process(os.getpid())
        peak = getattr(process.memory_info(), "peak_wset", None)
        mem_mb = (peak if peak else process.memory_info().rss) / 1024 / 1024
        log.info(f"Peak memory usage: {mem_mb:.2f} MB")

    except Exception as e:
        if debug:
            # Preserve full traceback and let debugger/pytest see the original error.
            raise
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


# ---------------------------------------------------------------------------
# launch.py helpers
# ---------------------------------------------------------------------------

# Re-export for convenience so launchers only need one import line.
from util.prompt import is_truthy, prompt_for  # noqa: E402


def require_data_root() -> str:
    """Return ``DATA_ROOT`` env var or raise with a helpful message.

    Copilot-generated function.
    """
    from termcolor import colored

    data_root = os.environ.get("DATA_ROOT")
    if not data_root:
        raise RuntimeError(
            colored(
                "\nDATA_ROOT environment variable is not set. Please set it in your .env file\n\n  e.g. DATA_ROOT=/Users/Shared/RiverscapesData\n",
                "red",
            )
        )
    return data_root


def prompt_geojson(env_var: str = "RSI_AOI_GEOJSON") -> Path:
    """Resolve an AOI geojson path from *env_var* or prompt from ``example/`` folder.

    Copilot-generated function.
    """
    import inquirer
    from termcolor import colored

    env_val = os.environ.get(env_var)
    if env_val:
        p = Path(env_val)
        if not p.exists():
            raise RuntimeError(colored(f"\n{env_var} is set to '{env_val}' but that file does not exist.\n", "red"))
        return p

    # Fall back to choosing from the report's example/ folder.
    # The caller's module lives next to the example/ directory, so we walk
    # the stack to find it.  Alternatively the caller can pass example_dir.
    import inspect

    caller_file = Path(inspect.stack()[1].filename)
    example_dir = caller_file.parent / "example"
    choices = sorted(p.name for p in example_dir.glob("*.geojson"))
    if not choices:
        raise RuntimeError(colored(f"\nNo example geojson files found in {example_dir}.\n", "red"))

    selected = prompt_for(
        [inquirer.List("geojson", message="Select a geojson file to use as the AOI", choices=choices)],
        "geojson",
    )
    return (example_dir / selected).resolve()


def prompt_unit_system(env_var: str = "UNIT_SYSTEM") -> str:
    """Return unit system from env or prompt.

    Copilot-generated function.
    """
    import inquirer
    from termcolor import colored

    env_val = os.environ.get(env_var)
    if env_val:
        if env_val not in ("SI", "imperial"):
            raise RuntimeError(colored(f"\n{env_var} must be 'SI' or 'imperial', got '{env_val}'.\n", "red"))
        return env_val

    return prompt_for(
        [inquirer.List("unit_system", message="Select a unit system to use", choices=["SI", "imperial"], default="SI")],
        "unit_system",
    )


def prompt_include_pdf(env_var: str = "INCLUDE_PDF") -> bool:
    """Return include-PDF flag from env or prompt.

    Copilot-generated function.
    """
    import inquirer

    env_val = os.environ.get(env_var)
    if env_val is not None:
        return is_truthy(env_val)

    return prompt_for(
        [inquirer.Confirm("include_pdf", message="Include a PDF version of the report? (Default is No)", default=False)],
        "include_pdf",
    )


def prompt_parquet(env_var: str = "RSI_PARQUET_PATH") -> str:
    """Return an optional Parquet override path from env or prompt.

    Returns an empty string when the user declines.

    Copilot-generated function.
    """
    import inquirer
    from termcolor import colored

    env_val = os.environ.get(env_var)
    if env_val:
        if not Path(env_val).exists():
            raise RuntimeError(colored(f"\n{env_var} is set to '{env_val}' but that path does not exist.\n", "red"))
        return env_val

    raw = prompt_for(
        [
            inquirer.Text(
                "parquet_path",
                message="Optional: path to Parquet folder/file (leave blank to query Athena)",
                default="",
            )
        ],
        "parquet_path",
    )
    return raw.strip().strip('"').strip("'") if raw else ""


def prompt_keep_parquet(env_var: str = "RSI_KEEP_PARQUET", *, has_parquet: bool = False) -> bool:
    """Return keep-parquet flag from env or prompt.

    When *has_parquet* is True (i.e. the user already supplied Parquet) the
    default flips to True so we don't delete data the user pointed us at.

    Copilot-generated function.
    """
    import inquirer

    env_val = os.environ.get(env_var)
    if env_val is not None:
        return is_truthy(env_val)

    if has_parquet:
        return True

    return prompt_for(
        [inquirer.Confirm("keep_parquet", message="Keep downloaded Parquet files after processing?", default=False)],
        "keep_parquet",
    )


def derive_report_name(
    geojson_file: Path | str,
    suffix: str,
    env_var: str = "RSI_REPORT_NAME",
) -> str:
    """Return a report name from env or derive from the geojson stem.

    Copilot-generated function.
    """
    env_val = os.environ.get(env_var)
    if env_val:
        return env_val
    stem = Path(geojson_file).stem.replace(" ", "_")
    return f"{stem} - {suffix}"


def build_output_path(data_root: str, report_slug: str, report_name: str) -> Path:
    """Build the standard output directory path for a report.

    Copilot-generated function.
    """
    return Path(data_root) / report_slug / report_name.replace(" ", "_")


def build_common_launch_args(
    data_root: str,
    report_slug: str,
    report_name: str,
    geojson_file: Path | str,
    *,
    unit_system: str = "SI",
    include_pdf: bool = False,
    parquet_path: str = "",
    keep_parquet: bool = False,
) -> list[str]:
    """Assemble the argv list shared by most AOI-based report launchers.

    Returns a list like::

        [output_path, geojson_file, report_name, "--unit_system", "SI", ...]

    Copilot-generated function.
    """
    args: list[str] = [
        str(build_output_path(data_root, report_slug, report_name)),
        str(geojson_file),
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
