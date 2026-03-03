"""Upload report outputs to the Riverscapes API."""

from __future__ import annotations

from pathlib import Path

from pint import UnitRegistry
from rsxml import Logger

_UNITS = UnitRegistry()


def collect_output_files(outputs_dir: str | Path, log_only: bool = False) -> list[tuple[str, str]]:
    """Collect all output files from a directory.

    Args:
        outputs_dir (str): Path to the directory containing output files.
        log_only (bool, optional): If True, only upload log the files. Defaults to False.

    Returns:
        List[Tuple[str, str]]: A list of tuples containing local and S3 paths for the collected files.
    """
    log = Logger("Collect Output Files")
    collected: list[tuple[str, str]] = []
    outputs_dir = Path(outputs_dir)
    for file_path in outputs_dir.rglob('*'):
        if not file_path.is_file():
            continue

        if log_only and not file_path.suffix.lower() == '.log':
            continue

        local_path = str(file_path)
        # as_posix() guarantees standard forward slashes (/) for S3 paths
        s3_path = file_path.relative_to(outputs_dir).as_posix()

        # Log the file as prepared for upload
        collected.append((local_path, s3_path))

        size = file_path.stat().st_size
        size_units = size * _UNITS.byte
        compact_size = size_units.to_compact()
        log.info(f"Found File: {local_path} {compact_size:.2f~#P}")

    return collected
