"""Upload report outputs to the Riverscapes API."""

from __future__ import annotations

import os
from typing import List, Tuple
from rsxml import Logger
from pint import UnitRegistry

_UNITS = UnitRegistry()


def collect_output_files(outputs_dir: str, log_only: bool = False) -> List[Tuple[str, str]]:
    """ Collect all output files from a directory.

    Args:
        outputs_dir (str): Path to the directory containing output files.
        log_only (bool, optional): If True, only log the files without collecting them. Defaults to False.

    Returns:
        List[Tuple[str, str]]: A list of tuples containing local and S3 paths for the collected files.
    """
    log = Logger("Collect Output Files")
    collected: List[Tuple[str, str]] = []

    for root, _dirs, files in os.walk(outputs_dir):

        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, outputs_dir)
            s3_path = relative_path.replace(os.sep, "/")
            if log_only and not filename.lower().endswith((".log")):
                continue
            # Log the file as prepared for upload
            collected.append((local_path, s3_path))
            size = os.path.getsize(local_path)
            size_units = size * _UNITS.byte
            compact_size = size_units.to_compact()
            log.info(f"Found File: {local_path} {compact_size:.2f~#P}")

    return collected
