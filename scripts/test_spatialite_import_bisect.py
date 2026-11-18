"""Spatialite import-order bisect helper.

Created: 2026-11-17
Created by Copilot.

Observed findings from this repository environment (2026-11-17):
- `apsw` + `mod_spatialite.dll` load succeeds in a clean process baseline.
- In the same process, after importing `pandas`, extension loading fails with
  `apsw.ExtensionLoadingError: The specified module could not be found`.
- This script exists to reproduce that transition step-by-step and identify the
  first import after which extension loading fails.
"""

from __future__ import annotations

import importlib
import os
import traceback
from collections.abc import Callable

import apsw

DLL_DIR = r"C:\OSGeo4W\bin"
DLL = rf"{DLL_DIR}\mod_spatialite.dll"


def _load_spatialite() -> tuple[bool, str]:
    """Attempt to load mod_spatialite and return success flag and details."""
    original_path = os.environ.get("PATH", "")
    dll_handle = os.add_dll_directory(DLL_DIR)
    try:
        os.environ["PATH"] = DLL_DIR + os.pathsep + original_path
        conn = apsw.Connection(":memory:")
        conn.enable_load_extension(True)
        conn.load_extension(DLL)
        version = conn.execute("select spatialite_version()").fetchall()
        conn.close()
        return True, f"Loaded OK, spatialite_version={version}"
    except Exception:  # diagnostic script
        return False, traceback.format_exc()
    finally:
        os.environ["PATH"] = original_path
        dll_handle.close()


def _run_step(label: str, import_fn: Callable[[], None]) -> bool:
    """Run one import step then test extension loading."""
    print(f"\n=== STEP: {label} ===")

    try:
        import_fn()
        print("Import: OK")
    except Exception:
        print("Import: FAILED")
        print(traceback.format_exc())
        return False

    ok, details = _load_spatialite()
    if ok:
        print(f"Spatialite load: OK ({details})")
    else:
        print("Spatialite load: FAILED")
        print(details)
    return ok


def main() -> int:
    """Run sequential imports and report first failing step."""
    if not os.path.exists(DLL):
        print(f"Missing DLL: {DLL}")
        return 2

    steps: list[tuple[str, Callable[[], None]]] = [
        ("baseline (apsw only)", lambda: None),
        ("import numpy", lambda: importlib.import_module("numpy")),
        ("import pandas", lambda: importlib.import_module("pandas")),
        ("import pyarrow.parquet", lambda: importlib.import_module("pyarrow.parquet")),
        ("import geopandas", lambda: importlib.import_module("geopandas")),
        ("import util", lambda: importlib.import_module("util")),
        ("import util.athena", lambda: importlib.import_module("util.athena")),
        ("import reports.rpt_igo_project.rawrme_to_igos_project", lambda: importlib.import_module("reports.rpt_igo_project.rawrme_to_igos_project")),
        ("import reports.rpt_igo_project.main", lambda: importlib.import_module("reports.rpt_igo_project.main")),
    ]

    for label, step in steps:
        ok = _run_step(label, step)
        if not ok:
            print(f"\nFirst failure at: {label}")
            return 1

    print("\nAll steps loaded Spatialite successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
