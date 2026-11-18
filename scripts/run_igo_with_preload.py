"""Development-only IGO bootstrap with Windows Spatialite priming.

Created: 2026-11-17
Created by copilot.

Observed in local dev: APSW Spatialite loading can fail once debug/analysis imports
have initialized native DLL state. This wrapper primes Spatialite first, then
runs the normal report entrypoint.

This script is intended for launcher-driven local development only; production
automation should call the report entrypoint directly.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import apsw

_DLL_DIR_HANDLE: object | None = None
_PRIME_CONN: apsw.Connection | None = None


def _prime_windows_spatialite(spatialite_path: str) -> list[str]:
    """Prime mod_spatialite load with APSW before importing the report module."""
    global _DLL_DIR_HANDLE, _PRIME_CONN
    if os.name != "nt":
        return []

    spatialite_dll = Path(spatialite_path.strip('"').strip("'").strip())
    if not spatialite_dll.exists():
        raise FileNotFoundError(f"SPATIALITE_PATH does not exist: {spatialite_dll}")

    osgeo_bin = spatialite_dll.parent
    os.environ["PATH"] = str(osgeo_bin) + os.pathsep + os.environ.get("PATH", "")
    if hasattr(os, "add_dll_directory") and _DLL_DIR_HANDLE is None:
        # Keep handle alive for the process lifetime so DLL search path remains active.
        _DLL_DIR_HANDLE = os.add_dll_directory(str(osgeo_bin))

    warnings: list[str] = []
    try:
        if _PRIME_CONN is None:
            # Keep this connection open until process exit to preserve extension state.
            _PRIME_CONN = apsw.Connection(":memory:")
            _PRIME_CONN.enable_load_extension(True)
            _PRIME_CONN.load_extension(str(spatialite_dll))
    except apsw.ExtensionLoadingError as exc:
        warnings.append(f"Spatialite priming failed: {exc}")
    return warnings


def main() -> int:
    """Run IGO report main after optional DLL preloading."""
    if len(sys.argv) < 2:
        raise ValueError("Expected spatialite_path as first argument")

    preload_warnings = _prime_windows_spatialite(sys.argv[1])
    if preload_warnings:
        print(f"[WARN] Spatialite priming warnings: {'; '.join(preload_warnings)}")

    src_root = Path(__file__).resolve().parents[1] / "src"
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))

    from reports.rpt_igo_project import main as igo_main

    sys.argv = ["reports.rpt_igo_project.main", *sys.argv[1:]]
    try:
        igo_main.main()
        return 0
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1
    finally:
        if _PRIME_CONN is not None:
            _PRIME_CONN.close()
        if _DLL_DIR_HANDLE is not None:
            _DLL_DIR_HANDLE.close()


if __name__ == "__main__":
    raise SystemExit(main())
