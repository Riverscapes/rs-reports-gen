"""Check report version drift across repos.

Usage:
    uv run python .\\scripts\\devtools\\check_report_version_drift.py

Assumes:
    - This script is run from the rs-reports-gen repo root.
    - rs-reports-monorepo is a sibling directory.
"""

import re
import sys
from pathlib import Path

PY_REPORTS = Path(__file__).parent.parent.parent / 'src' / 'reports'
TS_REPORTDEFS = Path(__file__).parent.parent.parent.parent / 'rs-reports-monorepo' / 'packages' / 'common-server' / 'src' / 'reportDefs.ts'

# Explicit aliases for cases where report package names and UI ids intentionally differ.
PY_TO_TS_ID_ALIASES = {
    'rpt_igo_project': 'igo-scraper',
    'rpt_rivers_need_space': 'rivers-need-space',
    'rpt_riverscapes_inventory': 'riverscapes-inventory',
    'rpt_watershed_summary': 'rpt-watershed',
}


def get_python_versions() -> dict[str, str]:
    """Discover report package versions from __version__.py files."""
    versions: dict[str, str] = {}
    for pkg in PY_REPORTS.iterdir():
        if not pkg.is_dir() or pkg.name.startswith('__'):
            continue
        version_file = pkg / '__version__.py'
        if version_file.exists():
            with open(version_file, encoding='utf-8') as file_obj:
                match = re.search(r"__version__\s*=\s*['\"]([^'\"]+)['\"]", file_obj.read())
                if match:
                    versions[pkg.name] = match.group(1)
    return versions


def get_ts_versions() -> dict[str, str]:
    """Parse reportDefs.ts and extract report id/version pairs."""
    versions: dict[str, str] = {}
    with open(TS_REPORTDEFS, encoding='utf-8') as file_obj:
        text = file_obj.read()
    for match in re.finditer(r"id:\s*'([^']+)'[\s\S]+?version:\s*'([^']+)'", text):
        report_id, version = match.group(1), match.group(2)
        versions[report_id] = version
    return versions


def report_pkg_to_ts_id(pyid: str) -> str:
    """Map a Python report package name to its TypeScript report id."""
    return PY_TO_TS_ID_ALIASES.get(pyid, pyid.replace('_', '-'))


def has_alias_mapping(pyid: str) -> bool:
    """Return True when a Python package uses an explicit alias mapping."""
    return pyid in PY_TO_TS_ID_ALIASES


def check_versions(py_versions: dict[str, str], ts_versions: dict[str, str]) -> list[tuple[str | None, str | None, str | None]]:
    """Compare versions and return mismatches and unmapped report ids."""
    drift: list[tuple[str | None, str | None, str | None]] = []
    matched_ts_ids: set[str] = set()

    for pyid, pyver in py_versions.items():
        tsid = report_pkg_to_ts_id(pyid)
        if tsid not in ts_versions:
            drift.append((pyid, pyver, None))
            continue
        matched_ts_ids.add(tsid)
        if ts_versions[tsid] != pyver:
            drift.append((pyid, pyver, ts_versions[tsid]))

    for tsid, tsver in ts_versions.items():
        if tsid not in matched_ts_ids:
            drift.append((None, None, f'{tsid}:{tsver}'))

    return drift


def main() -> int:
    """Run version drift check and print actionable output."""
    if len(sys.argv) > 1:
        print('This script runs the drift check directly; no arguments are required.')
        print('Usage: uv run python .\\scripts\\devtools\\check_report_version_drift.py')
        return 2

    py_versions = get_python_versions()
    ts_versions = get_ts_versions()
    drift = check_versions(py_versions, ts_versions)

    if not drift:
        print('All report versions are in sync.')
        return 0

    print('Version drift detected:')
    for pyid, pyver, tsver in drift:
        if pyid is None:
            print(f'  TS-only report id (no Python mapping): {tsver}')
            continue
        tsid = report_pkg_to_ts_id(pyid)
        mapping_note = f' [alias -> {tsid}]' if has_alias_mapping(pyid) else f' [mapped -> {tsid}]'
        print(f'  {pyid}: Python={pyver}  TypeScript={tsver}{mapping_note}')
    return 1


if __name__ == '__main__':
    sys.exit(main())
