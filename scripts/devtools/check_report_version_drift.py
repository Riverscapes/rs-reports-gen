"""Check report version drift across repos.

Prints findings; makes no changes.
Doesn't really matter as I don't think the version in the ts file is displayed or used for anything, but the mapping between reports could be useful.

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
    'rpt_downstream_geomorphic': 'rpt-downstream-geomorphic-styles',
    'rpt_igo_project': 'custom-rs-metrics',
    'rpt_inventory_of_resources': 'inventory-of-resources',
    'rpt_project_context': 'q-project-context',
    'rpt_rivers_need_space': 'rivers-need-space',
    'rpt_riverscapes_inventory': 'riverscapes-inventory',
    'rpt_watershed_context': 'q-watershed-context',
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


def check_versions(py_versions: dict[str, str], ts_versions: dict[str, str]) -> tuple[list[tuple[str, str, str, str, str]], bool]:
    """Compare versions and return tabular rows plus whether drift exists."""
    rows: list[tuple[str, str, str, str, str]] = []
    has_drift = False
    matched_ts_ids: set[str] = set()

    for pyid in sorted(py_versions):
        pyver = py_versions[pyid]
        tsid = report_pkg_to_ts_id(pyid)
        if tsid not in ts_versions:
            rows.append(('PY_ONLY', pyid, pyver, tsid, ''))
            has_drift = True
            continue
        matched_ts_ids.add(tsid)
        tsver = ts_versions[tsid]
        if tsver != pyver:
            rows.append(('DRIFT', pyid, pyver, tsid, tsver))
            has_drift = True
        else:
            rows.append(('MATCH', pyid, pyver, tsid, tsver))

    for tsid in sorted(ts_versions):
        tsver = ts_versions[tsid]
        if tsid not in matched_ts_ids:
            rows.append(('TS_ONLY', '', '', tsid, tsver))
            has_drift = True

    return rows, has_drift


def main() -> int:
    """Run version drift check and print actionable output."""
    if len(sys.argv) > 1:
        print('This script runs the drift check directly; no arguments are required.')
        print('Usage: uv run python .\\scripts\\devtools\\check_report_version_drift.py')
        return 2

    py_versions = get_python_versions()
    ts_versions = get_ts_versions()
    rows, has_drift = check_versions(py_versions, ts_versions)

    print('status\tpython_pkg                      \tpython_ver\tts_id                           \tts_ver\tnote')
    for status, pyid, pyver, tsid, tsver in rows:
        note = ''
        if pyid:
            note = 'alias-map' if has_alias_mapping(pyid) else 'default-map'
        print(f'{status}\t{str(pyid + ' ' * 31)[:32]}\t     {pyver}\t{str(tsid + ' ' * 31)[:32]}\t{tsver}\t{note}')

    if has_drift:
        print('\nVersion drift detected. See DRIFT/PY_ONLY/TS_ONLY rows above.')
        return 1

    print('\nAll report versions are in sync.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
