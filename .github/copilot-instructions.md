# Riverscapes Reports Gen — Copilot AI Agent Instructions

> **Note:** This platform is in early, active development. Recommendations for improved patterns and best practices are welcome.

## Project Overview
- This repository generates scientific reports from Riverscapes Consortium data, primarily sourced from AWS Athena and geospatial files.
- Each report type is a Python package under `src/reports/`, with its own logic, templates, and entry point.
- The codebase is structured for modularity: shared utilities are in `src/util/`, API helpers in `src/api/`, and report-specific code in subfolders of `src/reports/`.
- The production environment is AWS Fargate (Linux), but developers use both Windows and Mac. Ensure cross-platform compatibility.
- The web UI for launching reports is maintained in a separate repository: [`rs-reports-monorepo`](https://github.com/Riverscapes/rs-reports-monorepo).

## Key Workflows
- **Build/Install:** Use Python 3.12. **Always use `uv` for dependency management and installation** (`uv pip install .[dev]`). See `pyproject.toml` for details. Avoid using `pip` directly unless necessary.
- **Run Reports:** Each report has a CLI entry point (see `[project.scripts]` in `pyproject.toml`). Example: `python -m reports.rpt_igo_project.main` or use the script alias if installed.
- **Automation:** Fargate launch scripts are in `scripts/automation/` for cloud execution. These require specific environment variables (see script headers).
- **Testing:** Tests are in `tests/` and use `pytest`. Run with `pytest` from the repo root.

## Project Conventions
- **Typing:** Use Python 3.12+ type hints (prefer `str`, `list`, etc. over `typing.List`).
- **Watershed IDs:** HUC codes are left-padded with zeros and indicate nested watershed hierarchy (see `.github/prompts/prompt.md`).
- **Templates:** HTML/Jinja2 templates for reports are in each report's `templates/` folder. Shared HTML boilerplate is in `src/util/html/templates/`.
- **Data:** Example data and outputs are in each report's `example/` folder.
- **Metadata Pattern:** Field/column metadata is a first-class concern. Reports export metadata as CSVs or in Excel (see `field_metadata_to_file` in `rpt_igo_project/main.py`), and metadata is also embedded in GeoPackages. Always keep metadata in sync with data outputs.
- **Units & Pint:** All quantitative fields use explicit units, managed with the `pint` and `pint-pandas` libraries. Always use Pint for unit conversions and ensure units are attached to DataFrames and outputs. See usage in `pyproject.toml` and report modules.

## Integration & Patterns
- **Cross-report utilities:** Use `src/util/` for common helpers (e.g., `file_utils.py`, `figures.py`).
- **API/Upload:** Use `src/api/` for S3 and API interactions.
- **Report Generation:** Each report's `main.py` is the entry point; `launch.py` may provide additional orchestration.
- **HTML Reports:** Reports are rendered using Jinja2 templates, with figures and tables injected as context variables.
- **Metadata & Units:** Always propagate metadata and units through all processing steps. Use Pint for all calculations involving units.
- **External Services:** AWS Athena, S3, and Fargate are key integration points. Credentials/configuration are expected via environment variables or config files.

## Examples
- To generate an IGOs project report:
  - Run: `python -m reports.rpt_igo_project.main --help`
  - See: `src/reports/rpt_igo_project/README.md` and `templates/template_readme.md`
- To run the Riverscapes Inventory report:
  - Run: `python -m reports.rpt_riverscapes_inventory.main --help`
  - See: `src/reports/rpt_riverscapes_inventory/README.md`

## References
- [README.md](../README.md): High-level project info
- [pyproject.toml](../pyproject.toml): Dependencies, entry points
- [src/reports/](../src/reports/): Report modules
- [src/util/](../src/util/): Shared utilities
- [scripts/automation/](../scripts/automation/): Fargate/cloud scripts
- [tests/](../tests/): Test suite
- [rs-reports-monorepo](https://github.com/Riverscapes/rs-reports-monorepo): Web UI for launching reports

---
For more domain context, see `.github/prompts/prompt.md`.
