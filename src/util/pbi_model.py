"""Generate a fresh PBIP semantic model from a data dictionary CSV.

Reads ``data_dictionary.csv`` produced by the Data Mart export and writes
a complete Power BI ``.pbip`` project folder with TMDL definition files and
a PBIR-format (Enhanced Report Format) Report definition.

Friendly names, units, display folders, tooltips, format strings, and
sorting logic all inherit from the Athena-sourced metadata captured in the CSV.

The model uses a ``DataMartRoot`` parameter for data connectivity: users set
it to the local Data Mart output folder and Power BI loads Parquet files
from the ``exports/`` subfolder.

Usage::

    python scripts/update_pbi_model.py path/to/data_dictionary.csv output_dir --name datamart

Copilot-generated module.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

import pint

from util.pbi_format import python_format_to_pbi

# ── Deterministic lineageTag generation ─────────────────────────────────
# Fixed namespace UUID (generated once, committed).  All lineageTags are
# uuid5(NAMESPACE, stable_key) so output is byte-identical across runs on
# the same data dictionary.
NAMESPACE_UUID = uuid.UUID("a3e89f6c-4b2d-4e7a-9c1f-8d5b3a6e7f00")

# Standalone registry so the script doesn't depend on the application
# registry being configured with custom units.
_ureg = pint.UnitRegistry()

# All generated TMDL files use Windows line endings (PBI Desktop requirement).
CRLF = "\r\n"

# Deterministic 20-char hex page name for the blank initial report page.
# Derived from UUID5 so regeneration is byte-identical.
_INITIAL_PAGE_NAME: str = uuid.uuid5(NAMESPACE_UUID, "report.page.1").hex[:20]

# Bare TMDL identifier: letters, digits, underscores, starting with a letter.
_BARE_IDENT_RE = re.compile(r"^[a-zA-Z_]\w*$")

# ── Known geospatial data categories ────────────────────────────────────
DATA_CATEGORIES: dict[str, str] = {
    "latitude": "Latitude",
    "longitude": "Longitude",
    "state": "StateOrProvince",
    "county": "County",
}

# ── Logical dtype → PBI dataType ────────────────────────────────────────
DTYPE_TO_PBI_TYPE: dict[str, str] = {
    "INTEGER": "int64",
    "FLOAT": "double",
    "STRING": "string",
    "BOOLEAN": "boolean",
    "DATETIME": "dateTime",
    "DECIMAL": "decimal",
}

# Raw column names that should never be aggregated regardless of dtype.
NO_SUMMARIZE_COLUMNS: frozenset[str] = frozenset(
    {
        "latitude",
        "longitude",
        "fcode",
    }
)


# ── Data structures ─────────────────────────────────────────────────────


@dataclass
class ColumnDef:
    """One column's metadata, loaded from the data dictionary and enriched
    with computed PBI properties during resolution.

    Copilot-generated class.
    """

    table_name: str
    column_name: str
    friendly_name: str
    description: str = ""
    export_unit: str = ""
    dtype: str = "STRING"
    preferred_format: str = ""
    theme: str = ""

    # Computed during _resolve_columns
    pbi_name: str = ""
    pbi_data_type: str = ""
    lineage_tag: str = ""
    format_string: str | None = None
    summarize_by: str = ""
    data_category: str = ""
    sort_by_column: str = ""
    display_folder: str = ""


# ── Pure helper functions ───────────────────────────────────────────────


def _lineage_tag(key: str) -> str:
    """Deterministic UUID v5 from a stable identifier.

    Copilot-generated function.
    """
    return str(uuid.uuid5(NAMESPACE_UUID, key))


def _quote_name(name: str) -> str:
    """Wrap a TMDL identifier in single quotes when it contains special chars.

    Copilot-generated function.
    """
    if _BARE_IDENT_RE.match(name):
        return name
    return f"'{name}'"


def _pbi_column_name(friendly_name: str, export_unit: str) -> str:
    """Build PBI display name: ``'Friendly Name (unit_abbrev)'`` or plain name.

    Copilot-generated function.
    """
    if not export_unit or export_unit.strip().lower() in ("", "dimensionless"):
        return friendly_name
    try:
        unit_obj = _ureg.Unit(export_unit)
        abbrev = f"{unit_obj:~P}"
        if abbrev and abbrev.strip():
            return f"{friendly_name} ({abbrev})"
    except Exception:
        # Unknown unit string – use it verbatim.
        return f"{friendly_name} ({export_unit})"
    return friendly_name


def _pbi_data_type(dtype: str) -> str:
    """Map a logical dtype string to a PBI ``dataType`` value.

    Copilot-generated function.
    """
    return DTYPE_TO_PBI_TYPE.get(dtype, "string")


def _summarize_by(dtype: str, column_name: str) -> str:
    """Determine the PBI ``summarizeBy`` value for a column.

    Copilot-generated function.
    """
    if dtype in ("STRING", "BOOLEAN", "GEOMETRY", "STRUCTURED"):
        return "none"
    if column_name.lower() in NO_SUMMARIZE_COLUMNS:
        return "none"
    if column_name.endswith("_color"):
        return "none"
    return "sum"


# ── Data dictionary loading & resolution ────────────────────────────────


def _load_data_dictionary(csv_path: Path) -> dict[str, list[ColumnDef]]:
    """Read *data_dictionary.csv* and return columns grouped by table name.

    Copilot-generated function.
    """
    tables: dict[str, list[ColumnDef]] = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row["table_name"]
            col = ColumnDef(
                table_name=table,
                column_name=row["column_name"],
                friendly_name=row["friendly_name"],
                description=row.get("description", ""),
                export_unit=row.get("export_unit", ""),
                dtype=row.get("dtype", "STRING"),
                preferred_format=row.get("preferred_format", ""),
                theme=row.get("theme", ""),
            )
            tables.setdefault(table, []).append(col)
    return tables


def _resolve_columns(tables: dict[str, list[ColumnDef]]) -> dict[str, list[ColumnDef]]:
    """Enrich each :class:`ColumnDef` with computed PBI properties.

    Two passes per table: first assigns PBI names and basic metadata, then
    resolves cross-column references (``sortByColumn`` for bin columns).

    Copilot-generated function.
    """
    for table_name, columns in tables.items():
        # Pass 1: basic property assignment
        name_map: dict[str, str] = {}  # column_name → pbi_name
        for col in columns:
            col.pbi_name = _pbi_column_name(col.friendly_name, col.export_unit)
            col.pbi_data_type = _pbi_data_type(col.dtype)
            col.lineage_tag = _lineage_tag(f"{table_name}.column.{col.column_name}")
            col.format_string = python_format_to_pbi(col.preferred_format) if col.preferred_format else None
            col.summarize_by = _summarize_by(col.dtype, col.column_name)
            col.data_category = DATA_CATEGORIES.get(col.column_name.lower(), "")
            col.display_folder = col.theme
            name_map[col.column_name] = col.pbi_name

        # Pass 2: link _bin columns to their _bin_sort counterparts
        for col in columns:
            if col.column_name.endswith("_bin") and not col.column_name.endswith("_bin_sort"):
                sort_col_name = col.column_name + "_sort"
                if sort_col_name in name_map:
                    col.sort_by_column = name_map[sort_col_name]

    return tables


# ── TMDL generators ─────────────────────────────────────────────────────


def _generate_column_tmdl(col: ColumnDef) -> list[str]:
    """Generate TMDL lines for a single column definition.

    Property order mirrors the convention in the reference model:
    dataType → displayFolder → formatString → lineageTag → summarizeBy →
    sourceColumn → dataCategory → sortByColumn → description.

    Copilot-generated function.
    """
    lines: list[str] = []
    qname = _quote_name(col.pbi_name)

    # Description is a /// comment above the column definition
    if col.description:
        clean = col.description.replace("\r", "").replace("\n", " ").replace("\t", " ").strip()
        if clean:
            lines.append(f"\t/// {clean}")

    lines.append(f"\tcolumn {qname}")
    lines.append(f"\t\tdataType: {col.pbi_data_type}")

    if col.display_folder:
        lines.append(f"\t\tdisplayFolder: {col.display_folder}")

    if col.format_string:
        lines.append(f"\t\tformatString: {col.format_string}")
    elif col.pbi_data_type == "int64":
        lines.append("\t\tformatString: 0")

    lines.append(f"\t\tlineageTag: {col.lineage_tag}")
    lines.append(f"\t\tsummarizeBy: {col.summarize_by}")
    lines.append(f"\t\tsourceColumn: {col.column_name}")

    if col.data_category:
        lines.append(f"\t\tdataCategory: {col.data_category}")

    if col.sort_by_column:
        lines.append(f"\t\tsortByColumn: {_quote_name(col.sort_by_column)}")

    # changedProperty must go after column properties, before annotations
    if col.sort_by_column:
        lines.append("")
        lines.append("\t\tchangedProperty = SortByColumn")

    lines.append("")
    lines.append("\t\tannotation SummarizationSetBy = Automatic")

    # Doubles without an explicit format get the general-number hint
    if col.pbi_data_type == "double" and not col.format_string:
        lines.append("")
        lines.append('\t\tannotation PBI_FormatHint = {"isGeneralNumber":true}')

    return lines


def _generate_table_tmdl(table_name: str, columns: list[ColumnDef]) -> str:
    """Generate a complete ``<table>.tmdl`` file.

    Copilot-generated function.
    """
    lines: list[str] = []
    qname = _quote_name(table_name)
    table_tag = _lineage_tag(f"{table_name}.table")

    lines.append(f"table {qname}")
    lines.append(f"\tlineageTag: {table_tag}")

    for col in columns:
        lines.append("")
        lines.extend(_generate_column_tmdl(col))

    # Partition: load Parquet directly; TMDL handles column name mapping.
    lines.append("")
    lines.append(f"\tpartition {qname} = m")
    lines.append("\t\tmode: import")
    lines.append("\t\tsource =")
    lines.append("\t\t\t\tlet")
    lines.append(f'\t\t\t\t    Source = Parquet.Document(File.Contents(DataMartRoot & "\\exports\\{table_name}.parquet"))')
    lines.append("\t\t\t\tin")
    lines.append("\t\t\t\t    Source")

    lines.append("")
    lines.append("\tannotation PBI_NavigationStepName = Navigation")
    lines.append("")
    lines.append("\tannotation PBI_ResultType = Table")

    return CRLF.join(lines) + CRLF


def _generate_expressions_tmdl(data_mart_root: str = "") -> str:
    """Generate ``expressions.tmdl`` with ``DataMartRoot`` and ``fn_LoadParquetFolder``.

    M expressions are written inline (no triple-backtick delimiters) to match
    the PBIR format that PBI Desktop saves.

    Args:
        data_mart_root: Pre-populate the ``DataMartRoot`` parameter value.  When
            supplied the generated model opens without requiring the user to set
            the path manually.  Backslashes are kept verbatim (M language does
            not use backslash as an escape character).

    Copilot-generated function.
    """
    lines = [
        "/// combine all Parquet files from a local folder into a single table",
        "expression fn_LoadParquetFolder =",
        "\t\t/**",
        "\t\t * Reusable function: combine all Parquet files from a folder into one table.",
        '\t\t * Input: FolderPath (e.g., "C:\\Data\\ParquetFolder\\")',
        "\t\t * Output: A consolidated table.",
        "\t\t */",
        "\t\t(FolderPath as text) as table =>",
        "\t\tlet",
        "\t\t    Source = Folder.Files(FolderPath),",
        '\t\t    FilteredRows = Table.SelectRows(Source, each ([Extension] = "") or ([Extension] = ".parquet")),',
        '\t\t    AddedCustom = Table.AddColumn(FilteredRows, "Data", each Parquet.Document([Content])),',
        '\t\t    OnlyDataColumn = Table.SelectColumns(AddedCustom, {"Data"}),',
        "\t\t    ColumnNames = Table.ColumnNames(OnlyDataColumn{0}[Data]),",
        '\t\t    ExpandedContent = Table.ExpandTableColumn(OnlyDataColumn, "Data", ColumnNames)',
        "\t\tin",
        "\t\t    ExpandedContent",
        f"\tlineageTag: {_lineage_tag('fn_LoadParquetFolder')}",
        "",
        "\tannotation PBI_NavigationStepName = Navigation",
        "",
        "\tannotation PBI_ResultType = Function",
        "",
        "/// Source folder. Exports subfolder should have the parquet files.",
        f'expression DataMartRoot = "{data_mart_root}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
        f"\tlineageTag: {_lineage_tag('DataMartRoot')}",
        "",
        "\tannotation PBI_NavigationStepName = Navigation",
        "",
        "\tannotation PBI_ResultType = Text",
    ]
    return CRLF.join(lines) + CRLF


def _generate_model_tmdl(table_names: list[str]) -> str:
    """Generate ``model.tmdl`` referencing all tables and the culture.

    Copilot-generated function.
    """
    lines = [
        "model Model",
        "\tculture: en-US",
        "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
        "\tsourceQueryCulture: en-CA",
        "\tdataAccessOptions",
        "\t\tlegacyRedirects",
        "\t\treturnErrorValuesAsNull",
        "",
    ]

    # annotations and ref statements are root-level (no indent) in TMDL
    query_order = table_names + ["fn_LoadParquetFolder", "DataMartRoot"]
    lines.append(f"annotation PBI_QueryOrder = {json.dumps(query_order)}")
    lines.append("")
    lines.append('annotation PBI_ProTooling = ["DevMode","TMDLView_Desktop","TMDL-Extension"]')
    lines.append("")

    for name in table_names:
        lines.append(f"ref table {_quote_name(name)}")

    lines.append("")
    lines.append("ref cultureInfo en-US")

    return CRLF.join(lines) + CRLF


def _generate_database_tmdl() -> str:
    """Generate ``database.tmdl``.

    Copilot-generated function.
    """
    lines = [
        "database",
        "\tcompatibilityLevel: 1600",
    ]
    return CRLF.join(lines) + CRLF


def _generate_culture_tmdl() -> str:
    """Generate a minimal ``en-US.tmdl`` culture definition.

    PBI Desktop will regenerate linguistic metadata on first open.

    Copilot-generated function.
    """
    lines = [
        "cultureInfo en-US",
    ]
    return CRLF.join(lines) + CRLF


# ── JSON scaffold generators ───────────────────────────────────────────


def _generate_pbip_json(model_name: str) -> str:
    """Generate the top-level ``.pbip`` project file.

    Copilot-generated function.
    """
    obj = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
        "version": "1.0",
        "artifacts": [{"report": {"path": f"{model_name}.Report"}}],
        "settings": {"enableAutoRecovery": True},
    }
    return json.dumps(obj, indent=2) + "\n"


def _generate_pbism_json() -> str:
    """Generate ``definition.pbism`` for the semantic model.

    Copilot-generated function.
    """
    obj = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
        "version": "4.2",
        "settings": {},
    }
    return json.dumps(obj, indent=2) + "\n"


def _generate_pbir_json(model_name: str) -> str:
    """Generate ``definition.pbir`` for the report, linking to the semantic model.

    Copilot-generated function.
    """
    obj = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {"byPath": {"path": f"../{model_name}.SemanticModel"}},
    }
    return json.dumps(obj, indent=2) + "\n"


def _generate_platform_json(display_name: str, type_name: str, logical_id: str) -> str:
    """Generate a ``.platform`` file for Fabric git integration.

    Copilot-generated function.
    """
    obj = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": type_name, "displayName": display_name},
        "config": {"version": "2.0", "logicalId": logical_id},
    }
    return json.dumps(obj, indent=2) + "\n"


def _generate_diagram_layout_json() -> str:
    """Generate ``diagramLayout.json`` with an empty diagram.

    Copilot-generated function.
    """
    obj = {
        "version": "1.1.0",
        "diagrams": [
            {
                "ordinal": 0,
                "scrollPosition": {"x": 0, "y": 0},
                "nodes": [],
                "name": "All tables",
                "zoomValue": 100,
            }
        ],
        "selectedDiagram": "All tables",
        "defaultDiagram": "All tables",
    }
    return json.dumps(obj, indent=2) + "\n"


def _generate_editor_settings_json() -> str:
    """Generate ``SemanticModel/.pbi/editorSettings.json``.

    These are the default PBI Desktop editor options written when a model is
    first opened.  Committing them avoids spurious diffs on first open.

    Copilot-generated function.
    """
    obj = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/editorSettings/1.0.0/schema.json",
        "autodetectRelationships": True,
        "parallelQueryLoading": True,
        "typeDetectionEnabled": True,
        "relationshipImportEnabled": True,
        "shouldNotifyUserOfNameConflictResolution": True,
    }
    return json.dumps(obj, indent=2) + "\n"


def _generate_report_version_json() -> str:
    """Generate ``Report/definition/version.json`` for PBIR format.

    Copilot-generated function.
    """
    obj = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
        "version": "2.0.0",
    }
    return json.dumps(obj, indent=2) + "\n"


# ── Riverscapes brand theme ────────────────────────────────────────────
# Palette derived from base.css:
#   primary navy  #003166  (--header-color / --accent-color)
#   text          #222222  (--text-color)
#   muted bg      #f1f4f9  (--muted-bg)
#   chart bg      #e5ecf6  (--chart-bg)
#   error red     #c75146  (error-message border)
#   metric slate  #4a5764  (--metric-number)
# Data-series colors are derived from those anchors with enough contrast
# and hue variety to read clearly on white / light-gray backgrounds.
_RS_THEME_NAME = "Riverscapes"
_RS_THEME_FILE = "BaseThemes/Riverscapes.json"

# Standard PBI theme JSON (import format).
# ``dataColors`` drives chart/visual series colors.
# ``good`` / ``neutral`` / ``bad`` colour KPI and conditional-format visuals.
_RS_THEME: dict = {
    "name": _RS_THEME_NAME,
    "dataColors": [
        "#003166",  # deep navy — primary brand
        "#2171a8",  # river blue
        "#5ba4cf",  # sky / shallow water
        "#4a7c59",  # riparian green
        "#d97b2b",  # warm amber / sediment
        "#a05436",  # brown earth
        "#4a5764",  # slate (--metric-number)
        "#c75146",  # accent red
    ],
    "background": "#ffffff",
    "foreground": "#222222",
    "tableAccent": "#003166",
    "good": "#4a7c59",
    "neutral": "#4a5764",
    "bad": "#c75146",
}


def _generate_riverscapes_theme_json() -> str:
    """Generate the Riverscapes brand theme file.

    Written to ``StaticResources/SharedResources/BaseThemes/Riverscapes.json``
    so the theme is self-contained within the report and opens without
    depending on any particular version of PBI Desktop.

    Copilot-generated function.
    """
    return json.dumps(_RS_THEME, indent=2) + "\n"


def _generate_report_json() -> str:
    """Generate ``Report/definition/report.json`` for PBIR format.

    References the bundled Riverscapes theme via ``resourcePackages`` so the
    report opens correctly on any PBI Desktop version without relying on a
    built-in shared theme.

    Copilot-generated function.
    """
    obj = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.1.0/schema.json",
        "themeCollection": {
            "baseTheme": {
                "name": _RS_THEME_NAME,
                "reportVersionAtImport": {"visual": "2.5.0", "report": "3.1.0", "page": "2.3.0"},
                "type": "SharedResources",
            }
        },
        "resourcePackages": [
            {
                "name": "SharedResources",
                "type": "SharedResources",
                "items": [
                    {
                        "name": _RS_THEME_NAME,
                        "path": _RS_THEME_FILE,
                        "type": "BaseTheme",
                    }
                ],
            }
        ],
        "settings": {
            "useStylableVisualContainerHeader": True,
            "exportDataMode": "AllowSummarized",
            "defaultDrillFilterOtherVisuals": True,
            "allowChangeFilterTypes": True,
            "useEnhancedTooltips": True,
            "useDefaultAggregateDisplayName": True,
        },
    }
    return json.dumps(obj, indent=2) + "\n"


def _generate_pages_json(page_name: str) -> str:
    """Generate ``Report/definition/pages/pages.json`` for PBIR format.

    Copilot-generated function.
    """
    obj = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [page_name],
        "activePageName": page_name,
    }
    return json.dumps(obj, indent=2) + "\n"


def _generate_page_json(page_name: str) -> str:
    """Generate a minimal ``page.json`` for PBIR format.

    The page ``name`` must match the containing folder name.  PBI Desktop
    uses a 20-character hex string; we generate one deterministically via
    ``_INITIAL_PAGE_NAME``.

    Copilot-generated function.
    """
    obj = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
        "name": page_name,
        "displayName": "Page 1",
        "displayOption": "FitToPage",
        "height": 720,
        "width": 1280,
    }
    return json.dumps(obj, indent=2) + "\n"


# ── File I/O ────────────────────────────────────────────────────────────


def _write(path: Path, content: str) -> None:
    """Write *content* to *path*, creating parent directories as needed.

    Uses ``newline=""`` so CRLF sequences already in the content are
    written verbatim (no double-\\r on Windows).

    Copilot-generated function.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(content)


# ── Main generation entry point ─────────────────────────────────────────


def generate_pbip(
    data_dict_path: Path,
    output_dir: Path,
    model_name: str = "datamart",
    data_mart_root: Path | None = None,
) -> Path:
    """Generate a complete PBIP semantic model from a data dictionary CSV.

    Args:
        data_dict_path: Path to ``data_dictionary.csv``.
        output_dir: Root directory to write the ``.pbip`` project into.
        model_name: Base name for the ``.pbip`` / folder names.
        data_mart_root: If supplied, pre-populates the ``DataMartRoot`` Power
            Query parameter so the report opens ready to refresh without the
            user needing to set the path manually.

    Returns:
        Path to the generated ``.pbip`` file.

    Copilot-generated function.
    """
    # Load and resolve column metadata
    tables = _load_data_dictionary(data_dict_path)
    tables = _resolve_columns(tables)
    table_names = list(tables.keys())

    # SemanticModel directory structure
    sm_dir = output_dir / f"{model_name}.SemanticModel"
    def_dir = sm_dir / "definition"
    tables_dir = def_dir / "tables"
    cultures_dir = def_dir / "cultures"

    # Report directory structure
    rpt_dir = output_dir / f"{model_name}.Report"

    # ── TMDL files ──
    _write(def_dir / "database.tmdl", _generate_database_tmdl())
    _write(def_dir / "model.tmdl", _generate_model_tmdl(table_names))
    _write(def_dir / "expressions.tmdl", _generate_expressions_tmdl(str(data_mart_root) if data_mart_root else ""))
    _write(cultures_dir / "en-US.tmdl", _generate_culture_tmdl())

    for tbl_name, columns in tables.items():
        _write(tables_dir / f"{tbl_name}.tmdl", _generate_table_tmdl(tbl_name, columns))

    # ── JSON scaffold ──
    sm_logical_id = _lineage_tag("semantic_model")
    rpt_logical_id = _lineage_tag("report")

    _write(output_dir / f"{model_name}.pbip", _generate_pbip_json(model_name))
    _write(sm_dir / "definition.pbism", _generate_pbism_json())
    _write(sm_dir / ".platform", _generate_platform_json(model_name, "SemanticModel", sm_logical_id))
    _write(sm_dir / "diagramLayout.json", _generate_diagram_layout_json())
    _write(sm_dir / ".pbi" / "editorSettings.json", _generate_editor_settings_json())

    # ── PBIR Report definition ──
    _write(rpt_dir / "definition.pbir", _generate_pbir_json(model_name))
    _write(rpt_dir / ".platform", _generate_platform_json(model_name, "Report", rpt_logical_id))
    _write(rpt_dir / "definition" / "version.json", _generate_report_version_json())
    _write(rpt_dir / "definition" / "report.json", _generate_report_json())
    _write(rpt_dir / "definition" / "pages" / "pages.json", _generate_pages_json(_INITIAL_PAGE_NAME))
    _write(rpt_dir / "definition" / "pages" / _INITIAL_PAGE_NAME / "page.json", _generate_page_json(_INITIAL_PAGE_NAME))
    _write(rpt_dir / "StaticResources" / "SharedResources" / "BaseThemes" / "Riverscapes.json", _generate_riverscapes_theme_json())

    pbip_path = output_dir / f"{model_name}.pbip"
    print(f"PBIP project generated: {pbip_path}")
    return pbip_path


# ── CLI ─────────────────────────────────────────────────────────────────


def main() -> None:
    """CLI entry point for PBIP semantic model generation.

    Copilot-generated function.
    """
    parser = argparse.ArgumentParser(
        description="Generate a PBIP semantic model from a data dictionary CSV.",
    )
    parser.add_argument("data_dict", type=Path, help="Path to data_dictionary.csv")
    parser.add_argument("output_dir", type=Path, help="Root output directory for the PBIP project")
    parser.add_argument("--name", default="datamart", help="PBIP project name (default: datamart)")
    args = parser.parse_args()

    generate_pbip(args.data_dict, args.output_dir, args.name)


if __name__ == "__main__":
    main()
