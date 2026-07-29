from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pint
from openpyxl import load_workbook
from openpyxl.utils import absolute_coordinate, get_column_letter, quote_sheetname, range_boundaries
from openpyxl.workbook.defined_name import DefinedName
from rsxml import Logger

from util.pandas import RSFieldMeta

TEMPLATE_FILE_PATH = Path(__file__).parent / 'templates' / 'WatershedReportTemplate_016.xlsx'


@dataclass
class NamedValue:
    """A single named value for injection into the Excel template.

    Holds all metadata alongside the value so that callers (make_template,
    render_excel) can operate with a single uniform loop regardless of whether
    the value originated from a Pint-quantified aggregate column, a derived
    statistic, or a plain string (e.g. state abbreviation list, HUC codes).
    """

    value: Any
    """The scalar or string value to write into the named cell."""
    unit_str: str = ""
    """Human-readable unit label (empty string for unitless / string values)."""
    friendly_name: str = ""
    """Display name shown in the 'friendly_name' column of the aggregatedata sheet."""
    description: str = ""
    """Optional longer description."""


def build_named_values(
    df_aggregatedata: pd.DataFrame,
    stats_dict: dict[str, pint.Quantity],
    extra: dict[str, NamedValue] | None = None,
) -> dict[str, NamedValue]:
    """Assemble all named values into a single ordered dict for the Excel template.

    Sources are merged in priority order: aggregate-df columns come first, then
    derived stats (skipped if the key already exists), then any *extra* entries
    (e.g. state abbreviations, HUC codes, report name).  All Pint quantities are
    converted to their declared data_unit here so that make_template and
    render_excel only need a single uniform loop.

    Args:
        df_aggregatedata: One-row DataFrame of aggregate metrics with Pint units applied.
        stats_dict: Derived statistics from figures.statistics() — Pint Quantities
            keyed by field name.
        extra: Optional dict of plain NamedValue entries (strings, counts, etc.)
            that carry no Pint units.  Keys must not collide with df columns.

    Returns:
        Ordered dict mapping field name -> NamedValue, ready for template injection.
    """
    meta = RSFieldMeta()
    result: dict[str, NamedValue] = {}

    for column in df_aggregatedata.columns:
        fm = meta.get_field_meta(column)
        friendly = fm.friendly_name if fm else ''
        description = fm.description if fm else ''
        raw_val = df_aggregatedata[column].iloc[0]
        data_qty = _to_data_unit(raw_val, column)
        unit_str = str(data_qty.units) if data_qty is not None else ''
        scalar_val = data_qty.magnitude if data_qty is not None else (raw_val.magnitude if hasattr(raw_val, 'magnitude') else raw_val)
        result[column] = NamedValue(value=scalar_val, unit_str=unit_str, friendly_name=friendly, description=description)

    for stat_name, qty in stats_dict.items():
        if stat_name in result:
            continue
        fm = meta.get_field_meta(stat_name)
        friendly = fm.friendly_name if fm else stat_name.replace('_', ' ').title()
        description = fm.description if fm else ''
        data_qty = _to_data_unit(qty, stat_name)
        unit_str = str(data_qty.units) if data_qty is not None else ''
        scalar_val = data_qty.magnitude if data_qty is not None else (qty.magnitude if hasattr(qty, 'magnitude') else qty)
        result[stat_name] = NamedValue(value=scalar_val, unit_str=unit_str, friendly_name=friendly, description=description)

    for key, nv in (extra or {}).items():
        if key not in result:
            result[key] = nv

    return result


def make_template(named_values: dict[str, NamedValue]):
    """Build or incrementally update the 'aggregatedata' sheet in the template.

    Safe to call multiple times. On first call (sheet absent) it creates the sheet
    with a header row, writes all entries, and registers a workbook-scoped DefinedName
    for each one. On subsequent calls it only appends entries whose names are not
    already present as DefinedNames — existing rows and formula references in other
    sheets are left completely untouched.

    Column names go in column A, corresponding values in column B, friendly name in C,
    units in D. Each value cell is registered as a workbook-scoped DefinedName so that
    other sheets can reference it by the column name.

    Also renames the Ownership sheet ListObject from 'Table1' to 'tbl_ownership' so
    the template only ever needs this done once.

    Args:
        named_values: Assembled dict from build_named_values() — all aggregate
            columns, derived stats, and string extras merged and pre-converted.

    Call from main:
        `make_template(build_named_values(df_aggregatedata, stats, extra=...))`
    """
    log = Logger("MAKE template")
    wb = load_workbook(TEMPLATE_FILE_PATH)

    # # Rename Ownership table once so all future outputs use tbl_ownership
    # ws_own = wb['tbl_ownership']
    # if 'Table1' in ws_own.tables:
    #     ws_own.tables['Table1'].displayName = 'tbl_ownership'
    #     ws_own.tables['Table1'].name = 'tbl_ownership'
    # # Remove the now-redundant workbook DefinedName for ownership
    # if 'tbl_ownership' in wb.defined_names:
    #     del wb.defined_names['tbl_ownership']

    # Create the sheet on first call; on subsequent calls reuse it and only append new entries.
    if 'aggregatedata' not in wb.sheetnames:
        s = wb.create_sheet('aggregatedata')
        s['A1'] = 'field_name'
        s['B1'] = 'value'
        s['C1'] = 'friendly_name'
        s['D1'] = 'units'
    else:
        s = wb['aggregatedata']

    # Find the next empty row (scan column A past the header)
    row = 2
    while s[f'A{row}'].value is not None:
        row += 1

    # Build a lowercase set of already-registered DefinedNames to avoid duplicates
    existing_names: set[str] = {k.lower() for k in wb.defined_names.keys()}

    added = 0
    for key, nv in named_values.items():
        if key.lower() in existing_names:
            log.debug(f"Skipping {key} as there is already a named range for this.")
            continue  # already present — don't disturb existing cell or DefinedName
        s[f'A{row}'] = key
        s[f'B{row}'] = nv.value
        s[f'B{row}'].style = 'Input'
        s[f'C{row}'] = nv.friendly_name
        s[f'D{row}'] = nv.unit_str
        value_cell_ref = f"{quote_sheetname(s.title)}!{absolute_coordinate(f'B{row}')}"  # fully-qualified ref for the named range
        defn = DefinedName(key, attr_text=value_cell_ref)
        wb.defined_names[key] = defn
        row += 1
        added += 1

    # save with new name, next to the template
    output_path = TEMPLATE_FILE_PATH.with_stem(TEMPLATE_FILE_PATH.stem + '_edit')
    wb.save(output_path)
    log.info(f"make_template: {added} new named cell(s) added -> {output_path}")


def render_excel(named_values: dict[str, NamedValue], df_owners: pd.DataFrame, output_file: Path) -> Path:
    """Render an Excel file from a template by injecting data into named cells/tables.

    All numeric values are expected to already be in their declared data units
    (e.g. km, km^2, mm, m) — unit conversion is done upstream in build_named_values().
    The template uses its own conversion factors to display imperial equivalents.
    Named values whose keys do not match any workbook DefinedName are silently skipped.

    df_owners rows are written into the Ownership sheet table (tbl_ownership),
    replacing the existing placeholder rows.
    Columns expected: ownership_desc -> OwnerCode, sum_ownership_area -> AreaSqM.

    Args:
        named_values: Assembled dict from build_named_values() — all aggregate
            columns, derived stats, and string extras merged and pre-converted.
        df_owners: DataFrame with ownership breakdown; columns ownership_desc and
            sum_ownership_area.
        output_file: Destination path for the populated .xlsx file.

    Returns:
        The resolved output_file Path.

    Created by copilot.
    """
    log = Logger("Render Excel")
    log.debug(f"Rendering Excel from template: {TEMPLATE_FILE_PATH.name}")
    wb = load_workbook(TEMPLATE_FILE_PATH)

    # --- Write all named cells in a single loop ---
    written = 0
    for key, nv in named_values.items():
        if _write_named_cell(wb, key, nv.value):
            written += 1
    log.debug(f"Wrote {written} named cell(s) to template")

    # --- Write ownership table ---
    _write_ownership_table(wb, df_owners)
    log.debug(f"Wrote {len(df_owners)} ownership row(s)")

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_file)
    log.info(f"Saved Excel report -> {output_file}")
    return output_file


# ================================== PRIVATE HELPER FUNCTIONS =================


def _to_data_unit(value, field_name: str) -> 'pint.Quantity | None':
    """Convert a Pint Quantity to the field's declared data_unit.

    The Excel template is built around data units (e.g. km, km^2, mm, m) and uses
    its own conversion factors to display imperial equivalents.  This helper ensures
    that display-unit conversions (e.g. km -> miles applied by apply_units) are
    reversed back to data_unit before writing.

    If *value* has no Pint units, or no data_unit is declared in metadata, returns
    None so the caller can fall back to the raw scalar.

    Args:
        value: A Pint Quantity, pint-pandas scalar, or plain scalar.
        field_name: Column / stat name used to look up data_unit in RSFieldMeta.

    Returns:
        A Pint Quantity in data_unit, or None.

    Created by copilot.
    """
    if not hasattr(value, 'to'):
        return None
    meta = RSFieldMeta()
    fm = meta.get_field_meta(field_name)
    if fm is None or not fm.data_unit:
        return None
    try:
        return value.to(fm.data_unit)
    except Exception:
        return None


def _write_named_cell(wb, cellname: str, cellvalue) -> bool:
    """Write *cellvalue* to the cell referenced by the workbook-scoped DefinedName *cellname*.

    The lookup is case-insensitive: 'Sum_HucAreaSqKm' will match a defined name stored
    as 'sum_hucareasqkm'.  Returns True if the named cell was found and written,
    False if no matching DefinedName exists (so callers can log skipped fields if needed).

    Args:
        wb: openpyxl Workbook (already loaded).
        cellname: Name to look up in wb.defined_names.
        cellvalue: Scalar value to write.

    Returns:
        True if written, False if name not found.

    Created by copilot.
    """
    # Build a lowercase lookup once per call is fine at this scale
    lower_map = {k.lower(): k for k in wb.defined_names.keys()}
    canonical = lower_map.get(cellname.lower())
    if canonical is None:
        return False

    defn = wb.defined_names[canonical]
    destinations = list(defn.destinations)  # list of (sheet_title, cell_ref) tuples
    if not destinations:
        return False

    sheet_title, cell_ref = destinations[0]
    ws = wb[sheet_title]
    # cell_ref may be absolute (e.g. '$B$5'); openpyxl accepts that directly
    ws[cell_ref] = cellvalue
    return True


def _write_ownership_table(wb, df_owners: pd.DataFrame) -> None:
    """Overwrite the Ownership sheet table rows with data from df_owners.

    Clears any existing data rows in the table range, writes new data, and resizes
    the ListObject ref.  The table is renamed from 'Table1' to 'tbl_ownership' on
    first write so other sheets can reference it by that name directly — no separate
    workbook DefinedName needed (the legacy one is deleted if present).

    Expected df_owners columns:
        ownership_desc  -> OwnerCode  (column A)
        sum_ownership_area -> AreaSqM (column B)

    Args:
        wb: openpyxl Workbook (already loaded).
        df_owners: Ownership summary DataFrame.

    Created by copilot.
    """
    ws = wb['tbl_ownership']
    table = ws.tables['tbl_ownership']

    n_rows = len(df_owners)

    # Clear existing data rows (row 2 downwards within old table range)
    _min_col, _min_row, _max_col, _max_row = range_boundaries(table.ref)
    assert _min_col is not None and _min_row is not None and _max_col is not None and _max_row is not None, f"Could not parse table ref '{table.ref}'"
    min_col, min_row, max_col, max_row = int(_min_col), int(_min_row), int(_max_col), int(_max_row)
    for r in range(min_row + 1, max_row + 1):
        for c in range(min_col, max_col + 1):
            ws.cell(row=r, column=c).value = None

    # Write new data rows
    for i, (_, data_row) in enumerate(df_owners.iterrows()):
        excel_row = min_row + 1 + i
        owner_val = data_row['ownership_desc']
        area_val = data_row['sum_ownership_area']
        area_scalar = area_val.magnitude if hasattr(area_val, 'magnitude') else area_val
        ws.cell(row=excel_row, column=min_col).value = owner_val
        ws.cell(row=excel_row, column=min_col + 1).value = area_scalar

    # Delete the legacy workbook DefinedName if present — the ListObject name is sufficient.
    if 'tbl_ownership' in wb.defined_names:
        del wb.defined_names['tbl_ownership']

    # Resize the table ref to cover header + new data rows
    new_max_row = min_row + max(n_rows, 1)  # keep at least one data row so the table is valid
    first_col_letter = get_column_letter(min_col)
    last_col_letter = get_column_letter(max_col)
    new_ref = f"{first_col_letter}{min_row}:{last_col_letter}{new_max_row}"
    table.ref = new_ref
    if table.autoFilter:
        table.autoFilter.ref = new_ref
