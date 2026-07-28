from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils import absolute_coordinate, quote_sheetname
from openpyxl.workbook.defined_name import DefinedName

from util.pandas import RSFieldMeta

TEMPLATE_FILE_PATH = Path(__file__).parent / 'templates' / 'WatershedReportTemplate_010.xlsx'


def make_template(df_aggregatedata: pd.DataFrame):
    """ONE TIME build template: adds an 'aggregatedata' sheet and creates a named range
    for each column, then saves back to TEMPLATE_FILE_PATH.
    Column names go in column A, corresponding values in column B.
    Each value cell is registered as a workbook-scoped DefinedName so that
    other sheets can reference it by the column name.
    Call from main after have got the aggregate data, in SI units: `make_template(df_aggregatedata)`
    """
    meta = RSFieldMeta()
    wb = load_workbook(TEMPLATE_FILE_PATH)
    s = wb.create_sheet('aggregatedata')
    # Header row
    s['A1'] = 'field_name'
    s['B1'] = 'value'
    s['C1'] = 'friendly_name'
    s['D1'] = 'units'
    row = 2
    for column in df_aggregatedata.columns:
        field = meta.get_field_meta(column)
        friendly = field.friendly_name if field else ''
        unit = str(field.display_unit if field and field.display_unit else (field.data_unit if field and field.data_unit else ''))
        raw_val = df_aggregatedata[column].iloc[0]
        scalar_val = raw_val.magnitude if hasattr(raw_val, 'magnitude') else raw_val  # unwrap Pint Quantity to plain scalar
        s[f'A{row}'] = column
        s[f'B{row}'] = scalar_val
        s[f'C{row}'] = friendly
        s[f'D{row}'] = unit
        value_cell_ref = f"{quote_sheetname(s.title)}!{absolute_coordinate(f'B{row}')}"  # fully-qualified ref for the named range
        defn = DefinedName(column, attr_text=value_cell_ref)
        wb.defined_names[column] = defn
        row += 1

    # save with new name
    output_path = TEMPLATE_FILE_PATH.stem + '_edit.xlsx'
    wb.save(output_path)


def make_excel(df_aggregatedata: pd.DataFrame, df_owners: pd.DataFrame, df_states: pd.DataFrame, output_file: Path):
    """Inject data into excel template
    Assume df_aggregatedata has one row, and every column is named cell
    df_owners and df_states go into tables owners and states

    Pseudocode - load template
    for each column in df_aggregate, write to named cell (if found)
    nsert df_owners and df_states into named tables of the same name
    write to output_file path
    """


# ================================== PRIVATE


def write_named_cell(cellname: str, cellvalue):
    wb = load_workbook(TEMPLATE_FILE_PATH)
