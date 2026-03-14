"""Shared data dictionary export for all report types.

Generates a CSV describing every column in one or more DataFrames,
pulling rich metadata from RSFieldMeta where available and inferring
logical types from pandas dtypes.

Copilot-generated module.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pint_pandas
from rsxml import Logger

from util.pandas import RSFieldMeta

# Logical type enum matching the schema used by the metadata registry.
LOGICAL_TYPES = frozenset(
    {
        "INTEGER",
        "FLOAT",
        "STRING",
        "BOOLEAN",
        "DATETIME",
        "DECIMAL",
        "GEOMETRY",
        "STRUCTURED",
        "BINARY",
    }
)


def _infer_logical_type(series: pd.Series) -> str:
    """Map a pandas Series dtype to a logical type string.

    Copilot-generated function.
    """
    dtype = series.dtype

    # Pint-wrapped columns → inspect the underlying numpy dtype
    if isinstance(dtype, pint_pandas.PintType):
        dtype = dtype.numpy_dtype

    # Geometry (GeoSeries)
    if isinstance(series, gpd.GeoSeries) or dtype.name == "geometry":
        return "GEOMETRY"

    kind = getattr(dtype, "kind", None)
    name = getattr(dtype, "name", str(dtype)).lower()

    if kind == "i" or kind == "u":  # signed/unsigned int
        return "INTEGER"
    if kind == "f":  # float
        return "FLOAT"
    if kind == "b":  # bool
        return "BOOLEAN"
    if kind in ("M", "m"):  # datetime / timedelta
        return "DATETIME"
    if "decimal" in name:
        return "DECIMAL"
    # Categorical, object, string → STRING
    if kind in ("O", "U", "S") or name in ("category", "string"):
        return "STRING"
    return "STRING"


def _normalise_registry_dtype(registry_dtype: str) -> str:
    """Map an RSFieldMeta dtype value to the canonical logical type enum.

    The metadata registry uses mixed conventions (e.g. 'REAL', 'TEXT',
    'VARCHAR', 'INT').  Normalise them to the standard enum.

    Copilot-generated function.
    """
    if not registry_dtype:
        return ""
    upper = registry_dtype.strip().upper()
    mapping: dict[str, str] = {
        "REAL": "FLOAT",
        "DOUBLE": "FLOAT",
        "FLOAT": "FLOAT",
        "NUMERIC": "FLOAT",
        "INT": "INTEGER",
        "INTEGER": "INTEGER",
        "BIGINT": "INTEGER",
        "SMALLINT": "INTEGER",
        "TINYINT": "INTEGER",
        "TEXT": "STRING",
        "STRING": "STRING",
        "VARCHAR": "STRING",
        "CHAR": "STRING",
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        "DATETIME": "DATETIME",
        "TIMESTAMP": "DATETIME",
        "DATE": "DATETIME",
        "DECIMAL": "DECIMAL",
        "GEOMETRY": "GEOMETRY",
        "BINARY": "BINARY",
        "VARBINARY": "BINARY",
        "STRUCT": "STRUCTURED",
        "ARRAY": "STRUCTURED",
        "MAP": "STRUCTURED",
    }
    return mapping.get(upper, upper)


def export_data_dictionary(
    tables: dict[str, pd.DataFrame],
    output_path: Path,
    applied_units: dict[str, "pint.Unit | None"] | None = None,
) -> Path:
    """Write a CSV data dictionary describing columns across one or more tables.

    For each (table_name, DataFrame) pair, iterates the **actual columns**
    in the DataFrame (ground truth) and enriches each with metadata from
    the RSFieldMeta singleton where available.

    Output columns:
        table_name      – the dict key identifying the source table
        column_name     – from df.columns (ground truth)
        friendly_name   – RSFieldMeta or Title Case fallback
        description     – RSFieldMeta or empty
        data_unit       – RSFieldMeta (original unit on ingested data)
        display_unit    – RSFieldMeta (explicit display-unit override)
        export_unit     – resolved unit the data was actually converted to
        dtype           – logical type from the standard enum
        preferred_format – Python format string from RSFieldMeta
        theme           – grouping / display-folder label from RSFieldMeta
        in_registry     – True if RSFieldMeta had metadata for this column

    Args:
        tables: Mapping of table name → DataFrame.
        output_path: Path to write the CSV to.
        applied_units: Optional mapping of column name → pint.Unit (or None)
            as returned by ``RSFieldMeta.apply_units()``.  Used to populate
            the ``export_unit`` column.

    Returns:
        The output_path that was written.

    Copilot-generated function.
    """
    log = Logger("Data Dictionary")
    meta = RSFieldMeta()
    if applied_units is None:
        applied_units = {}
    rows: list[dict] = []

    for table_name, df in tables.items():
        for col in df.columns:
            fm = meta.get_field_meta(col)
            has_meta = fm is not None and bool(fm.friendly_name)

            if has_meta:
                friendly = fm.friendly_name
                desc = fm.description or ""
                data_unit = str(fm.data_unit) if fm.data_unit else ""
                display_unit = str(fm.display_unit) if fm.display_unit else ""
                dtype = _normalise_registry_dtype(fm.dtype) if fm.dtype else _infer_logical_type(df[col])
                preferred_format = fm.preferred_format or ""
                theme = fm.theme or ""
            else:
                friendly = col.replace("_", " ").title()
                desc = ""
                data_unit = ""
                display_unit = ""
                dtype = _infer_logical_type(df[col])
                preferred_format = ""
                theme = ""

            # Resolve export_unit from applied_units dict
            au = applied_units.get(col)
            export_unit = str(au) if au else ""

            rows.append(
                {
                    "table_name": table_name,
                    "column_name": col,
                    "friendly_name": friendly,
                    "description": desc,
                    "data_unit": data_unit,
                    "display_unit": display_unit,
                    "export_unit": export_unit,
                    "dtype": dtype,
                    "preferred_format": preferred_format,
                    "theme": theme,
                    "in_registry": has_meta,
                }
            )

    dd_df = pd.DataFrame(rows)
    dd_df.to_csv(output_path, index=False)
    log.info(f"Data dictionary written to {output_path} ({len(dd_df)} entries across {len(tables)} table(s))")
    return output_path
