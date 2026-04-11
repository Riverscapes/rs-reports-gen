"""Shared data dictionary export for all report types.

Generates a CSV describing every column in one or more DataFrames,
pulling rich metadata from RSFieldMeta where available and inferring
logical types from pandas dtypes.

Copilot-generated module.
"""

from pathlib import Path
from typing import NamedTuple

import geopandas as gpd
import pandas as pd
import pint
import pint_pandas
from rsxml import Logger

from util.pandas import RSFieldMeta


class TableEntry(NamedTuple):
    """A DataFrame bundled with its per-column applied units.

    This is a stepping stone toward full DataFrame-local metadata: eventually
    each DataFrame's ``attrs`` dict should carry its own field metadata and
    applied units, eliminating the need for the RSFieldMeta Borg singleton
    and this separate sidecar.  Until then, ``TableEntry`` keeps each table's
    applied units tightly coupled with its DataFrame so that
    ``export_data_dictionary`` can resolve ``export_unit`` per-table without
    cross-table key collisions.

    Attributes:
        df: The DataFrame containing the table data.
        applied_units: Mapping of column name → resolved pint.Unit (or None),
            as returned by ``RSFieldMeta.apply_units()``.  Pass an empty dict
            for tables that haven't been through unit conversion.
    """

    df: pd.DataFrame
    applied_units: dict[str, pint.Unit | None] = {}


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
    tables: dict[str, TableEntry],
    output_path: Path,
) -> Path:
    """Write a CSV data dictionary describing columns across one or more tables.

    For each (table_name, TableEntry) pair, iterates the **actual columns**
    in the DataFrame (ground truth) and enriches each with metadata from
    the RSFieldMeta singleton where available. ``layer_id`` resolution prefers
    ``df.attrs["layer_id"]`` (mirroring ``apply_units`` behaviour) and falls
    back to ``table_name`` when attrs are missing, so columns with the same
    name in different layers are disambiguated.

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
        tables: Mapping of table name → TableEntry(df, applied_units).
        output_path: Path to write the CSV to.

    Returns:
        The output_path that was written.

    Copilot-generated function.
    """
    log = Logger("Data Dictionary")
    meta = RSFieldMeta()
    rows: list[dict] = []

    for table_name, entry in tables.items():
        df = entry.df
        table_applied_units = entry.applied_units or {}
        # Resolve layer_id from df.attrs["layer_id"] when present (mirroring
        # apply_units), else fall back to table_name. This disambiguates
        # columns that share a name across layers (e.g. "huc" in rpt_rme vs
        # rs_context_huc10) and avoids ambiguous lookups when attrs are absent.
        # Note: _resolve_unique_id does NOT fall back to an all-layers search
        # when a layer_id is provided — columns not registered in that specific
        # layer will appear as "not in registry".  Only set attrs["layer_id"]
        # on DataFrames whose columns come from a single registry layer.
        # Future: once metadata is DataFrame-local (carried in attrs), this
        # lookup can be replaced by a direct attrs read.
        layer_id = RSFieldMeta._resolve_layer_context(df, None) or table_name

        for col in df.columns:
            fm = meta.get_field_meta(col, layer_id)
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

            # Resolve export_unit from this table's applied_units
            au = table_applied_units.get(col)
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
