"""
RSFieldMeta: Centralized Field Metadata Management for Reporting

This module provides the RSFieldMeta class and supporting utilities for managing
field (column) metadata in our reporting and data analysis pipelines. 

Metadata includes friendly names, units, data types,
conversion flags, and descriptions for each field.

General functions:
    * add_field_meta
    * get_field_meta

Pandas DataFrames specific functions: 
    * apply_units, bake_units
    * get_headers, get_headers_dict

Key Features:
- Centralized, shared metadata storage using the Borg pattern.
- Support for SI and imperial unit systems, with preferred unit mappings.
- Automatic unit conversion and header formatting for DataFrames.
- Integration with Pint for unit handling.
- columns are identified by both `table_name` and `name`, combined into an always-lowercase identifier `_unique_id`

Typical Usage:
    meta = RSFieldMeta()
    meta.add_field_meta(name="segment_area", friendly_name="Riverscape Area", data_unit="meter ** 2", description="Area of the riverscape segment.")
    friendly = meta.get_field_meta("segment_area").friendly_name
    df_baked, headers = meta.bake_units(df)

Classes:
    FieldMetaValues: Simple container for metadata of a single field.
    RSFieldMeta: Main class for managing and applying field metadata.

Authors: Matt Reimer, Lorin Gaertner
October 2025
"""

from typing import Optional, Dict, List, Tuple
import pint  # noqa: F401  # pylint: disable=unused-import
import pint_pandas  # noqa: F401  # pylint: disable=unused-import # this is needed
from rsxml import Logger
import pandas as pd

ureg = pint.get_application_registry()

SI_SYSTEMS = ['SI', 'imperial']

# I need a lookup table for converting units like meters to feet and km to miles based on the current unit system.
# So, for example, if I ask for "meters" and the current system is imperial, I should get "feet" back.
# This will be a mapping of dimensionality to preferred units for each system.
SI_TO_IMPERIAL: Dict[str, str] = {
    'meter': 'foot',
    'meter ** 2': 'foot ** 2',
    'kilometer': 'mile',
    '1 / kilometer': '1 / mile',
    'kilometer ** 2': 'acre',
    'kilogram': 'pound',
    # no conversion
    '%': '%',
    'count': 'count'
}
IMPERIAL_TO_SI: Dict[str, str] = {
    # Start by just reversing the SI_TO_IMPERIAL mapping
    **{v: k for k, v in SI_TO_IMPERIAL.items()},
    # Then add any additional mappings that don't have a direct reverse
    # e.g. 'foot ** 2' -> 'meter ** 2' is not a direct reverse of 'meter' -> 'foot'
    # but we can add it here for completeness
    'foot ** 2': 'meter ** 2',
    'yard': 'meter',
}


def _get_unique_id(table_name: Optional[str], column_name: str) -> str:
    """Create a unique, case-insensitive identifier for a field."""
    if not column_name:
        raise ValueError("column_name cannot be empty.")
    if table_name:
        return f"{str(table_name).lower()}.{str(column_name).lower()}"
    return str(column_name).lower()


class FieldMetaValues:
    """ A simple class to hold metadata values for a single field.
        """
    VALID_COLUMNS = ["table_name", "name", "friendly_name", "data_unit", "display_unit", "dtype", "no_convert", "description"]

    def __init__(self):
        self.table_name: str = ""
        self.name: str = ""
        self.friendly_name: str = ""
        self.data_unit: Optional[pint.Unit] = None
        self.display_unit: Optional[pint.Unit] = None
        self.dtype: str = ""
        self.no_convert: bool = False
        self.description: str = ""

    # Make it printable for easier debugging
    def __repr__(self):
        return (f"FieldMetaValues(table_name='{self.table_name}', name='{self.name}', friendly_name='{self.friendly_name}', "
                f"data_unit='{self.data_unit}', display_unit='{self.display_unit}', "
                f"dtype='{self.dtype}', no_convert={self.no_convert})")


class RSFieldMeta:
    """ A Borg pattern to share metadata across multiple DataFrames.

    Returns:
        _type_: _description_
    """
    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state
        self._log = Logger('RSFieldMeta')
        if not hasattr(self, "_field_meta"):
            self._field_meta: Optional[pd.DataFrame] = None
        if not hasattr(self, "_unit_system"):
            self._unit_system = 'SI'  # default to SI
            self._log.debug(f'Set default unit system to {self._unit_system}')

    def clear(self):
        """ Clear the shared metadata.
        """
        self._field_meta = None

    @property
    def field_meta(self) -> Optional[pd.DataFrame]:
        """Get the metadata DataFrame.

        Returns:
            Optional[pd.DataFrame]: The metadata DataFrame or None if not set.
        """
        return self._field_meta

    @property
    def unit_system(self) -> str:
        """Get the current unit system.

        Returns:
            str: The current unit system.
        """
        return self._unit_system

    @field_meta.setter
    def field_meta(self, value: pd.DataFrame):
        """ Set or extend the field metadata DataFrame. If metadata already exists it will be extended.

        We do a lot of cleaning here to make sure there are valid values being passed in

        Args:
            value (pd.DataFrame): The metadata DataFrame to set.
        """

        def _make_boolean(val):
            if pd.isna(val) or val is None:
                return False
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                val_lower = val.strip().lower()
                if val_lower in ('true', '1', 'yes'):
                    return True
                else:
                    return False
            if isinstance(val, (int, float)):
                return val != 0
            return False

        def _clean_string(val):
            if pd.isna(val) or val is None:
                return None
            return str(val).strip()

        def _try_apply_unit(unittext: str) -> pint.Unit | None:
            # NOTE: empty string does not get parsed as dimensionless (normally Pint would)
            try:
                if not unittext or pd.isna(unittext):
                    return None
                return ureg.Unit(unittext)
            except Exception as e:
                self._log.warning(f"Could not apply {unittext}: {e}")

        # First clean all the values as if they were strings
        value = value.copy()
        for col in FieldMetaValues.VALID_COLUMNS:
            if col in value.columns:
                value[col] = value[col].apply(_clean_string)
            else:
                # If the column is missing add it with default None values
                value[col] = None

        # We explicitly set the no_convert column to boolean and convert any possible values
        if "no_convert" in value.columns:
            # It's coming in possibly as object/string so we need to convert it
            value["no_convert"] = value["no_convert"].apply(_make_boolean)
            value["no_convert"] = value["no_convert"].astype(bool)
        else:
            value["no_convert"] = False

        # Create the unique ID for indexing
        value['_unique_id'] = value.apply(
            lambda row: _get_unique_id(row.get('table_name'), row.get('name')),
            axis=1
        )

        # Make our unit objects a little easier to work with
        if "data_unit" in value.columns:
            # Select rows where data_unit is not 'NA' and apply the function
            mask = value["data_unit"] != 'NA'
            value.loc[mask, "data_unit"] = value.loc[mask, "data_unit"].map(_try_apply_unit)
        if "display_unit" in value.columns:
            value["display_unit"] = value["display_unit"].apply(_try_apply_unit)

        # IF there is no meta then set it
        if self._field_meta is None:
            self._log.info("Setting metadata for the first time.")
            self._field_meta = value
            self._field_meta.set_index("_unique_id", inplace=True)
        else:
            # otherwise extend the existing metadata with the new dataframe
            self._log.warning("Extending existing metadata. This may overwrite existing columns.")
            value.set_index("_unique_id", inplace=True)
            self._field_meta = pd.concat([self._field_meta, value]).drop_duplicates()
            self._log.info("Metadata extended successfully.")

    @unit_system.setter
    def unit_system(self, system: str):
        """ Set the current unit system.

        Args:
            system (str): The unit system to set (e.g. 'SI' or 'imperial').

        Raises:
            ValueError: If the unit system is invalid.
        """
        normalized = system.strip()
        valid_systems = set(SI_SYSTEMS)
        if normalized not in valid_systems:
            raise ValueError(f"Invalid unit system '{system}'. Valid options are: {list(valid_systems)}")
        ureg.default_system = normalized
        canonical = ureg.get_system(normalized).name
        self._unit_system = canonical
        self._log.info(f'Set default unit system to {canonical}')

    @staticmethod
    def get_dimensionality_name(unit_obj: pint.Unit) -> Optional[str]:
        """Get a simple dimensionality name from a unit string, e.g. 'length', 'area', 'volume', 'time', 'mass'.

        Args:
            unit(str): A unit string compatible with Pint.

        Returns:
            Optional[str]: The dimensionality name or None if not found.
        """
        dimensionality = unit_obj.dimensionality
        if "[length]" in dimensionality:
            power = dimensionality["[length]"]
            if power == 1:
                dimension_class = "length"
            elif power == 2:
                dimension_class = "area"
            elif power == 3:
                dimension_class = "volume"
            else:
                dimension_class = f"length_{power}"
            return dimension_class
        elif "[mass]" in dimensionality:
            return "mass"
        elif "[time]" in dimensionality:
            return "time"

        # For all other cases, build a string like "length_per_time", "area_per_length", etc.
        num = []
        denom = []
        for dim, power in dimensionality.items():
            dim_clean = dim.strip("[]").replace(" ", "_")
            if power > 0:
                if power == 1:
                    num.append(dim_clean)
                else:
                    num.append(f"{dim_clean}{int(power)}")
            elif power < 0:
                if power == -1:
                    denom.append(dim_clean)
                else:
                    denom.append(f"{dim_clean}{abs(int(power))}")

        if num and denom:
            return f"{'_'.join(num)}_per_{'_'.join(denom)}"
        elif num:
            return '_'.join(num)
        elif denom:
            return 'per_' + '_'.join(denom)
        else:
            return None

    def add_field_meta(self,
                       name: str,
                       table_name: str = "",
                       friendly_name: str = "",
                       data_unit: str | pint.Unit | None = None,
                       display_unit: str | pint.Unit | None = None,
                       dtype: str = "",
                       no_convert: bool = False,
                       description: str = ""):
        """ Add a new column to the metadata DataFrame if it does not already exist.

        Args:
            name (str): The name of the column to add.
            table_name (str, optional): The name of the table the column belongs to. Defaults to "".
            friendly_name (str, optional): The friendly name for the column. Defaults to "".
            data_unit (str | pint.Unit | None, optional): The data unit for the actual data in the column. Defaults to None.
            display_unit (str | pint.Unit | None, optional): The display unit for the column. Defaults to None.
                NOTE 1: If this is empty the units will be "Preferred Units"
                NOTE 2: This will be ignored if no_convert is FALSE
            dtype (str, optional): The field type for the column. Defaults to "".
            no_convert (bool, optional): If True, do not convert units for this column. Defaults to False.
                NOTE: If this is TRUE the units will be display_units and then data_units as a fallback.
            description (str, optional): a description of the column to be displayed to end-users for example in tool-tips

        TODO make table_name mandatory
        """
        unique_id = _get_unique_id(table_name, name)

        if self._field_meta is None:
            self._field_meta = pd.DataFrame(index=[unique_id], columns=FieldMetaValues.VALID_COLUMNS)
            # Ensure the unit columns can hold pint objects
            for col in ['data_unit', 'display_unit']:
                self._field_meta[col] = self._field_meta[col].astype(object)

        # We need to be really strict here to stop users adding columns and innadvertently clobbering existing ones
        # similar names are really easy to miss
        elif unique_id in self._field_meta.index:
            self._log.error(f"Column '{unique_id}' already exists in metadata. SKIPPING ADDITION")
            return

        def to_unit(u: str | pint.Unit | None) -> pint.Unit | None:
            if isinstance(u, pint.Unit) or u is None:
                return u
            if u:
                try:
                    return ureg.Unit(u)
                except Exception as e:
                    self._log.warning(f"Could not parse unit '{u}': {e}")
                    return None
            return None

        self._field_meta.loc[unique_id, "name"] = name
        self._field_meta.loc[unique_id, "friendly_name"] = friendly_name if friendly_name else name
        self._field_meta.loc[unique_id, "table_name"] = table_name
        self._field_meta.loc[unique_id, "data_unit"] = to_unit(data_unit)
        self._field_meta.loc[unique_id, "display_unit"] = to_unit(display_unit)
        self._field_meta.loc[unique_id, "dtype"] = dtype
        self._field_meta.loc[unique_id, "no_convert"] = no_convert
        self._field_meta.loc[unique_id, "description"] = description

    def _no_data_warning(self):
        """ Warn if no metadata is set."""
        if self._field_meta is None:
            self._log.warning("No metadata set. Remember to instantiate the RSFieldMeta using RSFieldMeta().df = meta_df")

    def duplicate_meta(
        self, orig_name: str, new_name: str,
        orig_table_name: Optional[str] = None, new_table_name: Optional[str] = None,
        new_friendly: Optional[str] = None, new_description: Optional[str] = None,
        new_data_unit: Optional[str] = None, new_display_unit: Optional[str] = None,
        new_dtype: Optional[str] = None, new_no_convert: Optional[bool] = None
    ) -> Optional['FieldMetaValues']:
        """Duplicate a row of the metadata table, optionally overriding fields.

        Returns:
            str: The name of the new metadata row.
        Raises:
            RuntimeError: If metadata is not set.
            ValueError: If original does not exist or new already exists.
        """
        self._no_data_warning()
        if self._field_meta is None:
            raise RuntimeError("No metadata set. You need to instantiate RSFieldMeta and set the .meta property first.")

        orig_id = _get_unique_id(orig_table_name, orig_name)
        new_id = _get_unique_id(new_table_name, new_name)

        if orig_id not in self._field_meta.index:
            raise ValueError(f"Original column ID '{orig_id}' does not exist in metadata.")
        if new_id in self._field_meta.index:
            raise ValueError(f"New column ID '{new_id}' already exists in metadata.")

        new_row = self._field_meta.loc[orig_id].copy()
        new_row['name_orig'] = new_name
        new_row['table_name'] = new_table_name

        if new_friendly is not None:
            new_row["friendly_name"] = new_friendly
        if new_description is not None:
            new_row["description"] = new_description
        if new_data_unit is not None:
            new_row["data_unit"] = ureg.Unit(new_data_unit) if new_data_unit else None
        if new_display_unit is not None:
            new_row["display_unit"] = ureg.Unit(new_display_unit) if new_display_unit else None
        if new_dtype is not None:
            new_row["dtype"] = new_dtype
        if new_no_convert is not None:
            new_row["no_convert"] = bool(new_no_convert)

        # Ensure the index is set to new_name
        new_row.name = new_id
        self._field_meta = pd.concat([self._field_meta, pd.DataFrame([new_row])])
        self._log.info(f"Duplicated metadata from '{orig_id}' to '{new_id}'.")
        return self.get_field_meta(new_name, new_table_name)

    def get_field_meta(self, column_name: str, table_name: str | None = None) -> Optional[FieldMetaValues]:
        """Get the field metadata for a specific column. This returns a FieldMetaValues object.

        Args:
            col (str): The column name to get metadata for.
        Returns:
            Optional[FieldMetaValues]: The metadata object for the column or None if not found.
        TODO: include tablename
        """
        self._no_data_warning()
        if self._field_meta is None:
            return None

        unique_id = _get_unique_id(table_name, column_name)
        if unique_id not in self._field_meta.index:
            # If table name was provided and not found, don't try again
            if table_name:
                return None
            # If no table name, check for ambiguous matches
            possible_matches = self._field_meta[self._field_meta['name'].str.lower() == column_name.lower()]
            if len(possible_matches) > 1:
                self._log.warning(f"Ambiguous column '{column_name}'. Found in tables: {possible_matches['table_name'].tolist()}. Provide a table_name.")
                return None
            elif len(possible_matches) == 1:
                unique_id = possible_matches.index[0]
            else:
                return None

        meta_row = self._field_meta.loc[unique_id]
        meta_values = FieldMetaValues()
        meta_values.name = meta_row.get('name', '')
        meta_values.friendly_name = str(meta_row.get("friendly_name", ''))
        meta_values.table_name = str(meta_row.get("table_name", ''))
        meta_values.data_unit = meta_row.get("data_unit")
        meta_values.display_unit = meta_row.get("display_unit")
        meta_values.dtype = str(meta_row.get("dtype", ''))
        meta_values.no_convert = bool(meta_row.get("no_convert", False))
        meta_values.description = str(meta_row.get("description", ''))
        return meta_values

    def get_friendly_name(self, column_name: str, table_name: Optional[str] = None) -> str:
        """Get the friendly name for a column, or format version of column name if not found."""
        fm = self.get_field_meta(column_name, table_name)
        if fm and hasattr(fm, "friendly_name") and fm.friendly_name:
            friendly = fm.friendly_name
        else:
            friendly = column_name.replace('_', ' ').title()
        return friendly

    def get_description(self, column_name: str, table_name: Optional[str] = None) -> str:
        """Get the description for a column, or an empty string if not found."""
        fm = self.get_field_meta(column_name, table_name)
        if fm and hasattr(fm, "description") and fm.description:
            return fm.description
        return ""

    def __set_value(self, row_name, col_name, value, table_name: Optional[str] = None):
        """Generic Property setter. Use this for all the specific setters below."""
        self._no_data_warning()
        # Make sure col_name is a valid property of FieldMetaValues
        if col_name not in FieldMetaValues.VALID_COLUMNS:
            raise ValueError(f"Invalid metadata column '{col_name}'. Valid columns are: {FieldMetaValues.VALID_COLUMNS}")

        unique_id = _get_unique_id(table_name, row_name)
        if self._field_meta is None:
            # Initialize with just this column if needed
            self._field_meta = pd.DataFrame(index=[unique_id], columns=FieldMetaValues.VALID_COLUMNS)
        if unique_id not in self._field_meta.index:
            raise ValueError(f"Row ID '{unique_id}' does not exist in metadata.")
        self._field_meta.loc[unique_id, col_name] = value

    def set_friendly_name(self, col, friendly_name, table_name: Optional[str] = None):
        """Set the friendly name for a column in the metadata."""
        self.__set_value(col, "friendly_name", friendly_name, table_name)

    def set_data_unit(self, col, data_unit, table_name: Optional[str] = None):
        """Set the data unit for a column in the metadata."""
        self.__set_value(col, "data_unit", data_unit, table_name)

    def set_display_unit(self, col, display_unit, table_name: Optional[str] = None):
        """Set the display unit for a column in the metadata."""
        self.__set_value(col, "display_unit", display_unit, table_name)

    def set_dtype(self, col, dtype, table_name: Optional[str] = None):
        """Set the field type for a column in the metadata."""
        self.__set_value(col, "dtype", dtype, table_name)

    def apply_units(self, df: pd.DataFrame, table_name: Optional[str] = None) -> tuple[pd.DataFrame, dict]:
        """ Apply data type and units to a DataFrame based on the metadata. This returns a new (copied) DataFrame with units applied.

        Args:
            df (pd.DataFrame): The DataFrame to apply units to. This DataFrame is NOT modified in place.
            table_name (Optional[str]): The specific table context for applying units. If None, will match columns case-insensitively.

        Returns:
            tuple(pd.DataFrame,dict) : A new DataFrame with units applied, a dict of applied units
        """
        self._log.info("Applying Units")
        if self._field_meta is None:
            raise RuntimeError("No metadata set. You need to instantiate RSFieldMeta and set the .meta property first.")

        # First Make a copy of the source
        df_copy = df.copy()
        applied_units = {}

        # Now loop over the dataframe columns and apply units
        for col in df_copy.columns:
            fm = self.get_field_meta(col, table_name)
            if not fm:  # just to be sure it exists
                continue

            # We specify the field type in our metadata so try to coerce the column to that type first
            if fm.dtype in ('TEXT', 'STRING', 'VARCHAR', 'CHAR', 'UUID'):
                df_copy[col] = df_copy[col].astype("string")
            elif fm.dtype in ('BOOLEAN', 'BOOL'):
                df_copy[col] = df_copy[col].astype('boolean')
            elif fm.dtype in ('DATE', 'DATETIME', 'TIMESTAMP'):
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
            elif fm.dtype in ('INT', 'INTEGER', 'SMALLINT', 'BIGINT', 'MEDIUMINT', 'TINYINT'):
                if not isinstance(df_copy[col].dtype, pint_pandas.PintType):
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce', downcast='integer').astype('Int64')
            elif fm.dtype in ('FLOAT', 'DOUBLE', 'DECIMAL', 'REAL', 'NUMERIC', 'NUMBER'):
                if not isinstance(df_copy[col].dtype, pint_pandas.PintType):
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce', downcast='float').astype('Float64')
            else:
                self._log.debug(f"Unknown field type '{fm.dtype}' for column '{col}'. Skipping type coercion.")
            # If there is any kind of data unit we can work with it
            applied_unit = None
            try:
                if not fm.data_unit:
                    self._log.debug(f'No data unit for column {col}. Skipping unit application.')
                    continue
                df_copy[col] = df_copy[col].astype(f"pint[{fm.data_unit}]")
                if fm.no_convert:
                    # If no_convert is true then we use the display_unit if it exists
                    if fm.display_unit:
                        # Even though no_convert is true we still convert it to the display unit
                        df_copy[col] = df_copy[col].pint.to(fm.display_unit)
                        # put back the dtype
                        # df_copy[col] = df_copy[col].astype(dtype)
                        applied_unit = fm.display_unit
                        self._log.debug(f'Applied {fm.display_unit} to {col} with no_convert using display unit {preferred_unit}')
                    else:
                        applied_unit = fm.data_unit
                        self._log.debug(f'Applied {fm.data_unit} to {col} with no_convert using data unit {preferred_unit}')
                else:
                    preferred_unit = self.get_field_unit(col)
                    df_copy[col] = df_copy[col].pint.to(preferred_unit)
                    # put back the dtype
                    # df_copy[col] = df_copy[col].astype(dtype)
                    applied_unit = preferred_unit
                    self._log.debug(f'Converted {col} from {fm.data_unit} to {preferred_unit} for {self._unit_system} system')

            except Exception as exc:  # pragma: no cover - log unexpected issues
                self._log.warning(f"Unable to apply units '{fm.data_unit}' to column '{col}': {exc}")
            finally:
                applied_units[col] = applied_unit

        self._log.debug('Applied units to dataframe using meta info')
        return df_copy, applied_units

    def get_field_unit(self, name: str, no_convert: bool = False) -> Optional[pint.Unit]:
        """ Get the display unit for a specific column based on the metadata and current unit system.

        This will take into account the display_unit, data_unit, no_convert, and preferred units for the current system.

        Args:
            name (str): The column name to get the display unit for.
            no_convert (bool, optional): If True, return the raw `data_unit` without any conversion. Defaults to False.
        """
        fm = self.get_field_meta(name)
        if not fm:
            return None

        if fm.data_unit:
            # If no_convert is requested, return the raw data_unit immediately.
            if no_convert:
                return fm.data_unit

            # The data unit is always the fallback
            applied_unit = ureg.Unit(fm.data_unit)
            try:
                if fm.display_unit:
                    applied_unit = ureg.Unit(fm.display_unit)
                if fm.no_convert:
                    # If no_convert is true then we use the display_unit if it exists
                    if fm.display_unit is not None and str(fm.display_unit).strip() != "":
                        applied_unit = fm.display_unit
                    else:
                        applied_unit = fm.data_unit
                else:
                    self._log.debug(f'Applied {fm.data_unit} to {name}')
                    applied_unit = self.get_system_units(applied_unit)
            except Exception as exc:  # pragma: no cover - log unexpected issues
                self._log.warning(f"Unable to apply units '{fm.data_unit}' to column '{name}': {exc}")
            return applied_unit
        return None

    def get_field_header(self, name: str, include_units: bool = True, unit_fmt=" ({unit})") -> str:
        """ Get the column header for a specific column based on the metadata.

        Args:
            name (str): The column name to get the header for.
            include_units (bool, optional): Whether to include units in the header name. Defaults to True.
            unit_fmt (str, optional): The format string to use for units. Defaults to " ({unit})".

        Returns:
            str: The column header with friendly name and units if specified.
        """
        fm = self.get_field_meta(name)
        if not fm:
            return name

        # Determine the header label
        header_text = fm.friendly_name if fm and fm.friendly_name else name

        if include_units:
            preferred_unit = self.get_field_unit(name)
            if preferred_unit:
                unit_text = unit_fmt.format(unit=f"{preferred_unit:~P}")
                header_text = f"{header_text}{unit_text}"

        return header_text

    def get_headers(self, df: pd.DataFrame, include_units: bool = True, unit_fmt=" ({unit})") -> List[str]:
        """ Get the column headers for a DataFrame based on the metadata. This will return a list of column 
        headers with friendly names and units if specified.

        Args:
            df (pd.DataFrame): The original dataframe
            include_units (bool, optional): Whether to include units in the header names. Defaults to True.
            unit_fmt (str, optional): The format string to use for units. Defaults to " ({unit})".

        Returns:
            Dict[str, str]: A lookup list of column names to friendly names
        """
        column_headers: List[str] = []
        # Now we loop over all the columns and apply formatting and units if necessary
        for column in list(df.columns):

            fm = self.get_field_meta(column)
            # Determine the header label # TODO consider use get_friendly instead
            header_text = fm.friendly_name if fm and fm.friendly_name else column

            if include_units:
                preferred_unit = self.get_field_unit(column)
                if preferred_unit:
                    unit_text = unit_fmt.format(unit=f"{preferred_unit:~P}")
                    header_text = f"{header_text}{unit_text}"

            column_headers.append(header_text)

        return column_headers

    def get_headers_dict(self, df: pd.DataFrame, include_units: bool = True, unit_fmt=" ({unit})") -> Dict[str, str]:
        """ Get the column headers for a DataFrame based on the metadata. This will return a lookup dict of column 
        names to friendly names with units if specified.

        Args:
            df (pd.DataFrame): The original dataframe
            include_units (bool, optional): Whether to include units in the header names. Defaults to True.
            unit_fmt (str, optional): The format string to use for units. Defaults to " ({unit})".

        Returns:
            Dict[str, str]: A lookup list of column names to friendly names
        """
        column_headers: Dict[str, str] = {}
        headers = list(df.columns)
        friendly_headers = self.get_headers(df, include_units=include_units, unit_fmt=unit_fmt)

        for i, column in enumerate(headers):
            column_headers[column] = friendly_headers[i]

        return column_headers

    def get_system_units(self, in_units: pint.Unit) -> pint.Unit:
        """ get system units

        Args:
            in_units (pint.Unit): _description_

        Returns:
            pint.Unit: _description_
        """

        if self._unit_system == 'SI':
            # We are in SI so convert any imperial units to SI
            lookup = IMPERIAL_TO_SI
            reverse_lookup = SI_TO_IMPERIAL
        else:
            # We are in imperial so convert any SI units to imperial
            lookup = SI_TO_IMPERIAL
            reverse_lookup = IMPERIAL_TO_SI

        lookup_val = lookup.get(str(in_units), None)
        reverse_val = reverse_lookup.get(str(in_units), None)

        # Test if we need a conversion and if not just return the input
        if lookup_val is None or lookup_val == in_units:
            # If we didn't find a conversion and the unit is not in the reverse lookup then warn
            if lookup_val is None and reverse_val is None:
                self._log.warning(f"No conversion found for unit '{in_units}' in current system '{self._unit_system}'.")
            return in_units
        try:
            out_unit = ureg.Unit(lookup_val)
            return out_unit
        except Exception as exc:  # pragma: no cover - log unexpected issues
            self._log.warning(f"Unable to convert unit '{in_units}' to '{lookup_val}': {exc}")
            return in_units

    def get_system_unit_value(self, in_qty: pint.Quantity) -> pint.Quantity:
        """ Use SI_TO_IMPERIAL and IMPERIAL_TO_SI to get explicit units for a given input unit based on the current unit system.
        Fail safely with a warning if the unit is not in the lookup table

        Args:
            in_qty (pint.Quantity): A pint quantity with units to convert

        Returns:
            A pint quantity in the preferred units for the current system
        """
        if not isinstance(in_qty, pint.Quantity):
            raise ValueError("Input must be a Pint Quantity with units.")

        sys_units = self.get_system_units(ureg.Unit(in_qty.units))

        try:
            out_value = in_qty.to(sys_units)
            return out_value
        except Exception as exc:  # pragma: no cover - log unexpected issues
            self._log.warning(f"Unable to convert unit '{in_qty.units}' to '{sys_units}': {exc}")
            return in_qty

    def bake_units(self, df: pd.DataFrame, header_units: bool = True) -> Tuple[pd.DataFrame, List[str]]:
        """ Apply units to a DataFrame based on the metadata. Returns a copy of the dataframe and the corresponding headers.

        Args:
            df (pd.DataFrame): The DataFrame to apply units to. This DataFrame is not modified in place.

        Returns:
            Tuple[pd.DataFrame, List[str]]: The modified DataFrame with units applied and the corresponding headers.

        Issue: Since the return df has different headers from the original, it loses the connection to the metadata (will not be able to get description)
        """

        # First apply the units
        df_baked, _ = self.apply_units(df)
        # Now get the headers
        headers = self.get_headers(df, include_units=header_units)

        # Vectorized conversion for pint columns (robust check)
        for column in list(df_baked.columns):
            if isinstance(df_baked[column].dtype, pint_pandas.PintType):
                df_baked[column] = df_baked[column].pint.magnitude

        self._log.debug('Baked units into dataframe using meta info')

        # We return the object and its nice headers separately so the user
        # can decide when they want to overwrite the headers
        return df_baked, headers
