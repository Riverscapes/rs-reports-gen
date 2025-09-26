from typing import Optional, Dict
import pint  # noqa: F401  # pylint: disable=unused-import
import pint_pandas  # noqa: F401  # pylint: disable=unused-import # this is needed !?
from rsxml import Logger
# Custom DataFrame accessor for metadata - to be moved to util
import pandas as pd

ureg = pint.get_application_registry()

VALID_COLUMNS = ["name", "friendly_name", "unit", "dtype", "no_convert"]


# These are the default preferred units for SI and imperial systems for our report.
# You can override these by setting RSFieldMeta().preferred_units = { ... }
_PREFERRED_UNITS: Dict[str, Dict[str, str]] = {
    'SI': {
        'length': 'meter',
        'area': 'kilometer ** 2',
        'volume': 'meter ** 3',
        'mass': 'kilogram',
        'time': 'second',
    },
    'imperial': {
        'length': 'foot',
        'area': 'acres',
        'volume': 'foot ** 3',
        'mass': 'pound',
        'time': 'second',
    },
}


class RSFieldMeta:
    """ A Borg pattern to share metadata across multiple DataFrames.

    Returns:
        _type_: _description_
    """
    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state
        self._log = Logger('RSFieldMeta')
        if not hasattr(self, "_meta"):
            self._meta: Optional[pd.DataFrame] = None
        if not hasattr(self, "_unit_system"):
            self._unit_system = 'SI'  # default to SI
            self._log.debug(f'Set default unit system to {self._unit_system}')
        if not hasattr(self, "_preferred_units"):
            self._preferred_units = _PREFERRED_UNITS

    @property
    def meta(self) -> Optional[pd.DataFrame]:
        """Get the metadata DataFrame.

        Returns:
            Optional[pd.DataFrame]: The metadata DataFrame or None if not set.
        """
        return self._meta

    @property
    def unit_system(self) -> str:
        """Get the current unit system.

        Returns:
            str: The current unit system.
        """
        return self._unit_system

    @property
    def preferred_units(self) -> Dict[str, Dict[str, str]]:
        """Get the preferred units mapping.

        Returns:
            Dict[str, Dict[str, str]]: The preferred units mapping.
        """
        return self._preferred_units

    @meta.setter
    def meta(self, value: pd.DataFrame):
        """ Set or extend the metadata DataFrame.

        Args:
            value (pd.DataFrame): The metadata DataFrame to set.
        """

        def _make_boolean(val):
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                val_lower = val.strip().lower()
                if val_lower in ('true', '1', 'yes'):
                    return True
                elif val_lower in ('false', '0', 'no'):
                    return False
            if isinstance(val, (int, float)):
                return val != 0
            return False

        # We explicitly set the no_convert column to boolean and convert any possible values
        if "no_convert" in value.columns:
            # It's coming in possibly as object/string so we need to convert it
            value["no_convert"] = value["no_convert"].apply(_make_boolean)
            value["no_convert"] = value["no_convert"].astype(bool)
        else:
            value["no_convert"] = False

        # IF there is no meta then set it
        if self._meta is None:
            self._log.info("Setting metadata for the first time.")
            self._meta = value
            self._meta.set_index("name", inplace=True)
        else:
            # otherwise extend the existing metadata with the new dataframe
            self._log.warning("Extending existing metadata. This may overwrite existing columns.")
            self._meta = pd.concat([self._meta, value]).drop_duplicates().set_index("name")
            self._log.info("Metadata extended successfully.")

    @unit_system.setter
    def unit_system(self, system: str):
        """Set the unit system for rendering columns with units."""
        normalized = system.strip()
        valid_systems = set(_PREFERRED_UNITS.keys())
        if normalized not in valid_systems:
            raise ValueError(f"Invalid unit system '{system}'. Valid options are: {list(valid_systems)}")
        ureg.default_system = normalized
        canonical = ureg.get_system(normalized).name
        self._unit_system = canonical
        self._log.info(f'Set default unit system to {canonical}')

    @preferred_units.setter
    def preferred_units(self, mapping: Dict[str, Dict[str, str]]):
        """Set the preferred units mapping.

        Args:
            mapping (Dict[str, Dict[str, str]]): The preferred units mapping.
        """
        # Test to make sure only SI and imperial are present in the keys
        if not all(key in _PREFERRED_UNITS for key in mapping.keys()):
            raise ValueError("Preferred units mapping contains unknown unit systems. Valid systems are 'SI' and 'imperial'.")
        self._preferred_units = mapping
        self._log.info("Preferred units mapping updated.")

    def preferred_unit_for(self, unit_obj: pint.Unit) -> Optional[str]:
        """Return the preferred unit string for the current system and dimensionality."""
        system_units = self._preferred_units.get(self._unit_system)
        if not system_units:
            self._log.warning(f"No preferred units mapping found for unit system '{self._unit_system}'.")
            return None
        dim_name = RSFieldMeta.get_dimensionality_name(unit_obj)
        if dim_name is None:
            self._log.warning(f"Could not determine dimensionality for unit '{unit_obj}'.")
            return None
        preferred = system_units.get(dim_name)
        if preferred is None:
            self._log.warning(f"No preferred unit found for dimensionality '{dim_name}' in system '{self._unit_system}'.")
        return preferred

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

    def clear(self):
        """ Clear the shared metadata.
        """
        self._meta = None

    def add_meta_column(self, col_name: str, friendly_name: str = "", unit: str = "", field_type: str = ""):
        """ Add a new column to the metadata DataFrame if it does not already exist.

        Args:
            col_name (str): The name of the column to add.
            friendly_name (str, optional): The friendly name for the column. Defaults to "".
            unit (str, optional): The unit for the column. Defaults to "".
            field_type (str, optional): The field type for the column. Defaults to "".
        """
        if self._meta is None:
            self._meta = pd.DataFrame(index=[col_name], columns=VALID_COLUMNS)

        # We need to be really strict here to stop users adding columns and innadvertently clobbering existing ones
        # similar names are really easy to miss
        if col_name in self._meta.index:
            raise ValueError(f"Column '{col_name}' already exists in metadata.")

        self._meta.loc[col_name, "friendly_name"] = friendly_name if friendly_name else col_name
        self._meta.loc[col_name, "unit"] = unit
        self._meta.loc[col_name, "dtype"] = field_type
        self._meta.loc[col_name, "no_convert"] = False

    def _no_data_warning(self):
        """ Warn if no metadata is set."""
        if self._meta is None:
            self._log.warning("No metadata set. Remember to instantiate the RSFieldMeta using RSFieldMeta().df = meta_df")

    def friendly(self, col):
        """Get the friendly name for a column."""
        self._no_data_warning()
        if self._meta is None:
            return col
        if col in self._meta.index:
            val = self._meta.loc[col, "friendly_name"]
            return val if pd.notnull(val) and str(val).strip() != "" else col
        return col

    def field_type(self, col):
        """Get the field type for a column."""
        self._no_data_warning()
        if self._meta is None:
            return ""
        return self._meta.loc[col, "type"] if col in self._meta.index else ""

    def unit(self, col):
        """Get the unit for a column."""
        self._no_data_warning()
        if self._meta is None:
            return ""
        return self._meta.loc[col, "unit"] if col in self._meta.index else ""

    def no_convert(self, col) -> bool:
        """Get the no_convert flag for a column."""
        self._no_data_warning()
        if self._meta is None:
            return False
        return bool(self._meta.loc[col, "no_convert"]) if col in self._meta.index else False

    def set_friendly(self, col, friendly_name):
        """Set the friendly name for a column in the metadata."""
        self._no_data_warning()
        if self._meta is None:
            # Initialize with just this column if needed
            self._meta = pd.DataFrame(index=[col], columns=VALID_COLUMNS)
        if col not in self._meta.index:
            raise ValueError(f"Column '{col}' does not exist in metadata. Cannot set friendly name.")
        self._meta.loc[col, "friendly_name"] = friendly_name

    def set_unit(self, col, unit):
        """Set the unit for a column in the metadata."""
        self._no_data_warning()
        if self._meta is None:
            self._meta = pd.DataFrame(index=[col], columns=VALID_COLUMNS)
        if col not in self._meta.index:
            raise ValueError(f"Column '{col}' does not exist in metadata. Cannot set unit.")
        self._meta.loc[col, "unit"] = unit

    def set_type(self, col, field_type):
        """Set the field type for a column in the metadata."""
        self._no_data_warning()
        if self._meta is None:
            self._meta = pd.DataFrame(index=[col], columns=VALID_COLUMNS)
        if col not in self._meta.index:
            raise ValueError(f"Column '{col}' does not exist in metadata. Cannot set field type.")
        self._meta.loc[col, "type"] = field_type

    def apply_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply units to a DataFrame based on the metadata. This returns a new (copied) DataFrame with units applied.

        Args:
            df (pd.DataFrame): The DataFrame to apply units to. This DataFrame is NOT modified in place.

        Returns:
            pd.DataFrame: A new DataFrame with units applied.
        """
        if self._meta is None:
            raise RuntimeError("No metadata set. You need to instantiate RSFieldMeta and set the .meta property first.")

        # First Make a copy of the source
        df_copy = df.copy()
        applied_units = {}

        # Now loop over the metadata and apply units where possible
        for col, row in self._meta.iterrows():
            # Only apply if the column exists in the dataframe.
            if col not in df_copy.columns:
                continue

            unit = row["unit"]
            no_convert = row.get("no_convert", False)
            # We specify the field type in our metadata so try to coerce the column to that type first
            field_type = row.get("type", "").upper() if "type" in row else ""
            if field_type in ('TEXT', 'STRING', 'VARCHAR', 'CHAR', 'UUID', ''):
                df_copy[col] = df_copy[col].astype("string")
            if field_type in ('BOOLEAN', 'BOOL'):
                df_copy[col] = df_copy[col].astype('boolean')
            if field_type in ('DATE', 'DATETIME', 'TIMESTAMP'):
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
            if field_type in ('INT', 'INTEGER', 'SMALLINT', 'BIGINT', 'MEDIUMINT', 'TINYINT'):
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce', downcast='integer').astype('Int64')
            if field_type in ('FLOAT', 'DOUBLE', 'DECIMAL', 'REAL', 'NUMERIC', 'NUMBER'):
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce', downcast='float').astype('Float64')

            # Finally apply the unit if it is not null or empty
            if pd.notnull(unit) and str(unit).strip() != "":
                unit_str = str(unit).strip()
                applied_unit = unit_str
                try:
                    df_copy[col] = df_copy[col].astype(f"pint[{unit_str}]")
                    unit_obj = ureg.Unit(unit_str)
                    preferred_unit = self.preferred_unit_for(unit_obj)
                    if not no_convert and preferred_unit and preferred_unit != unit_str:
                        df_copy[col] = df_copy[col].pint.to(preferred_unit)
                        applied_unit = preferred_unit
                        self._log.debug(f'Converted {col} from {unit_str} to {preferred_unit} for {self._unit_system} system')
                    else:
                        self._log.debug(f'Applied {unit_str} to {col}')
                except Exception as exc:  # pragma: no cover - log unexpected issues
                    self._log.debug(f"Unable to apply units '{unit_str}' to column '{col}': {exc}")
                finally:
                    applied_units[col] = applied_unit

        self._log.debug('Applied units to dataframe using meta info')
        return df_copy, applied_units
