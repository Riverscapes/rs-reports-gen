from typing import Callable, Dict, List, Optional
import pint  # noqa: F401  # pylint: disable=unused-import
import pint_pandas  # noqa: F401  # pylint: disable=unused-import # this is needed !?
from pint.errors import UndefinedUnitError
from rsxml import Logger
# Custom DataFrame accessor for metadata - to be moved to util
import pandas as pd
import geopandas as gpd
from util.pandas.RSFieldMeta import RSFieldMeta

ureg = pint.get_application_registry()


class RSGeoDataFrame(gpd.GeoDataFrame):
    """ A module to extend pandas DataFrames with remote sensing specific functionality.
    """

    def __init__(self, df: gpd.GeoDataFrame, *args, **kwargs):
        super().__init__(df, *args, **kwargs)
        self.log = Logger('RSDataFrame')
        self._meta_df = RSFieldMeta()

    def __repr__(self):
        return f"RSDataFrame({self.shape})"

    def to_html(self, *args,
                include_units=True,
                use_friendly=True,
                unit_fmt=" ({unit})",
                include_columns: Optional[List[str]] = None,
                exclude_columns: Optional[List[str]] = None,
                **kwargs):
        """Render the DataFrame as HTML with friendly column headings.

        Args:
            include_units(bool): Append unit text for columns that have units.
            use_friendly(bool): Replace raw column names with friendly names when available.
            unit_fmt(str): Format string applied when appending units(must include ``{unit}``).
            include_columns(Optional[List[str]]): If provided, limit output to these columns.
            exclude_columns(Optional[List[str]]): Columns to drop from the output.
            **kwargs: Forwarded to: meth: `pandas.DataFrame.to_html`.
        """

        # Start with a copy of the DataFrame. This is ideally a unitted Dataframe if possible
        # If we are including units, we need to apply them first
        display_df, applied_units = self._meta_df.apply_units(self) if include_units else [self.copy(), {}]

        def _to_magnitude(val):
            return val.magnitude if hasattr(val, 'magnitude') else val

        def _format_int(val):
            if pd.isna(val):
                return ""
            try:
                return f"{int(round(float(val))):,}"
            except (TypeError, ValueError):
                return str(val)

        def _format_float(val):
            if pd.isna(val):
                return ""
            try:
                return f"{float(val):,.2f}"
            except (TypeError, ValueError):
                return str(val)

        def _format_datetime(val):
            if pd.isna(val):
                return ""
            if hasattr(val, 'strftime'):
                return val.strftime('%Y-%m-%d %H:%M:%S')
            return str(val)

        def _format_boolean(val):
            if pd.isna(val):
                return ""
            if isinstance(val, str):
                lowered = val.strip().lower()
                if lowered in {'true', 't', 'yes', 'y', '1'}:
                    return 'Yes'
                if lowered in {'false', 'f', 'no', 'n', '0'}:
                    return 'No'
                return val
            return 'Yes' if bool(val) else 'No'

        def _format_text(val):
            if pd.isna(val):
                return ""
            elif isinstance(val, str):
                return val.strip()
            else:
                return str(val)

        # Filter columns if needed. First inclusively then exclusively
        if include_columns is not None:
            include_existing = [col for col in include_columns if col in display_df.columns]
            display_df = display_df.loc[:, include_existing]
        if exclude_columns is not None:
            display_df = display_df.drop(columns=exclude_columns, errors='ignore')

        column_headers: List[str] = []
        column_classes: Dict[str, str] = {}
        formatters: Dict[str, Callable[[object], str]] = {}

        # Now we loop over all the columns and apply formatting and units if necessary
        for column in list(display_df.columns):
            # These are the classes we apply to the columns
            class_tokens: List[str] = []

            # Determine the header label
            header_text = column
            if use_friendly:
                header_text = self._meta_df.friendly(column)

            unit_str = ""
            if include_units:
                unit = applied_units.get(column) or self._meta_df.unit(column)
                if isinstance(unit, str) and unit.strip():
                    raw_unit = unit.strip()
                    try:
                        unit_obj = ureg.Unit(raw_unit)
                        unit_str = f"{unit_obj:~P}"
                        dim_name = RSFieldMeta.get_dimensionality_name(unit_obj)
                        if dim_name:
                            class_tokens.append(dim_name)
                    except UndefinedUnitError:
                        unit_str = raw_unit
                    except Exception as exc:  # pragma: no cover - log unexpected issues
                        self.log.debug(f"Unable to parse unit '{raw_unit}' for column '{column}': {exc}")
                        unit_str = raw_unit

                if unit_str:
                    header_text = f"{header_text}{unit_fmt.format(unit=unit_str)}"

            # Always convert to magnitude for type checking
            col_magnitude = display_df[column].apply(_to_magnitude)

            is_integer_type = pd.api.types.is_integer_dtype(col_magnitude)
            is_decimal_type = pd.api.types.is_float_dtype(col_magnitude)
            is_bool_type = pd.api.types.is_bool_dtype(col_magnitude)
            is_datetime_type = pd.api.types.is_datetime64_any_dtype(col_magnitude)
            is_text_type = pd.api.types.is_string_dtype(col_magnitude)
            is_all_nan = col_magnitude.isna().all()

            if is_all_nan:
                # just return '-' for these columns
                formatters[header_text] = lambda x: "-"
            elif is_bool_type:
                class_tokens.append('boolean')
                formatters[header_text] = _format_boolean
            elif is_datetime_type:
                class_tokens.append('datetime')
                formatters[header_text] = _format_datetime
            # Numeric types go last so we can convert them to magnitude first
            elif is_integer_type or is_decimal_type:
                class_tokens.append('numeric')
                display_df[column] = col_magnitude
                if is_integer_type:
                    class_tokens.append('integer')
                    formatters[header_text] = _format_int
                elif is_decimal_type:
                    class_tokens.append('decimal')
                    formatters[header_text] = _format_float
            else:
                class_tokens.append('text')
                formatters[header_text] = _format_text

            column_classes[header_text] = ' '.join(dict.fromkeys(class_tokens)) if class_tokens else ''
            column_headers.append(header_text)

        display_df.columns = column_headers

        styler: pd.io.formats.style.Styler = display_df.style
        styler = styler.format(formatters, na_rep="")

        # Set table attributes
        table_attributes = kwargs.pop('table_attributes', None)
        if table_attributes is None:
            table_classes = ['dataframe', 'rs-table']
            if table_classes:
                table_attributes = f'class="{' '.join(table_classes)}"'
        if table_attributes:
            styler = styler.set_table_attributes(table_attributes)

        # Create a DataFrame of classes if we have any column classes to apply
        if column_classes and not display_df.empty:
            class_matrix = {
                col: [column_classes.get(col, '') for _ in range(len(display_df))]
                for col in display_df.columns
            }
            classes_df = pd.DataFrame(class_matrix, index=display_df.index)
            styler = styler.set_td_classes(classes_df)
        # hide the index
        styler.hide()
        return styler.to_html(*args, **kwargs)
