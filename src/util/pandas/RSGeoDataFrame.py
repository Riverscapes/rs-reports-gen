from typing import Callable, Dict, List, Optional
import pint  # noqa: F401  # pylint: disable=unused-import
import pint_pandas  # noqa: F401  # pylint: disable=unused-import # this is needed !?
from rsxml import Logger
# Custom DataFrame accessor for metadata - to be moved to util
import pandas as pd
import geopandas as gpd
from util.pandas.RSFieldMeta import RSFieldMeta

ureg = pint.get_application_registry()


class RSGeoDataFrame(gpd.GeoDataFrame):
    """ A module to extend pandas DataFrames with remote sensing specific functionality.

    This is a subclass of GeoDataFrame so it should work just like a normal GeoDataFrame.

    For starters it doesn't "DO" anything too special except:

        1. Overrides the to_html method to provide friendly column names and units
        2. Provides a convenient property for adding footer rows to your dataframes


    """

    def __init__(self, df: gpd.GeoDataFrame, *args,
                 footer: Optional[pd.DataFrame] = None,
                 **kwargs):
        super().__init__(df, *args, **kwargs)
        self.log = Logger('RSDataFrame')

        # This is our metadata singleton. Mostly this is just here for convenience
        self._meta_df = RSFieldMeta()

        # This is a footer DataFrame that will be appended to the main DataFrame when rendering
        object.__setattr__(self, "_footer", footer if footer is not None else pd.DataFrame())

    @property
    def _constructor(self):
        """ This will ensure that every time we do an operation that returns a DataFrame,
        """
        return RSGeoDataFrame

    @property
    def _constructor_sliced(self):
        return pd.Series  # or a custom subclass if you have one

    def set_footer(self, footer: pd.DataFrame):
        """ Set a footer DataFrame that will be appended to the main DataFrame when rendering.

        Args:
            footer (pd.DataFrame): The footer DataFrame to append.
        """
        self._footer = footer

    def copy(self, deep: bool = True):
        """ Override the copy method to ensure the metadata is preserved.
        """
        df_copy = super().copy(deep=deep)
        footer = self._footer.copy(deep=deep)
        # Make sure the footer comes along for the ride
        return RSGeoDataFrame(df_copy, footer=footer)

    def export_excel(self, output_path: str):
        """ Export the GeoDataFrame to an Excel file with metadata.

        Args:
            output_path (str): The path to save the Excel file.
        """
        self.log.info(f"Exporting data to Excel at {output_path}")

        baked_gdf, baked_headers = self._meta_df.bake_units(self)
        baked_gdf.columns = baked_headers

        # Now add the footer columns if there are any
        if not self._footer.empty:
            self.log.debug("Adding footer to Excel export")
            footer = self._footer.copy()
            # Ensure footer has the same columns as baked_gdf
            for col in baked_gdf.columns:
                if col not in footer.columns:
                    footer[col] = pd.NA
            footer = footer[baked_gdf.columns]  # Reorder columns to match
            baked_gdf = pd.concat([baked_gdf, footer], ignore_index=True)

        # excel version
        with pd.ExcelWriter(output_path) as writer:
            baked_gdf.to_excel(writer, sheet_name="data")
            self._meta_df.field_meta.to_excel(writer, sheet_name="metadata")
        self.log.info(f"Excel export complete. See it here: {output_path}")

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
        # Filter columns if needed. First inclusively then exclusively
        display_df = self.copy()

        # Now we do our filtering
        if include_columns is not None:
            include_existing = [col for col in include_columns if col in display_df.columns]
            display_df = self.loc[:, include_existing]
        if exclude_columns is not None:
            display_df = self.drop(columns=exclude_columns, errors='ignore')

        # Now apply the units to the dataframe if needed
        display_df, applied_units = self._meta_df.apply_units(display_df) if include_units else [display_df, {}]
        headers = display_df.columns.to_list()

        if use_friendly is True:
            headers = self._meta_df.get_headers(display_df, include_units=include_units, unit_fmt=unit_fmt)

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

        column_classes: Dict[str, str] = {}
        formatters: Dict[str, Callable[[object], str]] = {}

        # Now we loop over all the columns and apply formatting and units if necessary
        for column in list(display_df.columns):
            # These are the classes we apply to the columns
            class_tokens: List[str] = []

            if include_units:
                unit_obj = applied_units.get(column)
                if isinstance(unit_obj, pint.Unit):
                    dim_name = RSFieldMeta.get_dimensionality_name(unit_obj)
                    if dim_name:
                        class_tokens.append(dim_name)

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
                formatters[column] = lambda x: "-"
            elif is_bool_type:
                class_tokens.append('boolean')
                formatters[column] = _format_boolean
            elif is_datetime_type:
                class_tokens.append('datetime')
                formatters[column] = _format_datetime
            # Numeric types go last so we can convert them to magnitude first
            elif is_integer_type or is_decimal_type:
                class_tokens.append('numeric')
                display_df[column] = col_magnitude
                if is_integer_type:
                    class_tokens.append('integer')
                    formatters[column] = _format_int
                elif is_decimal_type:
                    class_tokens.append('decimal')
                    formatters[column] = _format_float
            else:
                class_tokens.append('text')
                formatters[column] = _format_text

            column_classes[column] = ' '.join(dict.fromkeys(class_tokens)) if class_tokens else ''

        # If self._footer is set and not empty, append it to the display_df
        row_classes: Dict[int, str] = {}
        if not self._footer.empty:
            footer_df = self._footer.copy()
            # Ensure footer has the same columns as display_df
            for col in display_df.columns:
                if col not in footer_df.columns:
                    footer_df[col] = pd.NA
            footer_df = footer_df[display_df.columns]  # Reorder columns to match
            display_df = pd.concat([display_df, footer_df], ignore_index=True)
            # We also want to add a class called "footer" to the row of any footer rows
            footer_start_idx = len(display_df) - len(footer_df)
            for idx in range(footer_start_idx, len(display_df)):
                row_classes[idx] = "footer"

        styler: pd.io.formats.style.Styler = display_df.style
        styler = styler.format(formatters, na_rep="")

        # Set table HTML attributes
        table_attributes = kwargs.pop('table_attributes', None)
        if table_attributes is None:
            table_classes = ['dataframe', 'rs-table']
            if table_classes:
                table_attributes = f'class="{' '.join(table_classes)}"'
        if table_attributes:
            styler = styler.set_table_attributes(table_attributes)

        # Create a DataFrame of classes if we have any column classes to apply
        if not display_df.empty:
            combined_classes = {}
            for col in display_df.columns:
                col_classes = [column_classes.get(col, '') for _ in range(len(display_df))]
                # If row_classes exist, merge them in
                for i in range(len(display_df)):
                    row_class = row_classes.get(i, {})
                    # Combine both, avoiding duplicates and empty strings
                    tokens = set(filter(None, (col_classes[i], row_class)))
                    combined_classes.setdefault(col, []).append(' '.join(tokens))
            classes_df = pd.DataFrame(combined_classes, index=display_df.index)
            styler = styler.set_td_classes(classes_df)

        # Apply the headers
        display_df.columns = headers
        # hide the index
        styler.hide()
        return styler.to_html(*args, **kwargs)
