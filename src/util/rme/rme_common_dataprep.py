"""library of common data preparation options for Riverscape Metric Engine data"""

import re

import numpy as np
import pandas as pd
from rsxml import Logger

from util import round_down, round_up
from util.figures import get_bins_info  # future enhancement: move the fn since it is a data function not a figure function
from util.pandas import RSFieldMeta

# dictionary of functions is at the bottom


def add_common_rme_cols(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Add any calculated columns to the dataframe
    These could be bins.
    When adding columns to this function, add metadata at the same time
    Args:
        df (pd.DataFrame): Input dataframe
        columns (list[str]) : columns from the dictionary of CALCULATED_COLS

    Returns:
        pd.DataFrame: DataFrame with calculated columns added
    """

    for col in columns:
        if col in CALCULATED_COLS:
            fn = CALCULATED_COLS[col]
            df = fn(df)

    return df


def bin_continuous_column(df: pd.DataFrame, field_nm: str, bin_lookup_nm: str | None = None, binned_field_nm: str | None = None, binned_field_friendly: str | None = None) -> pd.DataFrame:
    """Bin a continuous column - ie create a new column with discrete values depending on values in field_nm
    Adds metadata as well

    Args:
        df : dataframe containing the column to bin on (field_nm)
        bin_lookup_nm : name to lookup in bins.json (defaults to same as field_nm if not provided)
        binned_field_nm  : name of the new binned field (defaults to field_nm_bin)
        binned_field_friendl_nm : friendly name for new binned field
    Returns:
        input df with new column added
    TODO: check if new column already exists, warn/error otherwise we are probably over-writing it
    """
    meta = RSFieldMeta()

    if not bin_lookup_nm:
        bin_lookup_nm = field_nm
    if not binned_field_nm:
        binned_field_nm = bin_lookup_nm + "_bins"
    if not binned_field_friendly:
        binned_field_friendly = meta.get_friendly_name(field_nm) + ' (binned)'
    edges, labels, _colours = get_bins_info(bin_lookup_nm)
    df[binned_field_nm] = pd.cut(df[field_nm], bins=edges, labels=labels, include_lowest=True)

    meta.add_field_meta(name=binned_field_nm, friendly_name=binned_field_friendly)  # the type is categorical, should we add that?
    return df


def bins_continuous_equal_width(df: pd.DataFrame, col_name: str, n_bins: int, rounding: int = 2, binned_col_name: str | None = None, binned_col_friendly: str | None = None) -> pd.DataFrame:
    """
    Add an equal-width binned column to the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.
        col_name (str): Name of the column to bin.
        n_bins (int): Number of bins.
        binned_col_name (str, optional): Name for the new binned column. Defaults to '{col_name}_bins'.
        include_lowest (bool): Whether the first interval should be left-inclusive.
        labels (list or None): Optional labels for the bins.

    Returns:
        pd.DataFrame: DataFrame with the new binned column added.

    Caution - although the lowest and highest parts of the bin are rounded, the intermediate bin edges are not
    TODO: check if binned_col_name already exists, warn if so
    """
    log = Logger('Bin equal-width')
    meta = RSFieldMeta()  # this now belongs to the borg collective

    if binned_col_name is None:
        binned_col_name = f"{col_name}_{n_bins}bins"
    min_val = df[col_name].min()
    max_val = df[col_name].max()
    # round min down, max up
    min_edge = round_down(min_val, rounding)
    max_edge = round_up(max_val, rounding)
    log.debug(f'Data for {col_name} from {min_val} to {max_val} to be placed into {n_bins} bins, from {min_edge} to {max_edge}')
    edges = np.linspace(min_edge, max_edge, n_bins + 1)  # magnitudes
    decimals = rounding if rounding >= 0 else 0
    # TODO: improve labels
    # to include number formatting (e.g. comma separating thousands)
    # and units (ie baked values) ~P - would have to multiply edges by the column units
    # see caution above though, edges are not actually rounded
    labels = [f"{round(edges[i], rounding):,.{decimals}f} to {round(edges[i + 1], rounding):,.{decimals}f}" for i in range(n_bins)]
    df[binned_col_name] = pd.cut(df[col_name], bins=edges, labels=labels, include_lowest=True)

    # add to metadata registry
    if not binned_col_friendly:
        binned_col_friendly = meta.get_friendly_name(col_name) + f' {n_bins} bins'
    binned_col_description = meta.get_field_header(col_name) + f' divided into {n_bins} equal-width bins'

    meta.add_field_meta(name=binned_col_name, friendly_name=binned_col_friendly, description=binned_col_description)  # the type is categorical, should we add that?

    return df


def channel_length_example(df: pd.DataFrame) -> pd.DataFrame:
    """Example bespoke function
    Args:
        dataframe - must contain columns 'rel_flow_length' & 'centerline_length'
    NOTE: This type of calculation could and indeed SHOULD be done in the athena view rpt_rme and the column metadata added there too
    Report-time calc columns Python manipulation should be for things that are expensive or not easy to do in SQL
    """
    fld_nm = 'channel_length_ex'
    df[fld_nm] = df['rel_flow_length'] * df['centerline_length']

    RSFieldMeta().add_field_meta(name=fld_nm, friendly_name='Channel Length EXAMPLE', description='Channel Length calculated from centerline length and relative length. Should match stream_length.', data_unit='m', dtype='REAL')

    return df


def riparian_veg_departure_as_departure(df: pd.DataFrame) -> pd.DataFrame:
    """the riparian_veg_departure field we get from rme is not actually departure, it's the raw ratio
    NOTE: We could and indeed SHOULD calculate this in the athena view rpt_rme
    Report-time calc columns Python manipulation should be for things that aren't this easy to do in SQL or are only needed by one report (and then by definition wouldnt be in this common module)
    But this works for now
    """
    fld_nm = 'riparian_veg_departure_as_departure'
    # 1 minus the value if the value is greater than 0. The -9999 will still be -9999
    df[fld_nm] = df["riparian_veg_departure"].apply(lambda x: 1 - x if x >= 0 else x)
    return df


def color_to_hex(color_str: str) -> str:
    """Convert an rgb(...), hsl(...), or #hex color string to a 7-character hex string.

    Supports:
        - '#rrggbb' / '#rgb' pass-through
        - 'rgb(r, g, b)' with integer 0-255 values
        - 'hsl(h, s%, l%)' with h in 0-360, s/l in 0-100

    Returns:
        str: e.g. '#ff0000'
    """
    s = color_str.strip()
    if s.startswith('#'):
        if len(s) == 4:
            return f'#{s[1] * 2}{s[2] * 2}{s[3] * 2}'
        return s.lower()

    rgb_match = re.match(r'rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)', s)
    if rgb_match:
        r, g, b = (int(x) for x in rgb_match.groups())
        return f'#{r:02x}{g:02x}{b:02x}'

    hsl_match = re.match(r'hsl\(\s*([\d.]+)\s*,\s*([\d.]+)%\s*,\s*([\d.]+)%\s*\)', s)
    if hsl_match:
        import colorsys

        h_deg, s_pct, l_pct = (float(x) for x in hsl_match.groups())
        r, g, b = colorsys.hls_to_rgb(h_deg / 360, l_pct / 100, s_pct / 100)
        return f'#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}'

    return s  # return as-is if format not recognised


# Mapping of DataFrame column name → bins.json lookup key.
# Where the column name matches the bins.json key, only one entry is needed.
# Where they differ (e.g. a calculated column), map explicitly.
# FUTURE ENHANCEMENT: these should come from `preferred_bin_definition` in Athena `layer_definitions_latest` view
# Will also require that bins.json be a shared published resource probably in xml.riverscapes.net
COLUMN_TO_BIN_KEY: dict[str, str] = {
    'low_lying_ratio': 'low_lying_ratio',
    'lf_riparian_prop': 'lf_riparian_prop',
    'fldpln_access': 'fldpln_access',
    'land_use_intens': 'land_use_intens',
    'lf_agriculture_prop': 'lf_agriculture_prop',
    'road_dens': 'road_density',
    'rail_dens': 'road_density',
    'confinement_ratio': 'confinement_ratio',
    'brat_hist_capacity': 'brat_capacity',
    'brat_capacity': 'brat_capacity',
    'riparian_condition': 'riparian_condition',
}


def apply_all_bins(
    df: pd.DataFrame,
    column_to_bin_key: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Apply bins from bins.json to every mappable column in the DataFrame.

    For each column listed in *column_to_bin_key* that exists in *df*, three
    new columns are added:
        * ``{col}_bin``      – the human-readable label from bins.json
        * ``{col}_color``    – the corresponding colour converted to hex
        * ``{col}_bin_sort`` – 0-based integer preserving the defined bin order

    This is designed to produce "Power BI ready" data: end-users can use
    the ``_color`` column directly for conditional formatting, the
    ``_bin`` column for slicers / legends, and ``_bin_sort`` for the
    "Sort by Column" feature in Power BI (since label sort order is
    lost when written to Parquet).

    Args:
        df: Input DataFrame (not modified in-place; a copy is returned).
        column_to_bin_key: Mapping of df column → bins.json key.
            Defaults to ``COLUMN_TO_BIN_KEY`` if not supplied.

    Returns:
        pd.DataFrame with the additional columns.
    """
    log = Logger('apply_all_bins')
    meta = RSFieldMeta()
    if column_to_bin_key is None:
        column_to_bin_key = COLUMN_TO_BIN_KEY

    for col, bin_key in column_to_bin_key.items():
        if col not in df.columns:
            log.debug(f"Column '{col}' not in DataFrame – skipping")
            continue
        edges, labels, colours = get_bins_info(bin_key)
        hex_colours = [color_to_hex(c) for c in colours]

        bin_col = f'{col}_bin'
        color_col = f'{col}_color'
        sort_col = f'{col}_bin_sort'

        df[bin_col] = pd.cut(df[col], bins=edges, labels=labels, include_lowest=True)
        # Map each bin label to its hex colour and sort order
        label_to_hex = dict(zip(labels, hex_colours))
        label_to_sort = {label: idx for idx, label in enumerate(labels)}
        df[color_col] = df[bin_col].map(label_to_hex)
        df[sort_col] = df[bin_col].map(label_to_sort)

        # Register metadata so the data dictionary picks up these columns
        source_friendly = meta.get_friendly_name(col)
        source_fm = meta.get_field_meta(col)
        source_theme = source_fm.theme if source_fm else ""
        meta.add_field_meta(
            name=bin_col,
            friendly_name=f'{source_friendly} (bin)',
            description=f'Categorical bin label for {source_friendly}, derived from bins.json',
            dtype='TEXT',
            theme=source_theme,
        )
        meta.add_field_meta(
            name=color_col,
            friendly_name=f'{source_friendly} (color)',
            description=f'Hex colour for the bin, use with Power BI conditional formatting "Format by field value"',
            dtype='TEXT',
            theme=source_theme,
        )
        meta.add_field_meta(
            name=sort_col,
            friendly_name=f'{source_friendly} (sort)',
            description=f'Sort order integer for {bin_col}; use Power BI "Sort by Column" on {bin_col}',
            dtype='INTEGER',
            theme=source_theme,
        )

        log.debug(f"Added {bin_col}, {color_col}, {sort_col} ({len(labels)} bins)")

    return df


# dictionary of functions that will add the named column to df add metadata
CALCULATED_COLS = {
    'channel_length_ex': channel_length_example,
    'low_lying_ratio_bins': lambda df: bin_continuous_column(df, 'low_lying_ratio'),
    'riparian_veg_departure_as_departure': riparian_veg_departure_as_departure,
    'riparian_veg_departure_bins': lambda df: bin_continuous_column(df, 'riparian_veg_departure_as_departure', bin_lookup_nm='riparian_veg_departure', binned_field_friendly='Riparian Vegetation Departure'),
    'land_use_intens_bins': lambda df: bin_continuous_column(df, 'land_use_intens'),
}
