"""library of common data preparation options for Riverscape Metric Engine data"""

import pandas as pd
from util.pandas import RSFieldMeta
from util.figures import get_bins_info  # future enhancement: move the fn since it is a data function not a figure function

# dictionary of functions is at the bottom


def add_common_rme_cols(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """ Add any calculated columns to the dataframe
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


def bin_continuous_column(df: pd.DataFrame, field_nm: str,
                          bin_lookup_nm: str | None = None,
                          binned_field_nm: str | None = None,
                          binned_field_friendly: str | None = None) -> pd.DataFrame:
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
    if not bin_lookup_nm:
        bin_lookup_nm = field_nm
    if not binned_field_nm:
        binned_field_nm = bin_lookup_nm + "_bin"
    if not binned_field_friendly:
        binned_field_friendly = field_nm + ' (binned)'
    edges, labels, _colours = get_bins_info(bin_lookup_nm)
    df[binned_field_nm] = pd.cut(df[field_nm], bins=edges, labels=labels, include_lowest=True)

    RSFieldMeta().add_field_meta(name=binned_field_nm,
                                 friendly_name=binned_field_friendly)  # the type is categorical, should we add that?
    return df


def channel_length_example(df: pd.DataFrame) -> pd.DataFrame:
    """Example bespoke function 
    Args: 
        dataframe - must contain columns 'rel_flow_length' & 'centerline_length'
    """
    fld_nm = 'channel_length_ex'
    df[fld_nm] = df['rel_flow_length'] * df['centerline_length']

    RSFieldMeta().add_field_meta(name=fld_nm,
                                 friendly_name='Channel Length EXAMPLE',
                                 description='Channel Length calculated from centerline length and relative length. Should match stream_length.',
                                 data_unit='m',
                                 dtype='REAL'
                                 )

    return df


# dictionary of functions that will generate the named column and add metadata
CALCULATED_COLS = {
    'channel_length_ex': channel_length_example,
    'low_lying_ratio_bins': lambda df: bin_continuous_column(df, 'low_lying_ratio'),
    'riparian_veg_departure_bins': lambda df: bin_continuous_column(df, 'riparian_veg_departure',
                                                                    binned_field_friendly='Riparian Vegetation Departure'),
}
