"""data preparation for Rivers Need Space"""

import pandas as pd
from util.pandas import RSFieldMeta
from util.figures import get_bins_info  # future enhancement: move the fn since it is a data function not a figure function


def add_calculated_cols(df: pd.DataFrame) -> pd.DataFrame:
    """ Add any calculated columns to the dataframe
    These could be bins. 
    When adding columns to this function, add metadata at the same time
    Args:
        df (pd.DataFrame): Input dataframe

    Returns:
        pd.DataFrame: DataFrame with calculated columns added
    """
    # Example:
    # df['channel_length'] = df['rel_flow_length']*df['centerline_length']
    # RSFieldMeta().add_field_meta(name='channel_length',
    #                              friendly_name='Channel Length',
    #                              data_unit='m',
    #                              dtype='REAL'
    #                              )
    meta = RSFieldMeta()

    # bin

    unbinnedfldnm = 'riparian_veg_departure'
    binnedflnm = unbinnedfldnm+'_bins'  # default
    binned_friendly_nm = 'Riparian Vegetation Departure'  # non-default
    binlookupnm = unbinnedfldnm  # default
    edges, labels, colours = get_bins_info(binlookupnm)

    df[binnedflnm] = pd.cut(df[unbinnedfldnm], bins=edges, labels=labels, include_lowest=True)
    meta.add_field_meta(name=binnedflnm, friendly_name=binned_friendly_nm)  # the type usually be categorical text

    return df
