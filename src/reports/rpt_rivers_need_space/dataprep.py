"""data preparation for Rivers Need Space"""

import pandas as pd
from util.pandas import RSFieldMeta


def add_calculated_cols(df: pd.DataFrame) -> pd.DataFrame:
    """ Add any calculated columns to the dataframe

    Args:
        df (pd.DataFrame): Input dataframe

    Returns:
        pd.DataFrame: DataFrame with calculated columns added
    """
    # TODO: add metadata for any added columns
    # TODO: add units
    df['channel_length'] = df['rel_flow_length']*df['centerline_length']
    RSFieldMeta().add_field_meta(name='channel_length',
                                 friendly_name='Channel Length',
                                 data_unit='m',
                                 dtype='REAL'
                                 )
    return df
