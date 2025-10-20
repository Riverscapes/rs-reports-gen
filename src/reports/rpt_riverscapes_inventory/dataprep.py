import pandas as pd
from util.rme.rme_common_dataprep import add_common_rme_cols


def add_calculated_rme_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Add calculated columns to the RME dataframe
    Returns: 
        dataframe with added columns
    """
    df = add_common_rme_cols(df, ['riparian_veg_departure_as_departure', 'riparian_veg_departure_bins', 'land_use_intens_bins'])
    # add any columns need FOR THIS REPORT ONLY here

    return df
