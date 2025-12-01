"""
Field metadata utility for Athena.
"""
import pandas as pd
from rsxml import Logger
from util.athena.athena import athena_select_to_dataframe


def get_field_metadata(where_clause: str = "") -> pd.DataFrame:
    """
    Query Athena for column metadata from rme_table_column_defs and return as a DataFrame.

    Returns:
        pd.DataFrame - DataFrame of metadata

    Example:
        metadata_df = get_field_metadata()
    TODO: change to query from `layer_definitions_latest` for the columns we need for the query
    """
    log = Logger('Get field metadata')
    log.info("Getting field metadata from athena")

    query = f"""
        SELECT table_name, name, theme_name, friendly_name, dtype, data_unit, description
        FROM table_column_defs {where_clause}
    """
    df = athena_select_to_dataframe(query)
    if df.empty:
        raise RuntimeError("Failed to retrieve metadata from Athena.")
    return df
