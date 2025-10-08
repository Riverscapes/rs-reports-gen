"""Generic Functions to generate figures for reports 
these should work with any report, any data frame input 
(may create new hard-coded fields such as Total or Percent of Total)
"""

# assume pint registry has been set up already

import pandas as pd
from util.pandas import RSFieldMeta, RSGeoDataFrame  # Custom DataFrame accessor for metadata


def total_x_by_y(df: pd.DataFrame, total_col: str, group_by_cols: list[str], with_percent: bool = True) -> RSGeoDataFrame:
    """summarize the dataframe with friendly name and units

    Args:
        df (pd.DataFrame): dataframe containing the data
        total_col (str): column to sum
        group_by_cols (list[str]): list of fields to group by
        with_percent (bool): include a Percent of Total column

    Returns:
        pd.DataFrame: result dataframe
    """
    # check there is something to group by
    if not group_by_cols or len(group_by_cols) == 0:
        raise ValueError('Expected list of columns, got empty list')
    # combine fields that we need
    fields = group_by_cols + [total_col]
    subset = df[fields].copy()
    # Pint-enabled DataFrame for calculation
    df = RSGeoDataFrame(subset.groupby(group_by_cols, as_index=False)[total_col].sum())
    if with_percent:

        # add the grand_total metadata
        grand_total = df[total_col].sum()  # Quantity
        df['pct_of_total'] = df[total_col] / grand_total * 100
        # Add friendly name for Percent of Total to metadata if not present - this is another way to do it
        RSFieldMeta().add_field_meta(name='pct_of_total', friendly_name='Percent of Total (%)', dtype='REAL')

        # Add a total row (as formatted strings)
        # first item (if exists) in group-by will be 'Total'
        # any others will be blank
        total_row_cols = {}
        for i, col in enumerate(group_by_cols, start=0):
            if i == 0:
                total_row_cols[col] = ['Total']
            else:
                total_row_cols[col] = ['']

        df.set_footer(pd.DataFrame({
            **total_row_cols,
            total_col: grand_total,
            'pct_of_total': [100]
        }))

    return df


def table_total_x_by_y(df: pd.DataFrame, total_col: str, group_by_cols: list[str], with_percent: bool = True) -> str:
    """return html table fragment for grouped-by with total (and optional percent) table"""
    df = total_x_by_y(df, total_col, group_by_cols, with_percent)
    return df.to_html(index=False, escape=False)
