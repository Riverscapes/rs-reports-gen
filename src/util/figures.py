"""Generic Functions to generate figures for reports
these should work with any report, any data frame input
(may create new hard-coded fields such as Total or Percent of Total)
"""

# assume pint registry has been set up already

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from util.pandas import RSFieldMeta, RSGeoDataFrame  # Custom DataFrame accessor for metadata


def format_value(column_name, value, decimals: int) -> str:
    """return value formatted with units

    Args:
        value (_type_): Quantity or any
        decimals (int): how many decimals to render

    Returns:
        string: formatted value ready to render

    insipired by get_headers and bake
    """
    meta = RSFieldMeta()
    # unit_fmt = " {unit}"  # just the plain unit, no brackets
    if hasattr(value, "magnitude"):
        preferred_unit = meta.get_field_unit(column_name)
        unit_text = ""
        if preferred_unit:
            value = value.to(preferred_unit)
            # unit_text = unit_fmt.format(unit=f"{preferred_unit:~P}")
        # let Pint handle the unit formatting, so no need to append unit_text
        formatted_val = f"{value:~P,.{decimals}f}{unit_text}"
    elif isinstance(value, (int, float)):
        formatted_val = f"{value:,.{decimals}f}"
    else:
        formatted_val = str(value)
    return formatted_val


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


def bar_group_x_by_y(df: pd.DataFrame, total_col: str, group_by_cols: list[str],
                     fig_params=None) -> go.Figure:
    """create bar chart of total x by y

    Args:
        df (pd.DataFrame): input dataframe
        total_col (str): column to sum
        group_by_cols (list[str]): list of fields to group by - only the first is used!
        TODO: can we do multiples??
        fig_params (dict, optional): override things like title, orientation.

    Returns:
        go.Figure: a plotly figure object
    """
    chart_data = total_x_by_y(df, total_col, group_by_cols, False)

    meta = RSFieldMeta()
    baked_header_lookup = meta.get_headers_dict(chart_data)
    baked_chart_data, baked_headers = RSFieldMeta().bake_units(chart_data)

    if fig_params is None:
        fig_params = {}
    if "orientation" not in fig_params:
        fig_params["orientation"] = "h"
    if "title" in fig_params:
        title = fig_params["title"]
    else:
        title = f"Total {meta.get_friendly_name(total_col)} by {meta.get_friendly_name(group_by_cols[0])}"

    bar_fig = px.bar(
        baked_chart_data,
        y=group_by_cols[0],
        x=total_col,
        title=title,
        labels=baked_header_lookup,
        **fig_params
    )
    bar_fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    bar_fig.update_xaxes(tickformat=",")
    return bar_fig
