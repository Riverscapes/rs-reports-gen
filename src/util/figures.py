"""Generic Functions to generate figures for reports
these should work with any report, any data frame input
(may create new hard-coded fields such as Total or Percent of Total)

FUTURE ENHANCEMENTs:
* more abstraction: sum is not the only aggregation we want, so instead of total_x_by_y(...) can be replaced by agg_x_by_y(['sum'],...)
* tables can have multiple so that would be an array
"""

# assume pint registry has been set up already

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import os
import json
from util.pandas import RSFieldMeta, RSGeoDataFrame  # Custom DataFrame accessor for metadata


def get_bins_info(key: str):
    """extract data from bins.json"""
    bins_path = os.path.join(os.path.dirname(__file__), "bins.json")
    with open(bins_path, "r") as f:
        bins_dict = json.load(f)
    info = bins_dict[key]
    edges = info["edges"]
    legend = info["legend"]
    labels = [item[1] for item in legend]
    colours = [item[0] for item in legend]
    return edges, labels, colours


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


def bar_total_x_by_ybins(df: pd.DataFrame, total_col: str, group_by_cols: list[str], fig_params: dict | None = None) -> go.Figure:
    """
    Uses bins.json to lookup the bins
    If more than one x_col provided, will use the binning identified for the first

    Args:
        df (pdf.DataFrame): dataframe containing all the data needed
        x_col (_type_): _description_
        y_col (_type_): _description_

    Returns:
        go.Figure: 

    Usage: 
        bar_total_x_by_ybins(data_gdf, 'segment_area', ['low_lying_ratio']) 
        produces same thing as low_lying_ratio_bins(data_gdf)

    future Enhancement: 
    * separate the dataframe generation from plotting - we might want a table as well
    * there's also lots of repetition with regular bar chart - mainly the colors are diff
    """
    fields: [] = group_by_cols + [total_col]
    chart_subset_df = df[fields].copy()
    edges, labels, colours = get_bins_info(group_by_cols[0])
    chart_subset_df['bin'] = pd.cut(chart_subset_df[group_by_cols[0]], bins=edges, labels=labels, include_lowest=True)
    # Aggregate total_col by bin - NB cut creates a Categorical dtype
    agg_data = chart_subset_df.groupby('bin', as_index=False, observed=False)[total_col].sum()

    # THIS IS WHERE WE COULD REGURN agg_data TO BE USED BY OTHER FUNCTIONS
    # however, colurs are not part of the agg_data and are needed - so we'd need to call the get_bins_info again
    # prepare the data
    meta = RSFieldMeta()
    baked_header_lookup = meta.get_headers_dict(agg_data)
    baked_agg_data, _baked_headers = RSFieldMeta().bake_units(agg_data)

    # give the axis a friendly name
    if len(group_by_cols) == 1:
        baked_header_lookup['bin'] = meta.get_friendly_name(group_by_cols[0])
    else:
        baked_header_lookup['bin'] = 'Bin'
    # do we want to change the header for the total to "Total xxx" ? if so this would be one way
    # baked_header_lookup[total_col] = f'Total {meta.get_friendly_name(total_col)}'
    # build the figure
    # set parameters
    if fig_params is None:
        fig_params = {}

    if "title" not in fig_params:
        group_names = [meta.get_friendly_name(col) for col in group_by_cols]
        fig_params["title"] = f"Total {meta.get_friendly_name(total_col)} by {', '.join(group_names)} Bins"

    fig = px.bar(
        baked_agg_data,
        x='bin',
        y=total_col,
        color='bin',
        color_discrete_sequence=colours,
        labels=baked_header_lookup,
        height=400,
        **fig_params
    )

    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return fig


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
    chart_subset_df = df[fields].copy()
    # Pint-enabled DataFrame for calculation
    df = RSGeoDataFrame(chart_subset_df.groupby(group_by_cols, as_index=False)[total_col].sum())
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
        group_by_cols (list[str]): fields to group by. First field will be the y axis, second field the color
        fig_params (dict, optional): override things like title, orientation.

    Returns:
        go.Figure: a plotly figure object
    """
    if len(group_by_cols) > 2:
        raise NotImplementedError("We don't make bar charts with more than 2 group bys")
    chart_data = total_x_by_y(df, total_col, group_by_cols, False)

    meta = RSFieldMeta()
    baked_header_lookup = meta.get_headers_dict(chart_data)
    baked_chart_data, _baked_headers = RSFieldMeta().bake_units(chart_data)

    # set parameters
    if fig_params is None:
        fig_params = {}
    if "orientation" not in fig_params:
        fig_params["orientation"] = "h"
    if "title" not in fig_params:
        group_names = [meta.get_friendly_name(col) for col in group_by_cols]
        fig_params["title"] = f"Total {meta.get_friendly_name(total_col)} by {', '.join(group_names)}"
    if len(group_by_cols) == 2 and "color" not in fig_params:
        fig_params["color"] = group_by_cols[1]

    bar_fig = px.bar(
        baked_chart_data,
        y=group_by_cols[0],
        x=total_col,
        labels=baked_header_lookup,
        **fig_params
    )
    bar_fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    bar_fig.update_xaxes(tickformat=",")
    return bar_fig
