"""Generic Functions to generate figures for reports
these should work with any report, any data frame input
(may create new hard-coded fields such as Total or Percent of Total)

FUTURE ENHANCEMENTs:
* more abstraction: sum is not the only aggregation we want, so instead of total_x_by_y(...) can be replaced by agg_x_by_y(['sum'],...)
* tables can have multiple so that would be an array
"""

# assume pint registry has been set up already

import os
import json
from typing import List, Tuple

import numpy as np
import pandas as pd
import pint
import geopandas as gpd
import plotly.graph_objects as go
import plotly.express as px

from rsxml import Logger
from util.pandas import RSFieldMeta, RSGeoDataFrame  # Custom DataFrame accessor for metadata


def get_bins_info(key: str):
    """extract data from bins.json"""
    bins_path = os.path.join(os.path.dirname(__file__), "bins.json")
    with open(bins_path, "r", encoding="utf-8") as f:
        bins_dict = json.load(f)
    info = bins_dict[key]
    edges = info["edges"]
    legend = info["legend"]
    labels = [item[1] for item in legend]
    colours = [item[0] for item in legend]
    return edges, labels, colours


def format_value(value, decimals: int) -> str:
    """return value formatted with units

    Args:
        value (_type_): Quantity or any
        decimals (int): how many decimals to render

    Returns:
        string: formatted value ready to render

    insipired by get_headers and bake
    """
    # unit_fmt = " {unit}"  # just the plain unit, no brackets
    if hasattr(value, "magnitude"):
        unit_text = ""
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
    fields: list = group_by_cols + [total_col]
    chart_subset_df = df[fields].copy()
    edges, labels, colours = get_bins_info(group_by_cols[0])
    # TODO: iterate through the group_by_cols, name each bein col_bin AND ensure it has metadata - units, description etc.
    chart_subset_df['bin'] = pd.cut(chart_subset_df[group_by_cols[0]], bins=edges, labels=labels, include_lowest=True)
    # Aggregate total_col by bin - NB cut creates a Categorical dtype
    # TO DO: aggregate by each bin
    agg_data = chart_subset_df.groupby('bin', as_index=False, observed=False)[total_col].sum()

    # THIS IS WHERE WE COULD REGURN agg_data TO BE USED BY OTHER FUNCTIONS
    # however, colurs are not part of the agg_data and are needed - so we'd need to call the get_bins_info again
    # prepare the data
    # TODO: for grouped bar chart we should merge the bins and do something different with the colors
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
    # TODO if we are doing multiples then it is a grouped and we add multiple traces and barmode='group'
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


def check_for_pdna(df, label):
    if df.isna().any().any():
        Logger('NATypeCheck').error(f"pd.NA found in DataFrame before plotting: {label}")
        # Optionally, print which columns/rows
        for col in df.columns:
            if df[col].isna().any():
                Logger('NATypeCheck').error(f"  Column '{col}' has NA at rows: {df[df[col].isna()].index.tolist()}")

# =========================
# Maps
# =========================


def make_map_with_aoi(gdf, aoi_gdf):
    """ make a map with the data and the AOI outlined

    Args:
        gdf (_type_): _description_
        aoi_gdf (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Prepare plotting DataFrame and geojson
    log = Logger("Make map with AOI")
    plot_cols = ["dgo_polygon_geom", "fcode_desc", "ownership_desc", "segment_area"]
    plot_gdf = gdf.reset_index(drop=True).copy()
    plot_gdf["id"] = plot_gdf.index
    # Keep only the columns needed for plotting and geojson
    plot_gdf = plot_gdf[["id", "dgo_polygon_geom", "fcode_desc", "ownership_desc", "segment_area"]]
    # Fill NAs for string/object columns with "-", for numeric columns with np.nan
    for col in plot_gdf.columns:
        if pd.api.types.is_string_dtype(plot_gdf[col]) or plot_gdf[col].dtype == "object":
            plot_gdf[col] = plot_gdf[col].fillna("-")
        elif pd.api.types.is_numeric_dtype(plot_gdf[col]):
            plot_gdf[col] = plot_gdf[col].fillna(np.nan)
        # Optionally handle other dtypes (categorical, boolean) as needed
    check_for_pdna(plot_gdf, log.method)
    plot_gdf = plot_gdf[["id"] + plot_cols]
    for col in plot_gdf.columns:
        if hasattr(plot_gdf[col], "pint"):
            plot_gdf[col] = plot_gdf[col].pint.magnitude
    geojson = plot_gdf.set_geometry("dgo_polygon_geom").__geo_interface__

    # Calculate zoom and center
    zoom, center = get_zoom_and_center(plot_gdf, "dgo_polygon_geom")

    # Bake in the units and names
    baked_header_lookup = RSFieldMeta().get_headers_dict(plot_gdf)
    baked_df, baked_headers = RSFieldMeta().bake_units(plot_gdf)
    # baked.columns = baked_headers

    # Create choropleth map with Plotly Express
    fig = px.choropleth_map(
        baked_df,
        geojson=geojson,
        locations="id",
        color="fcode_desc",
        featureidkey="properties.id",
        opacity=0.5,
        labels=baked_header_lookup,
        hover_name="fcode_desc",
        hover_data={"segment_area": True, "ownership_desc": True},
        center=center,
        zoom=zoom
    )

    # Add AOI outlines using go.Scattermap
    for _, row in aoi_gdf.iterrows():
        x, y = row['geometry'].exterior.xy
        fig.add_trace(go.Scattermap(
            lon=list(x),
            lat=list(y),
            mode='lines',
            line=dict(color='red', width=3),
            name='AOI',
            showlegend=True
        ))

    fig.update_maps(
        style="open-street-map"
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)

    fig.update_layout(
        legend=dict(
            y=0.95,
        )
    )
    return fig


def get_zoom_and_center(gdf: gpd.GeoDataFrame, geom_field_nm: str) -> tuple[int, dict[str, float]]:
    """return the zoom level and lat, lon of the center"""
    # Compute extent and center
    if gdf.empty:
        center = {"lat": 0.0, "lon": 0.0}
        zoom = 2
    else:
        bounds = gdf[geom_field_nm].total_bounds  # [minx, miny, maxx, maxy]
        center = {"lat": (bounds[1] + bounds[3]) / 2, "lon": (bounds[0] + bounds[2]) / 2}
        # Estimate zoom: smaller area = higher zoom
        lon_span = bounds[2] - bounds[0]
        if lon_span < 0.01:
            zoom = 14
        elif lon_span < 0.1:
            zoom = 12
        elif lon_span < 1:
            zoom = 10
        elif lon_span < 5:
            zoom = 8
        elif lon_span < 20:
            zoom = 6
        else:
            zoom = 4
    return (zoom, center)


def make_rs_area_by_featcode(gdf) -> go.Figure:
    """Create pie chart of total segment area by NHD feature code type"""
    chart_data = gdf.groupby('fcode_desc', as_index=False)['segment_area'].sum()

    meta = RSFieldMeta()
    baked_header_lookup = meta.get_headers_dict(chart_data)
    baked_chart_data, baked_headers = meta.bake_units(chart_data)

    total_name = baked_header_lookup.get('segment_area', meta.get_friendly_name('segment_area'))
    group_name = baked_header_lookup.get('fcode_desc', meta.get_friendly_name('fcode_desc'))
    title = f"Total {total_name} by {group_name}"

    fig = px.pie(
        baked_chart_data,
        names="fcode_desc",
        values="segment_area",
        labels=baked_header_lookup,  # legend/axis labels use your nice names
        title=title,
    )

    # Keep percent on slices; tooltip shows ONLY absolute with thousands commas
    fig.update_traces(
        textinfo="percent",
        hovertemplate=f"<b>{baked_header_lookup.get('segment_area', 'segment_area')} for {baked_header_lookup.get('fcode_desc', 'fcode_desc Code')} = %{{label}}</b>:<br>%{{value:,.0f}}<extra></extra>"
        # Use :,.1f or :,.2f if you want decimals.
    )

    # Prevent legend/hover name truncation
    fig.update_layout(hoverlabel=dict(namelength=-1))

    return fig


def common_statistics(gdf: gpd.GeoDataFrame) -> dict[str, pint.Quantity]:
    """ Calculate and return key statistics for riverscapes RME as a dictionary
    Args:
        gdf (GeoDataFrame): data_gdf input WITH UNITS APPLIED

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    Future Enhancement: move this out of figures, it is a data prep fn
    """
    # make a copy of dataframe (subset) and work with that so don't accidentally change incoming
    subset_df = RSGeoDataFrame(gdf[["segment_area", "centerline_length", "stream_length"]].copy())
    # Calculate totals
    total_segment_area = subset_df["segment_area"].sum()
    total_centerline_length = subset_df["centerline_length"].sum()
    total_stream_length = subset_df["stream_length"].sum()

    # Calculate integrated valley bottom width as ratio of totals
    integrated_valley_bottom_width = total_segment_area / total_centerline_length if total_centerline_length != 0 else float('nan')

    # if you want different units or descriptions then give them different names and add rsfieldmeta
    # Add field meta if not already present
    RSFieldMeta().add_field_meta(
        name='total_segment_area',
        friendly_name='Total Riverscape Area',
        data_unit='kilometer ** 2',
        dtype='REAL',
        description='Sum of the riverscape area for all DGOs captured in the report.'
    )
    RSFieldMeta().add_field_meta(
        name='total_centerline_length',
        friendly_name='Total Riverscape Length',
        data_unit='kilometer',
        dtype='REAL',
        description='Sum of the riverscape centerline lengths for all DGOs captured in the report.'
    )
    RSFieldMeta().add_field_meta(
        name='total_stream_length',
        friendly_name='Total Stream Length',
        data_unit='kilometer',
        dtype='REAL',
        description='Total length of all channel flow lines for all DGOs captured in the report.'
    )
    RSFieldMeta().add_field_meta(
        name='integrated_valley_bottom_width',
        friendly_name='Integrated Valley Bottom Width',
        data_unit='m',
        dtype='REAL',
        description='Total riverscape area divided by total riverscape length.'
    )

    # Compose result dictionary
    stats = {
        'total_segment_area': total_segment_area.to('kilometer ** 2'),  # acres and hectares will be interchangeable based on unit system
        'total_centerline_length': total_centerline_length.to('kilometer'),  # miles and km will be interchangeable based on unit system
        'total_stream_length': total_stream_length.to('kilometer'),  # miles and km will be interchangeable based on unit system
        # Here we specify yards (because yards converts to meters but meters converts to feet and we want yards for the imperial system)
        'integrated_valley_bottom_width': integrated_valley_bottom_width.to('yards'),
    }
    return stats


def extract_labels_from_legend(bins_legend_json: str) -> list[str]:
    x = json.loads(bins_legend_json)
    labels = [item[1] for item in x]
    return labels


def extract_colours_from_legend(bins_legend_json: str) -> list[str]:
    x = json.loads(bins_legend_json)
    colours = [item[0] for item in x]
    return colours


def prop_ag_dev(chart_data: pd.DataFrame) -> go.Figure:
    """example of figure with two measures"""
    # load shared bins
    bins, labels, _colours = get_bins_info("lf_agriculture_prop")

    # make a copy and work with that
    chart_data = chart_data[['lf_agriculture_prop', 'lf_developed_prop', 'segment_area']].copy()
    # Bin each metric separately
    chart_data['ag_bin'] = pd.cut(chart_data['lf_agriculture_prop'], bins=bins, labels=labels, include_lowest=True)
    chart_data['dev_bin'] = pd.cut(chart_data['lf_developed_prop'], bins=bins, labels=labels, include_lowest=True)

    # Aggregate segment_area by each bin
    ag_data = chart_data.groupby('ag_bin', observed=False)['segment_area'].sum().reset_index()
    ag_data = ag_data.rename(columns={'ag_bin': 'bin', 'segment_area': 'ag_segment_area'})

    dev_data = chart_data.groupby('dev_bin', observed=False)['segment_area'].sum().reset_index()
    dev_data = dev_data.rename(columns={'dev_bin': 'bin', 'segment_area': 'dev_segment_area'})

    # Merge for grouped bar chart
    agg_data = pd.merge(ag_data, dev_data, on='bin', how='outer')

    baked_header_lookup = RSFieldMeta().get_headers_dict(agg_data)
    baked_agg_data, baked_headers = RSFieldMeta().bake_units(agg_data)    # Plot bar chart

    baked_header_lookup['bin'] = 'Land Use Intensity'

    fig = go.Figure()
    fig.add_trace(go.Bar(x=baked_agg_data['bin'], y=baked_agg_data['ag_segment_area'], name='Agriculture'))
    fig.add_trace(go.Bar(x=baked_agg_data['bin'], y=baked_agg_data['dev_segment_area'], name='Development'))

    fig.update_layout(
        title='Agriculture and Development Proportion',
        barmode='group',
        margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return fig


def dens_road_rail(df: pd.DataFrame) -> go.Figure:
    """riverscape area by road and rail density bins"""
    bins, labels, _colours = get_bins_info("road_density")
    # Example: horizontal grouped bar chart for road and rail density
    chart_data = df[['road_dens', 'rail_dens', 'segment_area']].copy()

    # Bin each metric (customize bins as needed)
    chart_data['road_bin'] = pd.cut(chart_data['road_dens'], bins=bins, labels=labels, include_lowest=True)
    chart_data['rail_bin'] = pd.cut(chart_data['rail_dens'], bins=bins, labels=labels, include_lowest=True)

    # Aggregate segment_area by each bin
    road_data = chart_data.groupby('road_bin', observed=False)['segment_area'].sum().reset_index()
    road_data = road_data.rename(columns={'road_bin': 'bin', 'segment_area': 'road_segment_area'})

    rail_data = chart_data.groupby('rail_bin', observed=False)['segment_area'].sum().reset_index()
    rail_data = rail_data.rename(columns={'rail_bin': 'bin', 'segment_area': 'rail_segment_area'})

    # Merge for grouped bar chart
    agg_data = pd.merge(road_data, rail_data, on='bin', how='outer')

    baked_header_lookup = RSFieldMeta().get_headers_dict(agg_data)
    baked_agg_data, baked_headers = RSFieldMeta().bake_units(agg_data)    # Plot bar char

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=baked_agg_data['bin'],
        x=baked_agg_data['road_segment_area'],
        name='Road Density',
        orientation='h'
    ))
    fig.add_trace(go.Bar(
        y=baked_agg_data['bin'],
        x=baked_agg_data['rail_segment_area'],
        name='Rail Density',
        orientation='h'
    ))
    fig.update_layout(
        barmode='group',
        title='Riverscape Area by Road and Rail Density',
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        height=400,
        yaxis_title='Density',
        xaxis_title='Total Riverscape Area'
    )
    fig.update_xaxes(tickformat=",")
    return fig


def project_id_list(df: pd.DataFrame) -> List[Tuple[str, str]]:
    """generate html fragment representing the projects used

    Args:
        df (pd.DataFrame): data_gdf

    Returns:
        str: html fragment to insert in 
    """
    newdf = df[['rme_project_id', 'rme_project_name']].copy()
    newdf = newdf.drop_duplicates()
    # We can nicely hard-code the urls
    newdf["project_url"] = "https://data.riverscapes.net/p/" + newdf['rme_project_id'].astype(str)
    ret_val = list(newdf[['rme_project_name', 'project_url']].itertuples(index=False, name=None))
    return ret_val


# =========================
# Stats
# =========================


def metric_cards(metrics: dict) -> list[tuple[str, str, str]]:
    """transform a statistics dictionary into list of metric elements

    Args: 
        metrics (dict): metric_id, Quantity
        **uses Friendly name and description if they have been added to the RSFieldMeta**

    Returns:
        list of card elements: 
            * friendly metric name (title)
            * formatted metric value, including units
            * additional description (optional)

    Uses the order of the dictionary (guaranteed to be insertion order from Python 3.7 and later)
    FUTURE ENHANCEMENT - Should be modified to handle different number of decimal places depending on the metric
    """
    cards = []
    meta = RSFieldMeta()
    log = Logger('metric_cards')
    for key, value in metrics.items():
        friendly = meta.get_friendly_name(key)
        desc = meta.get_description(key)
        log.info(f"metric: {key}, friendly: {friendly}, desc: {desc}")
        # Make sure the value respects the unit system
        system_value = RSFieldMeta().get_system_unit_value(value)
        formatted = format_value(system_value, 0)
        cards.append((friendly, formatted, desc))
    return cards
