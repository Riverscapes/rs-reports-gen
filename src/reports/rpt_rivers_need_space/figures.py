"""generate specific figures
all of these take a geodataframe and return a plotly graph object
"""
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go


def make_map(gdf: gpd.GeoDataFrame) -> go.Figure:
    """Create Plotly map (GeoJSON polygons)"""

    # Ensure geometry column is correct and reset index for mapping
    gdf = gdf.copy()
    gdf = gdf.reset_index(drop=True)
    gdf["id"] = gdf.index  # unique id for each row

    geojson = gdf.set_geometry("dgo_polygon_geom").__geo_interface__
    # Compute extent and center
    if gdf.empty:
        center = {"lat": 0, "lon": 0}
        zoom = 2
    else:
        bounds = gdf["dgo_polygon_geom"].total_bounds  # [minx, miny, maxx, maxy]
        center = {"lat": (bounds[1] + bounds[3]) / 2, "lon": (bounds[0] + bounds[2]) / 2}
        # Estimate zoom: smaller area = higher zoom
        # This is a rough heuristic for zoom based on longitude span
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

    map_fig = px.choropleth_map(
        gdf,
        geojson=geojson,
        locations="id",
        color="fcode",
        featureidkey="properties.id",
        center=center,
        zoom=zoom,
        opacity=0.5,
        hover_name="fcode",
        hover_data={"segment_area": True, "ownership": True}
    )
    map_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)
    return map_fig


def make_rs_area_by_owner(gdf: gpd.GeoDataFrame) -> go.Figure:
    """ Create bar chart of total segment area by ownership

    Args:
        gdf (GeoDataFrame): _geodataframe with 'ownership' and 'segment_area' columns_

    Returns:
        _type_: _plotly figure object_
    """
    # Create horizontal bar chart (sum of segment_area by ownership)
    chart_data = gdf.groupby('ownership', as_index=False)['segment_area'].sum()
    bar_fig = px.bar(
        chart_data,
        y="ownership",
        x="segment_area",
        orientation="h",
        title="Total Riverscape Area (units) by Ownership",
        labels={"segment_area": "Total Segment Area", "ownership": "Ownership"},
        height=400
    )
    bar_fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return bar_fig

def make_rs_area_by_featcode(gdf) -> go.Figure:
    """Create pie chart of total segment area by NHD feature code type
    Args: 
        gdf with fcode_desc and segment_area
    """
    chart_data = gdf.groupby('fcode_desc', as_index=False)['segment_area'].sum()
    print(chart_data)
    fig = px.pie(
        chart_data,
        names = 'fcode_desc',
        values='segment_area',
        title='Total Riverscape Area (units) by Feature Code'
    )
    return fig
