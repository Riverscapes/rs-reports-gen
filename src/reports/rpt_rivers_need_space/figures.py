import pint
import pint_pandas # this is needed !?
from rsxml import Logger

# assume pint registry has been set up already

# Custom DataFrame accessor for metadata - to be moved to util
log = Logger('MetaAccessor')
import pandas as pd
@pd.api.extensions.register_dataframe_accessor("meta")
class MetaAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self._meta = None

    def attach_metadata(self, meta_df, apply_units=True):
        """Attach a metadata DataFrame (with 'name', 'unit', 'friendly_name').
        If apply_units is True, also apply Pint units to columns.
        Usually call apply_units after this, unless you want to keep columns as plain floats/ints"""
        df_columns = self._obj.columns
        if 'name' in meta_df.columns:
            filtered_meta = meta_df[meta_df['name'].isin(df_columns)]
            self._meta = filtered_meta.set_index("name")
        else:
            filtered_meta = meta_df.loc[meta_df.index.intersection(df_columns)]
            self._meta = filtered_meta
        if apply_units:
            self.apply_units()

    def apply_units(self):
        """Apply Pint units to columns based on metadata. Need to set_metadata first"""
        if self._meta is None:
            raise RuntimeError("No metadata set. Use .meta.attach_metadata(meta_df)")
        for col, row in self._meta.iterrows():
            unit = row["unit"]
            if col in self._obj.columns and pd.notnull(unit) and str(unit).strip() != "":
                self._obj[col] = self._obj[col].astype(f"pint[{unit}]")
                log.debug(f'Applied {unit} to {col}')
        log.debug('Applied units to dataframe using meta info')

    def friendly(self, col):
        """Get the friendly name for a column."""
        if self._meta is None:
            return col
        return self._meta.loc[col, "friendly_name"] if col in self._meta.index else col

    def unit(self, col):
        """Get the unit for a column."""
        if self._meta is None:
            return ""
        return self._meta.loc[col, "unit"] if col in self._meta.index else ""

    def set_friendly(self, col, friendly_name):
        """Set the friendly name for a column in the metadata."""
        if self._meta is None:
            # Initialize with just this column if needed
            self._meta = pd.DataFrame(index=[col], columns=["friendly_name", "unit"])
        if col not in self._meta.index:
            self._meta.loc[col, "unit"] = ""
        self._meta.loc[col, "friendly_name"] = friendly_name

    def set_unit(self, col, unit):
        """Set the unit for a column in the metadata."""
        if self._meta is None:
            self._meta = pd.DataFrame(index=[col], columns=["friendly_name", "unit"])
        if col not in self._meta.index:
            self._meta.loc[col, "friendly_name"] = col
        self._meta.loc[col, "unit"] = unit

"""generate specific figures
many of these take a geodataframe and return a plotly graph object
some an html table as string 
"""
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def _floatformat2(inval:float)->str:
    """2 decimals, commas for thousands, no units"""
    return f"{inval:,.2f}"

def subset_with_meta(idf: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Return a DataFrame with only the specified columns and the corresponding subset of metadata.
    """
    # Create the new DataFrame with a copy of the specified columns
    df = idf[columns].copy()

    # Attach only relevant metadata for the selected columns
    if hasattr(idf, 'meta') and hasattr(idf.meta, '_meta') and idf.meta._meta is not None:
        # Use attach_metadata to filter metadata to only columns present in df
        df.meta.attach_metadata(idf.meta._meta, apply_units=False)

    return df

def make_map_with_aoi(gdf, aoi_gdf):
    # Create the base map
    base_map = make_map(gdf)

    # Add AOI polygons as an outline (no fill)
    for _, row in aoi_gdf.iterrows():
        x, y = row['geometry'].exterior.xy
        lon = list(x)
        lat = list(y)
        base_map.add_trace(go.Scattermapbox(
            lon=lon,
            lat=lat,
            mode='lines',
            line=dict(color='red', width=3),
            name='AOI'
        ))
    base_map.update_layout(mapbox_style="open-street-map")
    return base_map

def make_map(gdf: gpd.GeoDataFrame) -> go.Figure:
    """Create Plotly map (GeoJSON polygons)"""

    # Create a plotting-safe DataFrame with only needed columns, convert PintArray to float
    plot_cols = ["dgo_polygon_geom", "fcode_desc", "ownership_desc", "segment_area"]
    plot_gdf = gdf.reset_index(drop=True).copy()
    plot_gdf["id"] = plot_gdf.index  # unique id for each row
    # Only keep necessary columns for plotting
    plot_gdf = plot_gdf[["id"] + plot_cols]
    # Convert PintArray columns to float (magnitude)
    for col in plot_gdf.columns:
        if hasattr(plot_gdf[col], "pint"):
            plot_gdf[col] = plot_gdf[col].pint.magnitude

    geojson = plot_gdf.set_geometry("dgo_polygon_geom").__geo_interface__
    # Compute extent and center
    if plot_gdf.empty:
        center = {"lat": 0, "lon": 0}
        zoom = 2
    else:
        bounds = plot_gdf["dgo_polygon_geom"].total_bounds  # [minx, miny, maxx, maxy]
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

    map_fig = px.choropleth_map(
        plot_gdf,
        geojson=geojson,
        locations="id",
        color="fcode_desc",
        featureidkey="properties.id",
        center=center,
        zoom=zoom,
        opacity=0.5,
        hover_name="fcode_desc",
        hover_data={"segment_area": True, "ownership_desc": True}
    )
    map_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)
    return map_fig

def statistics(gdf) -> dict:
    return {"total_riverscapes_area":gdf["segment_area"].sum(),
            "total_centerline":gdf["centerline_length"].sum(),
            }

def table_of_river_names(gdf) -> str:
    # Pint-enabled DataFrame for calculation
    df = gdf[["stream_name", "stream_length"]].copy()
    # Copy metadata so .meta.friendly/unit still works
    df.meta._meta = gdf.meta._meta.copy() if gdf.meta._meta is not None else None # is this copying the entire _meta dataframe? we only need the part that goes with the fields we have -- make a 'sub-set' function
    df['stream_name'] = df['stream_name'].fillna("unnamed") # this should be done in the upstream view so that it is always the same for anything that uses stream_name
    df = df.groupby('stream_name', as_index=False)['stream_length'].sum()
    if df.empty:
        df.loc[0] = ["no named streams", 0.0]
    total = df['stream_length'].sum() # this is a Quantity 
    percent = (df['stream_length'] / total * 100) # this is a Series. Values are a PintArray 
    df['Percent of Total'] = percent
    # Add friendly name for Percent of Total to metadata if not present
    df.meta.set_friendly('Percent of Total', 'Percent of Total')
    df.meta.set_unit('Percent of Total', '%')
    
    # Prepare display DataFrame (df_t) with formatted strings
    df_t = df.copy()
    # Convert PintArray to float for display # move this to a function that will do it for *all* columns
    # try the dequantify() function -- it is purpose built to retrieve the units information as a header row 
    if hasattr(df_t['stream_length'], 'pint'):
        df_t['stream_length'] = df_t['stream_length'].pint.magnitude
    # Format all values as strings for display
    df_t['stream_length'] = df_t['stream_length'].map(_floatformat2)
    df_t['Percent of Total'] = df_t['Percent of Total'].map(lambda x: f"{x:.1f}%")
    # Add a total row (as formatted strings)
    total_row = pd.DataFrame({
        'stream_name': ['Total'],
        'stream_length': [f"{_floatformat2(total.magnitude)}"],
        'Percent of Total': ["100.0%"]
    })
    df_t = pd.concat([df_t, total_row], ignore_index=True)
    # Use metadata for friendly names and units in headings # change this to a function that does it for all columns - no need to name each variable
    friendly_name = df.meta.friendly('stream_name')
    friendly_length = df.meta.friendly('stream_length')
    unit_length = df.meta.unit('stream_length')
    percent_friendly = df.meta.friendly('Percent of Total')
    percent_unit = df.meta.unit('Percent of Total')
    df_t.columns = [
        friendly_name,
        f"{friendly_length} ({unit_length})" if unit_length else friendly_length,
        f"{percent_friendly} ({percent_unit})" if percent_unit else percent_friendly
    ]
    return df_t.to_html(index=False, escape=False)

def table_of_ownership(gdf) -> str:
    # common elements with table_of_river_names to be separated out 
    # Pint-enabled DataFrame for calculation
    # Start from a copy with metadata. this isn't working df.meta.friendly <> gdf.meta.friendly
    df = subset_with_meta(gdf, ["ownership", "ownership_desc", "stream_length"])
    df = df.groupby(['ownership','ownership_desc'], as_index=False)['stream_length'].sum()
    total = df['stream_length'].sum() # Quantity
    percent = (df['stream_length'] / total * 100)
    df['Percent of Total'] = percent
    # Add friendly name for Percent of Total to metadata if not present - this is another way to do it
    df.meta.set_friendly('Percent of Total', 'Percent of Total')
    df.meta.set_unit('Percent of Total', '%')

    # Prepare display DataFrame (df_t) with formatted strings
    df_t = df.copy()
    # Convert PintArray to float for display
    if hasattr(df_t['stream_length'], 'pint'):
        df_t['stream_length'] = df_t['stream_length'].pint.magnitude
    # Format all values as strings for display
    df_t['stream_length'] = df_t['stream_length'].map(_floatformat2)
    df_t['Percent of Total'] = df_t['Percent of Total'].map(lambda x: f"{x:.1f}%")
    # Add a total row (as formatted strings)
    total_row = pd.DataFrame({
        'ownership': ['Total'],
        'ownership_desc' : [''],
        'stream_length': [f"{_floatformat2(total.magnitude)}"],
        'Percent of Total': ["100.0%"]
    })
    df_t = pd.concat([df_t, total_row], ignore_index=True)
    # Use metadata for friendly names and units in headings
    friendly_area = df.meta.friendly('stream_length')
    unit_area = df.meta.unit('stream_length')
    friendly_owner = df.meta.friendly('ownership')
    friendly_owner_desc = df.meta.friendly('ownership_desc')
    percent_friendly = df.meta.friendly('Percent of Total')
    percent_unit = df.meta.unit('Percent of Total')
    df_t.columns = [
        friendly_owner,
        friendly_owner_desc,
        f"{friendly_area} ({unit_area})" if unit_area else friendly_area,
        f"{percent_friendly} ({percent_unit})" if percent_unit else percent_friendly
    ]
    return df_t.to_html(index=False, escape=False)

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
