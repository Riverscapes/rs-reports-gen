import plotly.graph_objects as go
import plotly.express as px
import geopandas as gpd
import pint
import pint_pandas  # this is needed !?
import json
from rsxml import Logger

# assume pint registry has been set up already

# Custom DataFrame accessor for metadata - to be moved to util
import pandas as pd


@pd.api.extensions.register_dataframe_accessor("meta")
class MetaAccessor:
    log = Logger('MetaAccessor')

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
                self.log.debug(f'Applied {unit} to {col}')
        self.log.debug('Applied units to dataframe using meta info')

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


# =========================
# Helpers
# =========================


def _floatformat2(inval: float) -> str:
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


def extract_labels_from_legend(bins_legend_json: str) -> list[str]:
    x = json.loads(bins_legend_json)
    labels = [item[1] for item in x]
    return labels


def extract_colours_from_legend(bins_legend_json: str) -> list[str]:
    x = json.loads(bins_legend_json)
    colours = [item[0] for item in x]
    return colours


# =========================
# Maps
# =========================

def make_map_with_aoi(gdf, aoi_gdf):
    # Prepare plotting DataFrame and geojson
    plot_cols = ["dgo_polygon_geom", "fcode_desc", "ownership_desc", "segment_area"]
    plot_gdf = gdf.reset_index(drop=True).copy()
    plot_gdf["id"] = plot_gdf.index
    plot_gdf = plot_gdf[["id"] + plot_cols]
    for col in plot_gdf.columns:
        if hasattr(plot_gdf[col], "pint"):
            plot_gdf[col] = plot_gdf[col].pint.magnitude
    geojson = plot_gdf.set_geometry("dgo_polygon_geom").__geo_interface__

    # Calculate zoom and center
    zoom, center = get_zoom_and_center(plot_gdf, "dgo_polygon_geom")

    # Create choropleth map with Plotly Express
    import plotly.express as px
    fig = px.choropleth_map(
        plot_gdf,
        geojson=geojson,
        locations="id",
        color="fcode_desc",
        featureidkey="properties.id",
        opacity=0.5,
        hover_name="fcode_desc",
        hover_data={"segment_area": True, "ownership_desc": True},
        center=center,
        zoom=zoom
    )

    # Add AOI outlines using go.Scattermap
    import plotly.graph_objects as go
    for _, row in aoi_gdf.iterrows():
        x, y = row['geometry'].exterior.xy
        fig.add_trace(go.Scattermap(
            lon=list(x),
            lat=list(y),
            mode='lines',
            line=dict(color='red', width=3),
            name='AOI',
            showlegend=False
        ))

    fig.update_maps(
        style="open-street-map"
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)
    return fig


def make_aoi_map(gdf, aoi_gdf: gpd.GeoDataFrame):
    """
    **NOT USED** 
    # Create the base map 
    """

    base_map = go.Figure()

    # Add AOI polygons as an outline (no fill)
    for _, row in aoi_gdf.iterrows():
        x, y = row['geometry'].exterior.xy
        base_map.add_trace(go.Scattermapbox(
            lon=list(x),
            lat=list(y),
            mode='lines',
            line=dict(color='red', width=3),
            name='AOI'
        ))
    base_map.update_maps(
        style="open-street-map"
    )

    return base_map


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


def make_map(gdf: gpd.GeoDataFrame) -> go.Figure:
    """Create Plotly map (GeoJSON polygons). NOT USED"""

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
    zoom, center = get_zoom_and_center(plot_gdf, "dgo_polygon_geom")

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
    # conus bounds
    map_fig.update_layout(map_bounds={"west": -150, "east": -50, "south": 20, "north": 50})
    map_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)
    return map_fig

# =========================
# Tables - take dataframe and return html
# =========================


def project_id_table(df: pd.DataFrame) -> str:
    """generate html fragment representing the projects used

    Args:
        df (pd.DataFrame): data_gdf

    Returns:
        str: html fragment to insert in 
    """
    df = df[['rme_project_id', 'rme_project_name']].copy()
    df = df.drop_duplicates()
    df["project_url"] = "https://data.riverscapes.net/p/" + df['rme_project_id'].astype(str)
    df['link'] = df.apply(lambda row: f'<a href="{row["project_url"]}">{row["rme_project_name"]}</a>', axis=1)
    df = df[['link']].copy()
    html_table = df.to_html(escape=False)
    return html_table


def table_of_river_names(gdf: pd.DataFrame) -> str:
    """generate table summary of stream names and lengths

    Args:
        gdf (pd.DataFrame): data_gdf

    Returns:
        str: html fragment
    """
    # Pint-enabled DataFrame for calculation
    df = gdf[["stream_name", "stream_length"]].copy()
    # Copy metadata so .meta.friendly/unit still works
    df.meta._meta = gdf.meta._meta.copy() if gdf.meta._meta is not None else None  # is this copying the entire _meta dataframe? we only need the part that goes with the fields we have -- make a 'sub-set' function
    df['stream_name'] = df['stream_name'].fillna("unnamed")  # this should be done in the upstream view so that it is always the same for anything that uses stream_name
    df = df.groupby('stream_name', as_index=False)['stream_length'].sum()
    total = df['stream_length'].sum()  # this is a Quantity
    percent = (df['stream_length'] / total * 100)  # this is a Series. Values are a PintArray
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


def table_of_fcodes(gdf) -> str:
    """
    generate table summary of fcode descriptions and lengths
    """

    df = subset_with_meta(gdf, ["fcode_desc", "stream_length"])
    df = df.groupby('fcode_desc', as_index=False)['stream_length'].sum()
    total = df['stream_length'].sum()  # Quantity
    percent = (df['stream_length'] / total * 100)
    df['Percent of Total'] = percent
    # Add friendly name for Percent of Total to metadata if not present - this is another way to do it
    df.meta.set_friendly('Percent of Total', 'Percent of Total')
    df.meta.set_unit('Percent of Total', '%')

    total_row = pd.DataFrame({
        'fcode_desc': ['Total'],
        'stream_length': [f"{_floatformat2(total.magnitude)}"],
        'Percent of Total': ["100.0%"]
    })

    df = pd.concat([df, total_row], ignore_index=True)
    # Use metadata for friendly names and units in headings
    friendly_area = df.meta.friendly('stream_length')
    unit_area = df.meta.unit('stream_length')
    friendly_owner = df.meta.friendly('ownership')
    friendly_owner_desc = df.meta.friendly('ownership_desc')
    percent_friendly = df.meta.friendly('Percent of Total')
    percent_unit = df.meta.unit('Percent of Total')
    # df.columns = [
    #     friendly_owner,
    #     friendly_owner_desc,
    #     f"{friendly_area} ({unit_area})" if unit_area else friendly_area,
    #     f"{percent_friendly} ({percent_unit})" if percent_unit else percent_friendly
    # ]
    return df.to_html(index=False, escape=False)


def table_of_ownership(gdf) -> str:
    # common elements with table_of_river_names to be separated out
    # Pint-enabled DataFrame for calculation
    # Start from a copy with metadata. this isn't working df.meta.friendly <> gdf.meta.friendly
    df = subset_with_meta(gdf, ["ownership", "ownership_desc", "stream_length"])
    df = df.groupby(['ownership', 'ownership_desc'], as_index=False)['stream_length'].sum()
    total = df['stream_length'].sum()  # Quantity
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
        'ownership_desc': [''],
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


# =========================
# Figures - generate specific figures
# take a (geo)dataframe and return a plotly graph object
# =========================

def low_lying_ratio_bins(df: pd.DataFrame) -> go.Figure:
    # "legend" array from https://github.com/Riverscapes/RiverscapesXML/blob/master/Symbology/web/Shared/Low_Lying_Ratio.json
    bins_json = """[
    ["rgb(247, 252, 245)", "< 2%"],
    ["rgb(226, 244, 221)", "2% to 5%"],
    ["rgb(192, 230, 185)", "5% to 10%"],
    ["rgb(148, 211, 144)", "10% to 15%"],
    ["rgb(96, 186, 108)", "15% to 25%"],
    ["rgb(50, 155, 81)", "25% to 50%"],
    ["rgb(12, 120, 53)", "50% to 75%"],
    ["rgb(0, 68, 27)", "> 75%"]
    ]"""

    # from https://github.com/Riverscapes/RiverscapesXML/blob/master/Symbology/qgis/Shared/Low_Lying_Ratio.qml
    # didn't end up using this
    bins_xml = """<rules key="{74b18146-02e2-4bd4-ad4c-996748046586}">
      <rule label="&lt; 2%" symbol="0" key="{4e2991cc-a65b-4213-9cbe-900c7018ca7a}" filter="&quot;vbet_igo_low_lying_ratio&quot; &lt; 0.02"/>
      <rule label="2% to 5%" symbol="1" key="{b25eaed3-e38a-49d6-b8b5-c151647e9852}" filter="&quot;vbet_igo_low_lying_ratio&quot; >= 0.02 and &quot;vbet_igo_low_lying_ratio&quot; &lt; 0.05"/>
      <rule label="5% to 10%" symbol="2" key="{d1d038ba-632f-42de-b195-7d14d7db2b75}" filter="&quot;vbet_igo_low_lying_ratio&quot;>= 0.05 and &quot;vbet_igo_low_lying_ratio&quot; &lt; 0.1"/>
      <rule label="10% to 15%" symbol="3" key="{d7d5b108-b27e-43ea-8b09-70ac500cf74f}" filter="&quot;vbet_igo_low_lying_ratio&quot; >= 0.1 and &quot;vbet_igo_low_lying_ratio&quot; &lt; 0.15"/>
      <rule label="15% to 25%" symbol="4" key="{5dcf0a4e-c077-4839-b96f-8f5a04376f10}" filter="&quot;vbet_igo_low_lying_ratio&quot; >= 0.15 and &quot;vbet_igo_low_lying_ratio&quot; &lt; 0.25"/>
      <rule label="25% to 50% " symbol="5" key="{87131111-33cf-4724-b94c-a6e70b5d456a}" filter="&quot;vbet_igo_low_lying_ratio&quot; >= 0.25 and &quot;vbet_igo_low_lying_ratio&quot; &lt; 0.5"/>
      <rule label="50% to 75% " symbol="6" key="{c6db9e10-6619-439e-bf60-bb1e885b2b4c}" filter="&quot;vbet_igo_low_lying_ratio&quot; >= 0.5 and &quot;vbet_igo_low_lying_ratio&quot; &lt; 0.75"/>
      <rule label="> 75%" symbol="7" key="{3c29e498-5ded-4e5b-a89d-0ae78e081e85}" filter="&quot;vbet_igo_low_lying_ratio&quot; >= 0.75"/>
    </rules>
    """
    chart_data = df[['low_lying_ratio', 'segment_area']].copy()
    bins = [0, 0.02, 0.05, 0.10, 0.15, 0.25, 0.50, 0.75, 1]
    labels = extract_labels_from_legend(bins_json)
    colours = extract_colours_from_legend(bins_json)
    # Bin the low_lying_ratio values
    chart_data['bin'] = pd.cut(chart_data['low_lying_ratio'], bins=bins, labels=labels, include_lowest=True)
    # Aggregate segment_area by bin
    agg_data = chart_data.groupby('bin', as_index=False)['segment_area'].sum()
    # Plot bar chart
    fig = px.bar(
        agg_data,
        x='bin',
        y='segment_area',
        color='bin',
        color_discrete_sequence=colours,
        title='Total Segment Area by Low Lying Ratio Bin',
        labels={'bin': 'Low Lying Ratio', 'segment_area': 'Total Segment Area'},
        height=400
    )
    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return fig


def prop_riparian_bins(df: pd.DataFrame) -> go.Figure:
    # "legend" array from https://github.com/Riverscapes/RiverscapesXML/blob/master/Symbology/web/Shared/Prop_Rip.json
    bins_json = """[
    ["rgb(67, 41, 0)", "0%"],
    ["rgb(98, 73, 0)", "0 - 5%"],
    ["rgb(89, 83, 0)", "5 - 15%"],
    ["rgb(76, 93, 0)", "15 - 30%"],
    ["rgb(53, 103, 0)", "30 - 60%"],
    ["rgb(9, 112, 0)", "> 60%"]
    ]"""

    chart_data = df[['lf_riparian_prop', 'segment_area']].copy()
    bins = [0, 0.000000001, 0.05, 0.15, 0.3, 0.6, 1]
    labels = extract_labels_from_legend(bins_json)
    colours = extract_colours_from_legend(bins_json)
    # Bin the low_lying_ratio values
    chart_data['bin'] = pd.cut(chart_data['lf_riparian_prop'], bins=bins, labels=labels, include_lowest=True)
    # Aggregate segment_area by bin
    agg_data = chart_data.groupby('bin', as_index=False)['segment_area'].sum()
    # Plot bar chart
    fig = px.bar(
        agg_data,
        x='bin',
        y='segment_area',
        color='bin',
        color_discrete_sequence=colours,
        title='Total Segment Area by Proportion Riparian Bin',
        labels={'bin': 'Riparian Proportion', 'segment_area': 'Total Segment Area'},
        height=400
    )
    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return fig


def floodplain_access(df: pd.DataFrame) -> go.Figure:
    # "legend" array from https://github.com/Riverscapes/RiverscapesXML/blob/master/Symbology/web/Shared/Fldpln_Access.json
    bins_json = """[
    ["rgb(242, 0, 0)", "< 50%"],
    ["rgb(255, 0, 106)", "50 - 75%"],
    ["rgb(230, 57, 185)", "75 - 90%"],
    ["rgb(161, 114, 239)", "90 - 95%"],
    ["rgb(31, 147, 255)", "> 95%"]
  ]"""

    chart_data = df[['fldpln_access', 'segment_area']].copy()
    bins = [0, 0.50, 0.75, 0.90, 0.95, 1]
    labels = extract_labels_from_legend(bins_json)
    colours = extract_colours_from_legend(bins_json)
    # Bin the low_lying_ratio values
    chart_data['bin'] = pd.cut(chart_data['fldpln_access'], bins=bins, labels=labels, include_lowest=True)
    # Aggregate segment_area by bin
    agg_data = chart_data.groupby('bin', as_index=False)['segment_area'].sum()
    # Plot bar chart
    fig = px.bar(
        agg_data,
        x='bin',
        y='segment_area',
        color='bin',
        color_discrete_sequence=colours,
        title='Total Riverscapes Area by Floodplain Access',
        labels={'bin': 'Floodplain Access', 'segment_area': 'Total Riverscapes Area (units)'},
        height=400
    )
    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return fig


def land_use_intensity(df: pd.DataFrame) -> go.Figure:
    # "legend" array from https://github.com/Riverscapes/RiverscapesXML/blob/master/Symbology/web/Shared/Land_Use.json
    bins_json = """[
    ["rgb(38, 115, 0)", "Very Low"],
    ["rgb(164, 196, 0)", "Low"],
    ["rgb(255, 187, 0)", "Moderate"],
    ["rgb(245, 0, 0)", "High"]
  ]"""

    chart_data = df[['land_use_intens', 'segment_area']].copy()
    bins = [0, 0.00001, 33.0001, 66.001, 100]
    labels = extract_labels_from_legend(bins_json)
    colours = extract_colours_from_legend(bins_json)
    # Bin the values
    chart_data['bin'] = pd.cut(chart_data['land_use_intens'], bins=bins, labels=labels, include_lowest=True)
    # Aggregate segment_area by bin
    agg_data = chart_data.groupby('bin', as_index=False)['segment_area'].sum()
    # Plot bar chart
    fig = px.bar(
        agg_data,
        x='bin',
        y='segment_area',
        color='bin',
        color_discrete_sequence=colours,
        title='Total Riverscapes Area by Land Use Intensity',
        labels={'bin': 'Land Use Intensity', 'segment_area': 'Total Riverscapes Area (units)'},
        height=400
    )
    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return fig


def prop_ag_dev(df: pd.DataFrame) -> go.Figure:
    """example of figure with two measures"""
    # from https://github.com/Riverscapes/RiverscapesXML/blob/master/Symbology/web/Shared/lf_ag_rme3.json
    # lf_dev_rme3.json has the same ones
    bins_json = """[
        ["rgb(255, 255, 212)", "0%"],
        ["rgb(254, 227, 145)", "0 - 5%"],
        ["rgb(254, 196, 79)", "5 - 15%"],
        ["rgb(254, 153, 41)", "15 - 30%"],
        ["rgb(217, 95, 14)", "30 - 60%"],
        ["rgb(153, 52, 4)", "> 60%"]
        ]"""
    # make a copy and work with that
    df = df[['lf_agriculture_prop', 'lf_developed_prop', 'segment_area']].copy()
    bins = [0, 0.00001, 0.05, 0.15, 0.30, 0.60, 1.0]
    labels = extract_labels_from_legend(bins_json)
    colours = extract_colours_from_legend(bins_json)
    # Bin each metric separately
    df['ag_bin'] = pd.cut(df['lf_agriculture_prop'], bins=bins, labels=labels, include_lowest=True)
    df['dev_bin'] = pd.cut(df['lf_developed_prop'], bins=bins, labels=labels, include_lowest=True)

    # Aggregate segment_area by each bin
    ag_data = df.groupby('ag_bin')['segment_area'].sum().reset_index()
    ag_data = ag_data.rename(columns={'ag_bin': 'bin', 'segment_area': 'ag_segment_area'})

    dev_data = df.groupby('dev_bin')['segment_area'].sum().reset_index()
    dev_data = dev_data.rename(columns={'dev_bin': 'bin', 'segment_area': 'dev_segment_area'})

    # Merge for grouped bar chart
    agg_data = pd.merge(ag_data, dev_data, on='bin', how='outer')
    fig = go.Figure()
    fig.add_trace(go.Bar(x=agg_data['bin'], y=agg_data['ag_segment_area'], name='Agriculture'))
    fig.add_trace(go.Bar(x=agg_data['bin'], y=agg_data['dev_segment_area'], name='Development'))

    fig.update_layout(
        title='Agriculature and Development Proportion by Bin',
        barmode='group',
        margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return fig


def dens_road_rail(df: pd.DataFrame) -> go.Figure:
    """riverscape area by road and rail density bins"""
    bins_json = """[
    ["#ffffff", "0"],
    ["#4fac24", "0 - 0.01"],
    ["#93d31d", "0.01 - 0.025"],
    ["#ffef39", "0.025 - 0.1"],
    ["#fb9820", "0.1 - 1"],
    ["#ed2024", "> 1"]
    ]"""

    # Example: horizontal grouped bar chart for road and rail density
    chart_data = df[['road_dens', 'rail_dens', 'segment_area']].copy()
    # Bin each metric (customize bins as needed)
    bins = [0, 0.1, 0.5, 1, 2, 5, 10, 100]
    labels = ["<0.1", "0.1-0.5", "0.5-1", "1-2", "2-5", "5-10", ">10"]
    chart_data['road_bin'] = pd.cut(chart_data['road_dens'], bins=bins, labels=labels, include_lowest=True)
    chart_data['rail_bin'] = pd.cut(chart_data['rail_dens'], bins=bins, labels=labels, include_lowest=True)

    # Aggregate segment_area by each bin
    road_data = chart_data.groupby('road_bin')['segment_area'].sum().reset_index()
    road_data = road_data.rename(columns={'road_bin': 'bin', 'segment_area': 'road_segment_area'})

    rail_data = chart_data.groupby('rail_bin')['segment_area'].sum().reset_index()
    rail_data = rail_data.rename(columns={'rail_bin': 'bin', 'segment_area': 'rail_segment_area'})

    # Merge for grouped bar chart
    agg_data = pd.merge(road_data, rail_data, on='bin', how='outer')

    import plotly.graph_objects as go
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=agg_data['bin'],
        x=agg_data['road_segment_area'],
        name='Road Density',
        orientation='h'
    ))
    fig.add_trace(go.Bar(
        y=agg_data['bin'],
        x=agg_data['rail_segment_area'],
        name='Rail Density',
        orientation='h'
    ))
    fig.update_layout(
        barmode='group',
        title='Segment Area by Road and Rail Density Bin',
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        height=400,
        yaxis_title='Density Bin',
        xaxis_title='Total Segment Area'
    )
    return fig


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
        labels={"segment_area": "Total Segment Area (m²)", "ownership": "Ownership"},
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
        names='fcode_desc',
        values='segment_area',
        title='Total Riverscape Area (units) by Feature Code'
    )
    return fig

# =========================
# Stats
# =========================


def statistics(gdf) -> dict[str, pint.Quantity]:
    stats = {}
    stats["total_riverscapes_area"] = pint.Quantity(gdf["segment_area"].sum(), "m^2")
    stats["total_centerline"] = gdf["centerline_length"].sum()
    stats['integrated_valley_bottom_width'] = stats['total_riverscapes_area']/stats['total_centerline']
    stats['total_channel_length'] = gdf["channel_length"].sum()
    return stats
