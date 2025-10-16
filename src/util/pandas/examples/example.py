# Instantiate the custom DataFrame accessor
from pathlib import Path
from datetime import datetime

import pandas as pd
from jinja2 import Template
from rsxml import Logger
from shapely import wkt
import plotly.express as px
import pint
from util.pandas.RSFieldMeta import RSFieldMeta
from util.pandas.RSGeoDataFrame import RSGeoDataFrame
ureg = pint.get_application_registry()


def main():
    """ Simple test function to demonstrate usage of the MetaAccessor.
    """
    log = Logger('Pandas Test')
    log.title('Pandas RSDataFrames Test')
    this_dir = Path(__file__).resolve().parent

    _FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()

    # Load the Metadata and set up the Borg Singleton
    csv_meta_path = this_dir / 'dgo_sample_meta.csv'
    log.info(f'Loading data from {csv_meta_path}')
    # Instantiate the Borg; NOTE: THIS SHOULD COME BEFORE YOU USE ANYTHING INSIDE RSDataFrame
    _FIELD_META.field_meta = pd.read_csv(csv_meta_path)

    # Load the main data
    csv_path = this_dir / 'dgo_sample.csv'
    log.info(f'Loading data from {csv_path}')
    df = pd.read_csv(csv_path)
    df['dgo_polygon_geom'] = df['dgo_geom_obj'].apply(wkt.loads)
    data_gdf = RSGeoDataFrame(df, geometry='dgo_polygon_geom', crs='EPSG:4326')

    # Bake some new HTML tables that we will inject into our JINJA2 template
    figures = {
        "RAW Pandas to_html()": df.to_html(),
        # Give us one without units or friendly names to see what "vanilla" looks like
        "RSGeoDataFrame: Unitless RAW Data Table": data_gdf.to_html(
            include_units=False,
            use_friendly=False,
            exclude_columns=['dgo_geom_obj', 'dgo_polygon_geom']
        ),
        # This is just the default with geometry columns excluded
        "RSGeoDataFrame: Metric (default) Data Table": data_gdf.to_html(
            exclude_columns=['dgo_geom_obj', 'dgo_polygon_geom'],
        )
    }

    ################################################################################
    # A few utility functions
    ################################################################################

    # You can just get a nice friendly name for a field with the unit included:
    print(_FIELD_META.get_field_header('drainage_area'))
    # OUTPUT: Drainage Area (km²)

    # If you want a nice object with all the metadata for a field:
    drainage_area_meta = _FIELD_META.get_field_meta('drainage_area')
    print(drainage_area_meta)
    # OUTPUT: FieldMetaValues(name='drainage_area', friendly_name='Drainage Area', data_unit='kilometer ** 2',
    # display_unit='None', dtype='REAL', no_convert=False)

    print(_FIELD_META.get_dimensionality_name(drainage_area_meta.data_unit))
    # OUTPUT: area

    # Apply units gives back a dataframe with Pint objects in it
    # This can be useful for doing math since the units will add some safety to the calculation and
    # the results will (or should) also have appropriate units
    _FIELD_META.apply_units(data_gdf)

    # Bake units gives back a dataframe with magnitude applied (numbers, not Pint objects) and a list of headers with units baked in
    baked_df, headers = _FIELD_META.bake_units(data_gdf)

    # Retrieve a list of all headers with units baked in
    _FIELD_META.get_headers(data_gdf)
    # OUTPUT: ['Level Path', 'Segment distance (m)', 'Centerline length (m)', 'Segment Area (km²)', 'fcode', 'Feature Code Description', 'longitude', 'latitude', 'Ownership Code', 'Ownership description', 'State', 'County', 'Drainage Area (km²)', 'stream_name', 'stream_order', 'Stream Length (m)', 'huc12', 'rel_flow_length', 'channel_area', 'integrated_width', 'low_lying_ratio', 'elevated_ratio', 'floodplain_ratio', 'acres_vb_per_mile (acre/mi)', 'hect_vb_per_km (ha/km)', 'channel_width', 'lf_agriculture_prop', 'lf_agriculture', 'lf_developed_prop', 'lf_developed', 'lf_riparian_prop', 'lf_riparian', 'ex_riparian', 'hist_riparian', 'prop_riparian', 'hist_prop_riparian', 'develop', 'road_len', 'road_dens', 'rail_len', 'Rail Density', 'land_use_intens', 'road_dist', 'rail_dist', 'div_dist', 'canal_dist', 'infra_dist', 'fldpln_access', 'access_fldpln_extent', 'rme_project_id', 'rme_project_name', 'dgo_geom_obj', 'dgo_polygon_geom']

    # Retrieve a lookup dictionary of field name to header with units baked in
    _FIELD_META.get_headers_dict(data_gdf)
    # OUTPUT: {'level_path': 'Level Path', 'seg_distance': 'Segment distance (m)', 'centerline_length': 'Centerline length (m)', 'segment_area': 'Segment Area (km²)', 'fcode': 'fcode', 'fcode_desc': 'Feature Code Description', 'longitude': 'longitude', 'latitude': 'latitude', 'ownership': 'Ownership Code', 'ownership_desc': 'Ownership description', 'state': 'State', 'county': 'County', 'drainage_area': 'Drainage Area (km²)', 'stream_name': 'stream_name', 'stream_order': 'stream_order', 'stream_length': 'Stream Length (m)', 'huc12': 'huc12', 'rel_flow_length': 'rel_flow_length', 'channel_area': 'channel_area', 'integrated_width': 'integrated_width', 'low_lying_ratio': 'low_lying_ratio', 'elevated_ratio': 'elevated_ratio', 'floodplain_ratio': 'floodplain_ratio', 'acres_vb_per_mile': 'acres_vb_per_mile (acre/mi)', 'hect_vb_per_km': 'hect_vb_per_km (ha/km)', 'channel_width': 'channel_width', 'lf_agriculture_prop': 'lf_agriculture_prop', 'lf_agriculture': 'lf_agriculture', 'lf_developed_prop': 'lf_developed_prop', 'lf_developed': 'lf_developed', 'lf_riparian_prop': 'lf_riparian_prop', 'lf_riparian': 'lf_riparian', 'ex_riparian': 'ex_riparian', 'hist_riparian': 'hist_riparian', 'prop_riparian': 'prop_riparian', 'hist_prop_riparian': 'hist_prop_riparian', 'develop': 'develop', 'road_len': 'road_len', 'road_dens': 'road_dens', 'rail_len': 'rail_len', 'rail_dens': 'Rail Density', 'land_use_intens': 'land_use_intens', 'road_dist': 'road_dist', 'rail_dist': 'rail_dist', 'div_dist': 'div_dist', 'canal_dist': 'canal_dist', 'infra_dist': 'infra_dist', 'fldpln_access': 'fldpln_access', 'access_fldpln_extent': 'access_fldpln_extent', 'rme_project_id': 'rme_project_id', 'rme_project_name': 'rme_project_name', 'dgo_geom_obj': 'dgo_geom_obj', 'dgo_polygon_geom': 'dgo_polygon_geom'}

    ################################################################################
    # Explicitly Setting Units
    ################################################################################

    # If you don't want to use the systems but you still want system conversions (i.e. miles to km or acres to hectares)
    # You can use get_system_unit_value() to convert a Pint quantity to the appropriate unit for the current unit system
    length_in_miles = 10 * ureg.mile
    print(f"{_FIELD_META.get_system_unit_value(length_in_miles)}")
    # 16.09344 kilometer

    # If we're already in the desired unit system then it just returns the original quantity
    length_in_km = 20 * ureg.kilometer
    print(f"{_FIELD_META.get_system_unit_value(length_in_km)}")
    # 20 kilometer

    # All of this is dependent on the current unit system having a lookup in the SI_TO_IMPERIAL or IMPERIAL_TO_SI dicts
    # (see RSFieldMeta.py). If there is no mapping it will just return the original quantity with a warning in the console
    volume_in_teaspoons = 5 * ureg.teaspoon
    print(f"{_FIELD_META.get_system_unit_value(volume_in_teaspoons)}")
    # [WARNING] [RSFieldMeta] No conversion found for unit 'teaspoon' in current system 'SI'.
    # 5 teaspoon

    # the lookups are bidirectional since we don't always have 1:1 mappings (e.g. acre to hectare, yards or feet to meters etc.)
    length_in_meters = 100 * ureg.meter
    print(f"{_FIELD_META.get_system_unit_value(length_in_meters)}")
    # 100 meter

    length_in_yards = 100 * ureg.yard
    # Note here how meters convert to yards (not feet)
    print(f"{_FIELD_META.get_system_unit_value(length_in_yards)}")
    # 91.44 meter

    length_in_feet = 100 * ureg.foot
    print(f"{_FIELD_META.get_system_unit_value(length_in_feet)}")
    # 30.479999999999997 meter

    _FIELD_META.unit_system = 'imperial'  # valid choices are 'SI' and 'imperial'
    # Note here how meters convert to feet (not yards)
    print(f"{_FIELD_META.get_system_unit_value(length_in_meters)}")
    # 328.0839895013123 foot

    # End of example. reset to SI
    _FIELD_META.unit_system = 'SI'  # valid choices are 'SI' and 'imperial'

    ################################################################################
    # Adding new columns to your dataframe
    ################################################################################

    # let's create a dataframe that's a subset of data_gdf that only uses the column seg_distance
    seg_df = RSGeoDataFrame(data_gdf[['seg_distance']])

    # Now let's copy seg_distance to a new column called seg_distance_cm and seg_distance_imperial
    seg_df['seg_distance_cm'] = seg_df['seg_distance']
    seg_df['seg_distance_imperial'] = seg_df['seg_distance']

    # We will need to manually set the field metadata for these new columns
    # Note that we're setting "no_covert" False so this will always show as miles
    # you NEED to do this if you want "display_units" to be used. Otherwise the system
    # chooses your display units based on your unit system and PREFERRED_UNITS
    _FIELD_META.add_field_meta(
        name='seg_distance_cm',
        friendly_name='Segment Distance',
        data_unit='m',  # This is still the original data unit
        display_unit='cm',  # no_convert is true so the display units will be used
        dtype='INT',  # Note that we can coerse the dtype if we want here too (was FLOAT)
        no_convert=True
    )
    _FIELD_META.add_field_meta(
        name='seg_distance_imperial',
        friendly_name='Segment Distance',
        data_unit='m',  # This is still the original data unit
        display_unit='yards',  # no_convert is true so the display units will be used
        dtype='INT',  # Note that we can coerse the dtype if we want here too (was FLOAT)
        no_convert=True
    )

    # Now add our new table to the figures dict
    figures["Imperial Distance Column"] = seg_df.to_html()

    ################################################################################
    # Reducing data
    ################################################################################

    # let's create a dataset that only has segment_area and ownership and then sum the ownerships per area
    # I want area as km^2 and area as a percent of the whole and I want it to be an RSGeoDataFrame that I can
    # call .to_html on
    area_own_df = data_gdf[['segment_area', 'ownership']]
    area_own_df = area_own_df.groupby('ownership').agg({'segment_area': 'sum'}).reset_index()
    # Now we need a percent column
    total_area = area_own_df['segment_area'].sum()
    area_own_df['segment_area_pct'] = (area_own_df['segment_area'] / total_area) * 100
    # Now we need to manually set the metadata for this new column
    _FIELD_META.add_field_meta(
        name='segment_area_pct',
        friendly_name='Segment Distance %',
        dtype='REAL',  # Note that we can coerse the dtype if we want here too (was FLOAT)
    )
    area_own_df = RSGeoDataFrame(area_own_df)

    ################################################################################
    # Adding a footer row to the dataframe
    ################################################################################
    footer = pd.DataFrame({
        'segment_area': [area_own_df['segment_area'].sum()],
        'ownership': ['Total']
    })
    area_own_df.set_footer(footer)

    # Add our new table to the figures dict
    figures["Area by Ownership"] = area_own_df.to_html()

    ################################################################################
    # Charting Data with PLotly
    ################################################################################
    # let's continue the "Reducing Data" example above and make a pie chart of area by ownership
    # First bake a new dataframe so we can have units applied but free of Pint objects
    chart_data, headers = _FIELD_META.bake_units(area_own_df)

    pie_fig = px.pie(
        chart_data,
        names="ownership",          # Use the friendly name
        values=chart_data['segment_area'],     # Use the friendly name
        title="Total Riverscape Area (units) by Ownership %",
        labels={'segment_area': headers[1]},  # Use the friendly name with units baked in
        width=640,
        height=480,
    )

    # Now add the pie chart to our figures dict as HTML
    figures["Area by Ownership Pie Chart %"] = pie_fig.to_html(full_html=False, include_plotlyjs=False)

    ################################################################################
    # Working with unit systems
    ################################################################################

    # now switch the borg's unit system to imperial. Note that this affects ALL RSDataFrames but since
    # the tables above are already rendered as strings they remain unnaffected. In general you should
    # Really only call this once and only after instantiating the metadata

    _FIELD_META.unit_system = 'imperial'  # valid choices are 'SI' and 'imperial'
    figures["Imperial Units Data Table"] = data_gdf.to_html(
        exclude_columns=['dgo_geom_obj', 'dgo_polygon_geom'],
    )

    ################################################################################
    # Writing the HTML report
    ################################################################################

    # Now give us an HTML file
    template_path = this_dir / 'demo_template.html'
    css_path = this_dir / 'report.css'
    template = Template(template_path.read_text(encoding='utf-8'))
    css = css_path.read_text(encoding='utf-8')
    style_tag = f"<style>{css}</style>"
    now = datetime.now()
    report_name = "DEMO UNIT TABLES"
    html = template.render(
        report={
            'head': style_tag,
            'title': report_name,
            'date': now.strftime('%B %d, %Y - %I:%M%p'),
            'ReportType': "DEMO"
        },
        report_name=report_name,
        tables=figures
    )
    html_path = this_dir / 'test_output.html'
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    log.info(f"Wrote HTML report to {html_path}")
    log.info("---- DONE ----")
    return


if __name__ == "__main__":
    main()
