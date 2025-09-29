# Instantiate the custom DataFrame accessor
from pathlib import Path
from datetime import datetime

import pandas as pd
from jinja2 import Template
from rsxml import Logger
from shapely import wkt
import plotly.express as px
from util.pandas.RSFieldMeta import RSFieldMeta
from util.pandas.RSGeoDataFrame import RSGeoDataFrame


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
