# Instantiate the custom DataFrame accessor
from pathlib import Path
from datetime import datetime

import pandas as pd
from jinja2 import Template
from rsxml import Logger
from shapely import wkt
from termcolor import colored
from util.pandas.RSFieldMeta import RSFieldMeta
from util.pandas.RSGeoDataFrame import RSGeoDataFrame


def friendly_describe(df: pd.DataFrame):
    """A helper function to print a friendly description of a DataFrame with metadata."""
    log = Logger('FriendlyDescribe')
    log.info('DataFrame Info:')
    log.info(colored(f'Columns: {df.columns.tolist()}', 'cyan'))
    log.info(colored(f'Number of rows: {len(df)}', 'magenta'))
    df_no_units = df.copy()
    # Remove units for describe
    for col in df_no_units.columns:
        if hasattr(df_no_units[col].iloc[0], "magnitude"):
            df_no_units[col] = df_no_units[col].apply(lambda x: x.magnitude if hasattr(x, "magnitude") else x)
    desc = df_no_units.describe(include='all').transpose()
    log.info(colored(f"\n{desc}\n", 'green'))
    return desc


def main():
    """ Simple test function to demonstrate usage of the MetaAccessor.
    """
    log = Logger('Pandas Test')
    log.title('Pandas RSDataFrames Test')
    this_dir = Path(__file__).resolve().parent
    demo_dir = this_dir / 'demo'

    # Load the Metadata and set up the Borg Singleton
    csv_meta_path = demo_dir / 'dgo_sample_meta.csv'
    log.info(f'Loading data from {csv_meta_path}')
    # Instantiate the Borg; NOTE: THIS SHOULD COME BEFORE YOU USE ANYTHING INSIDE RSDataFrame
    RSFieldMeta().field_meta = pd.read_csv(csv_meta_path)

    # Load the main data
    csv_path = demo_dir / 'dgo_sample.csv'
    log.info(f'Loading data from {csv_path}')
    df = pd.read_csv(csv_path)
    df['dgo_polygon_geom'] = df['dgo_geom_obj'].apply(wkt.loads)
    data_gdf = RSGeoDataFrame(df, geometry='dgo_polygon_geom', crs='EPSG:4326')

    # Exclude dgo_geom_obj and dgo_polygon_geom from HTML output

    tables = {
        "Unitless RAW Data Table": data_gdf.to_html(
            include_units=False,
            use_friendly=False,
            exclude_columns=['dgo_geom_obj', 'dgo_polygon_geom']
        ),
        "Metric (default) Data Table": data_gdf.to_html(
            exclude_columns=['dgo_geom_obj', 'dgo_polygon_geom'],
        )
    }

    ################################################################################
    # Adding new columns to your dataframe
    ################################################################################

    # let's create a dataframe that's a subset of data_gdf that only uses the column seg_distance
    seg_df = RSGeoDataFrame(data_gdf[['seg_distance']])
    # Now I'm going to rename seg_distance to be seg_distance_cm so that I can have an opinion about it

    # Now let's copy seg_distance to a new column called seg_distance_imperial
    seg_df['seg_distance_cm'] = seg_df['seg_distance']
    seg_df['seg_distance_imperial'] = seg_df['seg_distance']

    # Now we need to manually set the metadata for this new column
    # Note that we're setting no_covert False so this will always show as miles
    RSFieldMeta().add_meta_column(
        name='seg_distance_cm',
        friendly_name='Segment Distance',
        data_unit='m',  # This is still the original data unit
        display_unit='cm',  # no_convert is true so the display units will be used
        dtype='INT',  # Note that we can coerse the dtype if we want here too (was FLOAT)
        no_convert=True
    )
    RSFieldMeta().add_meta_column(
        name='seg_distance_imperial',
        friendly_name='Segment Distance',
        data_unit='m',  # This is still the original data unit
        display_unit='yards',  # no_convert is true so the display units will be used
        dtype='INT',  # Note that we can coerse the dtype if we want here too (was FLOAT)
        no_convert=True
    )
    tables["Imperial Distance Column"] = seg_df.to_html()

    ################################################################################
    # Working with unit systems
    ################################################################################

    # now switch the borg's unit system to imperial. Note that this affects ALL RSDataFrames but since
    # the tables above are already rendered as strings they remain unnaffected. In general you should
    # Really only call this once and only after instantiating the metadata

    RSFieldMeta().unit_system = 'imperial'  # valid choices are 'SI' and 'imperial'
    tables["Imperial Units Data Table"] = data_gdf.to_html(
        exclude_columns=['dgo_geom_obj', 'dgo_polygon_geom'],
    )

    # Now give us an HTML file
    template_path = demo_dir / 'template.html'
    css_path = demo_dir / 'report.css'
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
        tables=tables
    )
    html_path = demo_dir / 'test_output.html'
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    return


if __name__ == "__main__":
    main()
