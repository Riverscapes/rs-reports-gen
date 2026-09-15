import html
from collections import defaultdict

import geopandas as gpd
import pandas as pd
import pint
import pint_pandas
import plotly.express as px
import plotly.graph_objects as go
from rsxml import Logger

from util.html.table import render_table
from util.pandas import RSFieldMeta, RSGeoDataFrame

# 1. Define a mapping of "Row Label" -> (Area Column, Count Column)
# should all be in the same units!
waterbody_col_map = {
    "Lake": ("sum_waterbodyLakesPondsAreaSqKm", "sum_waterbodyLakesPondsFeatureCount"),
    "Reservoir": ("sum_waterbodyReservoirAreaSqKm", "sum_waterbodyReservoirFeatureCount"),
    "Estuaries": ("sum_waterbodyEstuariesAreaSqKm", "sum_waterbodyEstuariesFeatureCount"),
    "Playa": ("sum_waterbodyPlayaAreaSqKm", "sum_waterbodyPlayaFeatureCount"),
    "Swamp Marsh": ("sum_waterbodySwampMarshAreaSqKm", "sum_waterbodySwampMarshFeatureCount"),
    "Ice/Snow": ("sum_waterbodyIceSnowAreaSqKm", "sum_waterbodyIceSnowFeatureCount"),
}


def ensure_pint_column(df: pd.DataFrame, column: str, unit: str | pint.Unit | None = "") -> pd.Series:
    """
    Ensure that a column in the DataFrame is a pint quantity.
    If the column is not already a pint quantity, convert it using the provided unit.
    """
    # Default to dimensionless if no unit is provided
    if unit is None:
        unit = "dimensionless"
    if not isinstance(df[column].dtype, pint_pandas.PintType):
        # Convert the column to a pint-enabled Series
        return pd.Series(df[column].values, index=df.index, name=column, dtype=pint_pandas.PintType(unit))
    return df[column]


def create_waterbody_summary_table(df: RSGeoDataFrame) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """Pivot waterbody columns into rows, maintaining units.
    Returns the main table and a footer dataframe containing the totals row.
    input: dataframe containing all the columns listed above. assumes there is just one row.
    """
    layer_id = 'waterbodies_summary'  # For metadata namespacing
    # Ensure we are working with the first row of data if df has multiple
    row_data = df.iloc[0]

    # 2. Extract data into a list of dicts (Pivoting)
    summary_rows = []
    for label, (area_col, count_col) in waterbody_col_map.items():
        summary_rows.append(
            {
                "Waterbodies": label,
                "Area": row_data[area_col.lower()],
                "Count": row_data[count_col.lower()],
            }
        )

    # Create the new DataFrame
    report_df = RSGeoDataFrame(pd.DataFrame(summary_rows))

    meta = RSFieldMeta()
    # Get the units from the source data and apply it to the new dataframe
    area_unit = meta.get_field_unit(waterbody_col_map["Lake"][0].lower())
    count_unit = meta.get_field_unit(waterbody_col_map["Lake"][1].lower())
    meta.add_field_meta(name='Area', layer_id=layer_id, data_unit=area_unit)
    meta.add_field_meta(name='Count', layer_id=layer_id, data_unit=count_unit, preferred_format='{:,.0f}')

    # Ensure both Area and Count columns are pint quantities
    report_df["Area"] = ensure_pint_column(report_df, "Area", area_unit)
    report_df["Count"] = ensure_pint_column(report_df, "Count", count_unit)

    # 3. Calculate Totals for the footer
    #    We calculate totals from the specific rows to ensure math consistency
    total_area = report_df["Area"].sum()
    total_count = report_df["Count"].sum()

    # 4. Vectorized Calculations (Percentages and Averages)
    #    We use numpy to handle division by zero safely
    # pint-pandas does not support broadcasting a scalar Quantity, so we create a Series of the total for both Area and Count
    report_df["% Area"] = (report_df["Area"] / pd.Series([total_area] * len(report_df), index=report_df.index)).fillna(0).astype('pint[percent]')
    report_df["% Count"] = (report_df["Count"] / pd.Series([total_count] * len(report_df), index=report_df.index)).fillna(0).astype('pint[percent]')
    report_df["Avg. Area"] = (report_df["Area"] / report_df["Count"]).fillna(0)
    meta.add_field_meta(name="Avg. Area", layer_id=layer_id, data_unit=area_unit)
    meta.add_field_meta(name="% Area", layer_id=layer_id, data_unit='percent')
    meta.add_field_meta(name="% Count", layer_id=layer_id, data_unit='percent')

    # 5. Formatting (Optional: Create the "Total" row)
    total_row = pd.DataFrame(
        [
            {
                "Waterbodies": "Total Waterbodies",
                "Area": total_area,
                "Count": total_count,
                "% Area": 100.0,
                "% Count": 100.0,
                "Avg. Area": (total_area / total_count) if total_count > 0 else 0,
            }
        ]
    )
    # Also convert the total row percentages to pint quantities
    total_row['% Area'] = total_row['% Area'].astype('pint[percent]')
    total_row['% Count'] = total_row['% Count'].astype('pint[percent]')

    return report_df.reset_index(drop=True), total_row.reset_index(drop=True)


hydrography_col_map = {
    'Perennial': ('sum_flowlineLengthPerennialKm'),
    'Intermittent': ('sum_flowlineLengthIntermittentKm'),
    'Ephemeral': ('sum_flowlineLengthEphemeralKm'),
    'Canals': ('sum_flowlineLengthCanalsKm'),
    'Total': ('sum_flowlineLengthAllKm'),
}


def statistics(aggregate_data_df: pd.DataFrame, hucs_df: pd.DataFrame, geo_data_df: gpd.GeoDataFrame, owner_data_df: gpd.GeoDataFrame, ecoregion_data_df: gpd.GeoDataFrame) -> dict[str, pint.Quantity]:
    """
    named, non-tabular statistics.

    Args:
        aggregate_data_df (DataFrame): the result of the aggregate query (should have just one row)
        hucs_df (DataFrame): the HUC data related to the watershed context
        geo_data_df (GeoDataFrame): the geology data related to the watershed context
        owner_data_df (GeoDataFrame): the ownership data related to the watershed context
        ecoregion_data_df (GeoDataFrame): the ecoregion data related to the watershed context

    Returns:
        dictionary of stats (pint Quantities) -- selected items (known to be Pint quantities) from the supplied aggregate_data_df plus some derived ones

    """
    layer_id = 'aggregate_stats'  # For metadata namespacing
    meta = RSFieldMeta()
    # everything in the aggregate dataframe
    # remember the dataframe comes from athena, and all columns are lowercase
    aggregate_data_stats = aggregate_data_df.iloc[0].to_dict()
    # some don't have units, so error if try to create a card for it. So just pick ones we want
    colnames_of_stats_we_want = [
        'sum_flowlinelengthperennialkm',
        'sum_flowlinelengthintermittentkm',
        'sum_flowlinelengthephemeralkm',
        'sum_flowlinelengthallkm',
        'sum_flowlinefeaturecount',
        'sum_hucareasqkm',
        'sum_precipcount',
        'sum_precipsum',
        'min_precipminimum',
        'max_precipmaximum',
        'sum_catchmentlength',
        'sum_slopecount',
        'sum_slopesum',
        'max_slopemaximum',
        'min_slopeminimum',
        'sum_demcount',
        'sum_demsum',
        'min_demminimum',
        'max_demmaximum',
        'countdistinct_huc',
        'min_circularityratio',
        'min_elongationratio',
        'min_formfactor',
    ]
    rpt_stats = {colname: aggregate_data_stats[colname] for colname in colnames_of_stats_we_want}
    # average segment length
    avg_segment_length = rpt_stats['sum_flowlinelengthallkm'] / rpt_stats['sum_flowlinefeaturecount']
    meta.add_field_meta(name='avg_segment_length', friendly_name='Average Segment Length', layer_id=layer_id, data_unit=avg_segment_length.units, preferred_format="{:.3g}")
    meta.add_field_meta(
        name='sum_hucareasqkm',
        friendly_name='Total HUC Area',
        description='Total area of the HUC10s that intersect the selected area',
        layer_id=layer_id,
        data_unit=rpt_stats['sum_hucareasqkm'].units,
    )
    meta.add_field_meta(name='countdistinct_huc', friendly_name='Number of HUC10s', description='Count of distinct HUC10s that intersect the selected area', layer_id=layer_id, data_unit=rpt_stats['countdistinct_huc'].units)
    meta.add_field_meta(
        name='sum_flowlinelengthallkm',
        friendly_name='Total Stream Length',
        description='Total length of all streams within the selected area',
        layer_id=layer_id,
        data_unit=rpt_stats['sum_flowlinelengthallkm'].units,
    )
    mean_precip_cell_value = rpt_stats['sum_precipsum'] / rpt_stats['sum_precipcount']
    meta.add_field_meta(
        name='mean_precip_cell_value',
        friendly_name='Mean Annual Precipitation',
        description='Mean of the 30-year Average Annual Precipitation across the selected area',
        layer_id=layer_id,
        data_unit=mean_precip_cell_value.units,
    )
    meta.add_field_meta(
        name="min_precipminimum",
        friendly_name="Minimum Annual Precipitation",
        description="Minimum of the 30-year Average Annual Precipitation across the selected area",
        layer_id=layer_id,
        data_unit=rpt_stats['min_precipminimum'].units,
    )
    meta.add_field_meta(
        name="max_precipmaximum",
        friendly_name="Maximum Annual Precipitation",
        description="Maximum of the 30-year Average Annual Precipitation across the selected area",
        layer_id=layer_id,
        data_unit=rpt_stats['max_precipmaximum'].units,
    )
    mean_elevation = rpt_stats['sum_demsum'] / rpt_stats['sum_demcount']
    meta.add_field_meta(name='mean_elevation', friendly_name='Mean Elevation', description='Mean elevation across the selected area', layer_id=layer_id, data_unit=mean_elevation.units)
    mean_slope = rpt_stats['sum_slopesum'] / rpt_stats['sum_slopecount']
    meta.add_field_meta(name='mean_slope', friendly_name='Mean Slope', description='Mean slope across the selected area', layer_id=layer_id, data_unit=mean_slope.units)
    total_relief = rpt_stats['max_demmaximum'] - rpt_stats['min_demminimum']
    source_meta = meta.get_field_meta('max_demmaximum')
    meta.add_field_meta(
        name='total_relief',
        friendly_name='Total Relief',
        description='Difference between highest and lowest elevation.',
        data_unit=rpt_stats['max_demmaximum'].units,
        layer_id=layer_id,
        preferred_format=source_meta.preferred_format if source_meta else None,
    )
    relief_ratio = total_relief.to("km") / rpt_stats['sum_catchmentlength'].to("km")
    meta.set_preferred_format('reliefratio', '{:.2f}', layer_id='rs_context_huc10')  # already defined in rs_context_huc10; just ensure format is set
    meta.add_field_meta(
        name='min_demminimum',
        friendly_name='Minimum Elevation',
        description='Minimum elevation across the selected area',
        layer_id=layer_id,
        data_unit=rpt_stats['min_demminimum'].units,
    )
    meta.add_field_meta(
        name='max_demmaximum',
        friendly_name='Maximum Elevation',
        description='Maximum elevation across the selected area',
        layer_id=layer_id,
        data_unit=rpt_stats['max_demmaximum'].units,
    )
    meta.add_field_meta(
        name="min_slopeminimum",
        friendly_name="Minimum Slope",
        description="Minimum slope across the selected area",
        layer_id=layer_id,
        data_unit=rpt_stats['min_slopeminimum'].units,
    )
    meta.add_field_meta(
        name="max_slopemaximum",
        friendly_name="Maximum Slope",
        description="Maximum slope across the selected area",
        layer_id=layer_id,
        data_unit=rpt_stats['max_slopemaximum'].units,
    )

    # drainage densities are total flowline length divided by total catchment area.
    # these are found as metrics in individual hucs but we re-calculate for aggregates.
    if meta.unit_system == 'imperial':
        dd_denominator = 'mi ** 2'
    else:
        dd_denominator = 'km ** 2'
    drainage_density_perennial = rpt_stats['sum_flowlinelengthperennialkm'] / rpt_stats['sum_hucareasqkm'].to(dd_denominator)
    drainage_density_non_perennial = (rpt_stats['sum_flowlinelengthintermittentkm'] + rpt_stats['sum_flowlinelengthephemeralkm']) / rpt_stats['sum_hucareasqkm'].to(dd_denominator)
    drainage_density_all = rpt_stats['sum_flowlinelengthallkm'] / rpt_stats['sum_hucareasqkm'].to(dd_denominator)
    meta.add_field_meta(
        name='drainage_density_non_perennial',
        friendly_name='Drainage Density - Non Perrenial',
        description='Total length of Intermittent and Ephemeral Streams, divided by Catchment Area',
        layer_id=layer_id,
        data_unit=drainage_density_non_perennial.units,
        preferred_format='{:.2f}',
    )
    meta.add_field_meta(
        name='drainage_density_perennial',
        friendly_name='Drainage Density - Perrenial',
        description='Total length of Perennial Streams, divided by Catchment Area',
        layer_id=layer_id,
        data_unit=drainage_density_perennial.units,
        preferred_format='{:.2f}',
    )
    meta.add_field_meta(
        name='drainage_density_all',
        friendly_name='Drainage Density - Entire Network',
        description='Total length of All Streams, divided by Catchment Area',
        layer_id=layer_id,
        data_unit=drainage_density_all.units,
        preferred_format='{:.2f}',
    )

    if rpt_stats['countdistinct_huc'] == 1:
        singlehucstats = {"circularityratio": rpt_stats['min_circularityratio'], "elongationratio": rpt_stats['min_elongationratio'], "formfactor": rpt_stats['min_formfactor']}
        # define them as 2 decimal floats - TODO this should be in the layerdef.json
        meta.set_preferred_format('circularityratio', '{:.2f}', layer_id='rs_context_huc10')
        meta.set_preferred_format('elongationratio', '{:.2f}', layer_id='rs_context_huc10')
        meta.set_preferred_format('formfactor', '{:.2f}', layer_id='rs_context_huc10')

    else:
        singlehucstats = {}

    utm_crs = geo_data_df.estimate_utm_crs()
    gdf_proj = geo_data_df.to_crs(utm_crs)
    aoi_area_unit = 'km ** 2' if meta.unit_system == 'SI' else 'acre'
    aoi_area = pint.Quantity(gdf_proj['geometry'].area.sum(), 'm ** 2').to(aoi_area_unit)
    meta.add_field_meta(name='aoi_area', friendly_name='AOI Area', layer_id=layer_id, data_unit=aoi_area_unit, preferred_format='{:.2f}')

    geol_table = geology_table(geo_data_df)
    geol_table.sort_values(by='area', ascending=False, inplace=True)
    geol_unit = geol_table.iloc[0]['unit_name']
    geol_type = geol_table.iloc[0]['primary_rock_type']
    geol_area = geol_table.iloc[0]['area']
    geol_frac = geol_area / geol_table['area'].sum()
    meta.add_field_meta(name='geol_frac', friendly_name='Dominant Geology Fraction', layer_id=layer_id, data_unit='unitless', preferred_format='{:.2%}')

    owner_table = ownership_table(owner_data_df)
    owner_table.sort_values(by='area', ascending=False, inplace=True)
    owner = owner_table.iloc[0]['ownership_desc']
    owner_area = owner_table.iloc[0]['area']
    owner_frac = owner_area / owner_table['area'].sum()
    meta.add_field_meta(name='owner_frac', friendly_name='Dominant Ownership Fraction', layer_id=layer_id, data_unit='unitless', preferred_format='{:.2%}')
    meta.add_field_meta(name='owner_area', friendly_name='Dominant Ownership Area', layer_id=layer_id, data_unit='hectare' if meta.unit_system == 'SI' else 'acre', preferred_format='{:.2f}')

    ecor_table = ecoregion_table(ecoregion_data_df)
    ecor_table.sort_values(by='area', ascending=False, inplace=True)
    ecoregion = ', '.join(ecor_table['ecoregion_iv'].astype(str))
    primary_ecoregion = ecor_table.iloc[0]['ecoregion_iv']
    ecoregion_area = ecor_table.iloc[0]['area']
    ecoregion_frac = ecoregion_area / ecor_table['area'].sum()
    number_ecorgions = len(ecor_table)
    meta.add_field_meta(name='ecoregion_frac', friendly_name='Dominant Ecoregion Fraction', layer_id=layer_id, data_unit='unitless', preferred_format='{:.2%}')
    meta.add_field_meta(name='ecoregion_area', friendly_name='Dominant Ecoregion Area', layer_id=layer_id, data_unit='hectare' if meta.unit_system == 'SI' else 'acre', preferred_format='{:.2f}')

    lc_table = land_cover_table(hucs_df)
    lc_table.sort_values(by='cell_count', ascending=False, inplace=True)
    dominant_veg_types = ', '.join(lc_table.iloc[0:3]['vegetation_type'].astype(str))
    dominant_veg = lc_table.iloc[0]['vegetation_type']
    dominant_veg_area = lc_table.iloc[0]['cell_count']
    dominant_veg_frac = dominant_veg_area / lc_table['cell_count'].sum()
    meta.add_field_meta(name='dominant_veg_frac', friendly_name='Dominant Vegetation Fraction', layer_id=layer_id, data_unit='unitless', preferred_format='{:.2%}')
    # meta.add_field_meta(name='dominant_veg_area', friendly_name='Dominant Vegetation Area', layer_id=layer_id, data_unit='hectare' if meta.unit_system == 'SI' else 'acre', preferred_format='{:.2f}')

    stats = {
        **rpt_stats,
        **singlehucstats,
        'aoi_area': aoi_area,
        'avg_segment_length': avg_segment_length,
        'mean_precip_cell_value': mean_precip_cell_value,
        'mean_elevation': mean_elevation,
        'mean_slope': mean_slope,
        'total_relief': total_relief,
        'reliefratio': relief_ratio,
        'drainage_density_all': drainage_density_all,
        'drainage_density_perennial': drainage_density_perennial,
        'drainage_density_non_perennial': drainage_density_non_perennial,
        'geol_unit': geol_unit,
        'geol_type': geol_type,
        'geol_area': geol_area,
        'geol_frac': geol_frac,
        'owner': owner,
        'owner_area': owner_area,
        'owner_frac': owner_frac,
        'ecoregion': ecoregion,
        'primary_ecoregion': primary_ecoregion,
        'ecoregion_area': ecoregion_area,
        'ecoregion_frac': ecoregion_frac,
        'number_ecoregions': number_ecorgions,
        'dominant_veg_types': dominant_veg_types,
        'dominant_veg': dominant_veg,
        'dominant_veg_area': dominant_veg_area,
        'dominant_veg_frac': dominant_veg_frac,
    }

    return stats


def create_hydrography_summary_table(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame | None, str | None]:
    """Create a hydrography summary table.
    Returns the main table, a footer dataframe for totals, and an optional footnote if totals diverge.
    """
    log = Logger('create hydro summary')
    layer_id = 'hydrography_summary'  # For metadata namespacing
    row_data = df.iloc[0]

    # Pull the source metrics once so we can build grouped rows without re-querying.
    length_values = {label: row_data[col.lower()] for label, col in hydrography_col_map.items()}
    footnote = None

    non_perennial = length_values['Intermittent'] + length_values['Ephemeral']
    total_stream_length = length_values['Total']
    total_itemized_stream_length = length_values['Perennial'] + length_values['Intermittent'] + length_values['Ephemeral'] + length_values['Canals']
    # these should both be Pint Quantities
    length_delta = abs(total_stream_length - total_itemized_stream_length)
    if isinstance(length_delta, pint.Quantity):
        tolerance = 0.01 * length_delta.units
        exceeds_tolerance = length_delta > tolerance
    else:
        exceeds_tolerance = length_delta > 0.01
    if exceeds_tolerance:
        log.debug(f"Totals off, likely due to presence of other FCodes not itemized. Total: {total_stream_length}. Itemized total: {total_itemized_stream_length}. Adding footnote.")
        readable_delta = f"{length_delta:.2f~P}" if isinstance(length_delta, pint.Quantity) else f"{length_delta:.2f}"
        footnote = f"* Itemized categories under-count the total network (difference {readable_delta}). Additional stream types (FCodes) contribute to the total."
    total_without_canals = total_stream_length - length_values['Canals']

    summary_rows = [
        {"flowline_length_category": "Perennial", "stream_network_distance": length_values['Perennial']},
        {"flowline_length_category": "Non-Perennial", "stream_network_distance": non_perennial},
        {"flowline_length_category": "Canals", "stream_network_distance": length_values['Canals']},
        {"flowline_length_category": "Total Stream Length", "stream_network_distance": total_stream_length},
        {"flowline_length_category": "Total Stream Length (w.o. Canals)", "stream_network_distance": total_without_canals},
    ]

    report_df = RSGeoDataFrame(pd.DataFrame(summary_rows))

    meta = RSFieldMeta()
    length_unit = meta.get_field_unit(hydrography_col_map['Perennial'].lower())
    meta.add_field_meta(name='stream_network_distance', friendly_name='Stream Network Distance', layer_id=layer_id, data_unit=length_unit)
    meta.add_field_meta(name='flowline_length_category', friendly_name='Stream Type', layer_id=layer_id, data_unit="NA")

    report_df['stream_network_distance'] = ensure_pint_column(report_df, 'stream_network_distance', length_unit)

    total_row_value = report_df.loc[report_df['flowline_length_category'] == 'Total Stream Length', 'stream_network_distance'].iloc[0]

    if getattr(total_row_value, 'magnitude', total_row_value) == 0:
        percent_series = pd.Series([0] * len(report_df), index=report_df.index).astype('pint[percent]')
    else:
        percent_series = (report_df['stream_network_distance'] / pd.Series([total_row_value] * len(report_df), index=report_df.index)).fillna(0).astype('pint[percent]')

    report_df['% of Total Stream Length'] = percent_series
    meta.add_field_meta(name='% of Total Stream Length', friendly_name='% of Total Stream Length', layer_id=layer_id, data_unit='percent')

    footer_mask = report_df['flowline_length_category'].isin({'Total Stream Length', 'Total Stream Length (w.o. Canals)'})
    footer_df = report_df.loc[footer_mask].copy()
    body_df = report_df.loc[~footer_mask].copy()

    if footer_df.empty:
        footer_df = None

    return body_df.reset_index(drop=True), (None if footer_df is None else footer_df.reset_index(drop=True)), footnote


def hydrography_table(df: pd.DataFrame) -> str:
    """make html table for hydrography, appending totals via footer and footnote when needed"""
    layer_id = 'hydrography_summary'
    body_df, footer_df, footnote = create_hydrography_summary_table(df)
    meta = RSFieldMeta()
    body_rdf = RSGeoDataFrame(body_df)
    body_rdf, _ = meta.apply_units(body_rdf, layer_id=layer_id)

    if footer_df is not None:
        footer_rdf = RSGeoDataFrame(footer_df)
        footer_rdf, _ = meta.apply_units(footer_rdf, layer_id=layer_id)
        body_rdf.set_footer(footer_rdf)

    table_html = render_table(body_rdf, footer=body_rdf._footer, layer_id=layer_id)
    if not footnote:
        return table_html

    footnote_html = f"<div class=\"table-footnote\"><small>{html.escape(footnote)}</small></div>"
    return f"{table_html}\n{footnote_html}"


def waterbody_summary_table(df: pd.DataFrame) -> str:
    """make html table for waterbodies with totals rendered via footer"""
    layer_id = 'waterbodies_summary'
    body_df, footer_df = create_waterbody_summary_table(df)
    meta = RSFieldMeta()
    body_rdf = RSGeoDataFrame(body_df)
    body_rdf, _ = meta.apply_units(body_rdf, layer_id=layer_id)

    if footer_df is not None:
        footer_rdf = RSGeoDataFrame(footer_df)
        footer_rdf, _ = meta.apply_units(footer_rdf, layer_id=layer_id)
        body_rdf.set_footer(footer_rdf)

    return render_table(body_rdf, footer=body_rdf._footer, layer_id=layer_id)


def hypsometry_data(huc_df: pd.DataFrame, bin_size: int = 100) -> pd.DataFrame:
    """
    Aggregate dem_bins from all rows, summing cell_count for each bin.
    Returns a DataFrame with columns: bin, total_cell_count.
    Fills missing bins (using bin_size) with zeros, sorted descending by bin.
    """
    log = Logger('hypsometry_data')
    log.info(f"Processing hypsometry data with bin size {bin_size}")
    if 'dem_bins' not in huc_df.columns:
        log.warning("No 'dem_bins' column found in DataFrame.")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=['bin', 'total_cell_count'])

    combined_bins = defaultdict(int)
    for dem_bin_dict in huc_df['dem_bins']:
        for b in dem_bin_dict.get('bins', []):
            combined_bins[b['bin']] += b['cell_count']

    if not combined_bins:
        return pd.DataFrame(columns=['bin', 'total_cell_count'])

    min_bin = min(combined_bins)
    max_bin = max(combined_bins)
    all_bins = list(range(min_bin, max_bin + bin_size, bin_size))

    filled_bins = {
        'bin': all_bins,
        'total_cell_count': [combined_bins.get(b, 0) for b in all_bins],
    }

    result_df = pd.DataFrame(filled_bins)
    result_df = result_df.sort_values('bin', ascending=True).reset_index(drop=True)
    return result_df


def hypsometry_fig(huc_df: pd.DataFrame) -> go.Figure:
    """
    Plot hypsometry as a bar chart: total_cell_count vs. bin.
    """
    df = hypsometry_data(huc_df)
    tot_cells = df['total_cell_count'].sum()
    print('HYPSOMETRY DATA')
    print(df)  # debug only

    fig = go.Figure(
        go.Bar(
            x=df['total_cell_count'] / tot_cells,
            y=df['bin'],
            orientation='h',
            marker_color='steelblue',
        )
    )
    fig.update_layout(
        title="Hypsometry",
        xaxis_title="Fraction of Total Area",
        yaxis_title="Elevation (m)",
        template="plotly_white",
    )
    return fig


def geology_table(geology_df: gpd.GeoDataFrame) -> pd.DataFrame:
    """make data frame for displaying geology information in a table"""
    layer_id = 'geology_summary'  # For metadata namespacing

    dissolved_gdf = geology_df.dissolve(by='unit_name', aggfunc='first')
    dissolved_gdf = dissolved_gdf.to_crs(geology_df.estimate_utm_crs())

    geo_df = RSGeoDataFrame(pd.DataFrame({'unit_name': dissolved_gdf.index, 'primary_rock_type': dissolved_gdf['rock_type'], 'area': dissolved_gdf.geometry.area}))

    # geometry.area (after projecting to a UTM CRS) is in square meters
    meta = RSFieldMeta()
    meta.add_field_meta(name='area', layer_id=layer_id, friendly_name='Area', data_unit='m**2', display_unit='kilometer ** 2')
    geo_df['area'] = ensure_pint_column(geo_df, 'area', 'm**2')

    return geo_df


def geology_summary_table(geology_df: gpd.GeoDataFrame) -> str:
    """make html table for geology summary"""
    summary_df = geology_table(geology_df)
    summary_df.sort_values('area', ascending=False, inplace=True)
    total_area = summary_df['area'].sum()
    if getattr(total_area, 'magnitude', total_area) == 0:
        percent_series = pd.Series([0] * len(summary_df), index=summary_df.index).astype('pint[percent]')
    else:
        percent_series = (summary_df['area'] / pd.Series([total_area] * len(summary_df), index=summary_df.index)).fillna(0).astype('pint[percent]')
    summary_df['% of Total Area'] = percent_series
    RSFieldMeta().add_field_meta(name='% of Total Area', friendly_name='% of Total Area', layer_id='geology_summary', data_unit='percent')
    return render_table(summary_df, layer_id='geology_summary')


def ecoregion_table(ecoregion_df: gpd.GeoDataFrame) -> pd.DataFrame:
    """make data frame for displaying ecoregion information in a table"""
    layer_id = 'ecoregion_summary'  # For metadata namespacing

    dissolved_gdf = ecoregion_df.dissolve(by='ecoregion_iv', aggfunc='first')
    dissolved_gdf = dissolved_gdf.to_crs(ecoregion_df.estimate_utm_crs())

    ecor_df = RSGeoDataFrame(pd.DataFrame({'ecoregion_iv': dissolved_gdf.index, 'area': dissolved_gdf.geometry.area}))

    meta = RSFieldMeta()
    meta.add_field_meta(name='area', layer_id=layer_id, friendly_name='Area', data_unit='m**2', display_unit='kilometer ** 2')
    meta.add_field_meta(name='ecoregion_iv', layer_id=layer_id, friendly_name='EPA Level IV Ecoregion', data_unit=None)
    ecor_df['area'] = ensure_pint_column(ecor_df, 'area', 'm**2')

    return ecor_df


def ecoregion_summary_table(ecoregion_df: gpd.GeoDataFrame) -> str:
    """make html table for ecoregion summary"""
    summary_df = ecoregion_table(ecoregion_df)
    summary_df.sort_values('area', ascending=False, inplace=True)
    total_area = summary_df['area'].sum()
    if getattr(total_area, 'magnitude', total_area) == 0:
        percent_series = pd.Series([0] * len(summary_df), index=summary_df.index).astype('pint[percent]')
    else:
        percent_series = (summary_df['area'] / pd.Series([total_area] * len(summary_df), index=summary_df.index)).fillna(0).astype('pint[percent]')
    summary_df['% of Total Area'] = percent_series
    RSFieldMeta().add_field_meta(name='% of Total Area', friendly_name='% of Total Area', layer_id='ecoregion_summary', data_unit='percent')
    return render_table(summary_df, layer_id='ecoregion_summary')


def ownership_table(ownership_df: pd.DataFrame) -> pd.DataFrame:
    """make data frame for displaying ownership information in a table"""
    layer_id = 'ownership_summary'  # For metadata namespacing

    dissolved_gdf = ownership_df.dissolve(by='ownership_desc', aggfunc='first')
    dissolved_gdf = dissolved_gdf.to_crs(ownership_df.estimate_utm_crs())

    meta = RSFieldMeta()
    meta.add_field_meta(name='area', layer_id=layer_id, friendly_name='Area', data_unit='m**2', display_unit='kilometer ** 2')
    meta.add_field_meta(name='ownership_desc', layer_id=layer_id, friendly_name='Ownership', data_unit=None)
    owner_df = RSGeoDataFrame(pd.DataFrame({'ownership_desc': dissolved_gdf.index, 'area': dissolved_gdf.geometry.area}))
    owner_df['area'] = ensure_pint_column(owner_df, 'area', 'm**2')

    return owner_df


def ownership_summary_table(ownership_df: pd.DataFrame) -> str:
    """make html table for ownership summary"""
    summary_df = ownership_table(ownership_df)
    summary_df.sort_values('area', ascending=False, inplace=True)
    total_area = summary_df['area'].sum()
    if getattr(total_area, 'magnitude', total_area) == 0:
        percent_series = pd.Series([0] * len(summary_df), index=summary_df.index).astype('pint[percent]')
    else:
        percent_series = (summary_df['area'] / pd.Series([total_area] * len(summary_df), index=summary_df.index)).fillna(0).astype('pint[percent]')
    summary_df['% of Total Area'] = percent_series
    RSFieldMeta().add_field_meta(name='% of Total Area', friendly_name='% of Total Area', layer_id='ownership_summary', data_unit='percent')
    return render_table(summary_df, layer_id='ownership_summary')


def land_cover_table(hucs_df: pd.DataFrame) -> pd.DataFrame:
    """make data frame for displaying land cover information in a table"""
    layer_id = 'landcover_summary'

    evt_df = pd.read_csv('https://raw.githubusercontent.com/Riverscapes/riverscapes-tools/refs/heads/master/packages/rcat/database/data/VegetationTypes.csv')
    lookup = {row['VegetationID']: row['Physiognomy'] for _, row in evt_df.iterrows()}

    aggregated_data = {}
    for i in range(len(hucs_df['existing_veg_bins'])):
        for x in hucs_df['existing_veg_bins'][i]['bins']:
            if x['category'] not in aggregated_data:
                aggregated_data[x['category']] = 0
            aggregated_data[x['category']] += x['cell_count']

    transformed_data = {lookup[int(k)]: v for k, v in aggregated_data.items()}

    meta = RSFieldMeta()
    meta.add_field_meta(name='vegetation_type', layer_id=layer_id, friendly_name='Vegetation Type')

    land_cover_df = RSGeoDataFrame(pd.DataFrame(list(transformed_data.items()), columns=['vegetation_type', 'cell_count']))
    return land_cover_df


def landcover_summary_table(hucs_df: pd.DataFrame) -> str:
    """make html table for land cover summary"""
    summary_df = land_cover_table(hucs_df)
    summary_df.sort_values('cell_count', ascending=False, inplace=True)
    total_cells = summary_df['cell_count'].sum()
    if total_cells == 0:
        percent_series = pd.Series([0] * len(summary_df), index=summary_df.index).astype('pint[percent]')
    else:
        percent_series = (summary_df['cell_count'] / pd.Series([total_cells] * len(summary_df), index=summary_df.index) * 100).fillna(0).astype('pint[percent]')
    summary_df['% of Total Area'] = percent_series
    RSFieldMeta().add_field_meta(name='% of Total Area', friendly_name='% of Total Area', layer_id='landcover_summary', data_unit='percent')
    return render_table(summary_df, layer_id='landcover_summary')


def watershed_area_by_category(gdf: pd.DataFrame, category: str) -> go.Figure:
    """Create pie chart of total segment area by owner"""
    if category not in ('ownership', 'ecoregion', 'landcover'):
        raise ValueError(f"Invalid category: {category}. Must be one of 'ownership', 'ecoregion', 'landcover'.")

    if category == 'ownership':
        chart_data = ownership_table(gdf)
        layer_id = 'ownership_summary'
        field = 'ownership_desc'
    elif category == 'ecoregion':
        chart_data = ecoregion_table(gdf)
        layer_id = 'ecoregion_summary'
        field = 'ecoregion_iv'
    elif category == 'landcover':
        chart_data = land_cover_table(gdf)
        layer_id = 'landcover_summary'
        field = 'vegetation_type'

    meta = RSFieldMeta()
    baked_header_lookup = meta.get_headers_dict(chart_data, layer_id=layer_id)
    baked_chart_data, baked_headers = meta.bake_units(chart_data, layer_id=layer_id)

    total_name = meta.get_friendly_name('area', layer_id=layer_id)
    group_name = baked_header_lookup.get(field, meta.get_friendly_name(field, layer_id=layer_id))
    title = f"Total {total_name} by {group_name}"

    fig = px.pie(
        baked_chart_data,
        names=field,
        values="area" if "area" in baked_chart_data.columns else "cell_count",
        color=field,
        labels=baked_header_lookup,  # legend/axis labels use your nice names
        title=title,
        # color_discrete_map=DEFAULT_OWNER_COLOR_MAP,
    )

    # Keep percent on slices; tooltip shows ONLY absolute with thousands commas
    fig.update_traces(
        textinfo="percent",
        hovertemplate=f"<b>{baked_header_lookup.get('area', 'area')} for {baked_header_lookup.get(field, 'Category')} = %{{label}}</b>:<br>%{{value:,.0f}}<extra></extra>",
        # Use :,.1f or :,.2f if you want decimals.
    )

    # Prevent legend/hover name truncation
    fig.update_layout(hoverlabel=dict(namelength=-1))

    return fig
