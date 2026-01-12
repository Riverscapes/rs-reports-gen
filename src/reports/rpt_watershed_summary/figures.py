import html

import pandas as pd
import pint
import pint_pandas
from rsxml import Logger
from util.pandas import RSGeoDataFrame, RSFieldMeta


# 1. Define a mapping of "Row Label" -> (Area Column, Count Column)
# should all be in the same units!
waterbody_col_map = {
    "Lake":        ("sum_waterbodyLakesPondsAreaSqKm", "sum_waterbodyLakesPondsFeatureCount"),
    "Reservoir":   ("sum_waterbodyReservoirAreaSqKm",  "sum_waterbodyReservoirFeatureCount"),
    "Estuaries":   ("sum_waterbodyEstuariesAreaSqKm",  "sum_waterbodyEstuariesFeatureCount"),
    "Playa":       ("sum_waterbodyPlayaAreaSqKm",      "sum_waterbodyPlayaFeatureCount"),
    "Swamp Marsh": ("sum_waterbodySwampMarshAreaSqKm", "sum_waterbodySwampMarshFeatureCount"),
    "Ice/Snow":    ("sum_waterbodyIceSnowAreaSqKm",    "sum_waterbodyIceSnowFeatureCount"),
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
    table_name = 'waterbodies_summary'  # For metadata namespacing
    # Ensure we are working with the first row of data if df has multiple
    row_data = df.iloc[0]

    # 2. Extract data into a list of dicts (Pivoting)
    summary_rows = []
    for label, (area_col, count_col) in waterbody_col_map.items():
        summary_rows.append({
            "Waterbodies": label,
            "Area": row_data[area_col.lower()],
            "Count": row_data[count_col.lower()]
        })

    # Create the new DataFrame
    report_df = RSGeoDataFrame(pd.DataFrame(summary_rows))

    meta = RSFieldMeta()
    # Get the units from the source data and apply it to the new dataframe
    area_unit = meta.get_field_unit(waterbody_col_map["Lake"][0].lower())
    count_unit = meta.get_field_unit(waterbody_col_map["Lake"][1].lower())
    meta.add_field_meta(name='Area', table_name=table_name, data_unit=area_unit)
    meta.add_field_meta(name='Count', table_name=table_name, data_unit=count_unit)

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
    meta.add_field_meta(name="Avg. Area", table_name=table_name, data_unit=area_unit)
    meta.add_field_meta(name="% Area", table_name=table_name, data_unit='percent')
    meta.add_field_meta(name="% Count", table_name=table_name, data_unit='percent')

    # 5. Formatting (Optional: Create the "Total" row)
    total_row = pd.DataFrame([{
        "Waterbodies": "Total Waterbodies",
        "Area": total_area,
        "Count": total_count,
        "% Area": 100.0,
        "% Count": 100.0,
        "Avg. Area": (total_area / total_count) if total_count > 0 else 0,
    }])
    # Also convert the total row percentages to pint quantities
    total_row['% Area'] = total_row['% Area'].astype('pint[percent]')
    total_row['% Count'] = total_row['% Count'].astype('pint[percent]')

    return report_df.reset_index(drop=True), total_row.reset_index(drop=True)


hydrography_col_map = {
    'Perennial': ('sum_flowlineLengthPerennialKm'),
    'Intermittent': ('sum_flowlineLengthIntermittentKm'),
    'Ephemeral': ('sum_flowlineLengthEphemeralKm'),
    'Canals': ('sum_flowlineLengthCanalsKm'),
    'Total': ('sum_flowlineLengthAllKm')
}


def statistics(aggregate_data_df: pd.DataFrame) -> dict[str, pint.Quantity]:
    """
    named, non-tabular statistics. 

    Args: 
        df (DataFrame): the result of the aggregate query (should have just one row)

    Returns:
        dictionary of stats (pint Quantities) -- selected items (known to be Pint quantities) from the supplied aggregate_data_df plus some derived ones

    """
    table_name = 'aggregate_stats'  # For metadata namespacing
    meta = RSFieldMeta()
    # everything in the aggregate dataframe
    # remember the dataframe comes from athena, and all columns are lowercase
    aggregate_data_stats = aggregate_data_df.iloc[0].to_dict()
    # some don't have units, so error if try to create a card for it. So just pick ones we want
    colnames_of_stats_we_want = ['sum_flowlinelengthallkm',
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
    stats_we_want = {
        colname: aggregate_data_stats[colname] for colname in colnames_of_stats_we_want}
    # average segment length
    avg_segment_length = stats_we_want['sum_flowlinelengthallkm']/stats_we_want['sum_flowlinefeaturecount']
    meta.add_field_meta(
        name='avg_segment_length',
        friendly_name='Average Segment Length',
        table_name=table_name,
        data_unit=avg_segment_length.units,
        preferred_format="{:.3g}"
    )
    mean_precip_cell_value = stats_we_want['sum_precipsum'] / stats_we_want['sum_precipcount']
    meta.add_field_meta(
        name='mean_precip_cell_value',
        friendly_name='Mean Average Precipitation',
        description='Mean of the 30-year Average Annual Precipitation across the selected area',
        table_name=table_name
    )
    mean_elevation = stats_we_want['sum_demsum'] / stats_we_want['sum_demcount']
    meta.add_field_meta(
        name='mean_elevation',
        friendly_name='Mean Elevation',
        description='Mean elevation across the selected area',
        table_name=table_name
    )
    mean_slope = stats_we_want['sum_slopesum'] / stats_we_want['sum_slopecount']
    meta.add_field_meta(
        name='mean_slope',
        friendly_name='Mean Slope',
        description='Mean elevation across the selected area',
        table_name=table_name
    )
    total_relief = stats_we_want['max_demmaximum'] - stats_we_want['min_demminimum']
    source_meta = meta.get_field_meta('max_demmaximum')
    meta.add_field_meta(
        name='total_relief',
        friendly_name='Total Relief',
        description='Difference between highest and lowest elevation.',
        data_unit=stats_we_want['max_demmaximum'].units,
        table_name=table_name,
        preferred_format=source_meta.preferred_format if source_meta else None
    )
    relief_ratio = total_relief.to("km") / stats_we_want['sum_catchmentlength'].to("km")
    meta.set_preferred_format('reliefratio', '{:.2f}', table_name='rs_context_huc10')
    if stats_we_want['countdistinct_huc'] == 1:
        singlehucstats = {
            "circularityratio": stats_we_want['min_circularityratio'],
            "elongationratio": stats_we_want['min_elongationratio'],
            "formfactor": stats_we_want['min_formfactor']
        }
        # define them as 2 decimal floats - TODO this should be in the layerdef.json
        meta.set_preferred_format('circularityratio', '{:.2f}', table_name='rs_context_huc10')
        meta.set_preferred_format('elongationratio', '{:.2f}', table_name='rs_context_huc10')
        meta.set_preferred_format('formfactor', '{:.2f}', table_name='rs_context_huc10')

    else:
        singlehucstats = {}
    stats = {
        **stats_we_want,
        **singlehucstats,
        'avg_segment_length': avg_segment_length,
        'mean_precip_cell_value': mean_precip_cell_value,
        'mean_elevation': mean_elevation,
        'mean_slope': mean_slope,
        'total_relief': total_relief,
        'reliefratio': relief_ratio,
    }

    return stats


def create_hydrography_summary_table(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame | None, str | None]:
    """Create a hydrography summary table.
    Returns the main table, a footer dataframe for totals, and an optional footnote if totals diverge.
    """
    log = Logger('create hydro summary')
    table_name = 'hydrography_summary'  # For metadata namespacing
    row_data = df.iloc[0]

    # Pull the source metrics once so we can build grouped rows without re-querying.
    length_values = {label: row_data[col.lower()] for label, col in hydrography_col_map.items()}
    footnote = None

    non_perennial = length_values['Intermittent'] + length_values['Ephemeral']
    total_stream_length = length_values['Total']
    total_itemized_stream_length = (length_values['Perennial'] +
                                    length_values['Intermittent'] +
                                    length_values['Ephemeral'] +
                                    length_values['Canals'])
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
        footnote = ("* Itemized categories under-count the total network (difference "
                    f"{readable_delta}). Additional stream types (FCodes) contribute to the total.")
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
    meta.add_field_meta(name='stream_network_distance',
                        friendly_name='Stream Network Distance',
                        table_name=table_name,
                        data_unit=length_unit)
    meta.add_field_meta(name='flowline_length_category',
                        friendly_name='Stream Type',
                        table_name=table_name,
                        data_unit="NA")

    report_df['stream_network_distance'] = ensure_pint_column(report_df,
                                                              'stream_network_distance',
                                                              length_unit)

    total_row_value = report_df.loc[
        report_df['flowline_length_category'] == 'Total Stream Length', 'stream_network_distance'
    ].iloc[0]

    if getattr(total_row_value, 'magnitude', total_row_value) == 0:
        percent_series = pd.Series([0] * len(report_df), index=report_df.index).astype('pint[percent]')
    else:
        percent_series = (
            report_df['stream_network_distance'] /
            pd.Series([total_row_value] * len(report_df), index=report_df.index)
        ).fillna(0).astype('pint[percent]')

    report_df['% of Total Stream Length'] = percent_series
    meta.add_field_meta(name='% of Total Stream Length',
                        friendly_name='% of Total Stream Length',
                        table_name=table_name,
                        data_unit='percent')

    footer_mask = report_df['flowline_length_category'].isin({
        'Total Stream Length', 'Total Stream Length (w.o. Canals)'
    })
    footer_df = report_df.loc[footer_mask].copy()
    body_df = report_df.loc[~footer_mask].copy()

    if footer_df.empty:
        footer_df = None

    return body_df.reset_index(drop=True), (
        None if footer_df is None else footer_df.reset_index(drop=True)
    ), footnote


def hydrography_table(df: pd.DataFrame) -> str:
    """make html table for hydrography, appending totals via footer and footnote when needed"""
    body_df, footer_df, footnote = create_hydrography_summary_table(df)
    meta = RSFieldMeta()
    body_rdf = RSGeoDataFrame(body_df)
    body_rdf, _ = meta.apply_units(body_rdf)

    if footer_df is not None:
        footer_rdf = RSGeoDataFrame(footer_df)
        footer_rdf, _ = meta.apply_units(footer_rdf)
        body_rdf.set_footer(footer_rdf)

    table_html = body_rdf.to_html(index=False, escape=False)
    if not footnote:
        return table_html

    footnote_html = f"<div class=\"table-footnote\"><small>{html.escape(footnote)}</small></div>"
    return f"{table_html}\n{footnote_html}"


def waterbody_summary_table(df: pd.DataFrame) -> str:
    """make html table for waterbodies with totals rendered via footer"""
    body_df, footer_df = create_waterbody_summary_table(df)
    meta = RSFieldMeta()
    body_rdf = RSGeoDataFrame(body_df)
    body_rdf, _ = meta.apply_units(body_rdf)

    if footer_df is not None:
        footer_rdf = RSGeoDataFrame(footer_df)
        footer_rdf, _ = meta.apply_units(footer_rdf)
        body_rdf.set_footer(footer_rdf)

    return body_rdf.to_html(index=False, escape=False)


def ownership_summary_table(df: pd.DataFrame) -> str:
    """make html table for ownership"""
    newrdf = RSGeoDataFrame(df)
    return newrdf.to_html(index=False, escape=False)
