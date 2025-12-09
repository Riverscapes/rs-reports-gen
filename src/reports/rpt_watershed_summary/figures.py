import pandas as pd
import pint
import pint_pandas
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


def create_waterbody_summary_table(df: RSGeoDataFrame) -> pd.DataFrame:
    """pivot individual waterbody columns into rows, maintaining units
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
        "% Area": 1.0,
        "% Count": 1.0,
        "Avg. Area": (total_area / total_count) if total_count > 0 else 0,
    }])
    # Also convert the total row percentages to pint quantities
    total_row['% Area'] = total_row['% Area'].astype('pint[percent]')
    total_row['% Count'] = total_row['% Count'].astype('pint[percent]')

    # Combine main data with total row
    final_df = pd.concat([report_df, total_row], ignore_index=True)

    return final_df


def waterbody_summary_table(df: RSGeoDataFrame) -> str:
    """make html table for waterbodies"""
    newdf = create_waterbody_summary_table(df)
    newrdf = RSGeoDataFrame(newdf)
    return newrdf.to_html(index=False, escape=False)
