"""demonstrate and test the binning function in rme_common_dataprep"""
import pandas as pd
from util.pandas import RSFieldMeta
from util.rme.rme_common_dataprep import bins_continuous_equal_width
from util.rme.rme_common_dataprep import add_common_rme_cols


def printdf(df, label="Dataframe:"):
    print(label)
    print(df)
    # print(df.info())
    # print(df.describe())


def run_and_print_bins_continuous_equal_width(df, col_name, n_bins, rounding):
    printdf(df, "Original DataFrame:")

    # Apply equal-width binning with rounding to nearest integer
    df_binned = bins_continuous_equal_width(df, col_name=col_name, n_bins=n_bins, rounding=rounding)

    printdf(df_binned, f"\nDataFrame with {n_bins}-bins (rounded to {rounding} decimals) binned column:")

    # use same name formula as default
    binned_col_name = f"{col_name}_{n_bins}bins"
    # Show bin labels and value counts
    print("\nBin value counts:")
    print(df_binned[binned_col_name].value_counts(sort=False))


def main():
    # Create a sample DataFrame with a continuous variable
    df = pd.DataFrame({
        'value': [2.3, 5.1, 7.8, 10.2, 12.5, 12.6, 15.0, 18.7, 20.0, 22.4, 25.6, 999]
    })

    # run_and_print_bins_continuous_equal_width(df, 'value', 5, -1)
    # return

    # use real data
    from util.pandas import load_gdf_from_csv
    from util.rme.field_metadata import get_field_metadata
    csv_data_path = r"C:\nardata\pydataroot\rpt-riverscapes-inventory\project_bounds_-_Riverscapes_Inventory\data\data.csv"
    data_gdf = load_gdf_from_csv(csv_data_path)
    _FIELD_META = RSFieldMeta()  # Instantiate the Borg singleton. We can reference it with this object or RSFieldMeta()
    _FIELD_META.field_meta = get_field_metadata()  # Set the field metadata for the report
    sub_gdf = data_gdf[['riparian_veg_departure', 'segment_area']].copy()
    #
    run_and_print_bins_continuous_equal_width(sub_gdf, 'riparian_veg_departure', 10, 0)
    df = add_common_rme_cols(sub_gdf, ['riparian_veg_departure_as_departure', 'riparian_veg_departure_bins'])
    printdf(df, "after adding common rme col riparian_veg_departure")
    print(df['riparian_veg_departure_bin'].value_counts(sort=False))


if __name__ == "__main__":
    main()
