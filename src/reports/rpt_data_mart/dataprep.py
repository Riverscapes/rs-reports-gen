import pandas as pd
from rsxml import Logger

from util.pandas import RSFieldMeta
from util.rme.rme_common_dataprep import add_common_rme_cols


def add_calculated_rme_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Add calculated columns to the RME dataframe
    Returns:
        dataframe with added columns
    """
    df = add_common_rme_cols(
        df,
        [
            'riparian_veg_departure_as_departure',
            'riparian_veg_departure_bins',
        ],
    )
    # add metadata
    meta = RSFieldMeta()
    meta.add_field_meta('riparian_veg_departure_as_departure', layer_id='dgo', theme='Vegetation Context')
    meta.add_field_meta('riparian_veg_departure_bins', layer_id='dgo', theme='Vegetation Context')

    # add any columns need FOR THIS REPORT ONLY here

    return df


def unnest_dem_bins(huc_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Unpack the ``dem_bins`` nested struct into flat columns and a long-format bins table.

    Scalar summary fields (dem_min, dem_max, dem_bin_size, dem_value_count, dem_nodata,
    dem_hist_type) are extracted from the struct and added as top-level columns on the
    returned HUC DataFrame so that RSFieldMeta unit conversion applies to them normally.
    The raw ``dem_bins`` struct column is removed from the HUC table.

    The second return value is a slim long-format DataFrame with one row per
    (HUC, elevation bin), keyed on ``huc`` so it can be joined back to the HUC table.

    Args:
        huc_df: HUC DataFrame containing a ``dem_bins`` struct column sourced from Athena.

    Returns:
        tuple:
            - Modified huc_df with ``dem_bins`` removed and DEM scalar columns added.
            - Long-format dem_bins_df with columns: huc, bin, cell_count.

    Copilot-generated function, directed by Lorin April 2026.
    """

    log = Logger("unnest_dem_bins")
    empty_bins: pd.DataFrame = pd.DataFrame(columns=["huc", "bin", "cell_count"])

    if "dem_bins" not in huc_df.columns:
        log.warning("'dem_bins' column not found in HUC DataFrame; skipping DEM unpack")
        return huc_df, empty_bins

    huc_df = huc_df.copy()

    def _scalar(d: dict | None, key: str):
        return d.get(key) if isinstance(d, dict) else None

    SCALAR_KEYS = ["min", "max", "bin_size", "value_count", "nodata", "hist_type"]  # noqa N806
    for key in SCALAR_KEYS:
        huc_df[f"dem_bins_{key}"] = huc_df["dem_bins"].apply(lambda x, k=key: _scalar(x, k))

    # Build slim long-format table: one row per HUC x elevation bin
    bin_rows: list[dict] = []
    for _, row in huc_df[["huc", "dem_bins"]].iterrows():
        d = row["dem_bins"]
        if not isinstance(d, dict):
            continue
        bins = d.get("bins")
        for b in bins if bins is not None else []:
            if not isinstance(b, dict):
                continue
            bin_rows.append(
                {
                    "huc": row["huc"],
                    "bin": b.get("bin"),
                    "cell_count": b.get("cell_count"),
                }
            )

    dem_bins_df = pd.DataFrame(bin_rows) if bin_rows else empty_bins.copy()
    huc_df = huc_df.drop(columns=["dem_bins"])

    log.info(f"DEM bins unpacked: {len(dem_bins_df)} elevation-bin rows from {len(huc_df)} HUCs; added dem_bins_min, dem_bins_max, dem_bins_bin_size, dem_bins_value_count, dem_bins_nodata, dem_bins_hist_type to HUC table")
    return huc_df, dem_bins_df
