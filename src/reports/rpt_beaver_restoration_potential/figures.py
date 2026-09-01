"""Figure builders for the Beaver Restoration Potential report.

Created 2026-07-07.
Created by copilot.
"""

import geopandas as gpd
import pandas as pd
import pint
import plotly.graph_objects as go
from rsxml import Logger

from util.figures import bar_from_summary
from util.pandas import RSGeoDataFrame, ureg

SUMMARY_TOTAL_FIELD = "segment_area"


def build_beaver_figures(summary_tables: dict[str, pd.DataFrame]) -> dict[str, go.Figure]:
    """Build the baseline figures for the Beaver Restoration Potential stub report.
    TODO: titles should come from metadata
    """
    log = Logger("BeaverFigures")
    figures = {
        "capacity_by_length": bar_from_summary(
            summary_tables.get("capacity", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_capacity",
            show_legend=False,
            height=600,
        ),
        "opportunity_by_length": bar_from_summary(
            summary_tables.get("opportunity", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_opportunity",
            show_legend=False,
            height=600,
        ),
        "limitation_by_length": bar_from_summary(
            summary_tables.get("limitation", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_limitation",
            show_legend=False,
            height=600,
        ),
        "risk_by_length": bar_from_summary(
            summary_tables.get("risk", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_risk",
            show_legend=False,
            height=600,
        ),
    }
    log.info(f"Built {len(figures)} figures for Beaver Restoration Potential")
    return figures


def main_statistics(df: pd.DataFrame | gpd.GeoDataFrame, actual_total_dam_count: int | None = None) -> dict[str, pint.Quantity]:
    """Calculate and return key statistics as a dictionary
    Args:
        df (DataFrame | GeoDataFrame): data_df input WITH UNITS APPLIED
        actual_total_dam_count: count of surveyed beaver dam points (rs_rpt.qris_beaver_activity)
            intersecting the AOI, used in place of the modeled dam_ct field for total_dams and
            its derived realized/remaining capacity stats. Falls back to modeled dam_ct if None
            (e.g. when the actual dam-point query could not be run).

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """

    subset_df = RSGeoDataFrame(df) if isinstance(df, gpd.GeoDataFrame) else pd.DataFrame(df)
    perennial = subset_df[subset_df["fcode"].isin([46006, 55800])]
    high_rp = subset_df[subset_df["brat_opportunity"].isin(['Conservation/Appropriate for Translocation', 'Encourage Beaver Expansion/Colonization'])]

    historic_dam_capacity = perennial.apply(lambda row: row["brat_hist_capacity"] * row["centerline_length"], axis=1).sum()
    total_dam_capacity = perennial.apply(lambda row: row["brat_capacity"] * row["centerline_length"], axis=1).sum()
    total_dams = actual_total_dam_count if actual_total_dam_count is not None else perennial["dam_ct"].sum()
    realized_capacity = ((total_dams / total_dam_capacity) * ureg.dimensionless).to("percent") if total_dam_capacity > 0 else 0 * ureg.percent
    remaining_capacity = total_dam_capacity - total_dams if total_dam_capacity > 0 else 0

    total_high_rp_capacity = high_rp.apply(lambda row: row["brat_capacity"] * row["centerline_length"], axis=1).sum()
    total_high_rp_dams = high_rp["dam_ct"].sum()
    realized_high_rp_capacity = ((total_high_rp_dams / total_high_rp_capacity) * ureg.dimensionless).to("percent") if total_high_rp_capacity > 0 else 0 * ureg.percent
    remaining_high_rp_capacity = total_high_rp_capacity - total_high_rp_dams if total_high_rp_capacity > 0 else 0

    stats = {
        'historic_dam_capacity': historic_dam_capacity,
        'total_dam_capacity': total_dam_capacity,
        'total_dams': total_dams,
        'realized_capacity': realized_capacity,
        'remaining_capacity': remaining_capacity,
        'total_dam_capacity_(actionable_opportunity)': total_high_rp_capacity,
        'total_dams_(actionable_opportunity)': total_high_rp_dams,
        'realized_capacity_(actionable_opportunity)': realized_high_rp_capacity,
        'remaining_capacity_(actionable_opportunity)': remaining_high_rp_capacity,
    }

    return stats
