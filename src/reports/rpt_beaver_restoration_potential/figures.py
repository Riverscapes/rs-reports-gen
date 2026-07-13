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
            height=420,
        ),
        "opportunity_by_length": bar_from_summary(
            summary_tables.get("opportunity", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_opportunity",
            show_legend=False,
            height=420,
        ),
        "limitation_by_length": bar_from_summary(
            summary_tables.get("limitation", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_limitation",
            show_legend=False,
            height=420,
        ),
        "risk_by_length": bar_from_summary(
            summary_tables.get("risk", pd.DataFrame()),
            total_col=SUMMARY_TOTAL_FIELD,
            group_col="group",
            count_col="segment_count",
            fallback_group_field="brat_risk",
            show_legend=False,
            height=420,
        ),
    }
    log.info(f"Built {len(figures)} figures for Beaver Restoration Potential")
    return figures


def main_statistics(df: pd.DataFrame | gpd.GeoDataFrame) -> dict[str, pint.Quantity]:
    """Calculate and return key statistics as a dictionary
    Args:
        df (DataFrame | GeoDataFrame): data_df input WITH UNITS APPLIED

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """

    subset_df = RSGeoDataFrame(df) if isinstance(df, gpd.GeoDataFrame) else pd.DataFrame(df)
    perennial = subset_df[subset_df["fcode"].isin([46006, 55800])]

    total_dam_capacity = perennial.apply(lambda row: row["brat_capacity"] * row["centerline_length"], axis=1).sum()
    total_dams = perennial["dam_ct"].sum()
    realized_capacity = ((total_dams / total_dam_capacity) * ureg.dimensionless).to("percent") if total_dam_capacity > 0 else 0 * ureg.percent

    stats = {'total_dam_capacity': total_dam_capacity, 'total_dams': total_dams, 'realized_capacity': realized_capacity}

    return stats


def high_rp_statistics(df: pd.DataFrame | gpd.GeoDataFrame) -> dict[str, pint.Quantity]:
    """Calculate and return key statistics for high restoration potential areas as a dictionary
    Args:
        df (DataFrame | GeoDataFrame): data_df input WITH UNITS APPLIED

    Returns:
        dict[str, pint.Quantity]: new summary statistics applicable to the whole dataframe
    """

    subset_df = RSGeoDataFrame(df) if isinstance(df, gpd.GeoDataFrame) else pd.DataFrame(df)
    high_rp = subset_df[subset_df["brat_opportunity"].isin(['Conservation/Appropriate for Translocation', 'Encourage Beaver Expansion/Colonization'])]

    total_high_rp_capacity = high_rp.apply(lambda row: row["brat_capacity"] * row["centerline_length"], axis=1).sum()
    total_high_rp_dams = high_rp["dam_ct"].sum()
    realized_high_rp_capacity = ((total_high_rp_dams / total_high_rp_capacity) * ureg.dimensionless).to("percent") if total_high_rp_capacity > 0 else 0 * ureg.percent

    stats = {'total_dam_capacity_(high_restoration_potential)': total_high_rp_capacity, 'total_dams_(high_restoration_potential)': total_high_rp_dams, 'realized_capacity_(high_restoration_potential)': realized_high_rp_capacity}

    return stats
