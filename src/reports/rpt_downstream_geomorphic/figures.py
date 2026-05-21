"""Figure builders for the downstream geomorphic longitudinal profile report.

Generates Plotly line charts showing how geomorphic indicators vary along
segment distance for each level path, similar to ``profile_chart.ipynb``.

Copilot-generated module.
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from rsxml import Logger

# Indicators to chart as default profile panels.
# Each tuple: (column_name, display_label, secondary_y).
DEFAULT_PROFILE_INDICATORS: list[tuple[str, str, bool]] = [
    ("elevation", "Elevation", False),
    ("channel_width", "Channel Width", False),
    ("drainage_area", "Drainage Area", True),
    ("prim_channel_gradient", "Channel Gradient", False),
    ("confinement_ratio", "Confinement Ratio", False),
    ("planform_sinuosity", "Sinuosity", False),
    ("integrated_width", "Integrated Width", False),
    ("floodplain_ratio", "Floodplain Ratio", False),
    ("lf_riparian_prop", "Riparian Proportion", False),
    ("fldpln_access", "Floodplain Access", False),
]


def _build_single_profile(
    df: pd.DataFrame,
    level_path: str,
    y1_col: str,
    y2_col: str,
    y1_label: str,
    y2_label: str,
) -> go.Figure:
    """Build a dual-axis profile chart for one level path.

    Copilot-generated function.
    """
    lp_df = df[df["level_path"] == level_path].sort_values("seg_distance")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=lp_df["seg_distance"],
            y=lp_df[y1_col],
            name=y1_label,
            mode="lines",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=lp_df["seg_distance"],
            y=lp_df[y2_col],
            name=y2_label,
            mode="lines",
        ),
        secondary_y=True,
    )

    stream_name = lp_df["stream_name"].dropna().unique()
    title_suffix = f" – {stream_name[0]}" if len(stream_name) else ""
    fig.update_layout(
        title_text=f"{y1_label} & {y2_label} along Segment Distance{title_suffix}",
        xaxis_title="Segment Distance",
        height=400,
        margin=dict(l=60, r=60, t=40, b=40),
    )
    fig.update_yaxes(title_text=y1_label, secondary_y=False)
    fig.update_yaxes(title_text=y2_label, secondary_y=True)

    return fig


def build_profile_figures(df: pd.DataFrame) -> dict[str, go.Figure]:
    """Build a set of longitudinal profile figures.

    For each level path, generates a primary profile chart
    (elevation + drainage area) and additional single-indicator charts.

    Copilot-generated function.

    Args:
        df: Prepared profile DataFrame (sorted by level_path, seg_distance).

    Returns:
        Dict mapping figure keys to Plotly Figure objects.
    """
    log = Logger("ProfileFigures")
    figures: dict[str, go.Figure] = {}

    level_paths = df["level_path"].unique()
    log.info(f"Building profile figures for {len(level_paths)} level path(s)")

    for lp in level_paths:
        lp_label = str(lp)

        # Primary dual-axis chart: elevation + drainage area
        figures[f"profile_{lp_label}"] = _build_single_profile(
            df,
            lp,
            y1_col="elevation",
            y2_col="drainage_area",
            y1_label="Elevation",
            y2_label="Drainage Area",
        )

        # Channel geometry
        figures[f"channel_{lp_label}"] = _build_single_profile(
            df,
            lp,
            y1_col="channel_width",
            y2_col="confinement_ratio",
            y1_label="Channel Width",
            y2_label="Confinement Ratio",
        )

        # Gradient + sinuosity
        figures[f"gradient_{lp_label}"] = _build_single_profile(
            df,
            lp,
            y1_col="prim_channel_gradient",
            y2_col="planform_sinuosity",
            y1_label="Channel Gradient",
            y2_label="Sinuosity",
        )

        # Floodplain indicators
        figures[f"floodplain_{lp_label}"] = _build_single_profile(
            df,
            lp,
            y1_col="floodplain_ratio",
            y2_col="fldpln_access",
            y1_label="Floodplain Ratio",
            y2_label="Floodplain Access",
        )

    log.info(f"Generated {len(figures)} profile figures")
    return figures
