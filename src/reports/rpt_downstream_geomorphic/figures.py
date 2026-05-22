"""Figure builders for the downstream geomorphic longitudinal profile report.

Generates Plotly line charts showing how geomorphic indicators vary along
segment distance for each level path, similar to ``profile_chart.ipynb``.

Copilot-generated module.
"""

import pandas as pd
import pint_pandas
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from rsxml import Logger

from util.pandas import RSFieldMeta

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


def _get_plot_values(series: pd.Series) -> pd.Series:
    """Return magnitudes for Pint series so Plotly gets plain numeric values.

    Copilot-generated function.
    """
    if isinstance(series.dtype, pint_pandas.PintType):
        return series.pint.magnitude
    return series


def _get_layer_id(df: pd.DataFrame) -> str | None:
    """Return layer_id from dataframe attrs when available.

    Copilot-generated function.
    """
    layer_id = df.attrs.get("layer_id")
    if layer_id is None:
        return None
    layer_text = str(layer_id).strip()
    return layer_text or None


def _get_field_label(df: pd.DataFrame, column_name: str, fallback_if_missing: str | None = None) -> str:
    """Get a metadata-aware label with optional fallback when metadata is absent.

    Copilot-generated function.
    """
    meta = RSFieldMeta()
    layer_id = _get_layer_id(df)

    try:
        has_meta = meta.get_field_meta(column_name, layer_id) is not None
    except ValueError:
        has_meta = False

    if fallback_if_missing is not None and not has_meta:
        return fallback_if_missing

    try:
        return meta.get_field_header(column_name, include_units=True, layer_id=layer_id)
    except ValueError:
        return fallback_if_missing or meta.get_friendly_name(column_name)


def _build_single_profile(
    df: pd.DataFrame,
    level_path: str,
    y1_col: str,
    y2_col: str,
) -> go.Figure:
    """Build a dual-axis profile chart for one level path.

    Copilot-generated function.
    """
    lp_df = df[df["level_path"] == level_path].sort_values("seg_distance")
    y1_label = _get_field_label(lp_df, y1_col)
    y2_label = _get_field_label(lp_df, y2_col)
    x_label = _get_field_label(lp_df, "seg_distance", fallback_if_missing="Segment Distance")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=_get_plot_values(lp_df["seg_distance"]),
            y=_get_plot_values(lp_df[y1_col]),
            name=y1_label,
            mode="lines",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=_get_plot_values(lp_df["seg_distance"]),
            y=_get_plot_values(lp_df[y2_col]),
            name=y2_label,
            mode="lines",
        ),
        secondary_y=True,
    )

    stream_name = lp_df["stream_name"].dropna().unique()
    title_suffix = f" – {stream_name[0]}" if len(stream_name) else ""
    fig.update_layout(
        title_text=f"{y1_label} & {y2_label} along {x_label}{title_suffix}",
        xaxis_title=x_label,
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
        )

        # Channel geometry
        figures[f"channel_{lp_label}"] = _build_single_profile(
            df,
            lp,
            y1_col="channel_width",
            y2_col="confinement_ratio",
        )

        # Gradient + sinuosity
        figures[f"gradient_{lp_label}"] = _build_single_profile(
            df,
            lp,
            y1_col="prim_channel_gradient",
            y2_col="planform_sinuosity",
        )

        # Floodplain indicators
        figures[f"floodplain_{lp_label}"] = _build_single_profile(
            df,
            lp,
            y1_col="floodplain_ratio",
            y2_col="fldpln_access",
        )

    log.info(f"Generated {len(figures)} profile figures")
    return figures
