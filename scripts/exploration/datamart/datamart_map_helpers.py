"""Shared map-preparation helpers for Data Mart exploration notebooks.

This module was written by GitHub Copilot Directed by Lorin 2026-04-15.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import ipywidgets as widgets
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from lonboard import Map, ScatterplotLayer

THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from util.figures import get_zoom_and_center


@dataclass(frozen=True)
class MapLabels:
    """Friendly labels used in map titles, legends, and hover text."""

    size_label: str
    bin_label: str
    color_label: str


@dataclass(frozen=True)
class MapData:
    """Prepared, shared data consumed by both Plotly and Lonboard examples."""

    data: pd.DataFrame
    labels: MapLabels


def load_dgo_points(datamart_root: Path) -> gpd.GeoDataFrame:
    """Load DGO points from the Data Mart parquet export as EPSG:4326 geometry."""

    df = pd.read_parquet(datamart_root / "exports" / "dgo.parquet")
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326",
    )
    return gdf


def load_data_dictionary(datamart_root: Path) -> pd.DataFrame:
    """Load the CSV data dictionary for friendly names and metadata lookups."""

    return pd.read_csv(datamart_root / "data_dictionary.csv")


def _friendly_name(
    data_dictionary: pd.DataFrame,
    table_name: str,
    column_name: str,
    fallback: str,
) -> str:
    """Resolve a friendly name for a column from data_dictionary rows."""

    mask = (data_dictionary["table_name"] == table_name) & (data_dictionary["column_name"] == column_name)
    match = data_dictionary.loc[mask, "friendly_name"]
    if match.empty:
        return fallback
    value = str(match.iloc[0]).strip()
    return value or fallback


def _hex_to_rgba(color_hex: str, alpha: int = 190) -> list[int]:
    """Convert #RRGGBB hex colors to RGBA integer lists for Lonboard."""

    value = str(color_hex).strip().lstrip("#")
    if len(value) != 6:
        return [140, 140, 140, alpha]
    return [int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16), alpha]


def prepare_beaver_capacity_map_data(
    dgo_gdf: gpd.GeoDataFrame,
    data_dictionary: pd.DataFrame,
) -> MapData:
    """Build shared map-ready columns for beaver size/color encodings.

    Size is based on historical beaver dam complex size and color is based on the
    existing beaver dam capacity bin color value.
    """

    required = [
        "longitude",
        "latitude",
        "brat_hist_complex_size",
        "brat_capacity_bin",
        "brat_capacity_color",
        "geometry",
    ]
    missing = [col for col in required if col not in dgo_gdf.columns]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"Missing required columns in DGO data: {missing_str}")

    map_df = dgo_gdf.loc[
        :,
        [
            "longitude",
            "latitude",
            "brat_hist_complex_size",
            "brat_capacity_bin",
            "brat_capacity_color",
            "geometry",
        ],
    ].copy()

    map_df = map_df.dropna(
        subset=[
            "longitude",
            "latitude",
            "brat_hist_complex_size",
            "brat_capacity_bin",
            "brat_capacity_color",
        ]
    )

    size = pd.to_numeric(map_df["brat_hist_complex_size"], errors="coerce").fillna(0.0)
    size = size.clip(lower=0.0)

    # Sqrt scaling keeps large values visible without overwhelming the map.
    sqrt_size = size.pow(0.5)
    max_sqrt = float(sqrt_size.max()) if len(sqrt_size) else 1.0
    if max_sqrt <= 0:
        max_sqrt = 1.0

    map_df["size_value"] = size
    map_df["size_scaled"] = 4.0 + (sqrt_size / max_sqrt) * 20.0
    map_df["capacity_bin"] = map_df["brat_capacity_bin"].astype(str)
    map_df["capacity_color_hex"] = map_df["brat_capacity_color"].astype(str)
    map_df["capacity_color_rgba"] = map_df["capacity_color_hex"].map(_hex_to_rgba)

    if "brat_capacity_bin_sort" in dgo_gdf.columns:
        map_df["capacity_bin_sort"] = pd.to_numeric(dgo_gdf.loc[map_df.index, "brat_capacity_bin_sort"], errors="coerce")
    else:
        map_df["capacity_bin_sort"] = pd.NA

    size_label = _friendly_name(
        data_dictionary,
        table_name="dgo",
        column_name="brat_hist_complex_size",
        fallback="Historical Beaver Dam Complex Size",
    )
    bin_label = _friendly_name(
        data_dictionary,
        table_name="dgo",
        column_name="brat_capacity_bin",
        fallback="Existing Beaver Dam Capacity (bin)",
    )
    color_label = _friendly_name(
        data_dictionary,
        table_name="dgo",
        column_name="brat_capacity_color",
        fallback="Existing Beaver Dam Capacity (color)",
    )

    labels = MapLabels(
        size_label=size_label,
        bin_label=bin_label,
        color_label=color_label,
    )
    return MapData(data=map_df, labels=labels)


def _build_lonboard_legend(shared: MapData) -> widgets.HTML:
    """Build an HTML legend widget for Lonboard capacity-bin colors."""

    legend_df = shared.data[["capacity_bin", "capacity_color_hex", "capacity_bin_sort"]].drop_duplicates()
    if legend_df["capacity_bin_sort"].notna().any():
        legend_df = legend_df.sort_values(["capacity_bin_sort", "capacity_bin"], na_position="last")
    else:
        legend_df = legend_df.sort_values("capacity_bin")

    items = []
    for _, row in legend_df.iterrows():
        color = str(row["capacity_color_hex"])
        label = str(row["capacity_bin"])
        items.append(f"<div style='display:flex; align-items:center; gap:8px; margin:2px 0;'><span style='display:inline-block; width:12px; height:12px; border:1px solid #333; background:{color};'></span><span>{label}</span></div>")

    html = f"<div style='font-family:Arial, sans-serif; font-size:12px; line-height:1.3;'><div style='font-weight:600; margin-bottom:6px;'>{shared.labels.bin_label}</div>" + "".join(items) + "</div>"
    return widgets.HTML(value=html)


def build_lonboard_map(shared: MapData) -> widgets.VBox:
    """Create a Lonboard scatter map with a matching color legend."""

    gdf = gpd.GeoDataFrame(shared.data, geometry="geometry", crs="EPSG:4326")
    radius_values = gdf["size_scaled"].to_numpy(dtype=np.float32)
    color_values = np.array(gdf["capacity_color_rgba"].tolist(), dtype=np.uint8)

    layer = ScatterplotLayer.from_geopandas(
        gdf,
        get_radius=radius_values,
        radius_units="pixels",
        radius_min_pixels=1,
        radius_max_pixels=25,
        get_fill_color=color_values,
        stroked=True,
        get_line_color=[35, 35, 35, 90],
        line_width_min_pixels=0.5,
        pickable=True,
    )
    map_widget = Map(layers=[layer])
    legend_widget = _build_lonboard_legend(shared)
    return widgets.VBox([map_widget, legend_widget])


def build_plotly_map(shared: MapData) -> go.Figure:
    """Create a Plotly map with bin-based legend and scaled marker sizes."""

    df = shared.data.copy()

    if "capacity_bin_sort" in df.columns and df["capacity_bin_sort"].notna().any():
        df = df.sort_values(["capacity_bin_sort", "capacity_bin"], na_position="last")
    else:
        df = df.sort_values("capacity_bin")

    fig = go.Figure()
    for bin_name, chunk in df.groupby("capacity_bin", sort=False):
        color = str(chunk["capacity_color_hex"].iloc[0])
        fig.add_trace(
            go.Scattermap(
                lon=chunk["longitude"],
                lat=chunk["latitude"],
                mode="markers",
                name=str(bin_name),
                marker={
                    "size": chunk["size_scaled"],
                    "sizemode": "diameter",
                    "color": color,
                    "opacity": 0.75,
                },
                customdata=chunk[["size_value"]],
                hovertemplate=(f"{shared.labels.bin_label}: %{{fullData.name}}<br>{shared.labels.size_label}: %{{customdata[0]:,.2f}}<extra></extra>"),
            )
        )

    zoom, center = get_zoom_and_center(
        gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326"),
        "geometry",
    )

    fig.update_layout(
        margin={"l": 0, "r": 0, "t": 60, "b": 0},
        height=550,
        legend_title_text=shared.labels.bin_label,
        title=(f"DGO Points | Size: {shared.labels.size_label} | Color: {shared.labels.color_label}"),
    )
    fig.update_maps(style="open-street-map", center=center, zoom=zoom)
    return fig
