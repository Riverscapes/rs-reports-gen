"""functions to generate figures specifically for rpt_riverscapes_dynamics reports"""


import geopandas as gpd
import numpy as np
import pandas as pd
import pint
import plotly.graph_objects as go
import plotly.express as px

from util.pandas import RSFieldMeta
from util.figures import bar_group_x_by_y


# --- Helper functions for longitudinal profile ---
def _subset_trunk(
    gdf_dgo: pd.DataFrame,
    trunk_suffix: str = "0001",
    level_path_col: str = "level_path",
    centerline_col: str = "centerline_length"
) -> pd.DataFrame:
    """
    Subset spatial layer to trunk (level_path endswith trunk_suffix).

    If multiple candidate trunks exist, selects the one with the most records;
    if tied, selects the one with the greatest total centerline length.

    Args:
        gdf_dgo: DataFrame of spatial segments.
        trunk_suffix: Suffix indicating trunk level_path.
        level_path_col: Name of the level_path column.
        centerline_col: Name of the centerline length column (for tie-breaker).

    Returns:
        DataFrame subset to the selected trunk.
    """
    # level_path cleanup (handles numeric saved as float)
    lp = gdf_dgo[level_path_col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    trunk_candidates = gdf_dgo[lp.str.endswith(trunk_suffix)].copy()
    if len(trunk_candidates) == 0:
        raise ValueError(f"No rows matched {level_path_col} ending with '{trunk_suffix}'")
    # Group by cleaned level_path
    trunk_candidates["_lp_clean"] = trunk_candidates[level_path_col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    trunk_groups = trunk_candidates.groupby("_lp_clean")
    # Find group(s) with max count
    group_sizes = trunk_groups.size()
    max_count = group_sizes.max()
    main_lps = group_sizes[group_sizes == max_count].index.tolist()
    if len(main_lps) == 1:
        main_lp = main_lps[0]
    else:
        # Tie: pick the one with the greatest total centerline length
        sums = trunk_groups[centerline_col].sum().loc[main_lps]
        main_lp = sums.idxmax()
    trunk = trunk_candidates[trunk_candidates["_lp_clean"] == main_lp].copy()
    trunk = trunk.drop(columns=["_lp_clean"])
    return trunk


def _compute_trunk_distance(trunk: pd.DataFrame, seg_dist_col: str = "seg_distance", centerline_col: str = "centerline_length", downstream_is: str = "min") -> pd.DataFrame:
    """
    Compute trunk distances for longitudinal profile plotting.

    Expects seg_dist_col and centerline_col to be Pint Quantities (with units attached).
    No unit conversion is performed here; all calculations preserve input units.
    The resulting columns (seg0, dist_mid) retain the same units as the input columns.

    Args:
        trunk: DataFrame with trunk segment data, with Pint Quantities for distance columns.
        seg_dist_col: Name of the segment distance column (should have units).
        centerline_col: Name of the centerline length column (should have units).
        dist_unit: (ignored; units are not converted here).
        downstream_is: 'min' or 'max' to set downstream reference.

    Returns:
        DataFrame with added columns 'seg0' and 'dist_mid', both with units.
    """
    # Ensure we are working with Pint Quantities; do not coerce to float
    trunk = trunk[trunk[seg_dist_col].notna()].copy()
    if downstream_is == "min":
        seg0 = trunk[seg_dist_col].min()
        trunk["seg0"] = trunk[seg_dist_col] - seg0
    elif downstream_is == "max":
        seg0 = trunk[seg_dist_col].max()
        trunk["seg0"] = seg0 - trunk[seg_dist_col]
    else:
        raise ValueError("DOWNSTREAM_IS must be 'min' or 'max'")
    trunk = trunk.sort_values("seg0").reset_index(drop=True)
    trunk["dist_mid"] = trunk["seg0"] + 0.5 * trunk[centerline_col]
    # No unit conversion; output units match input
    return trunk


def _select_trunk_slice(trunk: pd.DataFrame, dgo_start=None, dgo_end=None, reset_distance_zero=False) -> pd.DataFrame:
    # Accept dgo_start/dgo_end as int, str, or None
    if dgo_start is None and dgo_end is None:
        gdf_sel = trunk.copy()
    else:
        trunk_ids = set(trunk["dgo_id"].tolist())
        # Convert to int if possible
        try:
            dgo_start = int(dgo_start) if dgo_start is not None else None
            dgo_end = int(dgo_end) if dgo_end is not None else None
        except Exception:
            raise ValueError("dgo_start and dgo_end must be convertible to int")
        if (dgo_start not in trunk_ids) or (dgo_end not in trunk_ids):
            raise ValueError("One or both DGOs are not on the selected main trunk.")
        pos_start = int(trunk.index[trunk["dgo_id"] == dgo_start][0])
        pos_end = int(trunk.index[trunk["dgo_id"] == dgo_end][0])
        i0, i1 = sorted([pos_start, pos_end])
        gdf_sel = trunk.iloc[i0:i1+1].copy()
        if reset_distance_zero:
            gdf_sel["dist_mid"] = gdf_sel["dist_mid"] - float(gdf_sel["dist_mid"].min())
    return gdf_sel


def _prepare_metric_pivot(
    df_metrics: pd.DataFrame,
    gdf_sel: pd.DataFrame,
    metric_col: str,
    epochs_sorted: list[str]
) -> tuple[pd.DataFrame, np.ndarray]:
    """
    Prepare a pivoted metric table for plotting longitudinal profiles.

    Expects df_metrics to be unpivoted (one row per segment per epoch), with a column for metric_col and epoch_name.
    Returns a pivot table (index: segment, columns: epoch_name) and an array of segment distances.
    dgo_id is used only as a join key, not for display or ordering.

    Args:
        df_metrics: Metrics DataFrame (unpivoted).
        gdf_sel: Selected trunk segments (must have dist_mid).
        metric_col: Name of metric column to plot.
        epochs_sorted: List of epoch names to include as columns.

    Returns:
        pivot: DataFrame (index: segment, columns: epoch_name, values: metric_col)
        x: np.ndarray of segment distances (ordered to match pivot)
    """
    # Only use columns for dgo_id; never rely on index
    dgo_ids = gdf_sel["dgo_id"].unique()
    if "dgo_id" not in df_metrics.columns:
        raise ValueError("df_metrics must have a 'dgo_id' column for joining trunk segments.")
    dfm_p = df_metrics[df_metrics["dgo_id"].isin(dgo_ids)].copy()
    # Ensure dgo_id is numeric for join
    dfm_p["dgo_id"] = pd.to_numeric(dfm_p["dgo_id"], errors="coerce")
    dfm_p = dfm_p.dropna(subset=["dgo_id"])
    dfm_p["dgo_id"] = dfm_p["dgo_id"].astype(int)
    dfm_p[metric_col] = pd.to_numeric(dfm_p[metric_col], errors="coerce")
    pivot = (
        dfm_p.pivot_table(index="dgo_id", columns="epoch_name", values=metric_col, aggfunc="mean")
        .sort_index()
    )
    dist_map = gdf_sel.set_index("dgo_id")["dist_mid"]
    # Order pivot by dist_mid, not dgo_id
    pivot = pivot.loc[pivot.index.intersection(dist_map.index)].copy()
    pivot = pivot[epochs_sorted]
    pivot = pivot.reindex(dist_map.index)
    x = dist_map.reindex(pivot.index).to_numpy(dtype=float)
    return pivot, x


def _compute_minmax_band(pivot: pd.DataFrame, x: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    Y = pivot.to_numpy(dtype=float)
    y_min = np.nanmin(Y, axis=1)
    y_max = np.nanmax(Y, axis=1)
    valid = np.isfinite(x) & np.isfinite(y_min) & np.isfinite(y_max)
    x_band = x[valid]
    ymin_band = y_min[valid]
    ymax_band = y_max[valid]
    ordr = np.argsort(x_band)
    x_band = x_band[ordr]
    ymin_band = ymin_band[ordr]
    ymax_band = ymax_band[ordr]
    return x_band, ymin_band, ymax_band


def longitudinal_profile(gdf_dgo: gpd.GeoDataFrame, dynmetrics: pd.DataFrame, filters: dict[str, str]) -> go.Figure:
    """
    Generate a longitudinal profile figure for a river corridor, showing metric variation across epochs.

    gdf_dgo: spatial geodataframe(reach attributes)
    dynmetrics: metrics dataframe(per reach, per epoch)
    filters: dict with keys like 'project_id', 'landcover', 'epoch_length', 'confidence', 'metric_col', etc.
    """
    landcover = filters.get("landcover", "active")
    epoch_length = filters.get("epoch_length", "5")
    confidence = filters.get("confidence", "68")
    metric_col = filters.get("metric_col", "width")
    trunk_suffix = filters.get("trunk_suffix", "0001")
    level_path_col = filters.get("level_path_col", "level_path")
    seg_dist_col = filters.get("seg_distance_col", "seg_distance")
    centerline_col = filters.get("centerline_col", "centerline_length")
    downstream_is = filters.get("downstream_is", "min")
    dgo_start = filters.get("dgo_start")
    dgo_end = filters.get("dgo_end")
    reset_distance_zero = filters.get("reset_distance_zero", False)
    # Ensure reset_distance_zero is bool
    if isinstance(reset_distance_zero, str):
        reset_distance_zero = reset_distance_zero.lower() in ("true", "1", "yes")
    exclude_epochs = filters.get("exclude_epochs", [])

    # Subset trunk using spatial dataframe (no project_id required)
    trunk = _subset_trunk(gdf_dgo, trunk_suffix, level_path_col, centerline_col)
    trunk = _compute_trunk_distance(trunk, seg_dist_col, centerline_col, downstream_is)
    gdf_sel = _select_trunk_slice(trunk, dgo_start, dgo_end, reset_distance_zero)

    # Bake units for trunk (especially dist_mid)
    gdf_sel, _ = RSFieldMeta().bake_units(gdf_sel)
    headers_dict = RSFieldMeta().get_headers_dict(gdf_sel, layer_id='rsdynamics')

    # Filter metrics
    dfm_p = dynmetrics[
        (dynmetrics["landcover"] == landcover) &
        (dynmetrics["epoch_length"].astype(str) == str(epoch_length)) &
        (dynmetrics["confidence"].astype(str) == str(confidence))
    ].copy()

    # Epoch sorting
    ep = dfm_p["epoch_name"].astype(str).str.split("_", expand=True)
    dfm_p["epoch_start"] = pd.to_numeric(ep[0], errors="coerce")
    dfm_p["epoch_end"] = pd.to_numeric(ep[1], errors="coerce")
    dfm_p = dfm_p[np.isfinite(dfm_p["epoch_end"])]
    epoch_order = (
        dfm_p[["epoch_name", "epoch_end"]]
        .drop_duplicates()
        .sort_values(["epoch_end", "epoch_name"])
    )
    epochs_sorted = [e for e in epoch_order["epoch_name"].tolist() if e not in set(exclude_epochs)]
    if not epochs_sorted:
        raise ValueError("After excluding epochs, no epochs remain to plot.")

    pivot, x = _prepare_metric_pivot(dfm_p, gdf_sel, metric_col, epochs_sorted)
    x_band, ymin_band, ymax_band = _compute_minmax_band(pivot, x)

    # Plotly figure
    fig = go.Figure()
    band_x = np.concatenate([x_band, x_band[::-1]])
    band_y = np.concatenate([ymax_band, ymin_band[::-1]])
    fig.add_trace(go.Scatter(
        x=band_x,
        y=band_y,
        fill="toself",
        fillcolor="rgba(120,120,120,0.20)",
        line=dict(width=0),
        name="Range across epochs",
        hoverinfo="skip",
    ))
    nE = len(epochs_sorted)
    for i, ep_name in enumerate(epochs_sorted):
        y = pivot[ep_name].to_numpy(dtype=float)
        ok = np.isfinite(x) & np.isfinite(y)
        if not ok.any():
            continue
        is_latest = (i == nE - 1)
        if is_latest or nE <= 1:
            line_color = "rgba(255,140,0,0.95)"
            lw = 3
            ms = 5
            name = f"{ep_name} (latest)"
        else:
            t = i / (nE - 2) if (nE - 2) > 0 else 0.0
            shade = int(220 - t * (220 - 60))
            line_color = f"rgba({shade},{shade},{shade},0.75)"
            lw = 2
            ms = 4
            name = ep_name
        fig.add_trace(go.Scatter(
            x=x[ok],
            y=y[ok],
            mode="lines+markers",
            name=name,
            line=dict(width=lw, color=line_color),
            marker=dict(size=ms, color=line_color)
        ))
    x_label = headers_dict.get('dist_mid', "Longitudinal distance from downstream")
    fig.update_layout(
        title=f"Longitudinal {landcover} {metric_col} by epoch (epoch_length={epoch_length}, conf={confidence})",
        xaxis_title=x_label,
        yaxis_title=f"{landcover} {metric_col}",
        template="plotly_white",
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=90, b=40),
        xaxis2=dict(title_standoff=10)
    )
    # DGO id axis removed: dgo_id is not meaningful for display
    return fig


def linechart(df_metrics: pd.DataFrame, metric_colnm: str) -> go.Figure:
    """rsdynamics metric df by time and confidence
    TODO: use metadata for col including units
    TODO: probably move filtering upstream so don't have to redo it for every metric
    """
    # Filter data where epoch_length is 5 (including all confidence levels)
    filtered_df = df_metrics[(df_metrics['epoch_length'] == '5')].copy()
    # however when you do only that the epoch_length category still includes the epoch_length 30

    # 1. Isolate the Series for the 5-year epochs
    # Need to make sure epoch_name is categorical before using .cat accessor
    if df_metrics['epoch_name'].dtype == 'object':
        df_metrics['epoch_name'] = df_metrics['epoch_name'].astype('category')

    subset_series = df_metrics.loc[df_metrics['epoch_length'] == '5', 'epoch_name']

    # 2. Remove unused categories from the SERIES, then grab the remaining categories
    #    This returns a Pandas Index, which set_categories() accepts happily.
    valid_5yr_epochs = subset_series.cat.remove_unused_categories().cat.categories

    # 3. Apply to your filtered dataframe
    filtered_df['epoch_name'] = filtered_df['epoch_name'].cat.set_categories(valid_5yr_epochs)

    layer_id = df_metrics.attrs.get('layer_id') if hasattr(df_metrics, 'attrs') else None
    # sum isn't good for percent metrics

    if RSFieldMeta().get_field_unit(metric_colnm, layer_id=layer_id) == pint.Unit('%'):
        metric_summary = filtered_df.groupby(['landcover', 'epoch_name', 'confidence'], observed=False)[metric_colnm].mean().reset_index()
    else:
        metric_summary = filtered_df.groupby(['landcover', 'epoch_name', 'confidence'], observed=False)[metric_colnm].sum().reset_index()
    metric_summary = metric_summary.sort_values('epoch_name')

    # Try to resolve layer_id from dataframe attributes and add to new dataframe
    metric_summary.attrs['layer_id'] = layer_id
    baked_chart_data, _baked_headers = RSFieldMeta().bake_units(metric_summary)
    baked_header_lookup = RSFieldMeta().get_headers_dict(metric_summary, layer_id=layer_id)

    title = f'{metric_colnm.capitalize()} by Landcover over Time (Epoch Length 5)'
    # Create line chart
    fig = px.line(baked_chart_data, x='epoch_name', y=metric_colnm, color='landcover', line_dash='confidence',
                  title=title,
                  labels=baked_header_lookup,
                  line_dash_map={'95': 'solid', '68': 'dash'})
    return fig


def area_histogram(df_metrics: pd.DataFrame) -> go.Figure:
    """
    Generate a histogram of area for the 30-year epoch (1989_2024, confidence 95).
    """
    df_30yr_area = df_metrics[
        (df_metrics['epoch_name'] == '1989_2024') &
        (df_metrics['confidence'] == '95')
    ].copy()

    if df_30yr_area.empty:
        raise ValueError("No data for 30-year epoch found")

    # Use magnitude for cutting to ensure clean numeric labels
    area_vals = df_30yr_area['area']
    if hasattr(area_vals, 'pint'):
        area_vals = area_vals.pint.magnitude

    # Calculate bins: 1 bin if no variation, else 10 bins
    n_bins = 10 if (area_vals.max() - area_vals.min()) > 10 else 1

    df_30yr_area['area_bin_raw'] = pd.cut(area_vals, bins=n_bins)

    # Turn the bins into categories so we can show empty bins and format the labels
    if isinstance(df_30yr_area['area_bin_raw'].dtype, pd.CategoricalDtype):
        new_labels = [f"{i.left:,.0f} - {i.right:,.0f}" for i in df_30yr_area['area_bin_raw'].cat.categories]
        df_30yr_area['area_bins'] = df_30yr_area['area_bin_raw'].cat.rename_categories(new_labels)
    else:
        df_30yr_area['area_bins'] = df_30yr_area['area_bin_raw'].astype(str)

    df_30yr_area['record_count'] = 1
    return bar_group_x_by_y(df_30yr_area, total_col='record_count', group_by_cols=['area_bins', 'landcover'])


def statistics(df: pd.DataFrame) -> dict[str, pint.Quantity]:
    """Calculate and return key statistics for rsdynamics"""
    count_dgos = len(df)

    total_segment_area = df["segment_area"].sum()
    total_centerline_length = df["centerline_length"].sum()
    # Calculate integrated valley bottom width as ratio of totals
    integrated_valley_bottom_width = total_segment_area / total_centerline_length if total_centerline_length != 0 else float('nan') * total_segment_area.units / total_centerline_length.units

    stats = {
        'count_dgos': count_dgos,
        'total_segment_area': total_segment_area,
        'total_centerline_length': total_centerline_length,
        'integrated_valley_bottom_width': integrated_valley_bottom_width
    }
    return stats
