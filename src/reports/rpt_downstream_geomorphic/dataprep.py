"""Data preparation for downstream geomorphic longitudinal profiles.

Queries RME data from Athena and prepares it for profile charting,
closely following the exploration notebook ``scripts/exploration/profile_chart.ipynb``.

Copilot-generated module, edited by Lorin May 2026
"""

from pathlib import Path

import pandas as pd
from rsxml import Logger

from reports.rpt_downstream_geomorphic.selection_mode import SelectionMode
from util.athena import query_to_local_parquet
from util.pandas import RSFieldMeta, load_gdf_from_pq

RPT_RME_LAYER_ID = "rpt_rme"
SUMMARY_LAYER_ID = "rpt_downstream_geomorphic_summary"

# Fields of interest for longitudinal profiles.
# Extend this list as more indicators are needed.
PROFILE_FIELDS = (
    "level_path, centerline_length, segment_area, "
    "stream_name, stream_order, drainage_area, elevation, "
    "channel_width, confinement_ratio, constriction_ratio, "
    "integrated_width, floodplain_area, channel_area, "
    "low_lying_ratio, elevated_ratio, floodplain_ratio, active_channel_ratio, "
    "prim_channel_gradient, valleybottom_gradient, planform_sinuosity, "
    "fldpln_access, land_use_intens, "
    "lf_riparian_prop, lf_agriculture_prop, lf_developed_prop, "
    "rme_project_id, rme_project_name"
)


def _validate_level_path(level_path: str) -> bool:
    """return true if level_path meets expected criteria:
    * 13 or 14 charcters
    * all numeric
    Raises valueError otherwise.
    """
    if 13 <= len(level_path) <= 14:
        if str(level_path).isdecimal():
            return True
    raise ValueError(f'Level Path supplied ({level_path}) does not match expected pattern.')


def _query_downstream(node_id: str) -> str:
    query_str = """

    """
    raise NotImplementedError
    return query_str


def _query_whole_level_path(level_path: str) -> str:
    _validate_level_path(level_path)
    # TODO: build and use an rs_rpt table instead of rs_raw
    query_str = f"""WITH rme AS (
    SELECT
        rme.*,
        rme_seg.node_id,
        rme_seg.final_seg_dist,
        rme_seg.is_interhuc_lp,
        rme_seg.repair_status
    FROM rs_rpt.rpt_rme_intersections rme -- should be 1:1 relationship
    JOIN rs_raw.rme_corrected_seg_dist_huc2 rme_seg ON
        rme.huc2 = rme_seg.huc2 AND
        rme_seg.node_id = CONCAT(CAST (lat_key AS VARCHAR),'_', CAST (lon_key AS VARCHAR))
    )
    select {PROFILE_FIELDS}, final_seg_dist as seg_distance, is_interhuc_lp, repair_status
    from rme
    WHERE level_path = '{{level_path}}'
    """
    return query_str.format(level_path=level_path)


def query_rme_data(mode: SelectionMode, level_path: str, staging_path: Path) -> pd.DataFrame:
    """Query RME intersection data from Athena and save as local Parquet.


    Args:
        level_path -
        staging_path: Directory to write intermediate Parquet files.

    Returns:
        DataFrame with RME data for the AOI.
    """
    log = Logger("QueryRME")
    log.info("Querying Athena for RME data ...")

    if mode is SelectionMode.WHOLE:
        sql = _query_whole_level_path(level_path)
    else:
        raise NotImplementedError(f"Selection mode '{mode.value}' is not implemented for queries.")

    log.debug(f'Query:\n{sql}')
    query_to_local_parquet(
        sql,
        local_path=staging_path,
    )
    log.info(f"RME query complete -> {staging_path}")

    df = load_gdf_from_pq(staging_path)
    df.attrs["layer_id"] = RPT_RME_LAYER_ID
    log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from staging Parquet")
    return df


def _summarize_level_path_group(level_path: str | int, grp: pd.DataFrame) -> dict:
    """Summarize one level_path group into a single row dictionary.

    Copilot-generated function.
    """
    # Stream name with the greatest total centerline_length
    name_lengths = grp.dropna(subset=["stream_name"]).groupby("stream_name")["centerline_length"].sum()
    stream_name = name_lengths.idxmax() if not name_lengths.empty else None

    # Crosses HUC10 boundary
    crosses = "Yes" if grp["is_interhuc_lp"].any() else "No"

    # Repair status - uniform per level_path, take first non-null value
    repair_status = grp["repair_status"].dropna().iloc[0] if grp["repair_status"].notna().any() else None

    total_centerline_length = grp["centerline_length"].fillna(0).sum()

    return {
        "level_path": level_path,
        "stream_name": stream_name,
        "total_centerline_length": total_centerline_length,
        "num_dgos": len(grp),
        "crosses_huc10": crosses,
        "repair_status": repair_status,
    }


def _ensure_summary_metadata(source_layer_id: str) -> None:
    """Register derived summary-field metadata for this report.

    Copilot-generated function.
    """
    meta = RSFieldMeta()
    existing_meta = meta.get_field_meta("total_centerline_length", SUMMARY_LAYER_ID)
    if existing_meta:
        if not existing_meta.preferred_format:
            meta.set_preferred_format("total_centerline_length", "{value:,.2f}", SUMMARY_LAYER_ID)
        return

    try:
        meta.duplicate_meta(
            "centerline_length",
            "total_centerline_length",
            orig_layer_id=source_layer_id,
            new_layer_id=SUMMARY_LAYER_ID,
            new_friendly="Total Centerline Length",
            new_description="Sum of centerline length across all DGO segments within a level path.",
            new_preferred_format="{value:,.2f}",
        )
    except ValueError as exc:
        Logger("PrepSummary").warning(f"Unable to register summary metadata for total_centerline_length: {exc}")


def prepare_summary_data(df: pd.DataFrame) -> pd.DataFrame:
    """Return one row per level_path with summary statistics.

    Columns produced:
        stream_name   – name whose segments have the greatest total centerline_length
        total_centerline_length – sum of centerline_length across the level path
        num_dgos      – count of DGO rows
        crosses_huc10 – "Yes" / "No" based on is_interhuc_lp
        repair_status – single status value (uniform per level_path)

    Copilot-generated function.
    """
    source_layer_id = str(df.attrs.get("layer_id", RPT_RME_LAYER_ID))
    _ensure_summary_metadata(source_layer_id)

    rows: list[dict] = []

    for lp, grp in df.groupby("level_path"):
        rows.append(_summarize_level_path_group(str(lp), grp))

    summary_df = pd.DataFrame(rows).set_index("level_path")
    summary_df.attrs["layer_id"] = SUMMARY_LAYER_ID
    summary_df.attrs["source_layer_id"] = source_layer_id
    return summary_df


def prepare_summary_data_top_n(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Return top n level_paths by total_centerline_length and append an ALL OTHERS rollup row.

    If there are n or fewer level paths, this returns the same output shape as
    prepare_summary_data with no ALL OTHERS row.

    Copilot-generated function.
    """
    if n < 1:
        raise ValueError("n must be >= 1")

    summary_df = prepare_summary_data(df)
    if len(summary_df) <= n:
        return summary_df

    top_df = summary_df.sort_values("total_centerline_length", ascending=False).head(n)
    top_level_paths = set(top_df.index.astype(str))

    remaining_df = df[~df["level_path"].astype(str).isin(top_level_paths)]
    all_others = _summarize_level_path_group("ALL OTHERS", remaining_df)

    top_with_rollup_df = pd.concat([top_df, pd.DataFrame([all_others]).set_index("level_path")])
    top_with_rollup_df.attrs = summary_df.attrs.copy()
    return top_with_rollup_df


def prepare_summary_for_report(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Apply units and create report-ready display strings for summary rows.

    Copilot-generated function.
    """
    meta = RSFieldMeta()
    summary_layer_id = str(summary_df.attrs.get("layer_id", SUMMARY_LAYER_ID))
    source_layer_id = str(summary_df.attrs.get("source_layer_id", RPT_RME_LAYER_ID))
    _ensure_summary_metadata(source_layer_id)

    try:
        summary_display_df, _applied_units = meta.apply_units(summary_df, layer_id=summary_layer_id)
    except RuntimeError:
        summary_display_df = summary_df.copy()

    summary_display_df.attrs = summary_df.attrs.copy()

    try:
        total_centerline_length_header = meta.get_field_header(
            "total_centerline_length",
            include_units=True,
            layer_id=summary_layer_id,
        )
    except ValueError:
        total_centerline_length_header = "Total Centerline Length"

    if "total_centerline_length" in summary_display_df.columns:
        summary_series = summary_display_df["total_centerline_length"].where(
            summary_display_df["total_centerline_length"].notna(),
            None,
        )
        summary_display_df["total_centerline_length_display"] = summary_series.apply(
            lambda value: meta.format_scalar(
                "total_centerline_length",
                value,
                summary_layer_id,
                include_units=True,
            )
        )

    summary_display_df.attrs["total_centerline_length_header"] = total_centerline_length_header

    return summary_display_df


def prepare_profile_data(df: pd.DataFrame) -> pd.DataFrame:
    """Sort and enrich the raw RME data for profile charting.

    Groups by level_path, sorts by seg_distance within each group,
    and adds any derived columns needed for the charts.

    Args:
        df: RME DataFrame.

    Returns:
        Cleaned and sorted DataFrame ready for profile figures.
    """
    log = Logger("PrepProfile")
    layer_id = str(df.attrs.get("layer_id", RPT_RME_LAYER_ID))

    df = df.sort_values(["level_path", "seg_distance"])

    # Drop rows with no distance (can't chart them)
    before = len(df)
    df = df.dropna(subset=["seg_distance"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"Dropped {dropped} rows with null seg_distance")

    df.attrs["layer_id"] = layer_id
    log.info(f"Profile data ready: {len(df)} rows across {df['level_path'].nunique()} level paths")
    return df
