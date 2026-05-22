"""Data preparation for downstream geomorphic longitudinal profiles.

Queries RME data from Athena and prepares it for profile charting,
closely following the exploration notebook ``scripts/exploration/profile_chart.ipynb``.

Copilot-generated module, edited by Lorin May 2026
"""

from pathlib import Path

import pandas as pd
from rsxml import Logger

from util.athena import query_to_local_parquet
from util.pandas import load_gdf_from_pq

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


def _query_whole_level_path(level_path: str):
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


def query_rme_data(mode: str, level_path: str, staging_path: Path) -> pd.DataFrame:
    """Query RME intersection data from Athena and save as local Parquet.


    Args:
        level_path -
        staging_path: Directory to write intermediate Parquet files.

    Returns:
        DataFrame with RME data for the AOI.
    """
    log = Logger("QueryRME")
    log.info("Querying Athena for RME data …")

    # MODE: whole level path
    # assumes we are given the level path
    sql = _query_whole_level_path(level_path)
    log.debug(f'Query:\n{sql}')
    query_to_local_parquet(
        sql,
        local_path=staging_path,
    )
    log.info(f"RME query complete → {staging_path}")

    df = load_gdf_from_pq(staging_path)
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
    rows: list[dict] = []

    for lp, grp in df.groupby("level_path"):
        rows.append(_summarize_level_path_group(str(lp), grp))

    return pd.DataFrame(rows).set_index("level_path")


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

    return pd.concat([top_df, pd.DataFrame([all_others]).set_index("level_path")])


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

    df = df.sort_values(["level_path", "seg_distance"])

    # Drop rows with no distance (can't chart them)
    before = len(df)
    df = df.dropna(subset=["seg_distance"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"Dropped {dropped} rows with null seg_distance")

    log.info(f"Profile data ready: {len(df)} rows across {df['level_path'].nunique()} level paths")
    return df
