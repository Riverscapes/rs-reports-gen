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
    "level_path, seg_distance, centerline_length, segment_area, "
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
    select {PROFILE_FIELDS}, final_seg_dist
    from rme
    WHERE level_path = '{{level_path}}'
    """
    return query_str.format(level_path=level_path)


def query_rme_data(level_path: str, staging_path: Path) -> pd.DataFrame:
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


def prepare_profile_data(df: pd.DataFrame) -> pd.DataFrame:
    """Sort and enrich the raw RME data for profile charting.

    Groups by level_path, sorts by seg_distance within each group,
    and adds any derived columns needed for the charts.

    Copilot-generated function.

    Args:
        df: Raw RME DataFrame.

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
