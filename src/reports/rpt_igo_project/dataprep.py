"""Data preparation helpers for the IGO project summary report.

Created 2026-08-18.
Created by copilot.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pint
from rsxml import Logger

from util.athena import athena_unload_to_dataframe, get_field_metadata
from util.pandas import RSFieldMeta, load_gdf_from_pq

RME_LAYER_ID = "raw_rme"
PERENNIAL_FCODES = {46006, 55800}

ureg = pint.get_application_registry()


def define_fields(unit_system: str = "SI", field_meta_df: pd.DataFrame | None = None) -> None:
    """Load metadata and set report display-unit preferences.

    Created 2026-08-18.
    Created by copilot.
    """
    meta = RSFieldMeta()
    if field_meta_df is None:
        meta.field_meta = get_field_metadata(
            authority="data-exchange-scripts",
            tool_schema_name="rme_to_athena",
            layer_id="raw_rme",
        )
    else:
        meta.field_meta = field_meta_df.copy()
    meta.unit_system = unit_system

    # Keep a stable display system for cards/charts.
    meta.set_display_unit("segment_area", "kilometer ** 2", RME_LAYER_ID)
    meta.set_display_unit("centerline_length", "kilometer", RME_LAYER_ID)
    meta.set_display_unit("integrated_width", "meter", RME_LAYER_ID)


def _compute_aoi_area(aoi_gdf: gpd.GeoDataFrame) -> pint.Quantity:
    """Return AOI area in square kilometers.

    Created 2026-08-18.
    Created by copilot.
    """
    if aoi_gdf.empty:
        return 0 * ureg("kilometer ** 2")

    if aoi_gdf.crs is None:
        projected = aoi_gdf.set_crs(epsg=4326).to_crs(epsg=5070)
    else:
        projected = aoi_gdf.to_crs(epsg=5070)

    aoi_sq_m = float(projected.geometry.area.sum())
    return (aoi_sq_m * ureg("meter ** 2")).to("kilometer ** 2")


def _format_huc10(series: pd.Series) -> pd.Series:
    """Normalize HUC10 identifiers to zero-padded text.

    Created 2026-08-18.
    Created by copilot.
    """
    clean = series.astype("string").str.replace(r"\.0$", "", regex=True)
    clean = clean.str.strip()
    return clean.str.zfill(10)


def load_igo_report_data(parquet_source: Path) -> pd.DataFrame:
    """Load staged AOI parquet and prepare report-ready columns.

    Created 2026-08-18.
    Created by copilot.
    """
    log = Logger("IGO dataprep")
    df = load_gdf_from_pq(parquet_source)
    if df.empty:
        return df

    df.attrs["layer_id"] = RME_LAYER_ID

    if "watershed_id" in df.columns:
        df["watershed_id"] = _format_huc10(df["watershed_id"])

    if "fcode" in df.columns:
        fcode_series = pd.to_numeric(df["fcode"], errors="coerce")
        df["flow_permanence"] = np.where(fcode_series.isin(PERENNIAL_FCODES), "Perennial", "Non-perennial")
    else:
        df["flow_permanence"] = "Unknown"

    try:
        df, _applied_units = RSFieldMeta().apply_units(df, layer_id=RME_LAYER_ID)
    except Exception as exc:  # pragma: no cover
        log.warning(f"Unable to apply units from metadata: {exc}")

    return df


def summarize_flow_breakdown(df: pd.DataFrame) -> dict[str, dict[str, float | str]]:
    """Summarize perennial vs non-perennial contributions for key metrics.

    Created 2026-08-18.
    Created by copilot.
    """
    categories = ["Perennial", "Non-perennial"]
    flow_df = df.copy()
    if "flow_permanence" not in flow_df.columns:
        flow_df["flow_permanence"] = "Non-perennial"

    flow_df = flow_df[flow_df["flow_permanence"].isin(categories)]

    grouped = flow_df.groupby("flow_permanence", observed=False)

    length = grouped["centerline_length"].sum() if "centerline_length" in flow_df.columns else pd.Series(dtype=float)
    area = grouped["segment_area"].sum() if "segment_area" in flow_df.columns else pd.Series(dtype=float)
    seg_count = grouped.size() if len(flow_df) else pd.Series(dtype=int)

    def _extract_metric(series: pd.Series, default_unit: str) -> dict[str, float | str]:
        per_val = series.get("Perennial", 0)
        non_val = series.get("Non-perennial", 0)

        if hasattr(per_val, "to") or hasattr(non_val, "to"):
            quantity_val = per_val if hasattr(per_val, "to") else non_val
            unit_name = f"{quantity_val.units:~P}"

            if hasattr(per_val, "to"):
                per_float = float(per_val.to(quantity_val.units).magnitude)
            else:
                per_float = float(per_val)

            if hasattr(non_val, "to"):
                non_float = float(non_val.to(quantity_val.units).magnitude)
            else:
                non_float = float(non_val)
        else:
            unit_name = default_unit
            per_float = float(per_val) if pd.notna(per_val) else 0.0
            non_float = float(non_val) if pd.notna(non_val) else 0.0

        total = per_float + non_float
        per_pct = (per_float / total * 100.0) if total > 0 else 0.0
        non_pct = (non_float / total * 100.0) if total > 0 else 0.0

        return {
            "perennial": per_float,
            "non_perennial": non_float,
            "total": total,
            "unit": unit_name,
            "perennial_pct": per_pct,
            "non_perennial_pct": non_pct,
        }

    return {
        "length": _extract_metric(length, "km"),
        "area": _extract_metric(area, "km^2"),
        "segments": _extract_metric(seg_count, "count"),
    }


def summarize_cards(df: pd.DataFrame, aoi_gdf: gpd.GeoDataFrame) -> dict[str, object]:
    """Compute top-level summary values for highlight cards.

    Created 2026-08-18.
    Created by copilot.
    """
    aoi_area = _compute_aoi_area(aoi_gdf)

    total_segment_area = df["segment_area"].sum() if "segment_area" in df.columns and not df.empty else 0 * ureg("kilometer ** 2")
    total_centerline_length = df["centerline_length"].sum() if "centerline_length" in df.columns and not df.empty else 0 * ureg("kilometer")

    area_ratio_pct = (total_segment_area / aoi_area * 100.0) if aoi_area.magnitude > 0 else 0 * ureg("dimensionless")

    avg_area_per_length = (total_segment_area / total_centerline_length) if total_centerline_length.magnitude > 0 else 0 * ureg("meter")

    if "integrated_width" in df.columns and not df.empty:
        average_integrated_width = df["integrated_width"].mean()
    else:
        average_integrated_width = avg_area_per_length

    segments_count = int(len(df))
    source_project_count = int(df["rme_project_id"].dropna().astype("string").nunique()) if "rme_project_id" in df.columns else 0

    perennial_count = int((df["flow_permanence"] == "Perennial").sum()) if "flow_permanence" in df.columns else 0
    non_perennial_count = int((df["flow_permanence"] == "Non-perennial").sum()) if "flow_permanence" in df.columns else 0

    return {
        "aoi_area": aoi_area,
        "riverscape_area": total_segment_area,
        "area_ratio_pct": area_ratio_pct,
        "riverscape_length": total_centerline_length,
        "avg_area_per_length": avg_area_per_length,
        "avg_integrated_width": average_integrated_width,
        "segments_count": segments_count,
        "source_project_count": source_project_count,
        "perennial_count": perennial_count,
        "non_perennial_count": non_perennial_count,
    }


def _parse_exchange_timestamp(raw_value: object) -> str:
    """Parse Data Exchange bigint timestamps to YYYY-MM-DD.

    Handles both second and millisecond epochs.

    Created 2026-08-18.
    Created by copilot.
    """
    if raw_value is None:
        return ""

    if isinstance(raw_value, str) and raw_value.strip() == "":
        return ""

    value = pd.to_numeric(pd.Series([raw_value]), errors="coerce").iloc[0]
    if pd.isna(value):
        return ""

    unit = "ms" if float(value) >= 1_000_000_000_000 else "s"
    parsed = pd.to_datetime(value, unit=unit, utc=True, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _lookup_project_metadata(project_ids: list[str]) -> dict[str, dict[str, str]]:
    """Fetch Data Exchange project names and created dates for supplied project ids.

    Created 2026-08-18.
    Created by copilot.
    """
    if not project_ids:
        return {}

    clean_ids = sorted({str(pid).strip().lower() for pid in project_ids if str(pid).strip()})
    if not clean_ids:
        return {}

    id_sql = "(" + ",".join([f"'{pid}'" for pid in clean_ids]) + ")"
    sql = f"SELECT lower(uuid) AS uuid, name, createdonts AS createdon FROM rs_raw.data_exchange_projects WHERE lower(uuid) IN {id_sql}"

    try:
        lookup_df = athena_unload_to_dataframe(sql)
    except Exception as exc:  # pragma: no cover
        Logger("IGO dataprep").warning(f"Unable to fetch project metadata from rs_raw.data_exchange_projects: {exc}")
        return {}

    if lookup_df.empty:
        return {}

    metadata: dict[str, dict[str, str]] = {}
    for row in lookup_df.itertuples(index=False):
        uuid_val = str(getattr(row, "uuid", "")).strip().lower()
        if not uuid_val:
            continue
        metadata[uuid_val] = {
            "project_name": str(getattr(row, "name", "") or "").strip(),
            "created_on": _parse_exchange_timestamp(getattr(row, "createdon", None)),
        }

    return metadata


def build_source_project_table(df: pd.DataFrame) -> pd.DataFrame:
    """Build one row per contributing RME source project.

    Columns match report requirements (HUC10, watershed name, project info, citation, link).

    Created 2026-08-18.
    Created by copilot.
    """
    if df.empty or "rme_project_id" not in df.columns:
        return pd.DataFrame(
            columns=[
                "huc10_code",
                "project_name",
                "source_rme_project",
                "project_version_or_date",
                "citation",
                "project_url",
            ]
        )

    work_df = df.copy()
    work_df["source_rme_project"] = work_df["rme_project_id"].astype("string")
    if "watershed_id" in work_df.columns:
        work_df["huc10_code"] = _format_huc10(work_df["watershed_id"])
    else:
        work_df["huc10_code"] = ""

    if "rme_date_created_ts" in work_df.columns:
        work_df["project_date"] = pd.to_datetime(work_df["rme_date_created_ts"], errors="coerce", utc=True)
    else:
        work_df["project_date"] = pd.NaT

    if "rme_version" in work_df.columns:
        work_df["project_version"] = work_df["rme_version"].astype("string")
    else:
        work_df["project_version"] = pd.Series([pd.NA] * len(work_df), dtype="string")

    project_lookup = _lookup_project_metadata(work_df["source_rme_project"].dropna().astype(str).tolist())

    rows: list[dict[str, str]] = []
    for project_id, grp in work_df.groupby("source_rme_project", dropna=True):
        clean_project_id = str(project_id)
        project_meta = project_lookup.get(clean_project_id.lower(), {})
        project_name = project_meta.get("project_name", "") or "n/a"
        huc_codes = sorted({h for h in grp["huc10_code"].dropna().astype(str) if h})

        version_values = grp["project_version"].dropna().astype(str)
        version_value = version_values.iloc[-1] if not version_values.empty else ""

        preferred_date = project_meta.get("created_on", "")
        if preferred_date:
            date_value = preferred_date
        else:
            date_values = grp["project_date"].dropna()
            date_value = date_values.max().strftime("%Y-%m-%d") if not date_values.empty else ""

        if version_value and date_value:
            version_or_date = f"v{version_value} ({date_value})"
        elif version_value:
            version_or_date = f"v{version_value}"
        elif date_value:
            version_or_date = date_value
        else:
            version_or_date = "n/a"

        citation = f"Riverscapes Consortium RME source project {clean_project_id} ({project_name}), {version_or_date}."

        rows.append(
            {
                "huc10_code": ", ".join(huc_codes) if huc_codes else "n/a",
                "project_name": project_name,
                "source_rme_project": clean_project_id,
                "project_version_or_date": version_or_date,
                "citation": citation,
                "project_url": f"https://data.riverscapes.net/p/{clean_project_id}",
            }
        )

    out_df = pd.DataFrame(rows)
    if out_df.empty:
        return out_df

    return out_df.sort_values(by=["huc10_code", "source_rme_project"]).reset_index(drop=True)


def build_highlight_cards(card_summary: dict[str, object]) -> list[dict[str, object]]:
    """Convert summary metrics into macro-compatible highlight card payloads.

    Created 2026-08-18.
    Created by copilot.
    """

    def _fmt(value: object, decimals: int = 1) -> str:
        if hasattr(value, "magnitude"):
            return f"{value:~P,.{decimals}f}"
        if isinstance(value, (int, float)):
            return f"{value:,.{decimals}f}" if decimals > 0 else f"{value:,.0f}"
        return str(value)

    def _pct_str(value: object, decimals: int = 2) -> str:
        magnitude = getattr(value, "magnitude", value)
        if isinstance(magnitude, (int, float)):
            pct_value = float(magnitude)
        elif isinstance(magnitude, str):
            try:
                pct_value = float(magnitude)
            except ValueError:
                pct_value = 0.0
        else:
            pct_value = 0.0
        return f"{pct_value:,.{decimals}f}%"

    themes = ["blue", "green", "teal", "blue", "green", "teal", "blue", "green"]

    segments_text = f"Perennial: {card_summary['perennial_count']:,}; Non-perennial: {card_summary['non_perennial_count']:,}"

    card_specs = [
        ("area_chart", "Total Area of Interest", _fmt(card_summary["aoi_area"], 2), "public", "AOI footprint", _fmt(card_summary["aoi_area"], 2)),
        (
            "water",
            "Total Area of Riverscapes",
            _fmt(card_summary["riverscape_area"], 2),
            "layers",
            "Riverscape area captured",
            _fmt(card_summary["riverscape_area"], 2),
        ),
        (
            "percent",
            "Area of Interest in Riverscapes",
            _pct_str(card_summary.get("area_ratio_pct", 0.0), 2),
            "donut_small",
            "AOI represented by riverscapes",
            _pct_str(card_summary.get("area_ratio_pct", 0.0), 2),
        ),
        (
            "straighten",
            "Total Riverscape Length",
            _fmt(card_summary["riverscape_length"], 2),
            "timeline",
            "Summed centerline length",
            _fmt(card_summary["riverscape_length"], 2),
        ),
        (
            "swap_horiz",
            "Avg Area per Length of Riverscape",
            _fmt(card_summary["avg_area_per_length"], 2),
            "aspect_ratio",
            "Area-to-length intensity",
            _fmt(card_summary["avg_area_per_length"], 2),
        ),
        (
            "height",
            "Average Integrated Riverscape Width",
            _fmt(card_summary["avg_integrated_width"], 2),
            "analytics",
            "Mean integrated width",
            _fmt(card_summary["avg_integrated_width"], 2),
        ),
        (
            "view_timeline",
            "Riverscape Segments (DGOs)",
            f"{card_summary['segments_count']:,}",
            "dataset",
            segments_text,
            f"{card_summary['segments_count']:,}",
        ),
        (
            "hub",
            "Number of Source Projects",
            f"{card_summary['source_project_count']:,}",
            "account_tree",
            "Distinct RME project records",
            f"{card_summary['source_project_count']:,}",
        ),
    ]

    cards: list[dict[str, object]] = []
    for idx, spec in enumerate(card_specs):
        icon, header, primary, secondary_icon, secondary_text, footer_metric = spec
        cards.append(
            {
                "theme": themes[idx % len(themes)],
                "icon": icon,
                "header": header,
                "primary_value": primary,
                "secondary_stat": {"icon": secondary_icon, "text": secondary_text},
                "footer": {"metric": footer_metric, "label": "summary metric"},
            }
        )

    return cards
