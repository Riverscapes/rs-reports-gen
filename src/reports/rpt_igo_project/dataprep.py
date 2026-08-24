"""Data preparation helpers for the IGO project summary report.

Created 2026-08-18.
Created by copilot.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pandas as pd
import pint
import pyarrow.parquet as pq
from rsxml import Logger

from util.athena import get_field_metadata
from util.athena.athena_unload_utils import list_athena_unload_payload_files
from util.figures import HighlightCard, MetricBundle
from util.pandas import RSFieldMeta
from util.rs_geo_helpers import total_aoi_area_m2

RME_LAYER_ID = "raw_rme"
PERENNIAL_FCODES = {46006, 55800}  # Confirmed by Joe and Jordan 2026-08-20. Also built in to rs_rpt.rme_datamart_base_vw https://github.com/Riverscapes/rs-reports-gen/issues/177#issuecomment-5347530479

ureg = pint.get_application_registry()


@dataclass
class IGOReportArtifacts:
    """Typed artifact payload for IGO report data transfer.

    Created 2026-08-19.
    Created by copilot.
    """

    flow_summary: dict[str, dict[str, float | str]]
    card_metrics: MetricBundle
    source_rows_df: pd.DataFrame


def _extract_metric_values(per_val: object, non_val: object, default_unit: str) -> dict[str, float | str]:
    """Convert two metric values into a summary dict with percentages.

    Created 2026-08-19.
    Created by copilot.
    """

    def _as_float(value: object) -> float:
        value_any = cast(Any, value)
        if value_any is None or pd.isna(value_any):
            return 0.0
        try:
            return float(value_any)
        except (TypeError, ValueError):
            return 0.0

    per_any = cast(Any, per_val)
    non_any = cast(Any, non_val)

    if hasattr(per_any, "to") or hasattr(non_any, "to"):
        quantity_val = per_any if hasattr(per_any, "to") else non_any
        unit_name = f"{quantity_val.units:~P}"

        if hasattr(per_any, "to"):
            per_float = float(per_any.to(quantity_val.units).magnitude)
        else:
            per_float = _as_float(per_any)

        if hasattr(non_any, "to"):
            non_float = float(non_any.to(quantity_val.units).magnitude)
        else:
            non_float = _as_float(non_any)
    else:
        unit_name = default_unit
        per_float = _as_float(per_any)
        non_float = _as_float(non_any)

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


def _stream_flow_and_source_summary(parquet_source: Path, batch_size: int = 90_000) -> tuple[dict[str, float], dict[str, float], dict[str, dict[str, object]]]:
    """Stream parquet files to collect report totals and source-project summaries.

    Created 2026-08-19.
    Created by copilot.
    """
    if parquet_source.is_file():
        parquet_files = [parquet_source]
    else:
        parquet_files = list_athena_unload_payload_files(parquet_source)

    overall_totals: dict[str, float] = {
        "row_count": 0.0,
        "segment_area": 0.0,
        "centerline_length": 0.0,
        "integrated_width": 0.0,
        "integrated_width_count": 0.0,
    }

    flow_totals: dict[str, float] = {
        "perennial_length": 0.0,
        "non_perennial_length": 0.0,
        "perennial_area": 0.0,
        "non_perennial_area": 0.0,
        "perennial_count": 0.0,
        "non_perennial_count": 0.0,
    }

    source_rollup: dict[str, dict[str, Any]] = {}

    columns_to_read = ["fcode", "centerline_length", "segment_area", "integrated_width", "watershed_id", "rme_project_id", "rme_date_created_ts", "rme_version"]

    for parquet_file in parquet_files:
        pq_file = pq.ParquetFile(parquet_file)
        file_cols = set(pq_file.schema.names)
        active_cols = [col for col in columns_to_read if col in file_cols]
        if not active_cols:
            continue

        for batch in pq_file.iter_batches(columns=active_cols, batch_size=batch_size):
            batch_df = batch.to_pandas()
            if batch_df.empty:
                continue

            overall_totals["row_count"] += float(len(batch_df))

            if "fcode" in batch_df.columns:
                fcode_series = pd.to_numeric(batch_df["fcode"], errors="coerce")
                is_perennial = fcode_series.isin(PERENNIAL_FCODES)
            else:
                is_perennial = pd.Series([False] * len(batch_df), index=batch_df.index)

            lengths = pd.to_numeric(batch_df["centerline_length"], errors="coerce") if "centerline_length" in batch_df.columns else pd.Series([0.0] * len(batch_df), index=batch_df.index)
            areas = pd.to_numeric(batch_df["segment_area"], errors="coerce") if "segment_area" in batch_df.columns else pd.Series([0.0] * len(batch_df), index=batch_df.index)

            overall_totals["centerline_length"] += float(lengths.sum(skipna=True))
            overall_totals["segment_area"] += float(areas.sum(skipna=True))

            if "integrated_width" in batch_df.columns:
                integrated_width = pd.to_numeric(batch_df["integrated_width"], errors="coerce")
                overall_totals["integrated_width"] += float(integrated_width.sum(skipna=True))
                overall_totals["integrated_width_count"] += float(integrated_width.notna().sum())

            flow_totals["perennial_length"] += float(lengths.where(is_perennial, 0).sum(skipna=True))
            flow_totals["non_perennial_length"] += float(lengths.where(~is_perennial, 0).sum(skipna=True))
            flow_totals["perennial_area"] += float(areas.where(is_perennial, 0).sum(skipna=True))
            flow_totals["non_perennial_area"] += float(areas.where(~is_perennial, 0).sum(skipna=True))
            flow_totals["perennial_count"] += float(is_perennial.sum())
            flow_totals["non_perennial_count"] += float((~is_perennial).sum())

            if "rme_project_id" not in batch_df.columns:
                continue

            source_df = pd.DataFrame()
            source_df["project_id"] = batch_df["rme_project_id"].astype("string").str.strip().str.lower()
            source_df = source_df[source_df["project_id"].notna() & (source_df["project_id"] != "")]
            if source_df.empty:
                continue

            if "watershed_id" in batch_df.columns:
                source_df["huc10_code"] = _format_huc10(batch_df.loc[source_df.index, "watershed_id"])
            else:
                source_df["huc10_code"] = ""

            if "rme_date_created_ts" in batch_df.columns:
                source_df["project_date"] = pd.to_datetime(batch_df.loc[source_df.index, "rme_date_created_ts"], errors="coerce", utc=True)
            else:
                source_df["project_date"] = pd.NaT

            if "rme_version" in batch_df.columns:
                source_df["project_version"] = batch_df.loc[source_df.index, "rme_version"].astype("string")
            else:
                source_df["project_version"] = pd.Series([pd.NA] * len(source_df), dtype="string")

            for row in source_df.itertuples(index=False):
                project_id = str(row.project_id)
                record = source_rollup.setdefault(
                    project_id,
                    {
                        "huc10_codes": set(),
                        "project_date": pd.NaT,
                        "project_version": "",
                    },
                )

                huc = str(row.huc10_code).strip()
                if huc and huc.lower() != "<na>":
                    cast(set[str], record["huc10_codes"]).add(huc)

                proj_date_ts = pd.to_datetime(cast(Any, row.project_date), errors="coerce", utc=True)
                if pd.notna(proj_date_ts):
                    existing_date = pd.to_datetime(cast(Any, record["project_date"]), errors="coerce", utc=True)
                    if pd.isna(existing_date) or proj_date_ts > existing_date:
                        record["project_date"] = proj_date_ts

                proj_version = str(row.project_version).strip()
                if proj_version and proj_version.lower() != "<na>":
                    record["project_version"] = proj_version

    return overall_totals, flow_totals, source_rollup


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
    meta.set_display_unit_imperial("segment_area", "mile ** 2", RME_LAYER_ID)
    meta.set_display_unit("centerline_length", "kilometer", RME_LAYER_ID)
    meta.set_display_unit_imperial("centerline_length", "mile", RME_LAYER_ID)
    meta.set_display_unit("integrated_width", "meter", RME_LAYER_ID)
    meta.set_display_unit_imperial("integrated_width", "foot", RME_LAYER_ID)


def _format_huc10(series: pd.Series) -> pd.Series:
    """Normalize HUC10 identifiers to zero-padded text.

    Created 2026-08-18.
    Created by copilot.
    """
    clean = series.astype("string").str.replace(r"\.0$", "", regex=True)
    clean = clean.str.strip()
    return clean.str.zfill(10)


def load_igo_report_data(parquet_source: Path) -> IGOReportArtifacts:
    """Load staged AOI parquet and prepare typed report summary payload.

    Created 2026-08-18.
    Created by copilot.
    """
    log = Logger("IGO dataprep")
    overall_totals, flow_totals, source_rollup = _stream_flow_and_source_summary(parquet_source)

    totals_df = pd.DataFrame(
        [
            {
                "segment_area": overall_totals.get("segment_area", 0.0),
                "centerline_length": overall_totals.get("centerline_length", 0.0),
                "integrated_width": (overall_totals.get("integrated_width", 0.0) / overall_totals["integrated_width_count"] if overall_totals["integrated_width_count"] > 0 else 0.0),
            }
        ]
    )
    totals_df.attrs["layer_id"] = RME_LAYER_ID

    flow_df = pd.DataFrame(
        [
            {
                "centerline_length": flow_totals["perennial_length"],
                "segment_area": flow_totals["perennial_area"],
            },
            {
                "centerline_length": flow_totals["non_perennial_length"],
                "segment_area": flow_totals["non_perennial_area"],
            },
        ]
    )
    flow_df.attrs["layer_id"] = RME_LAYER_ID

    try:
        totals_df, _totals_units = RSFieldMeta().apply_units(totals_df, layer_id=RME_LAYER_ID)
        flow_df, _flow_units = RSFieldMeta().apply_units(flow_df, layer_id=RME_LAYER_ID)
    except Exception as exc:  # pragma: no cover
        log.warning(f"Unable to apply units from metadata: {exc}")

    per_length = flow_df.iloc[0]["centerline_length"]
    non_length = flow_df.iloc[1]["centerline_length"]
    per_area = flow_df.iloc[0]["segment_area"]
    non_area = flow_df.iloc[1]["segment_area"]

    precomputed_flow = {
        "length": _extract_metric_values(per_length, non_length, "km"),
        "area": _extract_metric_values(per_area, non_area, "km^2"),
        "segments": _extract_metric_values(flow_totals["perennial_count"], flow_totals["non_perennial_count"], "count"),
    }

    precomputed_source_rows: list[dict[str, object]] = []
    for project_id, details in source_rollup.items():
        precomputed_source_rows.append(
            {
                "source_rme_project": project_id,
                "huc10_codes": sorted(cast(set[str], details["huc10_codes"])),
                "project_version": str(details["project_version"]),
                "project_date": details["project_date"],
            }
        )

    card_metrics = MetricBundle(
        {
            "riverscape_area": totals_df.iloc[0]["segment_area"],
            "riverscape_length": totals_df.iloc[0]["centerline_length"],
            "avg_integrated_width": totals_df.iloc[0]["integrated_width"],
            "segments_count": int(overall_totals.get("row_count", 0.0)),
            "source_project_count": len(source_rollup),
            "perennial_count": int(flow_totals["perennial_count"]),
            "non_perennial_count": int(flow_totals["non_perennial_count"]),
        },
        layer_id=RME_LAYER_ID,
    )

    source_rows_df = pd.DataFrame(precomputed_source_rows)

    return IGOReportArtifacts(
        flow_summary=precomputed_flow,
        card_metrics=card_metrics,
        source_rows_df=source_rows_df,
    )


def _get_aoi_area_for_display(aoi_gdf: gpd.GeoDataFrame, unit_system: str) -> pint.Quantity:
    """Compute AOI area and convert it to report display units.

    Created 2026-08-19.
    Created by copilot.
    """
    aoi_area = total_aoi_area_m2(aoi_gdf)
    target_unit = "mile ** 2" if unit_system.strip().lower() == "imperial" else "kilometer ** 2"
    return aoi_area.to(target_unit)


def compute_summary_statistics(summary: IGOReportArtifacts, aoi_gdf: gpd.GeoDataFrame, unit_system: str = "SI") -> dict[str, object]:
    """Compute top-level summary values for highlight cards.

    Created 2026-08-18.
    Created by copilot.
    """
    aoi_area = _get_aoi_area_for_display(aoi_gdf, unit_system)

    card_metrics = summary.card_metrics
    total_segment_area = card_metrics["riverscape_area"]
    total_centerline_length = card_metrics["riverscape_length"]
    average_integrated_width = card_metrics["avg_integrated_width"]
    segments_count = int(cast(Any, card_metrics["segments_count"]))
    source_project_count = int(cast(Any, card_metrics["source_project_count"]))
    perennial_count = int(cast(Any, card_metrics["perennial_count"]))
    non_perennial_count = int(cast(Any, card_metrics["non_perennial_count"]))

    def _to_float_magnitude(value: object) -> float:
        raw_value = getattr(value, "magnitude", value)
        try:
            return float(cast(Any, raw_value))
        except (TypeError, ValueError):
            return 0.0

    area_ratio_pct = (cast(Any, total_segment_area) / aoi_area * 100.0) if aoi_area.magnitude > 0 else 0 * ureg("dimensionless")

    avg_area_per_length = (cast(Any, total_segment_area) / cast(Any, total_centerline_length)) if _to_float_magnitude(total_centerline_length) > 0 else 0 * ureg("meter")

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


def build_source_project_table(summary: IGOReportArtifacts, source_project_list_df: pd.DataFrame) -> pd.DataFrame:
    """Build one row per contributing RME source project.

    Columns match report requirements (HUC10, watershed name, project info, citation, link).

    Created 2026-08-18.
    Created by copilot.
    """
    output_columns = [
        "huc10_code",
        "project_name",
        "source_rme_project",
        "project_version_or_date",
        "citation",
        "project_url",
    ]

    source_rows_df = summary.source_rows_df
    if source_rows_df.empty:
        return pd.DataFrame(columns=output_columns)

    source_df = source_rows_df.copy()
    source_df["source_rme_project"] = source_df["source_rme_project"].astype(str).str.strip()
    source_df = source_df[source_df["source_rme_project"] != ""].copy()
    if source_df.empty:
        return pd.DataFrame(columns=output_columns)

    source_df["source_rme_project"] = source_df["source_rme_project"].str.lower()

    if source_project_list_df.empty:
        raise ValueError("source_project_list_df is required and cannot be empty")

    project_list_df = source_project_list_df.copy()
    if "project_id" in project_list_df.columns and "source_rme_project" not in project_list_df.columns:
        project_list_df = project_list_df.rename(columns={"project_id": "source_rme_project"})

    required_cols = {"source_rme_project", "project_name", "created_on", "project_url"}
    missing_cols = required_cols.difference(project_list_df.columns)
    if missing_cols:
        raise ValueError(f"source_project_list_df is missing required columns: {sorted(missing_cols)}")

    project_list_df = project_list_df[["source_rme_project", "project_name", "created_on", "project_url"]].copy()
    project_list_df["source_rme_project"] = project_list_df["source_rme_project"].astype(str).str.strip().str.lower()
    project_list_df["project_name"] = project_list_df["project_name"].fillna("").astype(str).str.strip()
    project_list_df.loc[project_list_df["project_name"] == "", "project_name"] = "n/a"
    project_list_df["created_on"] = project_list_df["created_on"].fillna("").astype(str).str.strip()
    project_list_df["project_url"] = project_list_df["project_url"].fillna("").astype(str).str.strip()
    project_list_df.loc[project_list_df["project_url"] == "", "project_url"] = "https://data.riverscapes.net/p/" + project_list_df.loc[project_list_df["project_url"] == "", "source_rme_project"]
    project_list_df = project_list_df.drop_duplicates(subset=["source_rme_project"], keep="last")

    out_df = source_df.merge(project_list_df, on="source_rme_project", how="left")

    stream_dates = pd.to_datetime(out_df["project_date"], errors="coerce", utc=True)
    stream_date_text = stream_dates.dt.strftime("%Y-%m-%d").fillna("")
    out_df["created_on"] = out_df["created_on"].fillna("").astype(str)
    out_df["date_value"] = out_df["created_on"].where(out_df["created_on"].str.strip() != "", stream_date_text)

    out_df["project_version"] = out_df["project_version"].fillna("").astype(str).str.strip()
    has_version = out_df["project_version"] != ""
    has_date = out_df["date_value"].str.strip() != ""

    out_df["project_version_or_date"] = "n/a"
    out_df.loc[has_date & ~has_version, "project_version_or_date"] = out_df.loc[has_date & ~has_version, "date_value"]
    out_df.loc[has_version & ~has_date, "project_version_or_date"] = "v" + out_df.loc[has_version & ~has_date, "project_version"]
    out_df.loc[has_version & has_date, "project_version_or_date"] = "v" + out_df.loc[has_version & has_date, "project_version"] + " (" + out_df.loc[has_version & has_date, "date_value"] + ")"

    def _huc_list_to_text(values: object) -> str:
        if not isinstance(values, list):
            return "n/a"
        cleaned_values = [str(v).strip() for v in values if str(v).strip()]
        return ", ".join(sorted(cleaned_values)) if cleaned_values else "n/a"

    out_df["huc10_code"] = out_df["huc10_codes"].map(_huc_list_to_text)
    out_df["citation"] = "Riverscapes Consortium RME source project " + out_df["source_rme_project"] + " (" + out_df["project_name"] + "), " + out_df["project_version_or_date"] + "."

    out_df = out_df[
        [
            "huc10_code",
            "project_name",
            "source_rme_project",
            "project_version_or_date",
            "citation",
            "project_url",
        ]
    ]

    if out_df.empty:
        return out_df

    return out_df.sort_values(by=["huc10_code", "source_rme_project"]).reset_index(drop=True)


def build_highlight_cards(card_summary: dict[str, object]) -> list[HighlightCard]:
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
        (
            "area_chart",
            "Total Area of Interest",
            _fmt(card_summary["aoi_area"], 1),
            "public",
            "AOI footprint",
            _fmt(
                card_summary["aoi_area"],
                2,
            ),
        ),
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

    cards: list[HighlightCard] = []
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
