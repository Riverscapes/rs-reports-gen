"""Retrieve data from PBR Explorer and shape for reporting"""

import json
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import pint
import pint_pandas
import requests
from rsxml import Logger

from util.figures import point_df_to_gdf
from util.pandas import RSFieldMeta

PBR_GRAPHQL_ENDPOINT = "https://api.pbr.riverscapes.net/"
PBR_PROJECTS_PAGE_SIZE = 400
PBR_PROJECTS_PAGE_FILENAME = "pbr_projects_page_{page_index:03d}.json"

SEARCH_PROJECTS_QUERY = """
query SearchProjects($limit: Int!, $offset: Int!, $searchTerms: SearchTermsInput!) {
    searchProjects(limit: $limit, offset: $offset, searchTerms: $searchTerms) {
        limit
        offset
        total
        results {
            access
            dateCreated
            dateUpdated
            name
            projectUrl
            streamName
            watershedName
            actions {
                action
                value
            }
            budget {
                usDollarVal
                items {
                    name
                    usDollarVal
                }
            }
            constructionElements
            dates {
                date
                name
            }
            extent
            geoCoding {
                continent
                country
                provState
            }
            goals
            id
            lengthKm
            location {
                geohash
                latitude
                longitude
            }
        }
    }
}
"""

PBR_PROJECTS_LAYER_ID = 'pbr_projects'
SUMMARY_METRICS_LAYER_ID = "pbr_summary_metrics"

PBR_ACTION_ENUMS: list[str] = [
    "AREA_PLANTED_HECTARES",
    "AREA_FLOODPLAIN_TREATMENT_HECTARES",
    "LEVEES_BANK_PROTECTIONS_REMOVED_KM",
    "STRUCTURES_BUILT",
    "STRUCTURES_REMOVED",
    "WOOD_ADDED",
    "TREES_FELLED",
    "IMPOUNDMENTS_REMOVED",
    "BEAVERS_TRANSLOCATED_INTRODUCED",
    "BEAVER_TRAPPING_CLOSURE",
    "STRUCTURES_MAINTAINED",
    "EXCLOSURE_FENCING",
]


def _page_cache_file(staging_path: Path, page_index: int) -> Path:
    """Return the page-cache filename for a paginated API response. Created by copilot."""
    return staging_path / PBR_PROJECTS_PAGE_FILENAME.format(page_index=page_index)


def _clear_page_cache_files(staging_path: Path) -> None:
    """Remove stale paged cache files from a previous run. Created by copilot."""
    for stale_file in staging_path.glob("pbr_projects_page_*.json"):
        stale_file.unlink(missing_ok=True)


def _write_page_cache(staging_path: Path, page_index: int, page_payload: dict[str, Any]) -> None:
    """Persist one page of API results to staged JSON cache. Created by copilot."""
    page_file = _page_cache_file(staging_path, page_index)
    with page_file.open("w", encoding="utf-8") as f:
        json.dump(page_payload, f, indent=2)


def _load_cached_project_pages(local_path: Path) -> list[dict[str, Any]]:
    """Load cached project rows from staged page JSON files. Created by copilot."""
    projects: list[dict[str, Any]] = []
    if not local_path.is_dir():
        raise ValueError(f"Expected a staging directory of paged JSON files, got: {local_path}")

    page_files = sorted(local_path.glob("pbr_projects_page_*.json"))
    if not page_files:
        raise FileNotFoundError(f"No paged cache files found in {local_path}")

    for page_file in page_files:
        with page_file.open(encoding="utf-8") as f:
            payload = json.load(f)
        projects.extend(payload.get("results", []))

    return projects


def define_fields(unit_system: str = "SI") -> None:
    """
    Load metadata for the PBR projects dataset, set display-unit preferences
    """
    log = Logger("Define Fields")
    source_path = Path(__file__).parent / 'pbr_project_meta.csv'
    log.info(f"Loading metadata from {source_path}")
    registry_field_meta = pd.read_csv(source_path)
    field_meta = RSFieldMeta()
    field_meta.field_meta = registry_field_meta
    field_meta.unit_system = unit_system


def fetch_pbr_projects(
    output_path: Path | None = None,
    search_terms: dict[str, Any] | None = None,
    page_size: int = PBR_PROJECTS_PAGE_SIZE,
) -> list[dict[str, Any]] | None:
    """Fetch projects from the PBR GraphQL API and optionally write staged page JSON files.

    Args:
        output_path: Staging directory where page JSON files should be written.
        search_terms: Search terms payload mapped to the GraphQL ``SearchTermsInput``.
            Typical keys include ``bbox``, ``continent``, ``country``, ``geohash``,
            ``orgAffiliateId``, ``provState``, and ``textSearch``.

    """
    log = Logger("Fetch PBR projects")
    headers = {"Content-Type": "application/json"}
    projects: list[dict[str, Any]] = []
    offset = 0
    page_index = 1
    total_results: int | None = None

    # Avoid mutating caller-provided dicts and drop null values.
    resolved_search_terms = dict(search_terms) if search_terms else {}
    resolved_search_terms = {k: v for k, v in resolved_search_terms.items() if v is not None}

    if output_path is not None:
        output_path.mkdir(parents=True, exist_ok=True)
        _clear_page_cache_files(output_path)

    while True:
        variables = {
            "limit": page_size,
            "offset": offset,
            "searchTerms": resolved_search_terms,
        }
        payload = {
            "query": SEARCH_PROJECTS_QUERY,
            "variables": variables,
        }
        log.info(f"Querying PBR GraphQL API at {PBR_GRAPHQL_ENDPOINT} (offset={offset}, limit={page_size}) ...")
        response = requests.post(PBR_GRAPHQL_ENDPOINT, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        if "errors" in data:
            log.error("GraphQL errors:", data["errors"])
            return None

        page_data = data.get("data", {}).get("searchProjects", {})
        page_results = page_data.get("results", [])
        if total_results is None:
            total_results = int(page_data.get("total", 0))

        if not page_results:
            break

        projects.extend(page_results)

        if output_path is not None:
            _write_page_cache(
                output_path,
                page_index,
                {
                    "limit": page_data.get("limit"),
                    "offset": page_data.get("offset"),
                    "total": page_data.get("total"),
                    "results": page_results,
                },
            )

        log.info(f"Fetched page {page_index} with {len(page_results)} projects.")
        page_index += 1
        offset += len(page_results)

        if total_results is not None and offset >= total_results:
            break
        if len(page_results) < page_size:
            break

    log.info(f"Fetched {len(projects)} projects total.")
    log.info(f"Saved paged project cache to {output_path}")
    return projects


def parse_project_data(projects_json: list[dict[str, Any]]) -> pd.DataFrame:
    """Take the JSON payload from the API and convert to dataframe for analysis"""
    if not projects_json:
        return pd.DataFrame()
    projects_df = pd.json_normalize(projects_json)
    projects_df.attrs["layer_id"] = PBR_PROJECTS_LAYER_ID
    return projects_df


def load_cached_pbr_data(local_path: Path) -> pd.DataFrame:
    """Load staged page JSON files from disk instead of querying the API.

    The returned DataFrame includes a ``fetch_status`` attribute:
    - ``ok``: one or more cached project rows were loaded.
    - ``no_results``: cache files were valid but contained zero project rows.
    """
    projects = _load_cached_project_pages(local_path)
    projects_df = parse_project_data(projects)
    projects_df.attrs["fetch_status"] = "ok" if len(projects) > 0 else "no_results"
    return projects_df


def get_gdf_bbox(gdf: gpd.GeoDataFrame) -> dict[str, float]:
    """Build a GraphQL ``BboxInput`` dictionary from a GeoDataFrame extent.
    Ref https://github.com/Riverscapes/pbr-explorer-monorepo/blob/dev/schemas/gql/Queries.gql
    """
    if gdf.empty:
        raise ValueError("Cannot compute bounding box from an empty GeoDataFrame.")
    if gdf.geometry is None:
        raise ValueError("GeoDataFrame has no active geometry column.")
    if gdf.crs is None:
        raise ValueError("GeoDataFrame CRS is undefined; cannot convert extent to WGS84.")

    # API expects longitude/latitude values; normalize to WGS84 when needed.
    gdf_wgs84 = gdf if gdf.crs and gdf.crs.to_epsg() == 4326 else gdf.to_crs(epsg=4326)
    min_lng, min_lat, max_lng, max_lat = gdf_wgs84.total_bounds

    return {
        "minLng": float(min_lng),
        "minLat": float(min_lat),
        "maxLng": float(max_lng),
        "maxLat": float(max_lat),
    }


def geofilter_projects(projects: list[dict[str, Any]], aoi: gpd.GeoDataFrame) -> list[dict[str, Any]]:
    """Filter projects by required point location intersecting AOI. Created by copilot."""
    if not projects:
        return []
    if aoi.empty:
        return []
    if aoi.geometry is None:
        raise ValueError("AOI GeoDataFrame has no active geometry column.")

    if aoi.crs is None:
        raise ValueError("AOI GeoDataFrame CRS is undefined; expected a geographic CRS.")

    aoi_wgs84 = aoi if aoi.crs.to_epsg() == 4326 else aoi.to_crs(epsg=4326)
    aoi_geom = aoi_wgs84.union_all()
    if aoi_geom is None or aoi_geom.is_empty:
        return []

    projects_df = pd.json_normalize(projects)
    points_gdf = point_df_to_gdf(projects_df, "location.latitude", "location.longitude")
    if points_gdf.empty:
        return []

    intersection_mask = points_gdf.geometry.intersects(aoi_geom)
    matching_indexes = points_gdf.index[intersection_mask]
    return [projects[int(idx)] for idx in matching_indexes]


def load_live_pbr_data(output_path: Path, query_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Query the PBR API for projects within the query_gdf polygon and return parsed project rows as a DataFrame.

    The returned DataFrame includes a ``fetch_status`` attribute to distinguish
    successful and unsuccessful empty results:
    - ``ok``: one or more project rows were returned.
    - ``no_results``: API request succeeded but matched zero projects.
    - ``error``: API request failed (for example, GraphQL errors).
    """
    log = Logger("Load PBR Projects")
    bbox = get_gdf_bbox(query_gdf)
    search_terms = {"bbox": bbox}
    projects = fetch_pbr_projects(output_path=output_path, search_terms=search_terms)
    if projects is None:
        projects_df = pd.DataFrame()  # empty data frame
        projects_df.attrs["fetch_status"] = "error"
    elif len(projects) == 0:
        projects_df = pd.DataFrame()  # empty data frame
        projects_df.attrs["fetch_status"] = "no_results"
    else:
        projects = _load_cached_project_pages(output_path)
        filtered_projects = geofilter_projects(projects, query_gdf)
        projects_df = parse_project_data(filtered_projects)
        log.debug(f"After geo-filtering and parsing, main dataframe has shape {projects_df.shape}.")
        projects_df.attrs["fetch_status"] = "ok"
    return projects_df


# =======================================
# POST-FETCH DATA PREPARATION FUNCTIONS
# =======================================


def _ensure_summary_metric_metadata() -> None:
    """Add metadata for computed summary metrics when missing."""
    meta = RSFieldMeta()
    length_field_meta = meta.get_field_meta("lengthKm", layer_id=PBR_PROJECTS_LAYER_ID)
    length_data_unit = length_field_meta.data_unit if length_field_meta and length_field_meta.data_unit else "kilometer"

    if meta.get_field_meta("number_of_projects", layer_id=SUMMARY_METRICS_LAYER_ID) is None:
        meta.add_field_meta(
            name="number_of_projects",
            layer_id=SUMMARY_METRICS_LAYER_ID,
            friendly_name="Number of Projects",
            dtype="INT",
            data_unit="count",
            description="Projects returned by PBR Explorer for this run.",
            preferred_format="{value:,.0f}",
        )

    if meta.get_field_meta("total_budget_usd", layer_id=SUMMARY_METRICS_LAYER_ID) is None:
        meta.add_field_meta(
            name="total_budget_usd",
            layer_id=SUMMARY_METRICS_LAYER_ID,
            friendly_name="Total Budget",
            dtype="FLOAT",
            description="Sum of reported PBR project budgets (USD).",
            preferred_format="$ {value:,.2f}",
        )

    if meta.get_field_meta("total_treatment_length", layer_id=SUMMARY_METRICS_LAYER_ID) is None:
        meta.add_field_meta(
            name="total_treatment_length",
            layer_id=SUMMARY_METRICS_LAYER_ID,
            friendly_name="Total Treatment Length",
            dtype="FLOAT",
            data_unit=length_data_unit,
            description="Sum of all treatment lengths reported by projects.",
            preferred_format="{value:,.2f}",
        )


def build_summary_metrics(data_df: pd.DataFrame) -> dict[str, object]:
    """Build summary metrics that can be rendered by util.figures.metric_cards."""
    _ensure_summary_metric_metadata()
    if data_df.empty:
        raise ValueError("No data returned from PBR API; summary metrics are not available.")

    meta = RSFieldMeta()
    length_field_meta = meta.get_field_meta("lengthKm", layer_id=PBR_PROJECTS_LAYER_ID)
    length_data_unit = length_field_meta.data_unit if length_field_meta and length_field_meta.data_unit else "kilometer"

    project_count = len(data_df)

    budget_series = pd.to_numeric(data_df["budget.usDollarVal"], errors="coerce")
    total_budget_usd = float(budget_series.sum())

    length_series = data_df["lengthKm"]
    if isinstance(length_series.dtype, pint_pandas.PintType):
        total_treatment_length = length_series.sum()
        try:
            total_treatment_length = total_treatment_length.to(length_data_unit)
        except Exception:
            pass
    else:
        numeric_length_series = pd.to_numeric(length_series, errors="coerce")
        total_length = float(numeric_length_series.sum())
        total_treatment_length = pint.Quantity(total_length, length_data_unit)

    return {
        "number_of_projects": project_count,
        "total_budget_usd": total_budget_usd,
        "total_treatment_length": total_treatment_length,
    }


# =======================================
# ACTION COLUMN PARSING & METRICS
# =======================================


def parse_actions_to_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Expand the nested ``actions`` list-of-dicts column into one column per action enum.

    Each row may have zero or more ``{action, value}`` entries.  This function creates
    a new column for each known action enum (default ``pd.NA``), populates matching
    rows with the corresponding numeric value, and drops the original ``actions`` column.

    Created by copilot.
    """
    if df.empty:
        return df
    if "actions" not in df.columns:
        return df

    for action_enum in PBR_ACTION_ENUMS:
        if action_enum not in df.columns:
            df[action_enum] = pd.NA

    for idx, actions_list in df["actions"].items():
        if not isinstance(actions_list, list):
            continue
        for entry in actions_list:
            if not isinstance(entry, dict):
                continue
            action_name = entry.get("action")
            value = entry.get("value")
            if action_name and action_name in PBR_ACTION_ENUMS and value is not None:
                df.at[idx, action_name] = value

    df = df.drop(columns=["actions"])
    return df


def _ensure_actions_metric_metadata() -> None:
    """Register metadata for each action enum under the summary-metrics layer.

    This allows ``metric_cards()`` to resolve friendly names, units, and format
    strings for action aggregates without requiring a separate DataFrame.

    Created by copilot.
    """
    meta = RSFieldMeta()
    for action_enum in PBR_ACTION_ENUMS:
        field_meta = meta.get_field_meta(action_enum, layer_id=PBR_PROJECTS_LAYER_ID)
        if field_meta is None:
            continue
        if meta.get_field_meta(action_enum, layer_id=SUMMARY_METRICS_LAYER_ID) is None:
            meta.add_field_meta(
                name=action_enum,
                layer_id=SUMMARY_METRICS_LAYER_ID,
                friendly_name=field_meta.friendly_name,
                data_unit=field_meta.data_unit,
                dtype=field_meta.dtype,
                description=field_meta.description,
                preferred_format=field_meta.preferred_format,
            )


def build_actions_metrics(data_df: pd.DataFrame) -> dict[str, object]:
    """Sum each action column into a dict suitable for ``metric_cards()``.

    Returns:
        ``{action_enum: pint.Quantity | float}`` for every action with a non-zero
        sum.  Empty actions and actions not present in the DataFrame are omitted.

    Created by copilot.
    """
    results: dict[str, object] = {}
    for action_enum in PBR_ACTION_ENUMS:
        if action_enum not in data_df.columns:
            continue
        series = data_df[action_enum].dropna()
        if len(series) == 0:
            continue
        total = series.sum()
        # skip actions whose total is effectively zero
        if isinstance(total, pint.Quantity):
            if total.magnitude == 0:
                continue
        elif total == 0:
            continue
        results[action_enum] = total
    return results


def count_projects_with_actions(data_df: pd.DataFrame) -> dict[str, int]:
    """Count how many projects have a non-null value for each action enum.

    Created by copilot.
    """
    counts: dict[str, int] = {}
    for action_enum in PBR_ACTION_ENUMS:
        if action_enum not in data_df.columns:
            continue
        count = int(data_df[action_enum].dropna().shape[0])
        if count > 0:
            counts[action_enum] = count
    return counts
