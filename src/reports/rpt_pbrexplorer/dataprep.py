"""Retrieve data from PBR Explorer and shape for reporting"""

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pint
import pint_pandas
import requests
from rsxml import Logger

from util.pandas import RSFieldMeta

PBR_GRAPHQL_ENDPOINT = "https://api.pbr.riverscapes.net/"
PBR_PROJECTS_PAGE_SIZE = 400
PBR_PROJECTS_PAGE_FILENAME = "pbr_projects_page_{page_index:03d}.json"

SEARCH_PROJECTS_QUERY = """
query SearchProjects($limit: Int!, $offset: Int!) {
    searchProjects(limit: $limit, offset: $offset, searchTerms: { textSearch: "" }) {
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
            draft
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
    page_size: int = PBR_PROJECTS_PAGE_SIZE,
) -> list[dict[str, Any]] | None:
    """Fetch projects from the PBR GraphQL API and optionally write staged page JSON files.

    Args:
        output_path: Staging directory where page JSON files should be written.
    """
    log = Logger("Fetch PBR projects")
    headers = {"Content-Type": "application/json"}
    projects: list[dict[str, Any]] = []
    offset = 0
    page_index = 1
    total_results: int | None = None

    if output_path is not None:
        output_path.mkdir(parents=True, exist_ok=True)
        _clear_page_cache_files(output_path)

    while True:
        payload = {
            "query": SEARCH_PROJECTS_QUERY,
            "variables": {"limit": page_size, "offset": offset},
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
    return projects


def parse_project_data(projects_json: list[dict[str, Any]]) -> pd.DataFrame:
    """Take the JSON payload from the API and convert to dataframe for analysis"""
    if not projects_json:
        return pd.DataFrame()
    projects_df = pd.json_normalize(projects_json)
    projects_df.attrs["layer_id"] = PBR_PROJECTS_LAYER_ID
    return projects_df


def load_cached_pbr_data(local_path: Path) -> pd.DataFrame:
    """Load data from local instead of fetching from API"""
    projects = _load_cached_project_pages(local_path)
    projects_df = parse_project_data(projects)
    return projects_df


def load_live_pbr_data(output_path: Path) -> pd.DataFrame:
    """TODO: how to handle failurs - is empty DF really what we want?"""
    log = Logger("Load PBR Projects")
    projects = fetch_pbr_projects(output_path=output_path)
    if not projects:
        projects_df = pd.DataFrame()  # empty data frame
    else:
        if output_path is not None:
            projects = _load_cached_project_pages(output_path)
            log.info(f"Saved paged project cache to {output_path}")
        # TODO when do we load metadata?
        projects_df = parse_project_data(projects)
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
