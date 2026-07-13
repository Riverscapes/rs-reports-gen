"""Retrieve data from PBR Explorer and shape for reporting"""

import json
from pathlib import Path

import pandas as pd
import requests
from rsxml import Logger

from util.pandas import RSFieldMeta

PBR_GRAPHQL_ENDPOINT = "https://api.pbr.riverscapes.net/"

SEARCH_PROJECTS_QUERY = """
query SearchProjects {
    searchProjects(limit: 400, offset: 0, searchTerms: { textSearch: "" }) {
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


def define_fields(unit_system: str = "SI") -> None:
    """
    Load metadata for the PBR projects dataset, set display-unit preferences
    """
    log = Logger("Define Fields")
    source_path = Path('pbr_project_meta.csv')
    log.info(f"Loading metadata from {source_path}")
    registry_field_meta = pd.read_csv(source_path)
    field_meta = RSFieldMeta()
    field_meta.field_meta = registry_field_meta
    field_meta.unit_system = unit_system


def fetch_pbr_projects() -> str | None:
    """This function fetches projects from the PBR GraphQL API and saves them to a JSON file.

    Args:
        output_path (Optional[str], optional): The path to the output JSON file. Defaults to None.
    """
    log = Logger("Fetch PBR projects")
    headers = {"Content-Type": "application/json"}
    payload = {"query": SEARCH_PROJECTS_QUERY}
    log.info(f"Querying PBR GraphQL API at {PBR_GRAPHQL_ENDPOINT} ...")
    response = requests.post(PBR_GRAPHQL_ENDPOINT, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    if "errors" in data:
        log.error("GraphQL errors:", data["errors"])
        return
    projects = data["data"]["searchProjects"]["results"]
    log.info(f"Fetched {len(projects)} projects.")

    return projects


def parse_project_data(projects_json: str) -> pd.DataFrame:
    """Take the JSON payload from the API and convert to dataframe for analysis"""
    # load json
    # attach layerid to dataframe attrs


def load_cached_pbr_data(local_path: Path) -> pd.DataFrame:
    """Load data from local instead of fetching from API"""
    with open(local_path) as f:
        projects = json.load(f)
    projects_df = parse_project_data(projects)
    return projects_df


def load_live_pbr_data(output_path: Path) -> pd.DataFrame:
    """TODO: how to handle failurs - is empty DF really what we want?"""
    log = Logger("Load PBR Projects")
    projects = fetch_pbr_projects()
    if not projects:
        projects_df = pd.DataFrame()  # empty data frame
    else:
        if output_path is not None:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(projects, f, indent=2)
            log.info(f"Saved project list to {output_path}")
        # TODO when do we load metadata?
        projects_df = parse_project_data(projects)
    return projects_df
