"""Helpers to work with the Climate Engine API for reporting platform
* get data
* get reports
* generate figures

Requires an API key - either pass to the functions or (preferably) set in environment variable CLIMATE_ENGINE_API_KEY
"""

import json
import os

import geopandas as gpd
import pandas as pd
import requests
from rsxml import Logger

CLIMATE_ENGINE_BASE_API_URL = "https://api.climateengine.org"


def extract_coordinates(gdf: gpd.GeoDataFrame) -> list:
    """
    Extracts coordinates from a GeoDataFrame for the Climate Engine API.
    https://support.climateengine.org/article/152-formatting-coordinates-for-api-requests
    Handles Point, Polygon, and MultiPolygon with correct nesting.
    """
    # Ensure coordinates are in WGS84 (Lon/Lat) as APIs usually require
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    def _to_ce_format(geom):
        coords = geom.__geo_interface__["coordinates"]
        if geom.geom_type == "Point":
            return [coords]
        elif geom.geom_type == "MultiPoint":
            # Always return flat list of points
            return [list(pt) for pt in coords]
        elif geom.geom_type == "Polygon":
            return coords
        elif geom.geom_type == "MultiPolygon":
            return [ring for polygon_rings in coords for ring in polygon_rings]
        else:
            return coords

    coord_list = gdf.geometry.map(_to_ce_format).to_list()

    # Check for geometry types to determine final outer nesting rules
    is_point_only = all(gdf.geometry.geom_type == "Point")
    is_multipoint_only = all(gdf.geometry.geom_type == "MultiPoint")
    is_polygon_multi = all(gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"]))

    # For a single Point, unwrap to match expected [[lon, lat]]
    if len(coord_list) == 1 and gdf.iloc[0].geometry.geom_type == "Point":
        return coord_list[0]
    # For multiple Points, flatten to [[lon, lat], ...]
    if is_point_only:
        return [coords[0] for coords in coord_list]

    # For a single MultiPoint, unwrap to match expected [ [lon, lat], ... ]
    if len(coord_list) == 1 and gdf.iloc[0].geometry.geom_type == "MultiPoint":
        return coord_list[0]

    # For multiple MultiPoints, flatten the list
    if is_multipoint_only:
        # coord_list is a list of lists of points
        return [pt for sublist in coord_list for pt in sublist]

    # For both single/multiple Polygons and MultiPolygons, return flat list of rings
    if is_polygon_multi:
        return [ring for sublist in coord_list for ring in sublist]

    return coord_list


def query_climate_engine(url: str, payload: dict, api_key: str | None = None, timeout: int = 120) -> dict:
    """Sends a POST request to the Climate Engine API and returns the JSON response
    raises:
    """
    log = Logger("Query Climate Engine")
    api_key = get_ce_api_key(api_key)

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    log.info(f"Query url: {url}")
    # log.debug(f"Query payload: {payload}")  # because the payload includes the geometry this can be quite large
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)
        response.raise_for_status()
        # Parse successful response
        content = response.json()
        return content.get("Data", None)
    except requests.exceptions.ReadTimeout as e:
        log.error("Query timeout (limit {timeout})")
        raise e
    except requests.exceptions.RequestException as e:
        # Catches HTTPError (4xx, 5xx), ConnectionError, etc.
        log.error(f"Request failed: {e}")
        log.debug(f"Reponse: {response.json()}")
        raise e
    except Exception as e:
        log.error(f"Unexpected error {e}")
        raise e


def get_ce_api_key(api_key: str | None = None) -> str:
    """retrieves the CLIMATE ENGINE API key from the explicit argument or environment variable.
    Raises ValueError if not found.
    This helper centralizes the logic for finding the key.

    This function relies on `os.environ`. Ensure `load_dotenv()` has been called
    in your application entry point or notebook if relying on a .env file.

    Args:
        api_key: Optional explicit key.

    Returns:
        str: The API key.

    Raises:
        ValueError: If key is not found in arg or environment.
    """

    # 1. Prefer explicit argument (Common in notebooks/testing)
    if api_key:
        return api_key
    # 2. Check environment (Populated by CLI env vars or load_dotenv)
    key = os.getenv("CLIMATE_ENGINE_API_KEY")

    if not key:
        raise ValueError("CLIMATE ENGINE API Key not found. Please provide it as an argument or set the 'CLIMATE_ENGINE_API_KEY' environment variable.")

    return key


def get_vegetation_cover_timeseries(aoi_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """query climate engine for RAP vegetation cover timeseries data for aoi"""
    url = f"{CLIMATE_ENGINE_BASE_API_URL}/timeseries/native/coordinates"
    coords = extract_coordinates(aoi_gdf)
    # dataset options: https://docs.climateengine.org/docs/build/html/datasets.html
    dataset = "RAP_COVER"  # RAP Cover - 30m - Yearly https://support.climateengine.org/article/81-rap
    # this comes from https://api.climateengine.org/metadata/dataset_variables?dataset=RAP_COVER
    variables = ["AFG", "PFG", "SHR", "TRE", "BGR", "LTR"]

    params = {
        "coordinates": json.dumps(coords),
        "user_email": "lorin@northarrowresearch.com",
        "area_reducer": "mean",
        "dataset": dataset,
        "variable": ",".join(variables),
        "compute_trends": "yes",
        "start_date": "2021-01-01",
        "end_date": "2025-12-31",
    }
    results = query_climate_engine(url, params, timeout=240)  # this raises error - wrap in try catch
    # print(results) # just when debugging
    # the format of results is list with dict with 'Metadata' and 'Data' keys, Data is list of dicts with Date and variable keys
    # but they aren't the exactly the same variable names as we supplied -- has units of measure as well (e.g. "tmmn (C°)").
    _sample_results = [
        {
            "Metadata": {
                "DRI_OBJECTID": "[-121.61, 38.78]",
                "Statistic over region": "mean",
            },
            "Data": [
                {"Date": "2018-01-01", "AFG (%)": 0.876, "PFG (%)": 7.2494},
                {"Date": "2019-01-01", "AFG (%)": 1.1776, "PFG (%)": 6.7425},
            ],
        }
    ]
    df = pd.DataFrame(results[0]["Data"])
    return df


def vegetation_cover_timeseries_charts(df: pd.DataFrame):
    """
    Creates a set of line charts for vegetation cover timeseries using Plotly Express.
    Returns a Plotly Figure object with subplots (one for each variable).
    """
    import plotly.express as px

    # Clean up column names to match our variable map keys
    # The API returns columns like "AFG (%)", "PFG (%)", etc.
    # We want to map these to full names used in the titles.
    # Define the mapping from code to full name
    variable_map = {
        "AFG": "Annual Forb and Grass Cover",
        "PFG": "Perennial Forb and Grass Cover",
        "SHR": "Shrub Cover",
        "TRE": "Tree Cover",
        "BGR": "Bare Ground Cover",
        "LTR": "Litter Cover",
    }

    # Find the matching columns in the dataframe
    df_renamed = df.copy()

    # Create dictionary to rename specific columns like 'AFG (%)' -> 'Annual Forb and Grass Cover'
    rename_cols = {}

    # We need to handle that the column names might have units like "AFG (%)" or "AFG"
    # We iterate over the columns in the dataframe and see if they start with our known codes.
    found_vars = []

    for col in df.columns:
        # Skip 'Date' column
        if col.lower() == "date":
            continue

        for code, full_name in variable_map.items():
            # Check if column starts with the code (e.g. "AFG" in "AFG (%)")
            if col.startswith(code):
                rename_cols[col] = full_name
                found_vars.append(full_name)
                break

    if not rename_cols:
        print(
            "Warning: No matching vegetation variables found in dataframe columns:",
            df.columns,
        )
        return None

    # Rename columns
    df_renamed = df_renamed.rename(columns=rename_cols)

    # Only keep Date + found variables
    target_cols = ["Date"] + found_vars
    # Use intersection to be safe against missing Date or other issues, though Date is expected
    available_cols = [c for c in target_cols if c in df_renamed.columns]
    df_subset = df_renamed[available_cols]

    # Melt to long format: Date, Variable, Value
    df_melted = df_subset.melt(id_vars=["Date"], var_name="Variable", value_name="Cover (%)")

    # Create line chart with facets (subplots)
    # Use facet_col and facet_col_wrap to create a grid
    fig = px.line(
        df_melted,
        x="Date",
        y="Cover (%)",
        facet_col="Variable",
        facet_col_wrap=2,  # 2 columns, 3 rows
        title="Vegetation Cover Timeseries (RAP)",
        height=900,
        markers=True,
        # Ensure y-axis range is fixed 0-100 for all
        range_y=[0, 100],
    )

    # Improve layout
    # matches='y' forces all y-axes to share the same axis (and range)
    # matches='x' forces all x-axes to share the same axis (and range)
    fig.update_yaxes(matches="y")
    fig.update_xaxes(matches="x")

    # Improve titles by removing "Variable=" prefix
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    return fig
