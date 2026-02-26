"""
Module: climate_engine_api
Purpose: POC for querying climate engine using coordinates from GeoDataFrame
Lorin Gaertner, Feb 2026
Typestring: Python module, Copilot-assisted
Some of the code also came from https://github.com/Riverscapes/QRiS/blob/6dc21763c07402ba5dcf1c2757ebacdfb1780901/src/lib/climate_engine.py
"""

import json
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon
from lonboard import viz
import requests
from dotenv import load_dotenv
from rsxml import Logger, dotenv


# call climate engine api and get report for this area
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
        # Get the standard GeoJSON 'coordinates' list
        coords = geom.__geo_interface__['coordinates']

        # Climate Engine Point requires [[lon, lat]] (GeoJSON is [lon, lat])
        if geom.geom_type == 'Point':
            return [coords]

        # For Polygons and MultiPolygons, the GeoJSON 'coordinates'
        # already matches the Climate Engine nesting requirement perfectly.
        return coords

    # .map() is faster than a standard 'for' loop in Python
    coord_list = gdf.geometry.map(_to_ce_format).to_list()

    # Handle the API's polymorphic input requirements based on the examples provided:
    # Multiple Points: [[-121,38], [-122,39]] -> This matches coord_list structure for Points.
    # Single Point: [[-121,38]]. This also matches coord_list structure if length is 1.

    # Polygon Example from docs: [[[x,y]...]] (Depth 3)
    # GeoJSON Polygon coords: [[[x,y]...]] (Depth 3)
    # coord_list for 1 Polygon: [[[[x,y]...]]]] (Depth 4) -> Too deep!

    # Multiple Polygons Example: [[[x,y]...], [[x,y]...]] (Depth 4)
    # coord_list for 2 Polygons: [[[[x,y]...]], [[[x,y]...]]]] (Depth 4) -> Matches!

    # Conclusion:
    # If it's a list of POINTS, the standard aggregation works for both 1 and N points.
    # If it's a list of POLYGONS, the standard aggregation works for N polygons,
    # BUT for 1 polygon, we must unwrap it to avoid the extra list layer.

    if len(coord_list) == 1 and gdf.iloc[0].geometry.geom_type != 'Point':
        return coord_list[0]

    return coord_list


def query_climate_engine(
        url: str,
        payload: dict,
        api_key: str | None = None,
        timeout: int = 120
) -> dict | None:
    """Sends a POST request to the Climate Engine API and returns the JSON response or None if error"""
    log = Logger('Query Climate Engine')
    api_key = get_ce_api_key(api_key)

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    # TODO WARNING this includes the key - do NOT print this in production / logs!
    print(f"Sending to url:{url}\n payload: {payload}\n headers: {headers}\n\n")
    print("---"*20)
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)
        response.raise_for_status()
        # Parse successful response
        content = response.json()
        return content.get('Data', None)
    except requests.exceptions.ReadTimeout:
        log.error("Query timeout (limit {timeout})")
        return None
    except requests.exceptions.RequestException as e:
        # Catches HTTPError (4xx, 5xx), ConnectionError, etc.
        log.error(f"Request failed: {e}")
    except Exception as e:
        log.error(f"Unexpected error {e}")
        return None


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
    key = os.getenv('CLIMATE_ENGINE_API_KEY')

    if not key:
        raise ValueError(
            "CLIMATE ENGINE API Key not found. Please provide it as an argument "
            "or set the 'CLIMATE_ENGINE_API_KEY' environment variable."
        )

    return key


def getDroughtReport(aoi_gdf: gpd.GeoDataFrame, site_name: str, site_description: str, api_key: str | None = None) -> str:
    """Request a drought report for specific area. 
    This just prints the output, which will include a link to the zip file on google storage
    worked 2026-02-25 
    """
    log = Logger('Get Drought Report')
    api_key = get_ce_api_key(api_key)
    endpoint = '/reports/drought/coordinates'  # need more than one minute to get result on this one
    params = {
        'simplify_geometry': 'None',
        'user_email': 'lorin@northarrowresearch.com',
        'site_name': site_name,
        'site_description': site_description,
        'mask_landcover': 'False',
        'mask_ownership': 'None'}
    coords = extract_coordinates(aoi_gdf)

    payload = {"coordinates": json.dumps(coords)} | params
    url = CLIMATE_ENGINE_BASE_API_URL + endpoint
    result = query_climate_engine(url, payload, api_key=api_key, timeout=240)
    if not result:
        return ''
    print(result)
    # A successful result will look like
    _sample_successful_result = {'Email': 'lorin@northarrowresearch.com',
                                 'Message': 'Report generated successfully!',
                                 'Report link': 'https://storage.googleapis.com/reports-drought/custom/reports/2ff7689c-1520-47c2-9e84-a372641c53c4/althouse_small_selection_1.zip',
                                 'Site description': 'example from rpt_rivers_need_space', 'Site name': 'althouse_small_selection_1', 'Site type': ''}
    # Check for "soft" errors where HTTP is 200 but the body contains an error message
    msg = result.get('Message', '')
    log.info(msg)
    if 'Server Error' in msg:
        print(f"\n❌ API returned a 200 OK, but the report generation failed internally:\n{msg}")

    link = result.get('Report link', '')
    return link


def get_dataset_date_range(dataset: str, api_key: str | None = None) -> dict:
    """
    get dataset for range of dates
    adapted from qris
    """
    api_key = get_ce_api_key(api_key)

    url = f'{CLIMATE_ENGINE_BASE_API_URL}/metadata/dataset_dates'

    params = {'dataset': dataset}
    result = query_climate_engine(url, params, api_key=api_key)

    return result


def get_vegetation_cover_timeseries(aoi_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """query climate engine for RAP vegetation cover timeseries data for aoi"""
    url = f'{CLIMATE_ENGINE_BASE_API_URL}/timeseries/native/coordinates'
    coords = extract_coordinates(aoi_gdf)
    # dataset options: https://docs.climateengine.org/docs/build/html/datasets.html
    dataset = 'RAP_COVER'  # RAP Cover - 30m - Yearly https://support.climateengine.org/article/81-rap
    # this comes from https://api.climateengine.org/metadata/dataset_variables?dataset=RAP_COVER
    variables = [
        "AFG",
        "PFG",
        "SHR",
        "TRE",
        "BGR",
        "LTR"
    ]
    variable_names = [
        "Annual Forb and Grass Cover",
        "Perennial Forb and Grass Cover",
        "Shrub Cover",
        "Tree Cover",
        "Bare Ground Cover",
        "Litter Cover"
    ]
    params = {
        'coordinates': json.dumps(coords),
        'user_email': 'lorin@northarrowresearch.com',
        'area_reducer': 'mean',
        'dataset': dataset,
        'variable': ','.join(variables),
        'compute_trends': 'yes',
        'start_date': '1986-01-01',
        'end_date': '2025-12-31'
    }
    results = query_climate_engine(url, params)
    print(results)
    # the format of results is list with dict with 'Metadata' and 'Data' keys, Data is list of dicts with Date and variable keys
    # but they aren't the exactly the same variable names as we supplied -- has units of measure as well (e.g. "tmmn (C°)").
    _sample_results = [
        {
            'Metadata': {
                'DRI_OBJECTID': '[-121.61, 38.78]',
                'Statistic over region': 'mean'
            },
            'Data': [
                {
                    'Date': '2018-01-01',
                    'AFG (%)': 0.876,
                    'PFG (%)': 7.2494
                },
                {
                    'Date': '2019-01-01',
                    'AFG (%)': 1.1776,
                    'PFG (%)': 6.7425
                }
            ]
        }
    ]
    _real_results = [{'Metadata': {'DRI_OBJECTID': '[[-123.50318841365818, 42.0636074852479], [-123.50492080174492, 42.06127663582211], [-123.50085756423239, 42.056709430866185], [-123.49862120870225, 42.04726004130219], [-123.49137667670318, 42.039007574416296], [-123.48847886390357, 42.038692594764164], [-123.48006890719161, 42.054063601788265], [-123.48658898599076, 42.05967023959624], [-123.49629035927646, 42.06338699949141], [-123.50318841365818, 42.0636074852479]]', 'Statistic over region': 'mean'}, 'Data': [{'Date': '1986-01-01', 'AFG (%)': 0.876, 'PFG (%)': 7.2494, 'SHR (%)': 6.6305, 'TRE (%)': 52.2582, 'BGR (%)': 1.4651, 'LTR (%)': 4.0706}, {'Date': '1987-01-01', 'AFG (%)': 1.1776, 'PFG (%)': 6.7425, 'SHR (%)': 3.9011, 'TRE (%)': 60.161, 'BGR (%)': 1.6142, 'LTR (%)': 4.2602}, {'Date': '1988-01-01', 'AFG (%)': 1.0353, 'PFG (%)': 8.5353, 'SHR (%)': 3.5827, 'TRE (%)': 56.1857, 'BGR (%)': 1.2888, 'LTR (%)': 4.2772}, {'Date': '1989-01-01', 'AFG (%)': 1.1488, 'PFG (%)': 7.4465, 'SHR (%)': 3.8304, 'TRE (%)': 44.3561, 'BGR (%)': 1.4411, 'LTR (%)': 3.9646}, {'Date': '1990-01-01', 'AFG (%)': 0.2324, 'PFG (%)': 3.1125, 'SHR (%)': 1.9458, 'TRE (%)': 63.5686, 'BGR (%)': 1.1027, 'LTR (%)': 3.9664}, {'Date': '1991-01-01', 'AFG (%)': 0.4339, 'PFG (%)': 3.9317, 'SHR (%)': 1.8242, 'TRE (%)': 63.052, 'BGR (%)': 0.9932, 'LTR (%)': 3.79}, {'Date': '1992-01-01', 'AFG (%)': 0.4104, 'PFG (%)': 5.4442, 'SHR (%)': 1.3655, 'TRE (%)': 60.0272, 'BGR (%)': 0.7201, 'LTR (%)': 3.6362}, {'Date': '1993-01-01', 'AFG (%)': 0.7645, 'PFG (%)': 7.1049, 'SHR (%)': 2.5982, 'TRE (%)': 65.8525, 'BGR (%)': 0.8849, 'LTR (%)': 4.9531}, {'Date': '1994-01-01', 'AFG (%)': 0.3692, 'PFG (%)': 5.6137, 'SHR (%)': 1.8488, 'TRE (%)': 71.8627, 'BGR (%)': 0.6489, 'LTR (%)': 3.8581}, {'Date': '1995-01-01', 'AFG (%)': 0.6158, 'PFG (%)': 4.1893, 'SHR (%)': 1.758, 'TRE (%)': 71.691, 'BGR (%)': 0.5641, 'LTR (%)': 3.2282}, {'Date': '1996-01-01', 'AFG (%)': 0.3792, 'PFG (%)': 4.0967, 'SHR (%)': 1.2467, 'TRE (%)': 73.2201, 'BGR (%)': 0.5443, 'LTR (%)': 3.2485}, {'Date': '1997-01-01', 'AFG (%)': 0.2654, 'PFG (%)': 2.9503, 'SHR (%)': 0.7353, 'TRE (%)': 78.014, 'BGR (%)': 0.5749, 'LTR (%)': 2.9045}, {'Date': '1998-01-01', 'AFG (%)': 0.1939, 'PFG (%)': 2.4875, 'SHR (%)': 0.4585, 'TRE (%)': 83.3, 'BGR (%)': 0.521, 'LTR (%)': 3.5978}, {'Date': '1999-01-01', 'AFG (%)': 0.4895, 'PFG (%)': 4.0781, 'SHR (%)': 1.6066, 'TRE (%)': 80.2447, 'BGR (%)': 0.7558, 'LTR (%)': 3.9925}, {'Date': '2000-01-01', 'AFG (%)': 0.3099, 'PFG (%)': 3.1444, 'SHR (%)': 1.3387, 'TRE (%)': 80.8737, 'BGR (%)': 0.9845, 'LTR (%)': 3.0187}, {'Date': '2001-01-01', 'AFG (%)': 0.153, 'PFG (%)': 2.7834, 'SHR (%)': 1.4422, 'TRE (%)': 86.4391, 'BGR (%)': 0.603, 'LTR (%)': 3.58}, {'Date': '2002-01-01', 'AFG (%)': 0.1809, 'PFG (%)': 2.6401, 'SHR (%)': 1.2101, 'TRE (%)': 86.9341, 'BGR (%)': 0.7549, 'LTR (%)': 3.5127}, {'Date': '2003-01-01', 'AFG (%)': 0.2019, 'PFG (%)': 1.4209, 'SHR (%)': 0.3325, 'TRE (%)': 90.1298, 'BGR (%)': 0.2192, 'LTR (%)': 3.0712}, {
        'Date': '2004-01-01', 'AFG (%)': 0.0808, 'PFG (%)': 1.3303, 'SHR (%)': 0.3522, 'TRE (%)': 92.9857, 'BGR (%)': 0.2564, 'LTR (%)': 3.3688}, {'Date': '2005-01-01', 'AFG (%)': 0.1656, 'PFG (%)': 1.6838, 'SHR (%)': 0.4001, 'TRE (%)': 90.8285, 'BGR (%)': 0.4148, 'LTR (%)': 3.4865}, {'Date': '2006-01-01', 'AFG (%)': 0.1335, 'PFG (%)': 1.8824, 'SHR (%)': 0.4693, 'TRE (%)': 90.1796, 'BGR (%)': 0.3077, 'LTR (%)': 3.287}, {'Date': '2007-01-01', 'AFG (%)': 0.1308, 'PFG (%)': 1.4534, 'SHR (%)': 0.2834, 'TRE (%)': 89.3823, 'BGR (%)': 0.344, 'LTR (%)': 2.8757}, {'Date': '2008-01-01', 'AFG (%)': 0.0333, 'PFG (%)': 1.0227, 'SHR (%)': 0.1185, 'TRE (%)': 93.4809, 'BGR (%)': 0.329, 'LTR (%)': 3.1565}, {'Date': '2009-01-01', 'AFG (%)': 0.2327, 'PFG (%)': 1.4972, 'SHR (%)': 0.3837, 'TRE (%)': 87.4762, 'BGR (%)': 0.4335, 'LTR (%)': 3.4178}, {'Date': '2010-01-01', 'AFG (%)': 0.1046, 'PFG (%)': 1.1962, 'SHR (%)': 0.2415, 'TRE (%)': 93.2397, 'BGR (%)': 0.3623, 'LTR (%)': 2.8326}, {'Date': '2011-01-01', 'AFG (%)': 0.1072, 'PFG (%)': 1.2153, 'SHR (%)': 0.2657, 'TRE (%)': 92.5022, 'BGR (%)': 0.4005, 'LTR (%)': 2.933}, {'Date': '2012-01-01', 'AFG (%)': 0.1163, 'PFG (%)': 1.3452, 'SHR (%)': 0.3999, 'TRE (%)': 92.0288, 'BGR (%)': 0.4718, 'LTR (%)': 2.7303}, {'Date': '2013-01-01', 'AFG (%)': 0.1535, 'PFG (%)': 1.7311, 'SHR (%)': 0.8577, 'TRE (%)': 91.0959, 'BGR (%)': 0.4756, 'LTR (%)': 2.504}, {'Date': '2014-01-01', 'AFG (%)': 0.0729, 'PFG (%)': 1.4173, 'SHR (%)': 0.7514, 'TRE (%)': 90.4558, 'BGR (%)': 0.4249, 'LTR (%)': 2.3847}, {'Date': '2015-01-01', 'AFG (%)': 0.0877, 'PFG (%)': 1.1808, 'SHR (%)': 0.4446, 'TRE (%)': 89.4551, 'BGR (%)': 0.484, 'LTR (%)': 2.216}, {'Date': '2016-01-01', 'AFG (%)': 0.0812, 'PFG (%)': 0.8718, 'SHR (%)': 0.5739, 'TRE (%)': 91.5122, 'BGR (%)': 0.3608, 'LTR (%)': 2.0393}, {'Date': '2017-01-01', 'AFG (%)': 0.0287, 'PFG (%)': 0.5391, 'SHR (%)': 0.2178, 'TRE (%)': 96.9193, 'BGR (%)': 0.2689, 'LTR (%)': 2.0144}, {'Date': '2018-01-01', 'AFG (%)': 0.0427, 'PFG (%)': 0.5128, 'SHR (%)': 0.1679, 'TRE (%)': 92.0312, 'BGR (%)': 0.544, 'LTR (%)': 2.0133}, {'Date': '2019-01-01', 'AFG (%)': 0.0133, 'PFG (%)': 0.1681, 'SHR (%)': 0.1081, 'TRE (%)': 96.6984, 'BGR (%)': 0.1381, 'LTR (%)': 2.3741}, {'Date': '2020-01-01', 'AFG (%)': 0.0408, 'PFG (%)': 0.4644, 'SHR (%)': 0.19, 'TRE (%)': 95.1517, 'BGR (%)': 0.167, 'LTR (%)': 2.7448}, {'Date': '2021-01-01', 'AFG (%)': 0.2237, 'PFG (%)': 0.5237, 'SHR (%)': 0.6628, 'TRE (%)': 87.656, 'BGR (%)': 0.5535, 'LTR (%)': 3.9629}, {'Date': '2022-01-01', 'AFG (%)': 1.0323, 'PFG (%)': 3.5415, 'SHR (%)': 2.7943, 'TRE (%)': 68.5925, 'BGR (%)': 2.2187, 'LTR (%)': 6.6812}, {'Date': '2023-01-01', 'AFG (%)': 0.6349, 'PFG (%)': 3.5292, 'SHR (%)': 3.5261, 'TRE (%)': 72.5498, 'BGR (%)': 1.8017, 'LTR (%)': 7.0499}, {'Date': '2024-01-01', 'AFG (%)': 1.1038, 'PFG (%)': 4.712, 'SHR (%)': 5.6614, 'TRE (%)': 63.4753, 'BGR (%)': 1.5861, 'LTR (%)': 6.1573}, {'Date': '2025-01-01', 'AFG (%)': 1.2696, 'PFG (%)': 5.116, 'SHR (%)': 6.6321, 'TRE (%)': 60.5396, 'BGR (%)': 1.9749, 'LTR (%)': 5.5952}]}]
    df = pd.DataFrame(results[0]['Data'])
    return df


def vegetation_cover_timeseries_charts(df: pd.DataFrame):
    pass


def main():
    """Example main function to demonstrate API usage
    """
    # envs = dotenv.parse_dotenv(Path(".env"))
    # api_key = envs['CLIMATE_ENGINE_API_KEY']

    load_dotenv()
    site_name = 'althouse_small_selection_1'
    site_description = 'example from rpt_rivers_need_space'
    path_to_geojson = Path(r"C:\nardata\localcode\rs-reports-gen\src\reports\rpt_rivers_need_space\example\althouse_small_selection_1.geojson")

    gdf = gpd.read_file(path_to_geojson)
    # print(gdf)
    # viz(gdf)
    # getDroughtReport(gdf, site_name, site_description)
    # get_dataset_date_range()
    vegdf = get_vegetation_cover_timeseries(gdf)
    vegetation_cover_timeseries_charts(vegdf)


if __name__ == '__main__':
    main()
