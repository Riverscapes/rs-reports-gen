import pandas as pd
import geopandas as gpd
import requests
from rsxml import Logger
from util.rme.rme_common_dataprep import add_common_rme_cols


def add_calculated_rme_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Add calculated columns to the RME dataframe
    Returns: 
        dataframe with added columns
    """
    df = add_common_rme_cols(df, ['riparian_veg_departure_as_departure', 'riparian_veg_departure_bins', 'land_use_intens_bins'])
    # add any columns need FOR THIS REPORT ONLY here

    return df


def get_nid_data(aoi_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Fetch data from USACE National Inventory of Dams matching the AOI.
    Uses Bounding Box of AOI for query.
    """
    log = Logger("NID Data Fetch")
    try:
        # Reproject to 4326 for web API
        aoi_4326 = aoi_gdf.to_crs(epsg=4326)
        bbox = aoi_4326.total_bounds  # [minx, miny, maxx, maxy]

        # Construct geometry param as comma separated string
        # xmin, ymin, xmax, ymax
        bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

        url = "https://geospatial.sec.usace.army.mil/dls/rest/services/NID/National_Inventory_of_Dams_Public_Service/FeatureServer/0/query"

        params = {
            "where": "1=1",
            "geometry": bbox_str,
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson"
        }

        log.info(f"Querying NID with bbox: {bbox_str}")
        resp = requests.post(url, data=params, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        if 'features' in data and len(data['features']) > 0:
            nid_gdf = gpd.GeoDataFrame.from_features(data['features'], crs="EPSG:4326")

            # Clip to actual AOI polygon (since we fetched by BBOX)
            nid_gdf = gpd.clip(nid_gdf, aoi_4326)

            log.info(f"Retrieved {len(nid_gdf)} dams from NID (after clipping).")
            return nid_gdf
        else:
            log.info("No dams found in AOI.")
            return gpd.GeoDataFrame()

    except Exception as e:
        log.error(f"Failed to fetch NID data: {e}")
        return gpd.GeoDataFrame()
