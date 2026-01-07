import pandas as pd
import geopandas as gpd
import requests
from rsxml import Logger
from util.rme.rme_common_dataprep import add_common_rme_cols
from util.pandas import RSFieldMeta


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


def prepare_nid_display_table(nid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Format NID data for display in the report:
    1. Define metadata/units
    2. Apply units
    3. Filter columns
    4. Create hyperlinks
    """
    if nid_gdf.empty:
        return pd.DataFrame()

    _FIELD_META = RSFieldMeta()
    table_name = 'NID'  # for disambiguating metadata

    # Define metadata for NID fields
    _FIELD_META.add_field_meta(name='NID_STORAGE', table_name=table_name, friendly_name='NID Storage', data_unit='acre_feet', display_unit='acre_feet', dtype='REAL')
    _FIELD_META.add_field_meta(name='NID_HEIGHT', table_name=table_name, friendly_name='NID Height', data_unit='foot', display_unit='foot', dtype='REAL')
    _FIELD_META.add_field_meta(name='DRAINAGE_AREA', table_name=table_name, friendly_name='Drainage Area', data_unit='mile**2', display_unit='mile**2', dtype='REAL')
    _FIELD_META.add_field_meta(name='MAX_DISCHARGE', table_name=table_name, friendly_name='Max Discharge', data_unit='foot**3 / second', display_unit='foot**3 / second', dtype='REAL')

    nid_display_cols = [
        'NAME', 'PRIMARY_OWNER_TYPE', 'RIVER_OR_STREAM', 'PRIMARY_PURPOSE',
        'PRIMARY_DAM_TYPE', 'NID_STORAGE', 'NID_HEIGHT', 'DRAINAGE_AREA', 'MAX_DISCHARGE', 'NIDID'
    ]

    # Ensure we only work with available columns
    cols_to_use = [c for c in nid_display_cols if c in nid_gdf.columns]

    # Apply units formatting
    # Pass table_name='NID' to resolve ambiguities
    display_gdf, _ = _FIELD_META.apply_units(nid_gdf[cols_to_use].copy(), table_name=table_name)

    # Create Hyperlink for NAME using NIDID
    if 'NIDID' in display_gdf.columns and 'NAME' in display_gdf.columns:
        display_gdf['NAME'] = display_gdf.apply(
            lambda row: f'<a href="https://nid.sec.usace.army.mil/nid/#/dams/system/{row["NIDID"]}/summary" target="_blank">{row["NAME"]}</a>',
            axis=1
        )

    # Final column selection for display
    final_cols = [
        'NAME', 'PRIMARY_OWNER_TYPE', 'RIVER_OR_STREAM', 'PRIMARY_PURPOSE',
        'PRIMARY_DAM_TYPE', 'NID_STORAGE', 'NID_HEIGHT', 'DRAINAGE_AREA', 'MAX_DISCHARGE'
    ]
    final_cols = [c for c in final_cols if c in display_gdf.columns]

    return display_gdf[final_cols]
