import geopandas as gpd
import pandas as pd
import requests
from rsxml import Logger

from util.pandas import RSFieldMeta
from util.rme.rme_common_dataprep import add_common_rme_cols


def add_calculated_rme_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Add calculated columns to the RME dataframe
    Returns:
        dataframe with added columns
    """
    df = add_common_rme_cols(df, ['riparian_veg_departure_as_departure', 'riparian_veg_departure_bins', 'land_use_intens_bins'])
    # add any columns need FOR THIS REPORT ONLY here

    return df


def get_nid_data(aoi_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame | None:
    """
    Fetch data from USACE National Inventory of Dams matching the AOI.
    Uses Bounding Box of AOI for query.
    In case of error, returns None.
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

        params = {"where": "1=1", "geometry": bbox_str, "geometryType": "esriGeometryEnvelope", "spatialRel": "esriSpatialRelIntersects", "outFields": "*", "returnGeometry": "true", "f": "geojson"}

        log.info(f"Querying NID with bbox: {bbox_str}")
        resp = requests.post(url, data=params, timeout=150)  # 2.5 minutes seems fine even for big areas
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
        return None


def define_nid_columns(nid_gdf: gpd.GeoDataFrame) -> tuple[pd.DataFrame, dict]:
    meta = RSFieldMeta()
    layer_id = nid_gdf.attrs.get('layer_id', 'NID')  # for disambiguating metadata

    # Define metadata for NID fields

    meta.add_field_meta(name='NAME', layer_id=layer_id, friendly_name='Dam Name', description='The official name of the dam. For dams that do not have an official name, the popular name is used.', theme='Description')
    meta.add_field_meta(name='DAM_TYPES', layer_id=layer_id, friendly_name='Category describing the type of dam. If more than one type, types are separated by a semi-colon.', theme='Structure')
    meta.add_field_meta(name='PURPOSES', layer_id=layer_id, friendly_name='Category describing the current purpose(s) for which the reservoir is used.', theme='Description')
    meta.add_field_meta(
        name='YEAR_COMPLETED', layer_id=layer_id, description='Year (four digits) when the original main dam structure was completed. If unknown, and reasonable estimate is unavailable, the value will be blank.', theme='Structure'
    )
    meta.add_field_meta(
        name='HAZARD_POTENTIAL',
        layer_id=layer_id,
        description='Hazard Potential Classification Category to indicate the potential hazard to the downstream area resulting from failure or mis-operation of the dam or facilities. '
        'It reflects probable loss of human life and impacts on economic, environmental, and lifeline interests. The hazard potential does not speak to the condition of the dam or the risk of the dam failing.',
        theme='Inspection and Evaluation',
    )
    meta.add_field_meta(name='CONDITION_ASSESMENT', layer_id=layer_id, description='Assessment that best describes the condition of the dam based on available information.', theme='Inspection and Evaluation')
    meta.add_field_meta(
        name='NID_STORAGE', layer_id=layer_id, friendly_name='NID Storage', description="General storage of the dam.", data_unit='acre_feet', display_unit='acre_feet', dtype='REAL', preferred_format="{:,.0f}", theme='Structure'
    )
    meta.add_field_meta(name='NID_HEIGHT', layer_id=layer_id, friendly_name='NID Height', data_unit='foot', display_unit='foot', dtype='REAL', theme='Structure')
    meta.add_field_meta(name='DRAINAGE_AREA', layer_id=layer_id, friendly_name='Drainage Area', data_unit='mile**2', display_unit='mile**2', dtype='REAL', theme='Structure')
    meta.add_field_meta(name='MAX_DISCHARGE', layer_id=layer_id, friendly_name='Max Discharge', data_unit='foot**3 / second', display_unit='foot**3 / second', dtype='REAL', theme='Structure')

    return meta.apply_units(nid_gdf, layer_id=layer_id)


def prepare_nid_display_table(nid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Format NID data for display in the report:
    1. Define metadata/units
    2. Apply units
    3. Filter columns
    4. Create hyperlinks
    5. Sort by Storage descending
    """
    if nid_gdf.empty:
        return pd.DataFrame()

    nid_display_cols = ['NAME', 'PRIMARY_OWNER_TYPE', 'RIVER_OR_STREAM', 'PRIMARY_PURPOSE', 'PRIMARY_DAM_TYPE', 'NID_STORAGE', 'NID_HEIGHT', 'DRAINAGE_AREA', 'MAX_DISCHARGE', 'NIDID']

    # Ensure we only work with available columns
    cols_to_use = [c for c in nid_display_cols if c in nid_gdf.columns]

    # Apply units formatting
    display_df, _ = define_nid_columns(nid_gdf[cols_to_use].copy())

    # Create Hyperlink for NAME using NIDID
    if 'NIDID' in display_df.columns and 'NAME' in display_df.columns:
        display_df['NAME'] = display_df.apply(lambda row: f'<a href="https://nid.sec.usace.army.mil/nid/#/dams/system/{row["NIDID"]}/summary" target="_blank">{row["NAME"]}</a>', axis=1)

    # Final column selection for display
    final_cols = ['NAME', 'PRIMARY_OWNER_TYPE', 'RIVER_OR_STREAM', 'PRIMARY_PURPOSE', 'PRIMARY_DAM_TYPE', 'NID_STORAGE', 'NID_HEIGHT', 'DRAINAGE_AREA', 'MAX_DISCHARGE']
    final_cols = [c for c in final_cols if c in display_df.columns]

    display_df = display_df.sort_values(by='NID_STORAGE', ascending=False)

    return display_df[final_cols]
