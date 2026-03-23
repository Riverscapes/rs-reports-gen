"""Quick EPA ATTAINS MapServer helpers for Data Mart prototyping.

This module provides a lightweight proof-of-concept query path to the EPA
ATTAINS ArcGIS MapServer for assessment lines (streams) and areas
(waterbodies) intersecting an AOI.

Copilot-generated module, directed by Lorin March 2026.
"""

import json

import geopandas as gpd
import pandas as pd
import requests
from rsxml import Logger

ATTAINS_MAPSERVER_BASE_URL = "https://gispub.epa.gov/arcgis/rest/services/OW/ATTAINS_Assessment/MapServer"

_STREAMS_LAYER_ID = 1
_WATERBODIES_LAYER_ID = 2


def _coerce_flag(value: object) -> object:
    """Normalize API flag values (Y/N, 1/0, true/false) to pandas nullable bool.

    Copilot-generated function.
    """
    if value is None:
        return pd.NA
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if text in {"y", "yes", "true", "t", "1"}:
        return True
    if text in {"n", "no", "false", "f", "0"}:
        return False
    return pd.NA


def _query_layer(
    query_gdf: gpd.GeoDataFrame,
    layer_id: int,
    feature_type: str,
    timeout: int,
    max_records: int,
) -> pd.DataFrame:
    """Query one ATTAINS assessment layer using AOI envelope intersects.

    Copilot-generated function.
    """
    if query_gdf.empty:
        return pd.DataFrame()

    gdf_wgs84 = query_gdf.to_crs(epsg=4326) if query_gdf.crs and query_gdf.crs.to_epsg() != 4326 else query_gdf
    minx, miny, maxx, maxy = gdf_wgs84.total_bounds

    geom_envelope = {
        "xmin": float(minx),
        "ymin": float(miny),
        "xmax": float(maxx),
        "ymax": float(maxy),
        "spatialReference": {"wkid": 4326},
    }

    common_out_fields = [
        "OBJECTID",
        "organizationid",
        "organizationname",
        "state",
        "reportingcycle",
        "assessmentunitidentifier",
        "assessmentunitname",
        "ircategory",
        "overallstatus",
        "isassessed",
        "isimpaired",
        "waterbodyreportlink",
    ]
    if layer_id == _STREAMS_LAYER_ID:
        out_fields = common_out_fields + ["Shape_Length"]
    elif layer_id == _WATERBODIES_LAYER_ID:
        out_fields = common_out_fields + ["Shape_Length", "Shape_Area"]
    else:
        out_fields = common_out_fields

    params = {
        "f": "json",
        "where": "1=1",
        "geometry": json.dumps(geom_envelope),
        "geometryType": "esriGeometryEnvelope",
        "inSR": 4326,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": ",".join(out_fields),
        "returnGeometry": "false",
        "resultRecordCount": max_records,
    }

    url = f"{ATTAINS_MAPSERVER_BASE_URL}/{layer_id}/query"
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()

    if "error" in payload:
        raise RuntimeError(f"ATTAINS layer {layer_id} query error: {payload['error']}")

    features = payload.get("features", [])
    rows = [f.get("attributes", {}) for f in features]
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]
    df["feature_type"] = feature_type

    # Keep only the flags and size metrics needed for rapid POC analysis.
    if "isassessed" in df.columns:
        df["isassessed"] = df["isassessed"].map(_coerce_flag).astype("boolean")
    if "isimpaired" in df.columns:
        df["isimpaired"] = df["isimpaired"].map(_coerce_flag).astype("boolean")

    if "shape_length" in df.columns:
        df["length_m"] = pd.to_numeric(df["shape_length"], errors="coerce")
    else:
        df["length_m"] = pd.Series(pd.NA, index=df.index, dtype="Float64")

    if "shape_area" in df.columns:
        df["area_m2"] = pd.to_numeric(df["shape_area"], errors="coerce")
    else:
        df["area_m2"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
    return df


def query_attains_assessments(
    query_gdf: gpd.GeoDataFrame,
    include_streams: bool = True,
    include_waterbodies: bool = True,
    timeout: int = 60,
    max_records_per_layer: int = 5000,
) -> pd.DataFrame:
    """Query EPA ATTAINS streams/waterbodies intersecting AOI and return a DataFrame.

    This function is intentionally simple for proof-of-concept usage in
    ``rpt_data_mart``. It uses AOI envelope intersection queries against:
    - Layer 1: ATTAINS Assessment Lines (streams)
    - Layer 2: ATTAINS Assessment Areas (waterbodies)

    Args:
        query_gdf: AOI geometries (any CRS; reprojected to EPSG:4326 if needed).
        include_streams: Include layer 1 features.
        include_waterbodies: Include layer 2 features.
        timeout: Request timeout (seconds).
        max_records_per_layer: ArcGIS query cap per layer.

    Returns:
        DataFrame with feature type, AU identifiers, assessed/impaired flags,
        and size metrics (`length_m`, `area_m2`) where available.

    Copilot-generated function.
    """
    log = Logger("ATTAINS")
    if not include_streams and not include_waterbodies:
        return pd.DataFrame()

    parts: list[pd.DataFrame] = []

    if include_streams:
        log.info("Querying EPA ATTAINS streams (assessment lines)")
        parts.append(
            _query_layer(
                query_gdf=query_gdf,
                layer_id=_STREAMS_LAYER_ID,
                feature_type="stream",
                timeout=timeout,
                max_records=max_records_per_layer,
            )
        )

    if include_waterbodies:
        log.info("Querying EPA ATTAINS waterbodies (assessment areas)")
        parts.append(
            _query_layer(
                query_gdf=query_gdf,
                layer_id=_WATERBODIES_LAYER_ID,
                feature_type="waterbody",
                timeout=timeout,
                max_records=max_records_per_layer,
            )
        )

    parts = [df for df in parts if not df.empty]
    if not parts:
        return pd.DataFrame()

    combined = pd.concat(parts, ignore_index=True)
    preferred_order = [
        "feature_type",
        "organizationid",
        "organizationname",
        "state",
        "reportingcycle",
        "assessmentunitidentifier",
        "assessmentunitname",
        "ircategory",
        "overallstatus",
        "isassessed",
        "isimpaired",
        "length_m",
        "area_m2",
        "waterbodyreportlink",
    ]
    existing = [c for c in preferred_order if c in combined.columns]
    rest = [c for c in combined.columns if c not in existing]
    return combined[existing + rest]
