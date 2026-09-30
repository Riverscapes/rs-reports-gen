"""Query and summarize Inventory of Resources data.

Created 2026-08-13.
Created by copilot.
"""

import geopandas as gpd
import pandas as pd
from rsxml import Logger

from util.athena import aoi_query_to_local_parquet
from util.athena.athena import aoi_query_to_dataframe
from util.binning import get_bins_info
from util.html.progress import ProgressCard, ProgressGroup
from util.pandas import RSFieldMeta

INVENTORY_FIELDS = "segment_area, centerline_length, watershed_id, ownership, ownership_desc, drainage_area, stream_name, stream_order, stream_length, waterbody_type, waterbody_type_desc, waterbody_extent, prim_channel_gradient, valleybottom_gradient, fcode, fcode_desc, confinement_ratio, constriction_ratio, lf_riparian, lf_riparian_prop, lf_agriculture, lf_developed, rme_project_id, rme_project_name"


def data_for_aoi_to_parquet(aoi_gdf: gpd.GeoDataFrame, parquet_path: str) -> None:
    """Query inventory data for an AOI and save it to a local Parquet file.

    Args:
            aoi_gdf: Area of interest used to filter RME DGO records.
            parquet_path: Path to the output Parquet file.
    """
    query = f"""
SELECT {INVENTORY_FIELDS}
FROM input_geom, rs_rpt.rme_datamart_base_vw
WHERE {{prefilter_condition}} AND {{intersects_condition}}
"""
    return aoi_query_to_local_parquet(
        query,
        geometry_field_expression="ST_GeomFromBinary(dgo_geom)",
        geom_bbox_field="dgo_geom_bbox",
        aoi_gdf=aoi_gdf,
        local_path=parquet_path,
    )


def get_nwi_data(aoi_gdf: gpd.GeoDataFrame, table='riparian') -> gpd.GeoDataFrame:
    """Query NWI polygons from the specified table, annotate ownership, and clip them to the AOI.

    Returns a GeoDataFrame of NWI riparian wetland polygons (attribute, wetland_type, modifier1_name, acres, ownership,
    ownership_desc, geometry) clipped to the boundary of aoi_gdf. Polygons crossing ownership boundaries are
    split at those boundaries, with acres prorated to each piece; polygons without a matching ownership record
    are retained with null ownership attributes.
    """
    log = Logger("Get NWI riparian data for AOI")

    if table == 'riparian':
        query_str = """
SELECT nwi.attribute, nwi.wetland_type, def.water_regime_name as water_regime_name, def.modifier1_name AS modifier1_name,
       sma.admin_agency_code AS ownership, lu_blm_o.edomvd AS ownership_desc,
       ST_AsBinary(
           CASE
               WHEN sma.geom_wkb IS NULL THEN ST_GeomFromBinary(nwi.geom_wkb)
               ELSE ST_Intersection(ST_GeomFromBinary(nwi.geom_wkb), ST_GeomFromBinary(sma.geom_wkb))
           END
       ) AS geom_wkb
FROM input_geom, ext_rpt.us_fws_nwi_riparian nwi
LEFT JOIN ext_rpt.us_blm_sma_ownership sma
    ON ST_Intersects(ST_GeomFromBinary(nwi.geom_wkb), ST_GeomFromBinary(sma.geom_wkb))
    AND ST_Area(ST_Intersection(ST_GeomFromBinary(nwi.geom_wkb), ST_GeomFromBinary(sma.geom_wkb))) > 0
LEFT JOIN lu_blm_ownership lu_blm_o
    ON upper(sma.admin_agency_code) = upper(lu_blm_o.edomv)
LEFT JOIN ext_raw.us_fws_nwi_code_definitions def
    ON nwi.attribute = def.attribute
WHERE {prefilter_condition} AND {intersects_condition}
"""
    else:
        query_str = """
SELECT nwi.attribute, nwi.wetland_type, def.water_regime_name as water_regime_name, def.modifier1_name AS modifier1_name,
       sma.admin_agency_code AS ownership, lu_blm_o.edomvd AS ownership_desc,
       ST_AsBinary(
           CASE
               WHEN sma.geom_wkb IS NULL THEN ST_GeomFromBinary(nwi.geom_wkb)
               ELSE ST_Intersection(ST_GeomFromBinary(nwi.geom_wkb), ST_GeomFromBinary(sma.geom_wkb))
           END
       ) AS geom_wkb
FROM input_geom, ext_rpt.us_fws_nwi_wetlands nwi
LEFT JOIN ext_rpt.us_blm_sma_ownership sma
    ON ST_Intersects(ST_GeomFromBinary(nwi.geom_wkb), ST_GeomFromBinary(sma.geom_wkb))
    AND ST_Area(ST_Intersection(ST_GeomFromBinary(nwi.geom_wkb), ST_GeomFromBinary(sma.geom_wkb))) > 0
LEFT JOIN lu_blm_ownership lu_blm_o
    ON upper(sma.admin_agency_code) = upper(lu_blm_o.edomv)
LEFT JOIN ext_raw.us_fws_nwi_code_definitions def
    ON nwi.attribute = def.attribute
WHERE {prefilter_condition} AND {intersects_condition}
"""
    df = aoi_query_to_dataframe(query_str, geometry_field_expression="ST_GeomFromBinary(nwi.geom_wkb)", geom_bbox_field=None, aoi_gdf=aoi_gdf, querylabel="nwi riparian")
    if table == 'riparian':
        df["type"] = "lotic"
    else:
        df["type"] = "lentic"

    if df.empty:
        log.info("No NWI riparian polygons intersect the AOI.")
        empty_gdf = gpd.GeoDataFrame(columns=["attribute", "wetland_type", "water_regime_name", "modifier1_name", "ownership", "ownership_desc", "geometry", "type"], geometry="geometry", crs=aoi_gdf.crs)
        empty_gdf["area"] = pd.Series(dtype="pint[meter ** 2]")
        return empty_gdf

    gdf = gpd.GeoDataFrame(df.drop(columns=["geom_wkb"]), geometry=gpd.GeoSeries.from_wkb(df["geom_wkb"]), crs=aoi_gdf.crs)
    area_crs = gdf.estimate_utm_crs()
    area_m2 = gdf.to_crs(area_crs).geometry.area
    gdf["area"] = area_m2.astype("pint[meter ** 2]")
    return gpd.clip(gdf, aoi_gdf)


def summarize_by_length(data_df: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """Aggregate stream length and record count by one or more inventory fields.

    Args:
            data_df: Normalized inventory data.
            group_columns: Fields that define each output row.

    Returns:
            Summary sorted by total stream length, then record count.
    """
    required_columns = [*group_columns, "stream_length"]
    missing_columns = [column for column in required_columns if column not in data_df]
    if missing_columns:
        raise ValueError(f"Inventory data is missing required columns: {', '.join(missing_columns)}")

    summary = (
        data_df.groupby(group_columns, dropna=False, as_index=False)
        .agg(stream_length=("stream_length", "sum"), record_count=("stream_length", "size"))
        .sort_values(["stream_length", "record_count", *group_columns], ascending=[False, False, *([True] * len(group_columns))], kind="mergesort")
        .reset_index(drop=True)
    )
    return summary


def build_report_summaries(data_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build the tables used by the Inventory of Resources report.

    Args:
            data_df: Normalized inventory data.

    Returns:
            Named report summary tables.
    """

    rows = []
    summary_df = data_df.copy()
    perennial = summary_df[summary_df["fcode"].isin([46006, 55800])]
    non_perennial = summary_df[~summary_df["fcode"].isin([46006, 55800])]
    streams = [
        'Stream Network',
        summary_df['stream_length'].sum(),
        summary_df[summary_df['ownership'] == 'BLM']["stream_length"].sum(),
        summary_df[summary_df['ownership'] == 'BLM']["stream_length"].sum() / summary_df['stream_length'].sum(),
    ]
    perennial_row = ['Perennial', perennial["stream_length"].sum(), perennial[perennial['ownership'] == 'BLM']["stream_length"].sum(), perennial[perennial['ownership'] == 'BLM']["stream_length"].sum() / perennial["stream_length"].sum()]
    non_perennial_row = [
        'Non-Perennial',
        non_perennial["stream_length"].sum(),
        non_perennial[non_perennial['ownership'] == 'BLM']["stream_length"].sum(),
        non_perennial[non_perennial['ownership'] == 'BLM']["stream_length"].sum() / non_perennial["stream_length"].sum(),
    ]
    rip = [
        'Riparian-Wetland',
        summary_df['lf_riparian'].sum(),
        summary_df[summary_df['ownership'] == 'BLM']['lf_riparian'].sum(),
        summary_df[summary_df['ownership'] == 'BLM']['lf_riparian'].sum() / summary_df['lf_riparian'].sum(),
    ]
    waterbodies = [
        'Waterbodies',
        summary_df['waterbody_extent'].sum(),
        summary_df[summary_df['ownership'] == 'BLM']['waterbody_extent'].sum(),
        summary_df[summary_df['ownership'] == 'BLM']['waterbody_extent'].sum() / summary_df['waterbody_extent'].sum(),
    ]
    springs = ['Springs', None, None, None]
    tot_area = ['Riverscape Area', summary_df['segment_area'].sum(), summary_df[summary_df['ownership'] == 'BLM']['segment_area'].sum(), summary_df[summary_df['ownership'] == 'BLM']['segment_area'].sum() / summary_df['segment_area'].sum()]
    anthro = [
        'Anthropogenic LULC',
        summary_df['lf_agriculture'].sum() + summary_df['lf_developed'].sum(),
        summary_df[summary_df['ownership'] == 'BLM']['lf_agriculture'].sum() + summary_df[summary_df['ownership'] == 'BLM']['lf_developed'].sum(),
        (summary_df[summary_df['ownership'] == 'BLM']['lf_agriculture'].sum() + summary_df[summary_df['ownership'] == 'BLM']['lf_developed'].sum()) / (summary_df['lf_agriculture'].sum() + summary_df['lf_developed'].sum()),
    ]
    rows.append(streams)
    rows.append(perennial_row)
    rows.append(non_perennial_row)
    rows.append(rip)
    rows.append(waterbodies)
    rows.append(springs)
    rows.append(tot_area)
    rows.append(anthro)
    summary_df = pd.DataFrame(rows, columns=['Resource Category', 'Total Inventory', 'Total BLM', 'BLM Managment'])

    # return {
    #     "ownership": summarize_by_length(data_df, ["ownership_desc"]),
    #     "feature_type": summarize_by_length(data_df, ["fcode_desc"]),
    #     "stream": summarize_by_length(data_df, ["stream_name", "stream_order"]),
    #     "watershed": summarize_by_length(data_df, ["watershed_id"]),
    #     "waterbody": summarize_by_length(data_df, ["waterbody_type"]),
    # }

    return {"inventory": summary_df}


def build_metric_cards(data_df: pd.DataFrame) -> dict[str, dict[str, str]]:
    """Build display-ready key indicators from normalized inventory data.

    Args:
            data_df: Normalized inventory data.

    Returns:
            Card payloads consumed by the shared metric-grid template macro.
    """
    total_length = data_df["stream_length"].sum(min_count=1) if "stream_length" in data_df else None
    watershed_count = data_df["watershed_id"].nunique() if "watershed_id" in data_df else 0
    named_stream_count = data_df.loc[data_df["stream_name"] != "Not specified", "stream_name"].nunique() if "stream_name" in data_df else 0
    median_drainage = data_df["drainage_area"].median() if "drainage_area" in data_df else None

    return {
        "records": {"title": "Inventory Records", "value": f"{len(data_df):,}", "details": "RME records intersecting the area of interest"},
        "stream_length": {"title": "Total Stream Length", "value": "Not available" if pd.isna(total_length) else f"{total_length:,.0f} m", "details": "Sum of reported stream lengths"},
        "watersheds": {"title": "Watersheds", "value": f"{watershed_count:,}", "details": "Distinct watershed identifiers represented"},
        "stream_names": {"title": "Named Streams", "value": f"{named_stream_count:,}", "details": "Distinct non-empty stream names"},
        "drainage_area": {"title": "Median Drainage Area", "value": "Not available" if pd.isna(median_drainage) else f"{median_drainage:,.2f}", "details": "Reported drainage-area value per inventory record"},
    }


def streams_by_type_cards(data_df: pd.DataFrame) -> list[ProgressCard]:

    if RSFieldMeta().unit_system == "imperial":
        length_label = 'mile'
        display_label = 'MI'
    else:
        length_label = 'km'
        display_label = 'KM'

    perennial = data_df[data_df['fcode'].isin([46006, 55800])]
    non_perennial = data_df[~data_df['fcode'].isin([46006, 55800])]
    total_perennial = perennial['stream_length'].sum().to(length_label)
    total_non_perennial = non_perennial['stream_length'].sum().to(length_label)
    blm_perennial = perennial[perennial['ownership'] == 'BLM']['stream_length'].sum().to(length_label)
    non_blm_perennial = perennial[perennial['ownership'] != 'BLM']['stream_length'].sum().to(length_label)
    blm_non_perennial = non_perennial[non_perennial['ownership'] == 'BLM']['stream_length'].sum().to(length_label)
    non_blm_non_perennial = non_perennial[non_perennial['ownership'] != 'BLM']['stream_length'].sum().to(length_label)

    return [
        ProgressCard(
            "Perennial",
            total=f'{int(total_perennial.m)} {display_label}',
            groups=[
                ProgressGroup("BLM", f"{int(blm_perennial.m)} / {int(total_perennial.m)} {display_label}", numerator=blm_perennial.m, denominator=total_perennial.m, color="indigo"),
                ProgressGroup("Non-BLM", f"{int(non_blm_perennial.m)} / {int(total_perennial.m)} {display_label}", numerator=non_blm_perennial.m, denominator=total_perennial.m, color="gray"),
            ],
        ),
        ProgressCard(
            "Non-Perennial",
            total=f'{int(total_non_perennial.m)} {display_label}',
            groups=[
                ProgressGroup("BLM", f"{int(blm_non_perennial.m)} / {int(total_non_perennial.m)} {display_label}", numerator=blm_non_perennial.m, denominator=total_non_perennial.m, color="indigo"),
                ProgressGroup("Non-BLM", f"{int(non_blm_non_perennial.m)} / {int(total_non_perennial.m)} {display_label}", numerator=non_blm_non_perennial.m, denominator=total_non_perennial.m, color="gray"),
            ],
        ),
    ]


def streams_by_order_cards(data_df: pd.DataFrame) -> list[ProgressCard]:

    if RSFieldMeta().unit_system == "imperial":
        length_label = 'mile'
        display_label = 'MI'
    else:
        length_label = 'km'
        display_label = 'KM'

    order_groups = data_df.groupby('stream_order')
    cards = []
    for order, group in order_groups:
        total_length = group['stream_length'].sum().to(length_label)
        blm_length = group[group['ownership'] == 'BLM']['stream_length'].sum().to(length_label)

        cards.append(
            ProgressCard(
                f"Order {order}",
                total=f'{int(total_length.m)} {display_label}',
                groups=[
                    ProgressGroup("BLM", f"{int(blm_length.m)} / {int(total_length.m)} {display_label}", numerator=blm_length.m, denominator=total_length.m, color="indigo"),
                ],
            )
        )

    return cards


def streams_by_slope_cards(data_df: pd.DataFrame) -> list[ProgressCard]:
    if RSFieldMeta().unit_system == "imperial":
        length_label = 'mile'
        display_label = 'MI'
    else:
        length_label = 'km'
        display_label = 'KM'

    edges, labels, colours = get_bins_info('prim_channel_gradient')

    cards = []
    for i, (edge_start, edge_end) in enumerate(zip(edges[:-1], edges[1:])):
        slope = f"{edge_start} - {edge_end}"
        group = data_df[(data_df['prim_channel_gradient'] >= edge_start) & (data_df['prim_channel_gradient'] < edge_end)]
        total_length = group['stream_length'].sum().to(length_label)
        blm_length = group[group['ownership'] == 'BLM']['stream_length'].sum().to(length_label)

        cards.append(
            ProgressCard(
                f"Slope {slope}",
                total=f'{int(total_length.m)} {display_label}',
                groups=[
                    ProgressGroup("BLM", f"{int(blm_length.m)} / {int(total_length.m)} {display_label}", numerator=blm_length.m, denominator=total_length.m, color="indigo"),
                ],
            )
        )

    return cards


def streams_by_valley_confinement_cards(data_df: pd.DataFrame) -> list[ProgressCard]:
    if RSFieldMeta().unit_system == "imperial":
        length_label = 'mile'
        display_label = 'MI'
    else:
        length_label = 'km'
        display_label = 'KM'

    edges, labels, colours = get_bins_info('confinement_ratio')

    cards = []
    for i, (edge_start, edge_end) in enumerate(zip(edges[:-1], edges[1:])):
        confinement = f"{edge_start} - {edge_end}"
        group = data_df[(data_df['confinement_ratio'] >= edge_start) & (data_df['confinement_ratio'] < edge_end)]
        total_length = group['stream_length'].sum().to(length_label)
        blm_length = group[group['ownership'] == 'BLM']['stream_length'].sum().to(length_label)

        cards.append(
            ProgressCard(
                f"Confinement {confinement}",
                total=f'{int(total_length.m)} {display_label}',
                groups=[
                    ProgressGroup("BLM", f"{int(blm_length.m)} / {int(total_length.m)} {display_label}", numerator=blm_length.m, denominator=total_length.m, color="indigo"),
                ],
            )
        )

    return cards


def wetlands_cards(data_df: pd.DataFrame) -> list[ProgressCard]:
    if RSFieldMeta().unit_system == "imperial":
        area_label = 'acre'
        display_label = 'AC'
    else:
        area_label = 'hectare'
        display_label = 'HA'

    lentic = data_df[data_df['type'] == 'lentic']
    lotic = data_df[data_df['type'] == 'lotic']
    blm_lentic = lentic[lentic['ownership'] == 'BLM']
    blm_lotic = lotic[lotic['ownership'] == 'BLM']

    cards = []
    cards.append(
        ProgressCard(
            "Lentic Wetlands",
            total=f'{int(lentic["area"].sum().to(area_label).m)} {display_label}',
            groups=[
                ProgressGroup(
                    "BLM",
                    f"{int(blm_lentic['area'].sum().to(area_label).m)} / {int(lentic['area'].sum().to(area_label).m)} {display_label}",
                    numerator=blm_lentic['area'].sum().to(area_label).m,
                    denominator=lentic['area'].sum().to(area_label).m,
                    color="green",
                ),
            ],
        )
    )
    cards.append(
        ProgressCard(
            "Lotic Wetlands",
            total=f'{int(lotic["area"].sum().to(area_label).m)} {display_label}',
            groups=[
                ProgressGroup(
                    "BLM",
                    f"{int(blm_lotic['area'].sum().to(area_label).m)} / {int(lotic['area'].sum().to(area_label).m)} {display_label}",
                    numerator=blm_lotic['area'].sum().to(area_label).m,
                    denominator=lotic['area'].sum().to(area_label).m,
                    color="green",
                ),
            ],
        )
    )

    return cards


def cowardin_cards(data_df: pd.DataFrame) -> list[ProgressCard]:
    if RSFieldMeta().unit_system == "imperial":
        area_label = 'acre'
        display_label = 'AC'
    else:
        area_label = 'hectare'
        display_label = 'HA'

    cards = []
    for cowardin_class in data_df['wetland_type'].unique():
        class_group = data_df[data_df['wetland_type'] == cowardin_class]
        blm_class_group = class_group[class_group['ownership'] == 'BLM']

        total_area = class_group['area'].sum().to(area_label)
        blm_area = blm_class_group['area'].sum().to(area_label)

        cards.append(
            ProgressCard(
                f"{cowardin_class}",
                total=f'{int(total_area.m)} {display_label}',
                groups=[
                    ProgressGroup(
                        "BLM",
                        f"{int(blm_area.m)} / {int(total_area.m)} {display_label}",
                        numerator=blm_area.m,
                        denominator=total_area.m,
                        color="green",
                    ),
                ],
            )
        )

    return cards


def hydrologic_regime_cards(data_df: pd.DataFrame) -> list[ProgressCard]:
    if RSFieldMeta().unit_system == "imperial":
        area_label = 'acre'
        display_label = 'AC'
    else:
        area_label = 'hectare'
        display_label = 'HA'

    cards = []
    for regime in data_df['water_regime_name'].unique():
        if regime is None:
            continue
        if pd.isnull(regime):
            continue
        regime_group = data_df[data_df['water_regime_name'] == regime]
        blm_regime_group = regime_group[regime_group['ownership'] == 'BLM']

        total_area = regime_group['area'].sum().to(area_label)
        blm_area = blm_regime_group['area'].sum().to(area_label)

        cards.append(
            ProgressCard(
                f"{regime}",
                total=f'{int(total_area.m)} {display_label}',
                groups=[
                    ProgressGroup(
                        "BLM",
                        f"{int(blm_area.m)} / {int(total_area.m)} {display_label}",
                        numerator=blm_area.m,
                        denominator=total_area.m,
                        color="darkcyan",
                    ),
                ],
            )
        )

    return cards


def nwi_modifier_cards(data_df: pd.DataFrame) -> list[ProgressCard]:
    if RSFieldMeta().unit_system == "imperial":
        area_label = 'acre'
        display_label = 'AC'
    else:
        area_label = 'hectare'
        display_label = 'HA'

    cards = []
    for modifier in data_df['modifier1_name'].unique():
        if modifier is None:
            continue
        if pd.isnull(modifier):
            continue
        modifier_group = data_df[data_df['modifier1_name'] == modifier]
        blm_modifier_group = modifier_group[modifier_group['ownership'] == 'BLM']

        total_area = modifier_group['area'].sum().to(area_label)
        blm_area = blm_modifier_group['area'].sum().to(area_label)

        cards.append(
            ProgressCard(
                f"{modifier}",
                total=f'{int(total_area.m)} {display_label}',
                groups=[
                    ProgressGroup(
                        "BLM",
                        f"{int(blm_area.m)} / {int(total_area.m)} {display_label}",
                        numerator=blm_area.m,
                        denominator=total_area.m,
                        color="goldenrod",
                    ),
                ],
            )
        )

    return cards
