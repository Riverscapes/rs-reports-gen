"""Query and summarize Inventory of Resources data.

Created 2026-08-13.
Created by copilot.
"""

import geopandas as gpd
import pandas as pd

from util.athena import aoi_query_to_local_parquet
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
