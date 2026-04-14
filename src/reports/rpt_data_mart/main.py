"""Generate a Data Mart export – enriched Parquet files with bins and colours
for Power BI, notebooks, and other self-service analytics consumers.
Also exports `data_dictionary.csv` containing valuable metadata.

Copilot-generated module, directed by Lorin March 2026.
"""

# Standard library
import argparse
import logging
import os
import shutil
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

# 3rd party
import geopandas as gpd
import markdown
import pandas as pd
import pint_pandas
import psutil
from jinja2 import Template
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from reports.rpt_data_mart import __version__ as report_version

# Local
from reports.rpt_data_mart.dataprep import add_calculated_rme_cols, unnest_dem_bins
from reports.rpt_riverscapes_inventory.dataprep import get_nid_data  # TODO no-cross-report imports - move nid stuff to util
from util import prepare_gdf_for_athena
from util.athena import aoi_query_to_local_parquet, get_field_metadata
from util.attains_assessment import query_attains_assessments
from util.climate_engine_connections import (
    enrich_vegetation_cover_df,
    get_vegetation_cover_timeseries,
)
from util.metadata_export import TableEntry, export_data_dictionary
from util.pandas import RSFieldMeta, load_gdf_from_pq, ureg
from util.rme.rme_common_dataprep import apply_all_bins

PBI_MODEL_NAME = "Riverscapes Data Mart Report"

# ---------------------------------------------------------------------------
# Dataset query configuration
# ---------------------------------------------------------------------------


@dataclass
class DatasetQuery:
    """Configuration for querying a single dataset from Athena.

    Copilot-generated class.
    """

    name: str
    query_template: str
    geometry_field_expression: str
    geom_bbox_field: str


def _build_dataset_queries(include_geometry: bool = False) -> list[DatasetQuery]:
    """Return query configurations for all Data Mart datasets.

    Copilot-generated function.
    """
    # DGO fields from the rs_rpt intersections Athena table
    dgo_fields = (
        "level_path, seg_distance, centerline_length, segment_area, "
        "fcode, fcode_desc, longitude, latitude, "
        "ownership, ownership_desc, state, county, drainage_area, "
        "stream_name, stream_order, stream_length, "
        "waterbody_type, waterbody_extent, ecoregion3, ecoregion4, elevation, geology, "
        "huc12, huc10, "
        "prim_channel_gradient, valleybottom_gradient, rel_flow_length, confluences, diffluences, tributaries, tribs_per_km, planform_sinuosity, lowlying_area, elevated_area, channel_area, floodplain_area, integrated_width, "
        "active_channel_ratio, low_lying_ratio, elevated_ratio, floodplain_ratio, "
        "acres_vb_per_mile, hect_vb_per_km, channel_width, confinement_ratio, constriction_ratio, confining_margins, constricting_margins, "
        "lf_evt, lf_bps, lf_agriculture_prop, lf_agriculture, lf_conifer_prop, lf_conifer, lf_conifer_hardwood_prop, lf_conifer_hardwood, lf_developed_prop, lf_developed, lf_exotic_herbaceous_prop, "
        "lf_exotic_herbaceous, lf_exotic_tree_shrub_prop, lf_exotic_tree_shrub, lf_grassland_prop, lf_grassland, lf_hardwood_prop, lf_hardwood, lf_riparian_prop, lf_riparian, lf_shrubland_prop, lf_shrubland, "
        "lf_sparsely_vegetated_prop, lf_sparsely_vegetated, lf_hist_conifer_prop, lf_hist_conifer, lf_hist_conifer_hardwood_prop, lf_hist_conifer_hardwood, lf_hist_grassland_prop, lf_hist_grassland, "
        "lf_hist_hardwood_prop, lf_hist_hardwood, lf_hist_hardwood_conifer_prop, lf_hist_hardwood_conifer, lf_hist_peatland_forest_prop, lf_hist_peatland_forest, lf_hist_peatland_nonforest_prop, "
        "lf_hist_peatland_nonforest, lf_hist_riparian_prop, lf_hist_riparian, lf_hist_savanna_prop, lf_hist_savanna, lf_hist_shrubland_prop, lf_hist_shrubland, lf_hist_sparsely_vegetated_prop, lf_hist_sparsely_vegetated, "
        "ex_riparian, hist_riparian, prop_riparian, hist_prop_riparian, riparian_veg_departure, "
        "ag_conversion, develop, grass_shrub_conversion, conifer_encroachment, invasive_conversion, riparian_condition, "
        "qlow, q2, splow, sphigh, "
        "road_len, road_dens, rail_len, rail_dens, land_use_intens, "
        "road_dist, rail_dist, div_dist, canal_dist, infra_dist, "
        "fldpln_access, access_fldpln_extent, "
        "brat_capacity, brat_hist_capacity, brat_risk, brat_opportunity, brat_limitation, brat_complex_size, brat_hist_complex_size, dam_setting, "
        "rme_project_id, rme_version, rme_project_name, pasture_rs_row_id, pasture_nm_pasture_id "
    )

    # HUC10 watershed boundary + RS Context metadata.
    huc_fields = "huc10.huc10 AS huc, huc10.name as hucname, huc10.areasqkm as hucareasqkm, rscontext.project_id, dem_bins, 100 * (ST_AREA(ST_INTERSECTION(huc10.geom, input_geom.geom)) / ST_AREA(huc10.geom)) AS percent_intersection"

    pastures_fields = "rs_row_id, allot_no, allot_name, past_no, past_name, admin_st, adm_ofc_cd, adm_unit_cd, st_allot_past, st_allot_past_name, st_allot_past_multi"
    pastures_nm_fields = "pasture_id, pasture_name, pasture_latitude, pasture_longitude"

    if include_geometry:
        dgo_fields += ", dgo_geom"
        # TODO (ENHANCEMENT): add geometry for other tables
        # huc_fields += ", huc10.geom" # [ERROR] [Setup] NOT_SUPPORTED: Unsupported Hive type: Geometry

    return [
        DatasetQuery(
            name="dgo",
            query_template=(f"SELECT {dgo_fields} FROM input_geom, rs_rpt.rpt_rme_intersections WHERE {{prefilter_condition}} AND {{intersects_condition}}"),
            geometry_field_expression="ST_GeomFromBinary(dgo_geom)",
            geom_bbox_field="dgo_geom_bbox",
        ),
        DatasetQuery(
            name="huc10_rscontext",
            query_template=(
                ", rme_huc10s AS (SELECT DISTINCT huc10 FROM rs_rpt.rpt_rme_intersections) "
                f" SELECT {huc_fields}, "
                "CASE WHEN rme_huc10s.huc10 IS NOT null THEN 1 ELSE 0 END AS has_rme "
                "FROM input_geom, "
                "(SELECT huc10, name, areasqkm, geometry_bbox, ST_GeomFromBinary(geometry) AS geom FROM wbdhu10_cleaned) huc10 "
                "LEFT JOIN rs_context_huc10 rscontext ON huc10.huc10 = rscontext.huc "
                "LEFT JOIN rme_huc10s ON huc10.huc10 = rme_huc10s.huc10 "
                "WHERE {prefilter_condition} AND {intersects_condition}"
            ),
            geometry_field_expression="huc10.geom",
            geom_bbox_field="geometry_bbox",
        ),
        DatasetQuery(
            name="pastures",
            query_template=(f"SELECT {pastures_fields} FROM input_geom, rs_raw.blm_natl_grazing_pasture_polygons WHERE {{prefilter_condition}} AND {{intersects_condition}}"),
            geometry_field_expression="ST_GeomFromBinary(geometry)",
            geom_bbox_field="geometry_bbox",
        ),
        DatasetQuery(
            name="pastures_nm_bootheel",
            query_template=f"SELECT {pastures_nm_fields} FROM input_geom, rs_raw.blm_natl_grazing_pasture_polygons_nm_bootheel_snapshot_20260402 WHERE {{prefilter_condition}} AND {{intersects_condition}}",
            geometry_field_expression="ST_GeomFromBinary(geometry)",
            geom_bbox_field="geometry_bbox",
        ),
    ]


# ---------------------------------------------------------------------------
# Helper functions for repeated operations
# ---------------------------------------------------------------------------


def _query_dataset(
    dataset: DatasetQuery,
    query_gdf: gpd.GeoDataFrame,
    staging_path: Path,
) -> None:
    """Run a single Athena spatial query and write results to local staging Parquet.

    Copilot-generated function.
    """
    log = Logger(f"Query {dataset.name}")
    log.info(f"Querying Athena for {dataset.name} data …")
    aoi_query_to_local_parquet(
        dataset.query_template,
        geometry_field_expression=dataset.geometry_field_expression,
        geom_bbox_field=dataset.geom_bbox_field,
        aoi_gdf=query_gdf,
        local_path=staging_path,
    )
    log.info(f"{dataset.name} query complete -> {staging_path}")


def _strip_pint_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Pint-typed columns to plain numeric dtypes for Parquet export.

    Copilot-generated function.
    """
    for col in list(df.columns):
        if isinstance(df[col].dtype, pint_pandas.PintType):
            df[col] = df[col].pint.magnitude
    return df


def _export_parquet(df: pd.DataFrame, output_path: Path) -> Path:
    """Strip Pint types, fix null-typed columns, and write a DataFrame to Parquet.

    All-null columns keep Arrow's ``null`` type which Power BI cannot map to a
    .NET DataColumn type, causing a crash on refresh.  Cast them to ``str``
    (becomes Arrow ``string``) so PBI always sees a valid type.

    Copilot-generated function.
    """
    df = _strip_pint_types(df)
    for col in list(df.columns):
        if df[col].isna().all():
            df[col] = df[col].astype(str)
    df.to_parquet(output_path, index=False)
    Logger("Export").info(f"Parquet written to {output_path} ({len(df)} rows, {len(df.columns)} cols)")
    return output_path


def _cleanup_staging(staging_path: Path) -> None:
    """Remove a staging directory if it exists.

    Copilot-generated function.
    """
    try:
        if staging_path.exists():
            shutil.rmtree(staging_path)
            Logger("Cleanup").info(f"Deleted staging folder {staging_path}")
    except Exception as err:
        Logger("Cleanup").warning(f"Failed to delete staging folder: {err}")


def define_fields(unit_system: str = "SI") -> None:
    """Set up the fields and units for this export.

    Copilot-generated function.
    """
    meta = RSFieldMeta()
    registry_field_meta = get_field_metadata(
        authority="data-exchange-scripts,riverscapes-tools",
        tool_schema_name="*",
        layer_id="raw_rme,rpt_rme,rs_context_huc10,blm-natl-grazing-pasture-polygons,blm-natl-grazing-pasture-polygons-nm-bootheel,usace-nid",
    )
    # Consolidate RME source layers under the Data Mart table layer id.
    registry_field_meta.loc[registry_field_meta["layer_id"].isin(["raw_rme", "rpt_rme"]), "layer_id"] = "dgo"

    # Translate registry dotted struct names to flat column names produced by unnest_dem_bins.
    # Scalar struct fields: replace '.' with '_' (e.g. dem_bins.min → dem_bins_min).
    scalar_mask = registry_field_meta["name"].str.startswith("dem_bins.") & ~registry_field_meta["name"].str.startswith("dem_bins.bins.")
    registry_field_meta.loc[scalar_mask, "name"] = registry_field_meta.loc[scalar_mask, "name"].str.replace(".", "_", regex=False)
    # Bins array leaf fields: reassign to the 'dem_bins' layer and map to semantic column names.
    bins_mask = registry_field_meta["name"].str.startswith("dem_bins.bins.")
    registry_field_meta.loc[bins_mask, "layer_id"] = "dem_bins"
    registry_field_meta.loc[registry_field_meta["name"] == "dem_bins.bins.bin", "name"] = "bin"
    registry_field_meta.loc[registry_field_meta["name"] == "dem_bins.bins.cell_count", "name"] = "cell_count"

    meta.field_meta = registry_field_meta
    meta.unit_system = unit_system
    # Display-unit overrides: convert raw Athena units to user-facing units.
    # Review: add set_display_unit calls here for any column whose data_unit
    # from Athena should be presented differently (e.g. m → km) or ft → mile based on SI_TO_IMPERIAL in RSFieldMeta

    # At the dgo level metres and feet make sense but as soon as aggregating a few hundred as we typically do, km and miles are better
    # BLM asked for miles and acres to be default so we'll set them all and then add back exceptions
    # override all m > km and m2 to km2
    for _, meta_row in meta.field_meta.iterrows():
        if meta_row["data_unit"] == ureg.meter:
            meta.set_display_unit(meta_row["name"], ureg.kilometer, meta_row["layer_id"])
        elif meta_row["data_unit"] == ureg.meter**2:
            meta.set_display_unit(meta_row["name"], ureg.kilometer**2, meta_row["layer_id"])

    # exceptions:
    meta.set_display_unit("elevation", ureg.meter, "dgo")
    meta.set_display_unit("q2", ureg.meter**3, "dgo")
    meta.set_display_unit("qlow", ureg.meter**3, "dgo")
    meta.set_display_unit("dem_bins_min", ureg.meter, "rs_context_huc10")
    meta.set_display_unit("dem_bins_max", ureg.meter, "rs_context_huc10")
    meta.set_display_unit("dem_bins_bin_size", ureg.meter, "rs_context_huc10")
    meta.set_display_unit("bin", ureg.meter, "dem_bins")

    # Duplicate 'huc' metadata into the 'dem_bins' layer so apply_units can resolve
    # it when processing the long-format dem_bins table (layer_id="dem_bins").
    meta.duplicate_meta("huc", "huc", orig_layer_id="rs_context_huc10", new_layer_id="dem_bins")


def generate_readme(report_dir: Path) -> None:
    """Generate a README.md file describing the Data Mart export.
    And a simple HTML variant as well (not a full report - maybe future enhancement)
    """
    src_dir = Path(__file__).parent
    template_path = src_dir / 'templates' / 'user_readme_template.md'
    with open(template_path, encoding='utf-8') as f:
        template = Template(f.read())

    context = {"report_version": report_version}
    readme_contents = template.render(context)

    # Write Markdown README
    readme_md_path = report_dir / 'README.md'
    with open(readme_md_path, 'w', encoding='utf-8') as f:
        f.write(readme_contents)
    Logger("Docs").info(f"Generated {readme_md_path.name}")

    # Write simple HTML variant
    html_contents = markdown.markdown(readme_contents, extensions=['tables', 'fenced_code'])
    html_template_path = src_dir / 'templates' / 'user_readme_template.html'
    with open(html_template_path, encoding='utf-8') as f:
        html_template = Template(f.read())

    html_doc = html_template.render(html_contents=html_contents)

    readme_html_path = report_dir / 'report.html'
    with open(readme_html_path, 'w', encoding='utf-8') as f:
        f.write(html_doc)
    Logger("Docs").info(f"Generated {readme_html_path.name}")


def export_data_mart(
    report_name: str,
    report_dir: Path,
    path_to_shape: str,
    unit_system: str = "SI",
    parquet_override: Path | None = None,
    keep_parquet: bool = False,
    include_geometry: bool = False,
) -> Path:
    """Orchestrate the Data Mart export.

    1. Read AOI shapefile and simplify for Athena.
    2. Query Athena for DGO, HUC, Pastures data in parallel
       (or reuse existing Parquet for DGO).
    3. Add calculated columns, apply units, and apply bins + colours to DGO.
    4. Write each dataset to ``report_dir/exports/<name>.parquet``.
    5. Write ``report_dir/data_dictionary.csv`` with metadata for all tables.

    Returns:
        Path to the exports subfolder containing all Parquet files.

    Copilot-generated function.
    """
    log = Logger("Data Mart Export")
    log.info("Data Mart export begun")

    aoi_gdf = gpd.read_file(path_to_shape)
    query_gdf, simplification_results = prepare_gdf_for_athena(aoi_gdf)
    if not simplification_results.success:
        log.warning("Unable to simplify input geometry sufficiently – query may fail.")
    if simplification_results.simplified:
        log.warning(f"Input polygon simplified (tolerance {simplification_results.tolerance_m} m) for Athena query.")

    safe_makedirs(str(report_dir))
    exports_dir = report_dir / "exports"
    safe_makedirs(str(exports_dir))
    staging_dir = report_dir / "staging"
    safe_makedirs(str(staging_dir))

    # ---- Determine which datasets need a live Athena query ----
    if parquet_override:
        if not Path(parquet_override).exists():
            raise FileNotFoundError(f"Parquet path '{parquet_override}' does not exist")
        log.info(f"Using supplied data at {parquet_override} rather than querying Athena")
        datasets_to_query = []
    else:
        datasets_to_query = _build_dataset_queries(include_geometry=include_geometry)

    # ---- Query all datasets + load metadata in parallel ----
    max_workers = len(datasets_to_query) + 4  # +1 metadata, +1 Climate Engine, +1 ATTAINS +1 NID
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        log.info("Starting background queries...")
        meta_future = executor.submit(define_fields, unit_system)
        ce_veg_future = executor.submit(get_vegetation_cover_timeseries, query_gdf)
        attains_future = executor.submit(query_attains_assessments, query_gdf)
        nid_future = executor.submit(get_nid_data, query_gdf) if not parquet_override else None
        query_futures = {ds.name: executor.submit(_query_dataset, ds, query_gdf, staging_dir / ds.name) for ds in datasets_to_query}

        # Wait for all Athena queries
        for name, future in query_futures.items():
            future.result()
            log.info(f"{name} query finished")

        meta_future.result()
        log.info("Field metadata loaded.")

        # Climate Engine – non-critical; log on failure and continue
        ce_veg_df: pd.DataFrame | None = None
        try:
            ce_veg_df = ce_veg_future.result()
            log.info("Climate Engine vegetation cover query finished")
        except Exception as e:
            log.warning(f"Climate Engine vegetation query failed (non-fatal): {e}")

        # ATTAINS – non-critical; log on failure and continue
        attains_df: pd.DataFrame | None = None
        try:
            attains_df = attains_future.result()
            if attains_df is not None and not attains_df.empty:
                log.info(f"ATTAINS query finished: {len(attains_df)} records")
            else:
                attains_df = None
                log.info("ATTAINS query returned no records")
        except Exception as e:
            log.warning(f"ATTAINS query failed (non-fatal): {e}")

        # Retrieve NID results
        nid_gdf = None
        if nid_future:
            try:
                nid_gdf = nid_future.result()
                if nid_gdf is None:
                    log.warning("No NID gdf returned.")
                else:
                    log.info(f"NID background task finished. Found {len(nid_gdf)} dams.")
                    # export raw to gpkg -
                    # NOTE: This step is not required in production since we just delete the staging_dir anyway
                    # but useful in dev, allows repeated processing without requerying
                    if not nid_gdf.empty:
                        nid_gdf.to_file(staging_dir / "nid_dams.gpkg", driver="GPKG")
            except Exception as e:
                log.error(f"Error retrieving NID results: {e}")
        elif parquet_override is not None:
            try:
                nid_path = Path(parquet_override) / "nid_dams.gpkg"
                if nid_path.exists():
                    nid_gdf = gpd.read_file(nid_path)
                    log.info(f"Loaded NID from override file: {nid_path} ({len(nid_gdf)} rows)")
                else:
                    log.info(f"No override NID file found at {nid_path}; skipping NID")
            except Exception as e:
                log.error(f"Error loading override NID file: {e}")

    # ---- Load, process, and export each dataset ----
    # Each table goes through apply_units for dtype coercion and unit conversion,
    # then its applied_units dict is bundled into a TableEntry so the data
    # dictionary records exactly what unit each column's data was converted to.
    #
    # Future direction (DataFrame-local metadata): Currently metadata lives in
    # the RSFieldMeta Borg singleton and applied_units is carried separately via
    # TableEntry.  As we move toward DataFrame-local metadata, each df.attrs
    # should carry its own metadata dict (field meta + applied units) so that
    # downstream consumers don't need the global singleton.  The per-table
    # TableEntry pattern and attrs["layer_id"] are stepping stones toward that.
    all_tables: dict[str, TableEntry] = {}
    meta = RSFieldMeta()

    # DGO: calculated cols → units → bins
    # DGO: metadata is normalized to the single layer id "dgo" in define_fields.
    dgo_staging = Path(parquet_override / "dgo") if parquet_override else staging_dir / "dgo"
    dgo_df = load_gdf_from_pq(dgo_staging)
    dgo_df.attrs["layer_id"] = "dgo"
    dgo_df = add_calculated_rme_cols(dgo_df)
    dgo_df, dgo_applied_units = meta.apply_units(dgo_df)
    dgo_df = apply_all_bins(dgo_df)
    log.info(f"DGO enriched: {len(dgo_df)} rows, {len(dgo_df.columns)} cols")
    _export_parquet(dgo_df, exports_dir / "dgo.parquet")
    all_tables["dgo"] = TableEntry(df=dgo_df, applied_units=dgo_applied_units)

    # HUC: dtype coercion + unit conversion
    huc_staging = Path(parquet_override / "huc10_rscontext") if parquet_override else staging_dir / "huc10_rscontext"
    huc_df = load_gdf_from_pq(huc_staging)
    huc_df, dem_bins_df = unnest_dem_bins(huc_df)
    huc_df.attrs["layer_id"] = "rs_context_huc10"
    huc_df, huc_applied_units = meta.apply_units(huc_df)
    log.info(f"HUC10_rscontext loaded: {len(huc_df)} rows, {len(huc_df.columns)} cols")
    _export_parquet(huc_df, exports_dir / "huc10_rscontext.parquet")
    all_tables["huc10_rscontext"] = TableEntry(df=huc_df, applied_units=huc_applied_units)

    # DEM elevation bins: long-format table derived from the dem_bins struct
    if dem_bins_df is not None and not dem_bins_df.empty:
        dem_bins_df.attrs["layer_id"] = "dem_bins"
        dem_bins_df, dem_bins_applied_units = meta.apply_units(dem_bins_df)
        log.info(f"DEM elevation bins: {len(dem_bins_df)} rows, {len(dem_bins_df.columns)} cols")
        _export_parquet(dem_bins_df, exports_dir / "huc10_dem_bins.parquet")
        all_tables["huc10_dem_bins"] = TableEntry(df=dem_bins_df, applied_units=dem_bins_applied_units)
    else:
        log.warning("DEM elevation bins unavailable; skipping huc10_dem_bins table")

    # Pastures: dtype coercion (currently no unit-bearing columns)
    pastures_staging = Path(parquet_override / "pastures") if parquet_override else staging_dir / "pastures"
    pastures_df = load_gdf_from_pq(pastures_staging)
    pastures_df.attrs["layer_id"] = "blm-natl-grazing-pasture-polygons"
    pastures_df, pastures_applied_units = meta.apply_units(pastures_df)
    log.info(f"Pastures loaded: {len(pastures_df)} rows, {len(pastures_df.columns)} cols")
    _export_parquet(pastures_df, exports_dir / "pastures.parquet")
    all_tables["pastures"] = TableEntry(df=pastures_df, applied_units=pastures_applied_units)

    # Pastures_nm_bootheel: dtype coercion (currently no unit-bearing columns)
    pastures_nm_bootheel_staging = Path(parquet_override / "pastures_nm_bootheel") if parquet_override else staging_dir / "pastures_nm_bootheel"
    pastures_nm_bootheel_df = load_gdf_from_pq(pastures_nm_bootheel_staging)
    pastures_nm_bootheel_df.attrs["layer_id"] = "blm-natl-grazing-pasture-polygons-nm-bootheel"
    pastures_nm_bootheel_df, pastures_nm_bootheel_applied_units = meta.apply_units(pastures_nm_bootheel_df)
    log.info(f"Pastures loaded: {len(pastures_nm_bootheel_df)} rows, {len(pastures_nm_bootheel_df.columns)} cols")
    _export_parquet(pastures_nm_bootheel_df, exports_dir / "pastures_nm_bootheel.parquet")
    all_tables["pastures_nm_bootheel"] = TableEntry(df=pastures_nm_bootheel_df, applied_units=pastures_nm_bootheel_applied_units)

    # Climate Engine: vegetation cover timeseries
    if ce_veg_df is not None:
        ce_veg_enriched = enrich_vegetation_cover_df(ce_veg_df)
        ce_veg_enriched.attrs["layer_id"] = "vegetation_cover"
        log.info(f"Climate Engine veg cover: {len(ce_veg_enriched)} rows, {len(ce_veg_enriched.columns)} cols")
        _export_parquet(ce_veg_enriched, exports_dir / "vegetation_cover.parquet")
        all_tables["vegetation_cover"] = TableEntry(df=ce_veg_enriched, applied_units={})
    else:
        log.warning("Climate Engine vegetation data unavailable; skipping vegetation_cover table")

    # ATTAINS: EPA water quality assessments
    if attains_df is not None:
        attains_df.attrs["layer_id"] = "attains"
        log.info(f"ATTAINS assessments: {len(attains_df)} rows, {len(attains_df.columns)} cols")
        _export_parquet(attains_df, exports_dir / "attains.parquet")
        all_tables["attains"] = TableEntry(df=attains_df, applied_units={})
    else:
        log.warning("ATTAINS data unavailable; skipping attains table")

    # NID (National inventory of dams)
    if nid_gdf is not None:
        log.info(f"National Inventory of Dams: {len(nid_gdf)} rows, {len(nid_gdf.columns)} cols")
        nid_gdf.attrs["layer_id"] = "usace-nid"
        # this is where metadata is added
        nid_display_cols = [
            'NAME',
            'NIDID',
            'PRIMARY_OWNER_TYPE',
            'PURPOSES',
            'LATITUDE',
            'LONGITUDE',
            'RIVER_OR_STREAM',
            'STATE_REGULATED',
            'FEDERALLY_REGULATED_DAM',
            'PRIMARY_DAM_TYPE',
            'DAM_TYPES',
            'DAM_HEIGHT',
            'HYDRAULIC_HEIGHT',
            'STRUCTURAL_HEIGHT',
            'NID_HEIGHT',
            'DAM_LENGTH',
            'DAM_VOLUME',
            'YEAR_COMPLETED',
            'NID_STORAGE',
            'MAX_STORAGE',
            'NORMAL_STORAGE',
            'DRAINAGE_AREA',
            'MAX_DISCHARGE',
            'SPILLWAY_TYPE',
            'SPILLWAY_WIDTH',
            'HAZARD_POTENTIAL',
            'CONDITION_ASSESSMENT',
            'pasture_id',
            'HUC10_Code',
        ]

        # add URL to USACE dam detail page before building cols_to_use
        # Create Hyperlink for NAME using NIDID
        if len(nid_gdf) > 0 and 'NIDID' in nid_gdf.columns:
            nid_gdf['NID_URL'] = nid_gdf.apply(lambda row: f'https://nid.sec.usace.army.mil/nid/#/dams/system/{row["NIDID"]}/summary', axis=1)
            meta.add_field_meta(name='NID_URL', layer_id=nid_gdf.attrs["layer_id"], friendly_name='URL for Dam Detail Page', description='Generated url linking to the dam detail web page', theme='Description')
            nid_display_cols.append('NID_URL')

        # Ensure we only work with available columns
        cols_to_use = [c for c in nid_display_cols if c in nid_gdf.columns]

        nid_display_df, nid_units = meta.apply_units(nid_gdf[cols_to_use].copy())
        # pandas currently preserves attrs through slice/copy and apply_units,
        # but set explicitly so metadata export always has deterministic layer context.
        nid_display_df.attrs["layer_id"] = "usace-nid"

        _export_parquet(nid_display_df, exports_dir / "nid.parquet")
        all_tables["nid"] = TableEntry(df=nid_display_df, applied_units=nid_units)
    else:
        log.warning("National Inventory of Dams unavailable; skipping dams table")

    # ---- Data dictionary covering all datasets ----
    dict_path = report_dir / "data_dictionary.csv"
    export_data_dictionary(all_tables, dict_path)

    # ---- Clean up staging ----
    if not keep_parquet:
        _cleanup_staging(staging_dir)

    # ---- Generate Documentation ----
    generate_readme(report_dir)

    log.title("Data Mart Export Complete")
    log.info(f"Output: {exports_dir}")
    return exports_dir


def main() -> None:
    """CLI entry point for the Data Mart export.

    Copilot-generated function.
    """
    parser = argparse.ArgumentParser(description="Generate a Data Mart Parquet export with bins and colours.")
    parser.add_argument("output_path", help="Folder to write outputs (will be created)", type=Path)
    parser.add_argument("path_to_shape", help="Path to the GeoJSON / shapefile AOI", type=str)
    parser.add_argument("report_name", help="Human-readable name for this export")
    parser.add_argument("--unit_system", help="Unit system: SI or imperial", type=str, default="SI")
    parser.add_argument(
        "--use-parquet",
        dest="parquet_path",
        type=Path,
        default=None,
        help="Reuse existing Parquet directory instead of querying Athena",
    )
    parser.add_argument("--keep-parquet", action="store_true", help="Keep staging Parquet files")
    parser.add_argument("--generate-pbi", action="store_true", help="Generate a Power BI (.pbip) project from the data dictionary")
    parser.add_argument("--include-geometry", action="store_true", help="Include polygon geometries in dataset (increases size significantly)")

    args = dotenv.parse_args_env(parser)

    output_path = Path(args.output_path)
    safe_makedirs(str(output_path))

    log = Logger("Setup")
    log_path = output_path / "data_mart.log"
    log.setup(log_path=log_path, log_level=logging.DEBUG)
    log.title("rpt-data-mart")
    log.info(f"Output: {output_path}")
    log.info(f"AOI: {args.path_to_shape}")
    log.info(f"Report name: {args.report_name}")
    log.info(f"Version: {report_version}")

    try:
        exports_dir = export_data_mart(
            args.report_name,
            output_path,
            args.path_to_shape,
            unit_system=args.unit_system,
            parquet_override=args.parquet_path,
            keep_parquet=args.keep_parquet,
            include_geometry=args.include_geometry,
        )
        log.info(f"Exports written to {exports_dir}")

        if args.generate_pbi:
            from util.pbi_model import generate_pbip

            pbi_dir = output_path / "pbi"
            dict_path = output_path / "data_dictionary.csv"
            generate_pbip(dict_path, pbi_dir, model_name=PBI_MODEL_NAME, data_mart_root=output_path)
            log.info(f"Power BI project generated in {pbi_dir} with model name '{PBI_MODEL_NAME}'")

        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().peak_wset / 1024 / 1024 if hasattr(process.memory_info(), "peak_wset") else process.memory_info().rss / 1024 / 1024
        log.info(f"Peak memory usage: {mem_mb:.2f} MB")

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
