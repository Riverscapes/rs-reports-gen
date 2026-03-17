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
import pandas as pd
import pint_pandas
import psutil
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

# Local
from reports.rpt_data_mart import __version__ as report_version
from reports.rpt_riverscapes_inventory.dataprep import add_calculated_rme_cols  # TODO no-cross-report imports
from util import prepare_gdf_for_athena
from util.athena import aoi_query_to_local_parquet, get_field_metadata
from util.metadata_export import TableEntry, export_data_dictionary
from util.pandas import RSFieldMeta, load_gdf_from_pq
from util.rme.rme_common_dataprep import apply_all_bins

# ---------------------------------------------------------------------------
# Field lists for each dataset
# ---------------------------------------------------------------------------

# DGO fields from the rpt_rme_pq Athena view.
DGO_FIELDS = (
    "level_path, seg_distance, centerline_length, segment_area, "
    "fcode, fcode_desc, longitude, latitude, "
    "ownership, ownership_desc, state, county, drainage_area, "
    "stream_name, stream_order, stream_length, huc12, "
    "rel_flow_length, channel_area, integrated_width, "
    "low_lying_ratio, elevated_ratio, floodplain_ratio, "
    "acres_vb_per_mile, hect_vb_per_km, channel_width, "
    "lf_agriculture_prop, lf_agriculture, lf_developed_prop, lf_developed, "
    "lf_riparian_prop, lf_riparian, ex_riparian, hist_riparian, "
    "prop_riparian, hist_prop_riparian, develop, "
    "road_len, road_dens, rail_len, rail_dens, land_use_intens, "
    "road_dist, rail_dist, div_dist, canal_dist, infra_dist, "
    "fldpln_access, access_fldpln_extent, confinement_ratio, "
    "brat_capacity, brat_hist_capacity, "
    "riparian_veg_departure, riparian_condition, "
    "rme_project_id, rme_project_name, graz_globalid"
)

# HUC10 watershed boundary + RS Context metadata.
HUC_FIELDS = "huc10.huc10 AS huc, rscontext.project_id, rscontext.hucname, rscontext.hucareasqkm, dem_bins, 100 * (ST_AREA(ST_INTERSECTION(huc10.geom, input_geom.geom)) / ST_AREA(huc10.geom)) AS percent_intersection"

# BLM National Grazing Allotments.
GRAZING_FIELDS = "allot_no, allot_name, admin_st, adm_ofc_cd, st_allot, globalid"


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


def _build_dataset_queries() -> list[DatasetQuery]:
    """Return query configurations for all Data Mart datasets.

    Copilot-generated function.
    """
    return [
        DatasetQuery(
            name="dgo",
            # TODO: move to a production source once it exists
            query_template=(f"SELECT {DGO_FIELDS} FROM input_geom, dev_riverscapes.materialized_rpt_rme_grazing_nm WHERE {{prefilter_condition}} AND {{intersects_condition}}"),
            geometry_field_expression="ST_GeomFromBinary(dgo_geom)",
            geom_bbox_field="dgo_geom_bbox",
        ),
        DatasetQuery(
            name="huc",
            query_template=(
                f"SELECT {HUC_FIELDS} "
                "FROM input_geom, "
                "(SELECT huc10, geometry_bbox, ST_GeomFromBinary(geometry) AS geom FROM wbdhu10_cleaned) huc10 "
                "LEFT JOIN rs_context_huc10 rscontext ON huc10.huc10 = rscontext.huc "
                "WHERE {prefilter_condition} AND {intersects_condition}"
            ),
            geometry_field_expression="huc10.geom",
            geom_bbox_field="geometry_bbox",
        ),
        DatasetQuery(
            name="grazing",
            query_template=(f"SELECT {GRAZING_FIELDS} FROM input_geom, default.blm_natl_grazing_allotments WHERE {{prefilter_condition}} AND {{intersects_condition}}"),
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
    """Strip Pint types and write a DataFrame to Parquet.

    Copilot-generated function.
    """
    df = _strip_pint_types(df)
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
    meta.field_meta = get_field_metadata(
        authority="data-exchange-scripts,riverscapes-tools",
        tool_schema_name="*",
        layer_id="raw_rme,rpt_rme,rs_context_huc10,blm_natl_grazing_allotments",
    )
    meta.unit_system = unit_system
    # Display-unit overrides: convert raw Athena units to user-facing units.
    # Review: add set_display_unit calls here for any column whose data_unit
    # from Athena should be presented differently (e.g. m → km).
    meta.set_display_unit("centerline_length", "kilometer")
    meta.set_display_unit("segment_area", "kilometer ** 2")


def export_data_mart(
    report_name: str,
    report_dir: Path,
    path_to_shape: str,
    unit_system: str = "SI",
    parquet_override: Path | None = None,
    keep_parquet: bool = False,
) -> Path:
    """Orchestrate the Data Mart export.

    1. Read AOI shapefile and simplify for Athena.
    2. Query Athena for DGO, HUC, and Grazing data in parallel
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
    all_datasets = _build_dataset_queries()
    datasets_to_query = [ds for ds in all_datasets if not (ds.name == "dgo" and parquet_override)]
    if parquet_override:
        if not Path(parquet_override).exists():
            raise FileNotFoundError(f"Parquet path '{parquet_override}' does not exist")
        log.info(f"Using supplied Parquet at {parquet_override} for DGO")

    # ---- Query all datasets + load metadata in parallel ----
    max_workers = len(datasets_to_query) + 1  # +1 for metadata
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        meta_future = executor.submit(define_fields, unit_system)
        query_futures = {ds.name: executor.submit(_query_dataset, ds, query_gdf, staging_dir / ds.name) for ds in datasets_to_query}

        # Wait for all queries
        for name, future in query_futures.items():
            future.result()
            log.info(f"{name} query finished")

        meta_future.result()
        log.info("Field metadata loaded.")

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

    # DGO: calculated cols → units → bins
    # Note: DGO columns span multiple registry layers (raw_rme, rpt_rme), so
    # we intentionally leave attrs["layer_id"] unset.  _resolve_unique_id
    # does not fall back from a layer-specific search to all-layers, so
    # pinning a single layer_id would cause columns from the other layer
    # to lose metadata.  Unique column names across layers avoid ambiguity.
    dgo_staging = Path(parquet_override) if parquet_override else staging_dir / "dgo"
    dgo_df = load_gdf_from_pq(dgo_staging)
    dgo_df = add_calculated_rme_cols(dgo_df)
    dgo_df, dgo_applied_units = RSFieldMeta().apply_units(dgo_df)
    dgo_df = apply_all_bins(dgo_df)
    log.info(f"DGO enriched: {len(dgo_df)} rows, {len(dgo_df.columns)} cols")
    _export_parquet(dgo_df, exports_dir / "dgo.parquet")
    all_tables["dgo"] = TableEntry(df=dgo_df, applied_units=dgo_applied_units)

    # HUC: dtype coercion + unit conversion
    huc_df = load_gdf_from_pq(staging_dir / "huc")
    huc_df.attrs["layer_id"] = "rs_context_huc10"
    huc_df, huc_applied_units = RSFieldMeta().apply_units(huc_df)
    log.info(f"HUC loaded: {len(huc_df)} rows, {len(huc_df.columns)} cols")
    _export_parquet(huc_df, exports_dir / "huc.parquet")
    all_tables["huc"] = TableEntry(df=huc_df, applied_units=huc_applied_units)

    # Grazing: dtype coercion (no unit-bearing columns currently in registry)
    grazing_df = load_gdf_from_pq(staging_dir / "grazing")
    grazing_df.attrs["layer_id"] = 'blm_natl_grazing_allotments'
    grazing_df, grazing_applied_units = RSFieldMeta().apply_units(grazing_df)
    log.info(f"Grazing loaded: {len(grazing_df)} rows, {len(grazing_df.columns)} cols")
    _export_parquet(grazing_df, exports_dir / "grazing.parquet")
    all_tables["grazing"] = TableEntry(df=grazing_df, applied_units=grazing_applied_units)

    # ---- Data dictionary covering all datasets ----
    dict_path = report_dir / "data_dictionary.csv"
    export_data_dictionary(all_tables, dict_path)

    # ---- Clean up staging ----
    if not keep_parquet:
        _cleanup_staging(staging_dir)

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
        )
        log.info(f"Exports written to {exports_dir}")

        if args.generate_pbi:
            from util.pbi_model import generate_pbip

            pbi_dir = output_path / "pbi"
            dict_path = output_path / "data_dictionary.csv"
            generate_pbip(dict_path, pbi_dir, model_name=args.report_name)
            log.info(f"Power BI project generated in {pbi_dir}")

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
