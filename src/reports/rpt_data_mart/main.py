"""Generate a Data Mart export – enriched Parquet files with bins and colours
for Power BI, notebooks, and other self-service analytics consumers.

Copilot-generated module.
"""

# Standard library
import argparse
import logging
import os
import shutil
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# 3rd party
import geopandas as gpd
import pint_pandas
import psutil
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

# Local
from reports.rpt_data_mart import __version__ as report_version
from reports.rpt_riverscapes_inventory.dataprep import add_calculated_rme_cols
from util import prepare_gdf_for_athena
from util.athena import aoi_query_to_local_parquet, get_field_metadata
from util.pandas import RSFieldMeta, load_gdf_from_pq
from util.rme.rme_common_dataprep import apply_all_bins

# Fields requested from the rpt_rme_pq Athena view.
# Same core set as rpt_riverscapes_inventory plus geometry for GeoParquet.
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
    "rme_project_id, rme_project_name"
)


def define_fields(unit_system: str = "SI") -> None:
    """Set up the fields and units for this export.

    Copilot-generated function.
    """
    meta = RSFieldMeta()
    meta.field_meta = get_field_metadata(
        authority="data-exchange-scripts",
        tool_schema_name="*",
        layer_id="raw_rme,rpt_rme,rs_context_huc10",
    )
    meta.unit_system = unit_system
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
    2. Query Athena for DGO data (or reuse existing Parquet).
    3. Add calculated columns, apply units, apply bins + colours.
    4. Write the enriched GeoParquet to ``report_dir/data_mart.parquet``.

    Returns:
        Path to the written Parquet file.

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

    # Background task: load metadata
    with ThreadPoolExecutor(max_workers=1) as executor:
        meta_future = executor.submit(define_fields, unit_system)

        # ---------- DGO data ----------
        if parquet_override:
            parquet_data_source = Path(parquet_override)
            if not parquet_data_source.exists():
                raise FileNotFoundError(f"Parquet path '{parquet_data_source}' does not exist")
            log.info(f"Using supplied Parquet at {parquet_override}")
        else:
            parquet_data_source = report_dir / "pq"
            log.info("Querying Athena for DGO data …")
            query_str = f"SELECT {DGO_FIELDS} FROM rpt_rme_pq WHERE {{prefilter_condition}} AND {{intersects_condition}}"
            aoi_query_to_local_parquet(
                query_str,
                geometry_field_expression="ST_GeomFromBinary(dgo_geom)",
                geom_bbox_field="dgo_geom_bbox",
                aoi_gdf=query_gdf,
                local_path=parquet_data_source,
            )

        data_gdf = load_gdf_from_pq(parquet_data_source)

        # Wait for metadata
        meta_future.result()
        log.info("Field metadata loaded.")

    # ---------- Enrich ----------
    data_gdf = add_calculated_rme_cols(data_gdf)
    data_gdf, _ = RSFieldMeta().apply_units(data_gdf)

    # Apply bins + hex colours for every mappable column
    data_gdf = apply_all_bins(data_gdf)
    log.info(f"DataFrame enriched: {len(data_gdf)} rows, {len(data_gdf.columns)} columns")

    # ---------- Export ----------
    # Strip Pint unit dtypes to plain numerics — PyArrow/Parquet does not support pint dtypes
    for col in list(data_gdf.columns):
        if isinstance(data_gdf[col].dtype, pint_pandas.PintType):
            data_gdf[col] = data_gdf[col].pint.magnitude

    output_path = report_dir / "data_mart.parquet"
    data_gdf.to_parquet(output_path, index=False)
    log.info(f"Data Mart Parquet written to {output_path}")

    # Clean up staging Parquet if not needed
    if not keep_parquet and not parquet_override:
        try:
            staging = report_dir / "pq"
            if staging.exists():
                shutil.rmtree(staging)
                log.info(f"Deleted staging folder {staging}")
        except Exception as err:
            log.warning(f"Failed to delete staging folder: {err}")

    log.title("Data Mart Export Complete")
    log.info(f"Output: {output_path}")
    return output_path


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
        export_data_mart(
            args.report_name,
            output_path,
            args.path_to_shape,
            unit_system=args.unit_system,
            parquet_override=args.parquet_path,
            keep_parquet=args.keep_parquet,
        )

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
