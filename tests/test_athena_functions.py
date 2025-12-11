import json
from pathlib import Path
import geopandas as gpd
from rsxml import Logger
from util import prepare_gdf_for_athena
from util.athena import athena_unload_to_dict, run_aoi_athena_query

# ===== TESTING FUNCTIONS ============


def test_unload_query():
    """just outputs result"""
    query_str = """
        SELECT * FROM rs_context_huc10 LIMIT 300
        """
    result = athena_unload_to_dict(query_str)
    print(json.dumps(result, indent=2))


def test_run_aoi_athena_query():
    """get an AOI geometry and query athena raw_rme for data within
    not intended to be called except for isolated testing these functions
    """
    # path_to_shape = r"C:\nardata\work\rme_extraction\20250827-rkymtn\physio_rky_mtn_system.geojson"
    path_to_shape = r"C:\nardata\work\rme_extraction\Price-riv\pricehuc10s.geojson"
    s3_bucket = "riverscapes-athena"
    aoi_gdf = gpd.read_file(path_to_shape)
    path_to_results = run_aoi_athena_query(aoi_gdf, s3_bucket)
    print(path_to_results)


def test_prepare_example_geojsons(
    example_root_path: Path | None = None,
    size_bytes: int = 261_000,
    max_attempts: int = 5
) -> list[dict]:
    """Prepare example AOIs for Athena
    parameters:
        example_root: any path that contains folder(s) named `example` which in turn have .geojson files (e.g. our report source is default)
    Returns metadata for each GeoJSON processed so callers can review any simplification.
    """
    log = Logger('Prepare example AOIs')
    if example_root_path is None:
        example_root_path = Path(__file__).resolve().parents[3] / 'src' / 'reports'

    test_results: list[dict] = []
    for geojson_path in sorted(example_root_path.rglob('example/*.geojson')):
        log.info(f'Processing example AOI: {geojson_path}')
        gdf = gpd.read_file(geojson_path)
        prepared_gdf, meta = prepare_gdf_for_athena(gdf, size_bytes=size_bytes, max_attempts=max_attempts)
        record = {
            'path': str(geojson_path),
            **vars(meta),
        }

        if meta.simplified:
            simplified_path = geojson_path.with_name(f"{geojson_path.stem}_simplified.geojson")
            prepared_gdf.to_file(simplified_path, driver="GeoJSON")
            record['prepared_path'] = str(simplified_path)
            log.info(f'Wrote simplified AOI to {simplified_path}')

        test_results.append(record)

    log.info(f'Processed {len(test_results)} example AOIs from {example_root_path}')
    return test_results


# do not normally run as a module, but if we want to run certain functions, this is a way to do it
if __name__ == '__main__':
    main_results = test_prepare_example_geojsons()
    import pprint
    pprint.pprint(main_results)
    # test_unload_query()
