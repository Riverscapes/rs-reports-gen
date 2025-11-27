"""
Reads a CSV file (optionally, first copies it from S3) and generates a GeoPackage (SQLite) file with tables
such as dgos, dgos_veg, dgo_hydro, etc.,
column-to-table-and-type mapping extracted from rme geopackage pragma
into rme_table_column_defs.csv. All tables will include a sequentially generated dgoid column.

Lorin Gaertner (with copilot)
August/Sept 2025

IMPLEMENTED: Sequential dgoid (integer), geometry handling for dgo_geom (SRID 4326, WKT conversion), foreign key syntax in table creation, error handling (throws on missing/malformed required columns), debug output for skipped/invalid rows.
Actual column names/types must be supplied in rme_table_column_defs.csv.
No batching/optimization for very large CSVs - but it handled 1.5 M records okay and managed 7M too.
No advanced validation or transformation beyond geometry and required columns.
Foreign key constraints are defined but not enforced unless PRAGMA foreign_keys=ON is set.
"""

from datetime import datetime
import csv
import json
import os
from pathlib import Path
import apsw
import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
from rsxml import Logger, ProgressBar
from rsxml.util import safe_makedirs
from rsxml.project_xml import (
    Log,
    Project,
    MetaData,
    Meta,
    ProjectBounds,
    Coords,
    BoundingBox,
    Realization,
    Geopackage,
    GeopackageLayer,
    GeoPackageDatasetTypes,
)

from util import est_rows_for_csv_file, get_bounds_from_gdf
from util.file_utils import list_unload_payload_files
from .__version__ import __version__


GEOMETRY_COL_TYPES = ('MULTIPOLYGON', 'POINT')
# TODO: change athena to not return geometry object if not needed  and use UNLOAD to parquet instead of CSV
csv.field_size_limit(10**7)  # temporary solution


def parse_table_defs(defs_csv_path) -> tuple[dict, dict, set]:
    """
    Parse rme_table_column_defs.csv and return a dict of table schemas; a list of column order; list of foreign key tables
    Returns: {table_name: {col_name: col_type, ...}, ...}, {table_name: [col_order]}, set(table_names)
    Adds sequential integer dgoid to all tables.
    """
    table_schema_map = {}
    table_col_order = {}
    fk_tables = set()
    with open(defs_csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row['table_name']
            col = row['name']
            col_type = row['type']
            if table not in table_schema_map:
                table_schema_map[table] = {}
                table_col_order[table] = []
            table_schema_map[table][col] = col_type
            table_col_order[table].append(col)
            if table != 'dgos':
                fk_tables.add(table)
        # Ensure dgoid is present in all tables
        for table in table_schema_map:
            table_schema_map[table]['dgoid'] = 'INTEGER'
            if 'dgoid' not in table_col_order[table]:
                table_col_order[table].insert(0, 'dgoid')
    return table_schema_map, table_col_order, fk_tables


def create_geopackage(gpkg_path: str, table_schema_map: dict, table_col_order: dict, fk_tables: set, spatialite_path: str) -> apsw.Connection:
    """
    Create a GeoPackage (SQLite) file and tables as specified in table_schema_map.
    Returns the APSW connection.
    """
    log = Logger('Create GeoPackage')
    log.info(f"Creating GeoPackage at {gpkg_path}")
    # apsw.Connection = The database is opened for reading and writing, and is created if it does not already exist.
    # this can be a problem if this script is run more than once
    # Delete the file if it exists
    if os.path.exists(gpkg_path):
        os.remove(gpkg_path)
        log.info(f"Deleted existing file {gpkg_path}")

    conn = apsw.Connection(gpkg_path)
    # Enable spatialite extension and initialize spatial metadata
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)
    add_geopackage_tables(conn)
    curs = conn.cursor()
    # create tables
    for table, columns in table_schema_map.items():
        col_defs = []
        for col in table_col_order[table]:
            coltype = columns[col]
            # treat dgoid specially
            if col == 'dgoid' and table == 'dgos':
                col_defs.append(f"{col} {coltype} PRIMARY KEY")
            # don't add geometry cols here - we'll do it
            elif coltype not in GEOMETRY_COL_TYPES:
                col_defs.append(f"{col} {coltype}")

        # Add FK syntax for child tables
        fk = ''
        # if table in fk_tables:
        #     fk = ', FOREIGN KEY(dgoid) REFERENCES dgos(dgoid)'
        curs.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)}{fk})")
        log.info(f"Created table {table}")

    # register relations
    # for table in fk_tables:
    #     curs.execute("""
    #         INSERT INTO gpkg_relations (
    #             name, type, base_table_name, base_primary_column, related_table_name, related_primary_column
    #         ) VALUES (?, 'association', ?, ?, ?, ?)
    #     """, (
    #         f'dgos_{table}',
    #         'dgos', 'dgoid',
    #         table, 'dgoid'
    #     ))

    # add and register geometry columns
    for table, columns in table_schema_map.items():
        for col, coltype in columns.items():
            if coltype in GEOMETRY_COL_TYPES:
                curs.execute(
                    "SELECT gpkgAddGeometryColumn (?, ?, ?, 0, 0, 4326)",
                    (table, col, coltype)
                )
                curs.execute(
                    "SELECT gpkgAddSpatialIndex(?, ?);",
                    (table, col)
                )
                log.info(f"Registered {table} as a spatial layer in GeoPackage with {col} as {coltype}.")

    return conn


def wkt_from_csv(csv_geom: str) -> str | None:
    """
    Convert geometry string from CSV (with | instead of ,) back to WKT.
    """
    if not csv_geom:
        return None
    return csv_geom.replace('|', ',')


def list_of_source_projects(local_csv_path: str) -> list[dict[str, str]]:
    """
    Iterate over a CSV to get unique project IDs and return a list of dicts with project_id and project_url.

    Args:
        local_csv_path (str): Path to the local CSV file.

    Returns:
        list[dict[str, str]]: List of dictionaries with keys 'project_id' and 'project_url'.

    Notes:
        - Could use project_id_list function in rivers_need_space but we don't use a dataframe for IGOs because the size can get huge (maybe if we switch to polars).
        - Would be more efficient to extract this as part of populate_tables_from_csv while we're looping over it there, but keeping it here for better separation of duties.
        - To get the name we'd need to join to conus_projects.
    """
    log = Logger('Get source projects')
    project_ids = set()
    with open(local_csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = est_rows_for_csv_file(local_csv_path)
        prog_bar = ProgressBar(rows, text="Transfer from csv to database table")
        for idx, row in enumerate(reader, start=1):
            project_ids.add(row['rme_project_id'])
            prog_bar.update(idx)
        prog_bar.finish()
    log.debug(f"{len(project_ids)} projects identified")
    return [{"project_id": x, "project_url": f'https://data.riverscapes.net/p/{x}'} for x in project_ids]


def write_source_projects_csv(project_ids: set[str], output_path: Path) -> None:
    """Persist deduplicated project IDs with matching portal URLs."""
    rows = [
        {"project_id": project_id, "project_url": f'https://data.riverscapes.net/p/{project_id}'}
        for project_id in sorted(project_ids)
    ]
    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)


def populate_tables_from_csv(csv_path: str, conn: apsw.Connection, table_schema_map: dict, table_col_order: dict) -> None:
    """
    Read the CSV and insert rows into the appropriate tables based on column mapping.

    IMPLEMENTED: Sequential dgoid, geometry handling for dgo_geom (called geom in dgos table), error handling, debug output for skipped/invalid rows.
    """
    log = Logger('Populate Tables')
    log.info(f"Populating tables from {csv_path}")
    rows = est_rows_for_csv_file(csv_path)
    log.debug(f"Estimated number of rows: {rows}")
    prog_bar = ProgressBar(rows, text="Transfer from csv to database table")
    curs = conn.cursor()
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        conn.execute('BEGIN')
        try:
            for idx, row in enumerate(reader, start=1):
                # Use index as sequential dgoid -- assumes there is no existing data
                dgoid = idx
                for table, columns in table_schema_map.items():
                    insert_cols = []
                    insert_values = []
                    # geom_col_idx = None
                    for col in table_col_order[table]:
                        coltype = columns[col]
                        if col == 'dgoid':
                            insert_cols.append(col)
                            insert_values.append(dgoid)
                        elif coltype in GEOMETRY_COL_TYPES:
                            insert_cols.append(col)
                            # assume the only col of this coltype is geom, which we will build from
                            geom_wkt = f"POINT({row.get('longitude')} {row.get('latitude')})"
                            # assume the only col of this coltype is geom, which we want to pop with dgo_geom
                            # geom_wkt = wkt_from_csv(row.get('dgo_geom', ''))
                            # if not geom_wkt:
                            #     raise ValueError(f"Missing or malformed geometry in row {idx}")
                            insert_values.append(geom_wkt)
                        else:
                            val = row.get(col, None)
                            # Check for required columns (notnull)
                            if val is None:
                                raise ValueError(f"Missing required column '{col}' in row {idx}")
                                # alternatively, warn and keep going
                                # print (f"Missing required column '{col}' in row {idx}")
                                # values.append(None)
                            else:
                                insert_cols.append(col)
                                insert_values.append(val)
                    sql_placeholders = []
                    for i, col in enumerate(insert_cols):
                        coltype = columns[col]
                        if coltype in GEOMETRY_COL_TYPES:
                            sql_placeholders.append("AsGPB(GeomFromText(?, 4326))")
                        else:
                            sql_placeholders.append("?")
                    sqlstatement = f"INSERT INTO {table} ({', '.join(insert_cols)}) VALUES ({', '.join(sql_placeholders)})"
                    # uncomment for debug printing first SQL statement - noisy
                    # if idx == 1:
                    #     log.debug(sqlstatement)
                    #     log.debug(insert_values)
                    curs.execute(sqlstatement, insert_values)

                prog_bar.update(idx)

            conn.execute('COMMIT')
            prog_bar.finish()
            log.info(f"Inserted {idx} rows.")
        except Exception as e:
            log.error(f"Error at row {idx}: {e}\nRow data: {row}\nSQL: {sqlstatement}")
            conn.execute('ROLLBACK')
            raise
    log.info("Table population complete.")


def populate_tables_from_parquet(
    parquet_path: str | Path,
    conn: apsw.Connection,
    table_schema_map: dict,
    table_col_order: dict,
    batch_size: int = 50_000,
) -> set[str]:
    """Insert rows into tables from one or more Parquet files.

    Args:
        parquet_path: Directory containing Parquet files or a single Parquet file path.
        conn: Open APSW connection.
        table_schema_map / table_col_order: Outputs from ``parse_table_defs``.
        batch_size: Row batch size when streaming Parquet content.
    Returns:
        Set of distinct ``rme_project_id`` values observed during ingestion.
    """

    log = Logger('Populate Tables (Parquet)')
    parquet_path = Path(parquet_path)
    if parquet_path.is_file():
        parquet_files = [parquet_path]
    else:
        parquet_files = list_unload_payload_files(parquet_path)

    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {parquet_path}")

    try:
        total_rows = sum(max(pq.ParquetFile(p).metadata.num_rows, 0) for p in parquet_files)
    except Exception as exc:  # pragma: no cover - metadata failures are rare
        log.warning(f"Unable to inspect Parquet metadata for progress tracking: {exc}")
        total_rows = 0

    progress_total = total_rows if total_rows > 0 else 1
    prog_bar = ProgressBar(progress_total, text='Transfer from parquet to database table')
    curs = conn.cursor()
    inserted_rows = 0
    project_ids: set[str] = set()  # track unique source projects during ingestion

    conn.execute('BEGIN')
    try:
        for parquet_file in parquet_files:
            pq_file = pq.ParquetFile(parquet_file)
            log.debug(f"Processing file {parquet_file}")
            for batch in pq_file.iter_batches(batch_size=batch_size):
                batch_df = batch.to_pandas()
                for row in batch_df.to_dict(orient='records'):
                    inserted_rows += 1
                    project_id = row.get('rme_project_id')
                    if project_id is not None and not pd.isna(project_id):
                        project_ids.add(str(project_id))
                    for table, columns in table_schema_map.items():
                        insert_cols: list[str] = []
                        insert_values: list[object] = []
                        for col in table_col_order[table]:
                            coltype = columns[col]
                            if col == 'dgoid':
                                insert_cols.append(col)
                                insert_values.append(inserted_rows)
                            elif coltype in GEOMETRY_COL_TYPES:
                                lon = row.get('longitude')
                                lat = row.get('latitude')
                                if lon is None or lat is None or pd.isna(lon) or pd.isna(lat):
                                    raise ValueError(f"Missing longitude/latitude for row {inserted_rows}")
                                insert_cols.append(col)
                                insert_values.append(f"POINT({float(lon)} {float(lat)})")
                            else:
                                if col not in row:
                                    raise ValueError(f"Missing required column '{col}' in row {inserted_rows}")
                                val = row.get(col)
                                if pd.isna(val):
                                    val = None
                                insert_cols.append(col)
                                insert_values.append(val)

                        sql_placeholders = []
                        for col in insert_cols:
                            if columns[col] in GEOMETRY_COL_TYPES:
                                sql_placeholders.append("AsGPB(GeomFromText(?, 4326))")
                            else:
                                sql_placeholders.append("?")
                        sqlstatement = (
                            f"INSERT INTO {table} ({', '.join(insert_cols)}) VALUES ({', '.join(sql_placeholders)})"
                        )
                        curs.execute(sqlstatement, insert_values)
                    prog_bar.update(inserted_rows)
        conn.execute('COMMIT')
        prog_bar.finish()
        log.info(
            f"Inserted {inserted_rows} rows from Parquet and tracked {len(project_ids)} distinct source projects."
        )
    except Exception as exc:
        conn.execute('ROLLBACK')
        log.error(f"Error while loading Parquet data: {exc}")
        raise
    finally:
        if inserted_rows == 0:
            prog_bar.finish()

    return project_ids


def add_geopackage_tables(conn: apsw.Connection):
    """
    Create required GeoPackage spatial_ref_sys and metadata tables: gpkg_contents and gpkg_geometry_columns.
    # initially I inserted with CREATE TABLE statements but the single spatialite function `gpkgCreateBaseTables` does all this 
    the Spatialite function gpkgInsertEpsgSRID(4326) is not needed because CreateBaseTables inserts that one already 
    """
    curs = conn.cursor()
    curs.execute("SELECT gpkgCreateBaseTables();")

    # commented out because defined relationships not required and setting them up is more work
    # copilot says the GeoPackage specification does not include the gpkg_relations table by default, and the Spatialite function gpkgCreateBaseTables() does not create it
    # so we create it to avoid warning in qgis
    # curs.execute("""
    #     CREATE TABLE IF NOT EXISTS gpkg_relations (
    #         id INTEGER PRIMARY KEY AUTOINCREMENT,
    #         name TEXT NOT NULL,
    #         type TEXT NOT NULL,
    #         mapping_table_name TEXT,
    #         base_table_name TEXT NOT NULL,
    #         base_primary_column TEXT NOT NULL,
    #         related_table_name TEXT NOT NULL,
    #         related_primary_column TEXT NOT NULL
    #     );
    # """)


def get_datasets(output_gpkg: str) -> list[GeopackageLayer]:
    """
    Returns a list of the datasets from the output GeoPackage.
    These are the spatial views that are created from the igos and dgos tables.
    ***COPIED FROM scrape_rme2.py*** and then 
    """

    conn = apsw.Connection(output_gpkg)
    conn.enable_load_extension(True)
    curs = conn.cursor()

    # Get the names of all the tables in the database
    curs.execute("SELECT table_name FROM gpkg_contents WHERE data_type='features'")
    datasets: list[GeopackageLayer] = []
    for row in curs.fetchall():
        table_name = str(row[0])
        datasets.append(
            GeopackageLayer(
                lyr_name=table_name,
                ds_type=GeoPackageDatasetTypes.VECTOR,  # pyright: ignore[reportArgumentType]
                name=table_name
            )
        )
    return datasets


def create_igos_project(project_dir: Path, project_name: str, gpkg_path: Path, log_path: Path, bounds_gdf: gpd.GeoDataFrame):
    """
    Create a Riverscapes project of type IGOS
    Modelled after scrape_rme2.py in data-exchange-scripts
    """
    log = Logger('Create IGOS project')

    # Build the bounds for the new RME scrape project using the AOI that was used to select the dgos to begin with
    bounds, centroid, bounding_rect = get_bounds_from_gdf(bounds_gdf)
    # we should be getting Path objects from caller but just in case
    project_dir = Path(project_dir)
    gpkg_path = Path(gpkg_path)
    log_path = Path(log_path)

    # export the original AOI as a geopackage layer so it can be displayed in the project
    aoi_path = project_dir / 'aoi.gpkg'
    bounds_gdf.to_file(aoi_path, driver='GPKG', layer='AOI', use_arrow=True)
    log.debug(f"Wrote AOI to {aoi_path} for inclusion in project")

    def _relative_posix_strict(target: Path, start: Path = project_dir) -> str:
        """return path relative to project dir as posix-style string, for writing to project.rs.xml
        target must be contained with start or raises ValueError"""
        rel_path = target.relative_to(start)
        return rel_path.as_posix()

    output_bounds_path = project_dir / 'project_bounds.geojson'
    with output_bounds_path.open("w", encoding='utf8') as f:
        json.dump(bounds, f)
    print(f"centroid = {centroid}")

    rs_project = Project(
        project_name,
        project_type='igos',
        description="""This project was generated as an extract from raw_rme which is itself an extract of Riverscapes Metric Engine projects in the Riverscapes Data Exchange produced as part of the 2025 CONUS run of Riverscapes tools. See https://docs.riverscapes.net/initiatives/CONUS-runs for more about this initiative. 
        At the time of extraction this dataset has not yet been thoroughly quality controlled and may contain errors or gaps. 
        """,
        meta_data=MetaData(values=[
            Meta('Report Type', 'IGO Scraper'),
            Meta('ModelVersion', __version__),
            Meta('Date Created', datetime.now().isoformat(timespec='seconds'))
        ]),
        bounds=ProjectBounds(
            Coords(centroid[0], centroid[1]),
            BoundingBox(bounding_rect[0], bounding_rect[1], bounding_rect[2], bounding_rect[3]),
            output_bounds_path.name),
        realizations=[Realization(
            name='Realization1',
            xml_id='REALIZATION1',
            date_created=datetime.now(),
            product_version='1.0.0',
            datasets=[
                Geopackage(
                    name='Riverscapes Metrics',
                    xml_id='RME',
                    path=_relative_posix_strict(gpkg_path),
                    layers=get_datasets(str(gpkg_path))
                ),
                Geopackage(
                    name="Input Area of Interest",
                    xml_id='InputAOI',
                    path=_relative_posix_strict(aoi_path),
                    layers=get_datasets(str(aoi_path))
                )
            ],
            logs=[
                Log(
                    xml_id='LOG',
                    name='Log File',
                    description='Processing log file',
                    path=_relative_posix_strict(log_path),
                ),
                Log(
                    xml_id="source_projects",
                    name="Source Projects CSV",
                    description="List of projects from Riverscapes Data Exchange",
                    path="source_projects.csv"
                )
            ]
        )]
    )
    merged_project_xml = project_dir / 'project.rs.xml'
    rs_project.write(str(merged_project_xml))
    log.info(f'Project XML file written to {merged_project_xml}')


def create_views(conn: apsw.Connection, table_col_order: dict):
    """
    create spatial views for each attribute table joined with the dgos (spatial) table 
    also creates a combined view 'vw_dgo_metrics' joining all attribute tables and `dgos`
    Registers each view in gpkg_contents and gpkg_geometry_columns so GIS tools recognize them as spatial layers.
    modeled after clean_up_gpkg in scrape_rme2 but using the dgo geom (which is a point)
    """
    log = Logger('Create views')
    curs = conn.cursor()

    dgo_tables = [t for t in table_col_order.keys() if t != 'dgos']
    log.debug(f'Attribute tables to create views for: {dgo_tables}')

    # Get the columns from the dgos table, but not the dgoid or geom because we'll add them manually
    curs.execute('PRAGMA table_info(dgos)')
    dgo_cols = [f"dgos.{col[1]}" for col in curs.fetchall() if col[1] not in ('dgoid', 'DGOID', 'geom', 'GEOM')]

    new_views = ['vw_dgo_metrics']
    # we don't have IGO table but we put the point geom in the DGO geom - same idea
    # I'm a little confused about whether we have DGO metrics or IGO metrics or some mix in raw_rme
    curs.execute('''
        CREATE VIEW vw_dgo_metrics AS
        SELECT dgo_desc.*, dgo_geomorph.*, dgo_veg.*, dgo_hydro.*, dgo_impacts.*, dgo_beaver.*, dgos.geom, dgos.level_path, dgos.seg_distance, dgos.centerline_length, dgos.segment_area, dgos.FCode
        FROM dgo_desc
        INNER JOIN dgo_geomorph ON dgo_desc.dgoid = dgo_geomorph.dgoid
        INNER JOIN dgo_veg ON dgo_desc.dgoid = dgo_veg.dgoid
        INNER JOIN dgo_hydro ON dgo_desc.dgoid = dgo_hydro.dgoid
        INNER JOIN dgo_impacts ON dgo_desc.dgoid = dgo_impacts.dgoid
        INNER JOIN dgo_beaver ON dgo_desc.dgoid = dgo_beaver.dgoid
        INNER JOIN dgos ON dgo_desc.dgoid = dgos.dgoid
        '''
                 )

    for dgo_table in dgo_tables:
        # Get the columns of the dgo side table
        curs.execute(f'PRAGMA table_info({dgo_table})')
        dgo_table_cols = ['t.' + str(col[1]) for col in curs.fetchall() if col[1] != 'dgoid' and col[1] != 'DGOID']

        view_name = f'vw_{dgo_table}_metrics'
        new_views.append(view_name)
        curs.execute(f'''
            CREATE VIEW {view_name} AS
            SELECT dgos.dgoid,
            dgos.geom,
            {",".join(dgo_table_cols)},
            {",".join(dgo_cols)}
            FROM {dgo_table} t INNER JOIN dgos ON t.dgoid=dgos.dgoid
        ''')
    # Register each view as a spatial layer in the GeoPackage metadata tables
    for view_name in new_views:
        curs.execute('''
            INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y)
            SELECT ?, 'features', ?, min_x, min_y, max_x, max_y FROM gpkg_contents WHERE table_name='dgos'
        ''', [view_name, view_name])

        curs.execute('''
            INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT ?, 'geom', 'POINT', 4326, 0, 0 FROM gpkg_geometry_columns WHERE table_name='dgos'
        ''', [view_name])
    log.info(f"{len(new_views)} views created and registered as spatial layers.")


def create_indexes(conn: apsw.Connection, table_col_order: dict):
    """
    Create indexes on dgoid columns for all tables except 'dgos'.
    """
    log = Logger('Create Indexes')
    curs = conn.cursor()
    for table in table_col_order:
        if table != 'dgos':
            index_name = f"idx_{table}_dgoid"
            log.debug(f"Creating index {index_name} on {table}(dgoid)")
            curs.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table}(dgoid)")
    log.info("Indexes created for dgoids.")


def create_gpkg_igos_from_csv(project_dir: str, spatialite_path: str, local_csv: str) -> str:
    """ orchestration of all the things we need to do ie 
        parse table defs, create GeoPackage, and populate tables.
        return path to the geopackage
    """
    defs_path = Path(__file__).resolve().parent / "rme_table_column_defs.csv"
    table_schema_map, table_col_order, fk_tables = parse_table_defs(str(defs_path))

    project_dir_path = Path(project_dir)
    outputs_dir = project_dir_path / 'outputs'
    safe_makedirs(str(outputs_dir))

    gpkg_path = outputs_dir / 'riverscape_metrics.gpkg'

    conn = create_geopackage(str(gpkg_path), table_schema_map, table_col_order, fk_tables, spatialite_path)
    populate_tables_from_csv(local_csv, conn, table_schema_map, table_col_order)
    create_indexes(conn, table_col_order)
    create_views(conn, table_col_order)
    return str(gpkg_path)


def create_gpkg_igos_from_parquet(project_dir: Path, spatialite_path: str, parquet_path: str | Path) -> Path:
    """Parquet counterpart to ``create_gpkg_igos_from_csv``."""

    defs_path = Path(__file__).resolve().parent / 'rme_table_column_defs.csv'
    table_schema_map, table_col_order, fk_tables = parse_table_defs(str(defs_path))

    outputs_dir = project_dir / 'outputs'
    safe_makedirs(str(outputs_dir))

    gpkg_path = outputs_dir / 'riverscape_metrics.gpkg'

    conn = create_geopackage(str(gpkg_path), table_schema_map, table_col_order, fk_tables, spatialite_path)
    project_ids = populate_tables_from_parquet(parquet_path, conn, table_schema_map, table_col_order)
    create_indexes(conn, table_col_order)
    create_views(conn, table_col_order)
    write_source_projects_csv(project_ids, project_dir / 'source_projects.csv')
    return gpkg_path
