"""
Using data from Athena, generates a GeoPackage (SQLite) file with tables
such as dgos, dgos_veg, dgo_hydro, etc.,
column-to-table-and-type mapping extracted from rme geopackage pragma and included in the layer_definitions for raw_rme
 All tables will include a sequentially generated dgoid column.

Lorin Gaertner (with copilot)
August/Sept 2025. 
Revised Dec 2025 See CHANGELOG. 
- move away from CSV for data transfer between our systems (we still write to CSV for user convenience)
* use layer_definitions

IMPLEMENTED: Sequential dgoid (integer), geometry handling for dgo_geom (SRID 4326, WKT conversion), 
foreign key syntax in table creation, error handling (throws on missing/malformed required columns), 
debug output for skipped/invalid rows.
Actual column names/types must be supplied in layer_definitions on Athena.
No advanced validation or transformation beyond geometry and required columns.
Foreign key constraints are defined but not enforced unless PRAGMA foreign_keys=ON is set.
"""

from datetime import datetime
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

from util import get_bounds_from_gdf
from util.athena.athena_unload_utils import list_athena_unload_payload_files
from .__version__ import __version__

GEOMETRY_COL_TYPES = ('MULTIPOLYGON', 'POINT')
SQLiteValue = None | int | float | str | bytes


def create_geopackage(gpkg_path: Path, table_defs: pd.DataFrame, spatialite_path: str) -> apsw.Connection:
    """
    Create a GeoPackage (SQLite) file and tables as specified in table_schema_map.
    Returns the APSW connection.
    dgos.dgoid will be made primary key
    geometry columns will be registered

    Args:
        gpkg_path: path to where the geopackage will be created (will delete any existing!)
        table_defs: dataframe containing the table_name, (column) name, and dtype (datatype) to create.    
        spatialite_path: where to find the extension
    """
    log = Logger('Create GeoPackage')
    log.info(f"Creating GeoPackage at {gpkg_path}")
    # apsw.Connection = The database is opened for reading and writing, and is created if it does not already exist.
    # this can be a problem if this script is run more than once
    # Delete the file if it exists
    if Path(gpkg_path).exists():
        os.remove(gpkg_path)
        log.info(f"Deleted existing file {gpkg_path}")

    conn = apsw.Connection(str(gpkg_path))
    # Enable spatialite extension and initialize spatial metadata
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)
    add_geopackage_tables(conn)
    curs = conn.cursor()

    # create tables from definitions df
    for table_name, group in table_defs.groupby('table_name'):
        col_defs = []
        for _, row in group.iterrows():
            col_name = row['name']
            col_type = row['dtype']

            # Treat `dgoid` specially
            if col_name == 'dgoid' and table_name == 'dgos':
                col_defs.append(f"{col_name} {col_type} PRIMARY KEY")
            elif col_type not in GEOMETRY_COL_TYPES:
                col_defs.append(f"{col_name} {col_type}")

        # Create the table
        curs.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})")
        log.info(f"Created table {table_name}")

    # add and register geometry columns
    geometry_rows = table_defs[table_defs['dtype'].isin(GEOMETRY_COL_TYPES)]
    for _, row in geometry_rows.iterrows():
        table_name = row['table_name']
        col_name = row['name']
        col_type = row['dtype']

        # Add the geometry column
        curs.execute(
            "SELECT gpkgAddGeometryColumn (?, ?, ?, 0, 0, 4326)",
            (table_name, col_name, col_type)
        )

        # Add the spatial index
        curs.execute(
            "SELECT gpkgAddSpatialIndex(?, ?);",
            (table_name, col_name)
        )

        log.info(f"Registered {table_name} as a spatial layer in GeoPackage with {col_name} as {col_type}.")

    return conn


def write_source_projects_csv(project_ids: set[str], output_path: Path) -> None:
    """Persist deduplicated project IDs with matching portal URLs."""
    rows = [
        {"project_id": project_id, "project_url": f'https://data.riverscapes.net/p/{project_id}'}
        for project_id in sorted(project_ids)
    ]
    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)


def populate_tables_from_parquet(
    parquet_path: str | Path,
    conn: apsw.Connection,
    table_defs: pd.DataFrame,
    batch_size: int = 50_000,
) -> set[str]:
    """Insert rows into tables from one or more Parquet files.

    Args:
        parquet_path: Directory containing Parquet files or a single Parquet file path.
        conn: Open APSW connection.
        table_defs: tables and columns we are populating 
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
        parquet_files = list_athena_unload_payload_files(parquet_path)

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
                    # group table_defs by table_name and process each table
                    for table_name, group in table_defs.groupby('table_name'):
                        insert_cols: list[str] = []
                        insert_values: list[SQLiteValue] = []
                        sql_placeholders = []
                        for _, col_row in group.iterrows():
                            col = col_row['name']
                            coltype = col_row['dtype']
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

                            if coltype in GEOMETRY_COL_TYPES:
                                sql_placeholders.append("AsGPB(GeomFromText(?, 4326))")
                            else:
                                sql_placeholders.append("?")

                        sqlstatement = (
                            f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES ({', '.join(sql_placeholders)})"
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


def create_views(conn: apsw.Connection, table_defs: pd.DataFrame):
    """
    create spatial views for each attribute table joined with the dgos (spatial) table 
    also creates a combined view 'vw_dgo_metrics' joining all attribute tables and `dgos`
    Registers each view in gpkg_contents and gpkg_geometry_columns so GIS tools recognize them as spatial layers.
    modeled after clean_up_gpkg in scrape_rme2 but using the dgo geom (which is a point)
    """
    log = Logger('Create views')
    curs = conn.cursor()

    # Get unique table names from table_defs, excluding 'dgos'
    dgo_tables = table_defs['table_name'].unique()
    dgo_tables = [t for t in dgo_tables if t != 'dgos']
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


def create_indexes(conn: apsw.Connection, table_defs: pd.DataFrame):
    """
    Create indexes on dgoid columns for all tables except 'dgos'.
    """
    log = Logger('Create Indexes')
    curs = conn.cursor()
    # Get unique table names from table_defs, excluding 'dgos'
    dgo_tables = table_defs['table_name'].unique()
    dgo_tables = [t for t in dgo_tables if t != 'dgos']
    for table_name in dgo_tables:
        index_name = f"idx_{table_name}_dgoid"
        log.debug(f"Creating index {index_name} on {table_name}(dgoid)")
        curs.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}(dgoid)")
    log.info("Indexes created for dgoids.")


def create_gpkg_igos_from_parquet(project_dir: Path, spatialite_path: str,
                                  parquet_path: Path, table_defs: pd.DataFrame) -> Path:
    """Create geopackage from parquet file"""

    outputs_dir = project_dir / 'outputs'
    safe_makedirs(str(outputs_dir))

    gpkg_path = outputs_dir / 'riverscape_metrics.gpkg'

    conn = create_geopackage(gpkg_path, table_defs, spatialite_path)
    project_ids = populate_tables_from_parquet(parquet_path, conn, table_defs)
    create_indexes(conn, table_defs)
    create_views(conn, table_defs)
    write_source_projects_csv(project_ids, project_dir / 'source_projects.csv')
    return gpkg_path
