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

import json
import os
from datetime import datetime
from pathlib import Path

import apsw
import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
from rsxml import Logger, ProgressBar
from rsxml.project_xml import (
    BoundingBox,
    Coords,
    Geopackage,
    GeoPackageDatasetTypes,
    GeopackageLayer,
    Log,
    Meta,
    MetaData,
    Project,
    ProjectBounds,
    Realization,
)
from rsxml.util import safe_makedirs

from util import get_bounds_from_gdf
from util.athena.athena_unload_utils import list_athena_unload_payload_files

from .__version__ import __version__

SQLiteValue = None | int | float | str | bytes
# SQlite will take anything, so we're really going with geopackage/qgis preferences here
LAYERDEF_GPKG_DTYPE_MAP = {
    "INTEGER": "INTEGER",
    "FLOAT": "REAL",
    "STRING": "TEXT",
    "BOOLEAN": "BOOLEAN",  # GeoPackage spec recognizes BOOLEAN
    "DATETIME": "DATETIME",  # GeoPackage spec recognizes DATETIME
    "DECIMAL": "REAL",
    "GEOMETRY": "GEOMETRY",  # Specially handled later
    "STRUCTURED": "TEXT",  # Usually JSON/Array data represented as text
    "BINARY": "BLOB",
}


def create_geopackage(gpkg_path: Path, table_defs: pd.DataFrame, spatialite_path: str) -> apsw.Connection:
    """
    Create a GeoPackage (SQLite) file and tables as specified in table_schema_map.
    Returns the APSW connection.
    dgos.dgoid will be made primary key
    geometry columns will be registered

    Args:
        gpkg_path: path to where the geopackage will be created (will delete any existing!)
        table_defs: dataframe containing the table_name, (column) name, and dtype (datatype) to create and the geometry columns to register
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
    # NOTE dtype FROM layer defs are logical types such as FLOAT, STRING which aren't real sqlite types
    # sqlite will happily create columns with any declared type you want, it really doesn't enforce types
    # but the resulting affinity of STRING is NUMERIC and that just seems wrong

    for table_name, group in table_defs.groupby('table_name'):
        col_defs = []
        for _, row in group.iterrows():
            col_name = row['name']
            logical_type = row['dtype']

            # Map logical type to stnadard types, fallback to TEXT
            sqlite_type = LAYERDEF_GPKG_DTYPE_MAP.get(logical_type, "TEXT")

            # Treat `dgoid` and Geometry columns specially
            if col_name == 'dgoid' and table_name == 'dgos':
                col_defs.append(f"{col_name} {sqlite_type} PRIMARY KEY")
            elif logical_type != 'GEOMETRY' and col_name != 'geom':
                col_defs.append(f"{col_name} {sqlite_type}")

        # Create the table
        curs.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})")
        log.info(f"Created table {table_name}")

    # add and register geometry columns
    # Pulling from table_defs assumes (a) table_defs has correct definitions for the geometries we want to create
    # (b) all will be assigned to type GEOMETRY - although could get the geometry type from the `dtype_parameters` column if it was present
    # geometry_col_defs = table_defs[table_defs['dtype'] == 'GEOMETRY']
    geometry_col_defs = pd.DataFrame([{'table_name': 'dgos', 'name': 'geom', 'dtype': 'POINT'}])
    for _, row in geometry_col_defs.iterrows():
        table_name = row['table_name']
        col_name = row['name']
        col_type = row['dtype']

        # Add the geometry column using Spatialite function see https://www.gaia-gis.it/gaia-sins/spatialite-sql-5.0.0.html
        # Parameters are table_name, gemoetry_column_name, geometry_type, with_z, with_m, srs_id
        # geometry_type is a normal WKT name GEOMETRY, POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULITPOLYGON, GEOMCOLLECTION
        curs.execute(
            "SELECT gpkgAddGeometryColumn (?, ?, ?, 0, 0, 4326)",
            (table_name, col_name, col_type),
        )

        # Add the spatial index
        curs.execute(
            "SELECT gpkgAddSpatialIndex(?, ?);",
            (table_name, col_name),
        )

        log.info(f"Registered {table_name} as a spatial layer in GeoPackage with {col_name} as {col_type}.")

    return conn


def write_source_projects_csv(project_ids: set[str], output_path: Path) -> None:
    """Persist deduplicated project IDs with matching portal URLs."""
    rows = [{"project_id": project_id, "project_url": f'https://data.riverscapes.net/p/{project_id}'} for project_id in sorted(project_ids)]
    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)


def get_parquet_files(parquet_path: str | Path) -> list[Path]:
    """
    Resolve a file or directory into a list of Parquet file paths.
    """
    parquet_path = Path(parquet_path)
    if parquet_path.is_file():
        return [parquet_path]
    return list_athena_unload_payload_files(parquet_path)


def populate_tables_from_parquet(
    parquet_path: str | Path,
    conn: apsw.Connection,
    table_defs: pd.DataFrame,
    batch_size: int = 90_000,
) -> set[str]:
    """Insert rows into tables from one or more Parquet files.

    This function is designed to handle very large out-of-core datasets (gigabytes)
    by streaming Parquet files in memory-efficient batches, mapping a single wide
    source row into multiple normalized destination tables, and executing bulk inserts
    using optimized SQLite spatial functions.

    Args:
        parquet_path: Directory containing Parquet files or a single Parquet file path.
        conn: Open APSW connection.
        table_defs: tables and columns we are populating
        batch_size: Row batch size when streaming Parquet content.
    Returns:
        Set of distinct ``rme_project_id`` values observed during ingestion.
    """

    log = Logger('Populate Tables (Parquet)')
    parquet_files = get_parquet_files(parquet_path)

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
    # Step 1: Precompute table groupings
    # We map the single wide Parquet schema into the respective normalized tables (dgos, dgo_veg, etc.)
    table_groups = {name: group for name, group in table_defs.groupby('table_name')}

    # Step 2: Precompute target columns and SQL statements for each table column indices for each table
    table_ops = {}
    # geometry columns are assumbed to WKB and will be processed differently
    geom_col = 'geom'
    for table_name, group in table_groups.items():
        insert_cols: list[str] = [col_row['name'] for _, col_row in group.iterrows()]
        # Convert the Athena Well-Known Binary (WKB) into GeoPackage Binary (GPB) on ingest
        sql_placeholders = ["AsGPB(GeomFromWKB(?, 4326))" if col_row['name'] == geom_col else "?" for _, col_row in group.iterrows()]
        sqlstatement = f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES ({', '.join(sql_placeholders)})"
        table_ops[table_name] = {'insert_cols': insert_cols, 'sqlstatement': sqlstatement}

    try:
        for parquet_file in parquet_files:
            pq_file = pq.ParquetFile(parquet_file)
            log.debug(f"Processing file {parquet_file}")

            # iter_batches streams the parquet file chunk-by-chunk to keep memory usage low
            for batch in pq_file.iter_batches(batch_size=batch_size):
                batch_df = batch.to_pandas()

                # Track project_ids in batch (vectorized)
                if 'rme_project_id' in batch_df.columns:
                    project_ids.update(batch_df['rme_project_id'].dropna().astype(str).unique())

                # Step 3: Route columns to proper tables and execute batch inserts
                for ops in table_ops.values():
                    insert_cols = ops['insert_cols']
                    sqlstatement = ops['sqlstatement']

                    batch_inserts = []
                    col_idx_map = {col: batch_df.columns.get_loc(col) for col in insert_cols if col in batch_df.columns}
                    # itertuples is much faster than iterrows for row-by-row mapping
                    for idx, row in enumerate(batch_df.itertuples(index=False, name=None)):
                        row_values: list[SQLiteValue] = []
                        for col in insert_cols:
                            if col == 'dgoid':
                                # Assign a sequentially generated int primary key across batches
                                row_values.append(inserted_rows + idx + 1)
                            elif col == geom_col and geom_col in batch_df.columns:
                                # Grab the WKB binary payload from Athena directly
                                row_values.append(row[batch_df.columns.get_loc(col)])
                            else:
                                # translate Pandas missing value types to None for SQLite NULL
                                i = col_idx_map.get(col)
                                val = row[i] if i is not None else None
                                if pd.isna(val):
                                    val = None
                                row_values.append(val)
                        batch_inserts.append(tuple(row_values))
                    curs.executemany(sqlstatement, batch_inserts)

                inserted_rows += len(batch_df)
                prog_bar.update(inserted_rows)
        conn.execute('COMMIT')
        prog_bar.finish()
        log.info(f"Inserted {inserted_rows} rows from Parquet and tracked {len(project_ids)} distinct source projects.")
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
    Creates all required GeoPackage tables including spatial_ref_sys and also metadata tables: gpkg_contents and gpkg_geometry_columns
    * initially I inserted with CREATE TABLE statements from the spec (https://www.geopackage.org/spec140/index.html#table_definition_sql)
    * but the single spatialite function `gpkgCreateBaseTables` does all this
    * Likewise no need to run the Spatialite function gpkgInsertEpsgSRID(4326) is not needed because CreateBaseTables inserts that one already
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
                name=table_name,
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
        meta_data=MetaData(
            values=[
                Meta('Report Type', 'IGO Scraper'),
                Meta('ModelVersion', __version__),
                Meta('Date Created', datetime.now().isoformat(timespec='seconds')),
            ]
        ),
        bounds=ProjectBounds(Coords(centroid[0], centroid[1]), BoundingBox(bounding_rect[0], bounding_rect[1], bounding_rect[2], bounding_rect[3]), output_bounds_path.name),
        realizations=[
            Realization(
                name='Realization1',
                xml_id='REALIZATION1',
                date_created=datetime.now(),
                product_version='1.0.0',
                datasets=[
                    Geopackage(name='Riverscapes Metrics', xml_id='RME', path=_relative_posix_strict(gpkg_path), layers=get_datasets(str(gpkg_path))),
                    Geopackage(name="Input Area of Interest", xml_id='InputAOI', path=_relative_posix_strict(aoi_path), layers=get_datasets(str(aoi_path))),
                ],
                logs=[
                    Log(
                        xml_id='LOG',
                        name='Log File',
                        description='Processing log file',
                        path=_relative_posix_strict(log_path),
                    ),
                    Log(xml_id="source_projects", name="Source Projects CSV", description="List of projects from Riverscapes Data Exchange", path="source_projects.csv"),
                ],
            )
        ],
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
        ''')

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
        curs.execute(
            '''
            INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y)
            SELECT ?, 'features', ?, min_x, min_y, max_x, max_y FROM gpkg_contents WHERE table_name='dgos'
        ''',
            [view_name, view_name],
        )

        curs.execute(
            '''
            INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT ?, 'geom', 'POINT', 4326, 0, 0 FROM gpkg_geometry_columns WHERE table_name='dgos'
        ''',
            [view_name],
        )
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


def populate_geopackage_metadata(conn: apsw.Connection, table_defs: pd.DataFrame, aliases: list[tuple[str, str]] | None = None):
    """
    Document tables and views in the GeoPackage using the GeoPackage Schema Extension.
    Populates gpkg_data_columns with metadata from table_defs using executemany.
    Args:
        conn: APSW connection to the GeoPackage.
        table_defs: DataFrame with table/column metadata (must include table_name, name, description, data_unit, friendly_name).
        aliases: Optional list [(viewname, tablename)] to additionally apply definitions to viewname using matching columns in tablename

    NOTE: we populate name, title, and description, but it looks like QGIS only really uses name and description.
    TODO: Move to use this more broadly in riverscapes tools and adjust as currently only the title includes the unit so QGIS users won't see that
    """
    log = Logger('Populate Geopackage metadata')
    curs = conn.cursor()

    # We have some Pandas NA values in Description, which are not the same as NULL and sqlite doesn't like them
    columns_needed = ['table_name', 'name', 'friendly_name', 'data_unit', 'description']
    table_defs_clean = table_defs[columns_needed].copy()
    table_defs_clean = table_defs_clean.applymap(lambda x: None if pd.isna(x) else x)

    def make_title(name, friendly_name, data_unit):
        if not friendly_name:
            friendly_name = name
        if data_unit and str(data_unit).strip() and str(data_unit).strip() != 'NA':
            return f"{friendly_name} ({data_unit})"
        return str(friendly_name)

    # Prepare rows for gpkg_data_columns
    def build_rows(df, table_name_override=None) -> list[tuple]:
        rows = []
        # Use itertuples for performance and attribute access
        for row in df.itertuples(index=False, name='Row'):
            table_name = table_name_override if table_name_override else getattr(row, 'table_name', None)
            column_name = getattr(row, 'name', None)
            key = (table_name, column_name)
            if key in table_column_set:
                log.debug(f"Duplicate gkpg_data_columns would be created for ({table_name}, {column_name}); skipping.")
                continue
            table_column_set.add(key)
            name = str(column_name).replace('_', ' ') if column_name is not None else None
            friendly_name = getattr(row, 'friendly_name', None)
            data_unit = getattr(row, 'data_unit', None)
            description = getattr(row, 'description', None)
            title = make_title(name, friendly_name, data_unit)
            rows.append((table_name, column_name, name, title, description, None, None))
        return rows

    # NOTE No two rows can have the same `name` value (unless it is NULL). BUT WE AREN'T Checking that yet.
    table_column_set = set()  # no two rows can have the same (table_name, column_name)
    data_columns_rows = build_rows(table_defs_clean)

    # Optionally, document views
    if aliases:
        for viewname, tablename in aliases:
            view_cols = table_defs_clean[table_defs_clean['table_name'] == tablename]
            data_columns_rows.extend(build_rows(view_cols, table_name_override=viewname))

    curs.executemany(
        """
        INSERT INTO gpkg_data_columns (
            table_name, column_name, name, title, description, mime_type, constraint_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        data_columns_rows,
    )
    log.info("GeoPackage data_columns table populated with metadata.")


def create_gpkg_igos_from_parquet(project_dir: Path, spatialite_path: str, parquet_path: Path, table_defs: pd.DataFrame) -> Path:
    """Create geopackage from parquet file"""
    log = Logger('Create GPKG from Parquet')

    outputs_dir = project_dir / 'outputs'
    safe_makedirs(str(outputs_dir))

    gpkg_path = outputs_dir / 'riverscape_metrics.gpkg'

    parquet_files = get_parquet_files(parquet_path)
    pq_schema = pq.read_schema(parquet_files[0])
    pq_cols = set(pq_schema.names)

    # columns generated during ingest
    generated_cols = ['dgoid', 'geom']

    # find columns that are defined in table_defs but missing in the parquet file using bitwise NOT operator on PANDAS boolean Series
    missing_mask = ~table_defs['name'].isin(pq_cols) & ~table_defs['name'].isin(generated_cols)
    if missing_mask.any():
        missing_cols = sorted(table_defs.loc[missing_mask, 'name'].unique())
        log.info(f"Columns defined in layer schema but missing from Parquet data. These will not be added to output gpkg: {missing_cols}")
        table_defs = table_defs[~missing_mask].copy()

    conn = create_geopackage(gpkg_path, table_defs, spatialite_path)
    project_ids = populate_tables_from_parquet(parquet_path, conn, table_defs)
    create_indexes(conn, table_defs)
    create_views(conn, table_defs)

    # Document tables/views using geopackage schema extension
    dgo_tables = table_defs['table_name'].unique()

    view_aliases = [(f'vw_{t}_metrics', t) for t in dgo_tables if t != 'dgos']
    # views are made by joining dgos with other tables so we'll add columns from the dgos
    view_aliases += [(f'vw_{t}_metrics', 'dgos') for t in dgo_tables if t != 'dgos']
    # vw_dgo_metrics combines everything together (and honestly I don't like it; it's too big, and unnecessary)
    view_aliases += [('vw_dgo_metrics', t) for t in dgo_tables]

    populate_geopackage_metadata(conn, table_defs, view_aliases)

    write_source_projects_csv(project_ids, project_dir / 'source_projects.csv')

    return gpkg_path
