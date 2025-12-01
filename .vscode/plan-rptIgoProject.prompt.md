## Plan: Athena Parquet Data & Metadata for rpt_igo_project

Switch `rpt_igo_project` to source its main data and metadata directly from Athena, using Parquet for data and Athena queries for metadata, instead of CSV files.

### Steps
1. Update data extraction logic in [`rpt_igo_project/main.py`](src/reports/rpt_igo_project/main.py) to use Athena UNLOAD to Parquet.
2. Implement Parquet file reading in Python (e.g., with `pandas.read_parquet`).
3. Replace metadata loading from `rme_table_column_defs.csv` with an Athena query in [`rpt_igo_project/main.py`](src/reports/rpt_igo_project/main.py) or a relevant module.
4. Refactor any downstream code that expects CSV or static metadata to handle the new formats.
5. Test the workflow end-to-end to ensure data and metadata are correctly loaded and processed.

### Further Considerations
1. Confirm Athena table/column names and access permissions for both data and metadata.
2. Ensure Parquet dependencies (e.g., `pyarrow`) are available in your environment.
3. Prefer shorter, cleaner final code rather than keeping old legacy approaches. Ie do not keep CSV fallback for legacy or debugging purposes.
4. Build out reusable functions in the athnea.py module 
5. we currently have an option to use a local csv file instead of downloading it from athena - keep that but it would be a local parquet file instead
6. Build reusable functions and patterns that all the report generators can use. 
7. Everything about the project is in early stages and can be revisited if there is a compelling case

## Athena Metadata table

CREATE EXTERNAL TABLE `layer_definitions`(
  `layer_id` string COMMENT 'Stable identifier of the layer or table, for example used for project.rs.xml id', 
  `layer_name` string COMMENT 'Human-readable layer or table name (may match layer_id)', 
  `layer_type` string COMMENT 'Layer category (CommonDatasetRef, Raster, Vector, Geopackage, etc.)', 
  `layer_path` string COMMENT 'Relative or absolute path to the delivered layer artifact', 
  `layer_theme` string COMMENT 'High level grouping for the layer (e.g., Hydrology, Vegetation)', 
  `layer_source_url` string COMMENT 'Provenance or documentation URL for the layer', 
  `layer_data_product_version` string COMMENT 'Data vintage/year or version string', 
  `layer_description` string COMMENT 'Human-readable summary of the layer', 
  `name` string COMMENT 'Column (or raster band) identifier', 
  `friendly_name` string COMMENT 'Display-friendly name for the column', 
  `theme` string COMMENT 'Grouping theme -- useful for very wide tables (e.g., Beaver, Hydrology)', 
  `data_unit` string COMMENT 'Pint-compatible unit string (e.g., m, km^2, %)', 
  `dtype` string COMMENT 'Data type (INTEGER, REAL, TEXT, etc.)', 
  `description` string COMMENT 'Detailed description of the column', 
  `is_key` boolean COMMENT 'Participates in a primary/unique key', 
  `is_required` boolean COMMENT 'True if field cannot be empty. Corresponds to SQL NOT NULL', 
  `default_value` string COMMENT 'Default value for new records', 
  `commit_sha` string COMMENT 'git commit at time of harvest from authority json')
COMMENT 'Unified Riverscapes layer column definitions (structural + descriptive metadata)'
PARTITIONED BY ( 
  `authority` string COMMENT 'Repository root name (publishing authority)', 
  `authority_name` string COMMENT 'Issuing package/tool authority name', 
  `tool_schema_version` string COMMENT 'Tool schema version (semver)')

# Plan: Shared Athena Metadata

Replace CSV/legacy metadata with a shared Athena-driven workflow built on layer_definitions, updating get_field_metadata and downstream consumers (field_metadata_to_file, report generators, schema builders).

Steps

1. Add `layer_definitions` query builder in util/rme/field_metadata.py using athena_select_to_dataframe, filtering by layer_id/partition columns and returning columns compatible with existing FieldMeta expectations.
2. Update field_metadata_to_file in main.py (and any other report modules) to call the new helper, keeping only the necessary subset/renames when writing column_metadata.csv.
Refactor shared metadata consumers (util/rme/field_metadata.py::get_field_metadata, util/rme/field_metadata.FieldMeta, tests under tests/test_rs_field_meta_*) to rely on the enriched DataFrame, ensuring unit/theme lookups continue to work across reports.
3. Document the new metadata contract in plan-rptIgoProject.prompt.md and ensure other report generators (e.g., main.py, util/rme/field_metadata.py clients) import the same helper instead of bespoke CSV logic.

Further Considerations
1. Need explicit filter strategy for layer_definitions (e.g., authority/tool schema version) to avoid cross-project collisions.
2. Decide whether to cache/export the Athena result (column_metadata.csv) for offline runs or rely purely on on-demand queries.
3. Coordinate timeline for replacing rme_table_column_defs.csv in athenacsv_to_rme.py so GeoPackage schema builds stay in sync with the new metadata source.