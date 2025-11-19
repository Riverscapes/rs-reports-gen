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

