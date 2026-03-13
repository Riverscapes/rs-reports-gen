## Plan: Power BI Semantic Model Automation

**TL;DR** 
Enhance the Python metadata export to include `theme`, `description`, and `preferred_format`. Then, create a translation script that automatically patches Power BI's TMDL definition files so that friendly names, units, folders, tooltips, format strings, and sorting logic all directly inherit from the Athena source metadata. Also, rely on the existing model's parameter-driven connectivity.

**Steps**

**Phase 1: Enhance Python Metadata System**
1. Update `RSFieldMeta` class to formally support `theme` and `preferred_format` (if not already fully captured).
2. Update `apply_all_bins` so that new bin/color columns inherit the `theme` of their source column, keeping related grouping intact.
3. Update `export_data_dictionary` to output `theme` and `preferred_format` to the `data_dictionary.csv`.

**Phase 2: Python to Power BI Format Translation**
1. Create a format translation function that reads Python format-specification mini-language (e.g. `,.2f`, `.1%`, `,`) and converts it to Power BI Custom Format Strings.
    - *Examples*:
    - `,.0f` -> `#,0` or `#,##0`
    - `,.2f` -> `#,0.00`
    - `.1%` -> `0.0%`
    - `.2f` -> `0.00`

**Phase 3: Scaffold and Update PBIP Model**
1. **Initialize PBIP Skeleton**: Use `example_data_with_bins.pbip` as a structural template to retain the `DataMartRoot` parameter.
2. Initialize `scripts/update_pbi_model.py`.
3. Read the `data_dictionary.csv`. For each table (e.g., `dgo`, `huc`, `grazing`), ensure the Power Query mapping adheres to the pattern:
   `source = Parquet.Document(File.Contents(DataMartRoot & "\exports\<table_name>.parquet"))`
4. For each column matching the dictionary, update the associated `.tmdl` to configure:
   - **Name**: `{friendly_name} ({display_unit})`
   - **Folder**: `displayFolder: {theme}`
   - **Tooltip**: `description: {description}`
   - **Sorting**: For `_bin` columns, append `sortByColumn: {friendly_name} (sort)`
   - **Format**: Insert `formatString: {translated_pbi_format}` using Phase 2 logic.
   - **Data Category**: Hardcode standard mappings for geospatial fields (`latitude` -> Latitude, `longitude` -> Longitude, `state` -> State or Province, `county` -> County).

**Relevant files**
- `src/util/pandas/RSFieldMeta.py` — Add/verify `theme` and `preferred_format`
- `src/util/rme/rme_common_dataprep.py` — Inherit `theme` in `apply_all_bins`
- `src/util/metadata_export.py` — Add `theme` and `preferred_format` to CSV output
- `scripts/update_pbi_model.py` — **New**, translates formats, applies DataMartRoot logic, and modifies TMDL files

**Verification**
1. Run unit tests (`pytest`).
2. Run the datamart report script on sample data to explicitly populate `data_dictionary.csv`.
3. Run `update_pbi_model.py` to target the new `.tmdl` files.
4. Open the resulting `.pbip` in Power BI Desktop. Wait for it to prompt for `DataMartRoot` (or ensure it automatically resolves).
5. Verify properties (tooltips, formats, folders, dynamic connections) operate seamlessly.

**Decisions**
- Using TMDL text updates via Python instead of Tabular Editor or PowerShell scripts, ensuring cross-platform support (Mac/Windows developers) and staying within the Python ecosystem.
- PBIP configuration hinges on `DataMartRoot` mapping rather than hard-coded filepaths, preserving code portability.
