## Plan: Power BI Semantic Model Automation

**TL;DR**
Enhance the Python metadata export to include `theme`, `preferred_format`, and resolved export units. Then, create a generation script that writes fresh TMDL definition files for a new Power BI (.pbip) semantic model, where friendly names, units, folders, tooltips, format strings, and sorting logic all inherit from the Athena source metadata. The model uses a `DataMartRoot` parameter for data connectivity.

STATUS: All except Phase 5 have been implemented. Also, we have moved to the Power BI Enhanced Report Format (PBIR) and this is now the standard we aim for (see https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-report for documentation).

---

### Phase 1: Enhance Python Metadata System

1. **Add `theme` to `FieldMetaValues`**: Add `theme` to `VALID_COLUMNS` and `FieldMetaValues.__init__`, and propagate it through `add_field_meta`, `get_field_meta`, and the `field_meta` setter. (`get_field_metadata` from Athena already returns a `theme` column — the setter's loop over `VALID_COLUMNS` will pick it up automatically once the field exists.)

2. **Inherit `theme` in `apply_all_bins`**: When `apply_all_bins` registers `_bin`, `_color`, and `_bin_sort` columns via `meta.add_field_meta(...)`, pass `theme=source_theme` so derived columns group with their parent.

3. **Add resolved `export_unit` to data dictionary**:  Restructure `export_data_dictionary` to accept per-table applied-units alongside each DataFrame, and output `theme`, `preferred_format`, and a new `export_unit` column.

   **Signature change**: Replace the flat `tables: dict[str, pd.DataFrame]` with `tables: dict[str, TableEntry]`, where `TableEntry` is a `NamedTuple(df, applied_units)`.  Each table's `applied_units` dict (column_name → pint.Unit | None) comes from the return value of `RSFieldMeta.apply_units()` and is the authoritative record of what unit each column's data was actually converted to. Tables without unit processing pass an empty dict.

   **Layer-id resolution**: Inside the per-table loop, resolve `layer_id` from `df.attrs["layer_id"]` (using `RSFieldMeta._resolve_layer_context`) and pass it to `get_field_meta(col, layer_id)` for disambiguation.  This mirrors the pattern `apply_units` already uses.

   `export_unit` is populated per-table from that table's `applied_units` dict — no cross-table key collision possible.  The `data_unit` and `display_unit` columns remain in the CSV for provenance.  Columns not in `applied_units` (e.g., bin columns added after `apply_units`) get an empty `export_unit`.

4. **Update tests**: Update assertions in `test_apply_all_bins.py` (and any other tests checking the data dictionary CSV schema) to expect the new columns.

---

### Phase 2: Python → Power BI Format Translation

1. Create a **hand-curated lookup table** mapping the Python format strings actually in use to Power BI Custom Format Strings. No need for a general-purpose parser — just cover the known formats:

   | Python format | PBI format   | Notes                        |
   |---------------|--------------|------------------------------|
   | `,.0f`        | `#,##0`      | Integer with thousands sep   |
   | `,.2f`        | `#,##0.00`   | 2 decimals + thousands       |
   | `.1%`         | `0.0%`       | 1 decimal percent            |
   | `.2f`         | `0.00`       | 2 decimals, no grouping      |
   | `,`           | `#,##0`      | Thousands separator only     |
   | `.0f`         | `0`          | Plain integer                |

2. Provide a fallback that logs a warning for any unmapped format, so unknown formats surface during development rather than silently producing wrong output.

3. Add unit tests for the translation table (`tests/test_pbi_format_translation.py`).

---

### Phase 3: Generate Fresh PBIP Semantic Model

1. **Initialize `scripts/update_pbi_model.py`** (new file). This script:
   - Reads `data_dictionary.csv` from the data mart output.
   - Generates a **complete, fresh** PBIP semantic model (not surgical patching of an existing one).
   - Uses the existing `example_data_with_bins.pbip` only as a reference for structural patterns (file layout, `DataMartRoot` parameter, Power Query expressions).

2. **Generate `expressions.tmdl`**: Emit the `DataMartRoot` parameter and the `fn_LoadParquetFolder` function. `DataMartRoot` should have `IsParameterQueryRequired=true`.

3. **Generate one `<table_name>.tmdl` per table** (`dgo`, `huc`, `grazing`). For each column in the data dictionary belonging to that table:
   - **Column name**: `'{friendly_name} ({export_unit})'` when a unit exists, else `'{friendly_name}'`
   - **sourceColumn**: the raw `column_name` from the CSV
   - **displayFolder**: the `theme` value (maps to folder in PBI field list)
   - **description**: the `description` value (shows as tooltip)
   - **formatString**: translated PBI format from Phase 2 lookup
   - **sortByColumn**: for `_bin` columns, point to the corresponding `_bin_sort` column's PBI name
   - **dataCategory**: hardcoded mappings for known geospatial fields (`latitude` → Latitude, `longitude` → Longitude, `state` → StateOrProvince, `county` → County)
   - **dataType**: inferred from `dtype` column (INTEGER→int64, FLOAT→double, STRING→string, BOOLEAN→boolean)
   - **lineageTag**: deterministic UUID v5 (see below)
   - **summarizeBy**: `none` for strings/IDs/geo, `sum` for numeric (sensible default; can be overridden later in PBI Desktop)

4. **Generate `database.tmdl`, `model.tmdl`** with minimal boilerplate (compatibility level, model culture, etc.).

5. **Generate `relationships.tmdl`** if cross-table relationships are known; otherwise emit an empty file and leave relationship creation to PBI Desktop.

6. **Deterministic lineageTags via UUID v5**: Use `uuid.uuid5(NAMESPACE, key)` with a fixed namespace UUID (generated once, committed to the script). Keys are built from stable identifiers — not display names:
   - Table: `f"{table_name}.table"`
   - Column: `f"{table_name}.column.{column_name}"` (raw Parquet column name)
   - Measure: `f"{table_name}.measure.{measure_name}"` (for future use)
   - Database/model: `"database"`, `"model"`

   This makes output **idempotent** — re-running on the same data dictionary produces byte-identical TMDL. It also provides **free lineageTag preservation**: renaming a friendly name or changing units won't change the GUID, so Power BI report visuals that reference the column by lineageTag survive model regeneration. Only truly structural changes (adding/removing a raw column) produce new GUIDs.

7. **Write all `.tmdl` files with `\r\n` line endings** for Windows/Power BI Desktop compatibility.

8. **Scaffold the surrounding PBIP structure**: `.pbip`, `.pbir`, `.pbism` files and folder layout matching the expected PBIP format.

---

### Phase 4: Unit Handling & main.py Plumbing

Wire up `apply_units` for all tables and build per-table `TableEntry` objects for the data dictionary:

1. **Capture applied_units for DGO**: Change `dgo_df, _ = RSFieldMeta().apply_units(dgo_df)` to `dgo_df, dgo_applied_units = RSFieldMeta().apply_units(dgo_df)`.
2. **Call `apply_units` on HUC and Grazing**: Even if they currently have few unit-bearing columns, `apply_units` also coerces dtypes (string, int, float) and future-proofs the pipeline:
   ```python
   huc_df, huc_applied_units = RSFieldMeta().apply_units(huc_df)
   grazing_df, grazing_applied_units = RSFieldMeta().apply_units(grazing_df)
   ```
3. **Build `TableEntry` per table and pass to `export_data_dictionary`**:
   ```python
   from util.metadata_export import export_data_dictionary, TableEntry
   all_tables: dict[str, TableEntry] = {
       "dgo": TableEntry(df=dgo_df, applied_units=dgo_applied_units),
       "huc": TableEntry(df=huc_df, applied_units=huc_applied_units),
       "grazing": TableEntry(df=grazing_df, applied_units=grazing_applied_units),
   }
   export_data_dictionary(all_tables, dict_path)
   ```
   Each table's `applied_units` stays coupled with its DataFrame — no cross-table key collision.
4. **Update `export_data_dictionary` signature and `layer_id` resolution**:
   - Change `tables` parameter type from `dict[str, pd.DataFrame]` to `dict[str, TableEntry]`.
   - Inside the per-table loop, resolve `layer_id` from `entry.df.attrs["layer_id"]` via `RSFieldMeta._resolve_layer_context(entry.df, None)`.
   - Pass `layer_id` to `meta.get_field_meta(col, layer_id)` for disambiguation.
   - Look up `export_unit` from `entry.applied_units.get(col)` — scoped to that table.
5. Review and expand `set_display_unit` calls in `rpt_data_mart/main.py` for any columns that should have user-facing unit labels different from `data_unit`.
6. **Unit abbreviation for PBI column names**: The `applied_units` dict contains `pint.Unit` objects. The PBIP script formats them with Pint's compact notation (`f"{unit:~P}"` → km, m²) for column display names. The data dictionary stores the full unit name for provenance; abbreviation happens at PBIP generation time.
7. **Update tests**: Existing tests that call `export_data_dictionary` with a plain `dict[str, pd.DataFrame]` must be updated to use `TableEntry` (with `applied_units={}` for tables that don't go through `apply_units`).

---

### Phase 5: Bin Dimension Tables & Hidden Helper Columns

Extract bin triplets from the fact table into star-schema dimension tables and hide implementation columns, making the model easier to work with in PBI Desktop — especially for conditional formatting by color.

1. **Detect bin triplets in `_resolve_columns`**: During Pass 2, identify groups of `{metric}_bin`, `{metric}_color`, `{metric}_bin_sort` columns sharing a common prefix. Collect these into a `BinGroup` dataclass (`metric_prefix`, `bin_col`, `color_col`, `sort_col`, `source_table`).

2. **Generate dimension table TMDL per bin group**: For each `BinGroup`, emit a `{metric_prefix}_bins.tmdl` with three columns (`_bin`, `_color`, `_bin_sort`). The columns use raw Parquet names as `sourceColumn` — no renaming. The partition uses Power Query that references the source fact table, selects the three columns, removes duplicates, and removes blank rows — mirroring the manually-created `brat_capacity_bins` pattern:

```
let
Source = {fact_table},
#"Removed Other Columns" = Table.SelectColumns(Source, {"{bin_col}", "{color_col}", "{sort_col}"}),
#"Removed Duplicates" = Table.Distinct(#"Removed Other Columns", {"{bin_col}"}),
#"Removed Blank Rows" = Table.SelectRows(#"Removed Duplicates", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null})))
in
#"Removed Blank Rows"
```

3. **Generate relationships**: Populate `_generate_relationships_tmdl` to emit one relationship per bin group, joining the fact table's `_bin_sort` column to the dimension table's `_bin_sort` column. Use deterministic UUID v5 for relationship IDs (`{table}.relationship.{metric_prefix}`). Relationship cardinality: many-to-one (fact → dimension).

4. **Hide `_color` and `_bin_sort` columns in the fact table**: Add `isHidden: true` to `_generate_column_tmdl` for columns whose raw name ends with `_color` or `_bin_sort`. These columns remain functional (sortByColumn and the relationship still reference them) but are hidden from the PBI field list, decluttering ~20 helper columns.

5. **Register dimension tables in `model.tmdl`**: Add each `{metric_prefix}_bins` to `PBI_QueryOrder` and emit `ref table` entries.

6. **lineageTag stability**: Dimension table and column lineageTags use `{metric_prefix}_bins.table` and `{metric_prefix}_bins.column.{raw_col}` as UUID v5 keys — stable across regenerations.





---

### Relevant Files

| File | Action |
|------|--------|
| `src/util/pandas/RSFieldMeta.py` | Add `theme` to `VALID_COLUMNS`, `FieldMetaValues`, `add_field_meta`, `get_field_meta` |
| `src/util/rme/rme_common_dataprep.py` | Pass `theme` in `apply_all_bins` metadata registration |
| `src/util/metadata_export.py` | Add `TableEntry` NamedTuple; restructure `export_data_dictionary` to accept `dict[str, TableEntry]`; resolve `layer_id` from `df.attrs`; add `theme`, `preferred_format`, `export_unit` to CSV output |
| `scripts/update_pbi_model.py` | Add `BinGroup` detection in `_resolve_columns`; add `_generate_dimension_table_tmdl`; populate `_generate_relationships_tmdl`; add `isHidden` to `_generate_column_tmdl` for `_color`/`_bin_sort` columns; register dimension tables in `_generate_model_tmdl` |
| `tests/test_apply_all_bins.py` | Update data dictionary assertions for new columns; update calls to use `TableEntry` |
| `tests/test_pbi_format_translation.py` | **New** — format translation tests |
| `src/reports/rpt_data_mart/main.py` | Capture applied_units dicts, call apply_units on all tables, build `TableEntry` per table, pass to export_data_dictionary |

---

### Verification

1. `pytest` — all existing + new tests pass.
2. Run the datamart report on sample data → produces `data_dictionary.csv` with `theme`, `preferred_format`, `export_unit` columns populated.
3. Run `update_pbi_model.py` targeting the data dictionary → generates a complete PBIP folder.
4. Open the `.pbip` in Power BI Desktop, provide `DataMartRoot`, verify:
   - Tables load from Parquet via `DataMartRoot`
   - Column display names show friendly names with units
   - Display folders group fields by theme
   - Tooltips show descriptions
   - Format strings render correctly
   - Bin columns sort by their sort-order column
5. Verify dimension tables: each bin group (e.g., `low_lying_ratio_bins`, `confinement_ratio_bins`, etc.) appears as a separate table with 3 columns and a relationship to `dgo`.
6. Verify `_color` and `_bin_sort` columns are hidden in the `dgo` field list but the `_bin` label columns remain visible.
7. Test conditional formatting: in a map or table visual, use "Format by field value" referencing the dimension table's `_color` column — colors should apply correctly via the relationship.

---

### Decisions

- **Fresh generation, not patching**: The script writes a complete PBIP from scratch each time. No need to parse or merge existing TMDL.
- **TMDL via Python**: Cross-platform, stays in the Python ecosystem, no .NET / Tabular Editor dependency.
- **Hand-curated format table**: Covers the ~6 formats actually in use rather than building a general translator.
- **`\r\n` line endings**: Required for Power BI Desktop compatibility.
- **Deterministic UUID v5 lineageTags**: Hash-based GUIDs from stable identifiers (table + raw column name). Idempotent output, free lineageTag preservation across regenerations.
- **Accept duplicate-meta risk for bins**: `add_field_meta` rejects duplicates; accepted since bin columns are generated fresh each run.
- **Per-table `TableEntry` for data dictionary**: `export_data_dictionary` accepts `dict[str, TableEntry]` where `TableEntry = NamedTuple(df, applied_units)`. This keeps each table's applied units scoped to its DataFrame, avoids cross-table key collisions, and is a step toward DataFrame-local metadata without requiring a full architectural rethink.
- **Star-schema for bins**: Dimension tables mirror the user's manually-created `brat_capacity_bins` pattern — Power Query references the fact table, deduplicates, and strips blanks. This makes `_color` columns usable for PBI's "Format by field value" conditional formatting across a relationship.
- **Hidden helper columns**: `_color` and `_bin_sort` remain in the fact table (required for sortByColumn and relationship joins) but are hidden from the UI to reduce clutter.

---

### Future Enhancements (out of scope)

- **Pre-generated DAX measures**: Generate a `_Measures` table with common aggregations — length-weighted averages for ratio metrics (e.g., Riparian Condition Index weighted by `centerline_length`), sum totals for area/length fields, DGO count, and bin distribution percentages (% of network length per category). Requires a measures registry (metadata-driven from a `measure_type` column in the data dictionary or a separate `measures.csv`) and a `_generate_measures_tmdl` function. The TMDL syntax is straightforward (`measure 'Name' = SUMX(...)` with `formatString` and `displayFolder`).
- **DataMartRoot default value**: Set a sensible default or empty string so PBI always prompts.
- **DataFrame-local metadata**: Move away from the Borg singleton toward DataFrames maintaining their own metadata (e.g. via `df.attrs`). The Athena-sourced registry would remain the initial source of truth, but post-processing steps (unit conversion, calculated columns, bins) would write metadata back into the DataFrame rather than a global store. This would make column provenance explicit per-table and eliminate ambiguity issues. The current `attrs["layer_id"]` and per-table `TableEntry.applied_units` patterns are steps in this direction.