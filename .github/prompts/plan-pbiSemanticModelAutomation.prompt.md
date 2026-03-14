## Plan: Power BI Semantic Model Automation

**TL;DR**
Enhance the Python metadata export to include `theme`, `preferred_format`, and resolved export units. Then, create a generation script that writes fresh TMDL definition files for a new Power BI (.pbip) semantic model, where friendly names, units, folders, tooltips, format strings, and sorting logic all inherit from the Athena source metadata. The model uses a `DataMartRoot` parameter for data connectivity.

---

### Phase 1: Enhance Python Metadata System

1. **Add `theme` to `FieldMetaValues`**: Add `theme` to `VALID_COLUMNS` and `FieldMetaValues.__init__`, and propagate it through `add_field_meta`, `get_field_meta`, and the `field_meta` setter. (`get_field_metadata` from Athena already returns a `theme` column — the setter's loop over `VALID_COLUMNS` will pick it up automatically once the field exists.)

2. **Inherit `theme` in `apply_all_bins`**: When `apply_all_bins` registers `_bin`, `_color`, and `_bin_sort` columns via `meta.add_field_meta(...)`, pass `theme=source_theme` so derived columns group with their parent.

3. **Add resolved `export_unit` to data dictionary**:  Update `export_data_dictionary` to accept a new `applied_units: dict[str, pint.Unit | None]` parameter and output `theme`, `preferred_format`, and a new `export_unit` column. `export_unit` is sourced from the `applied_units` dict returned by `RSFieldMeta.apply_units()` — this is the authoritative record of what unit each column's data was actually converted to, accounting for `display_unit` overrides, unit system preferences, and `no_convert` flags. The `data_unit` (to be renamed `original_data_unit`) and `display_unit` columns remain in the CSV for provenance. Columns not in `applied_units` (e.g., bin columns added after `apply_units`) get an empty `export_unit`.

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

Wire up `apply_units` for all tables and propagate the applied-units dicts through to the data dictionary:

1. **Capture applied_units for DGO**: Change `dgo_df, _ = RSFieldMeta().apply_units(dgo_df)` to `dgo_df, dgo_applied_units = RSFieldMeta().apply_units(dgo_df)`.
2. **Call `apply_units` on HUC and Grazing**: Even if they currently have few unit-bearing columns, `apply_units` also coerces dtypes (string, int, float) and future-proofs the pipeline:
   ```python
   huc_df, huc_applied_units = RSFieldMeta().apply_units(huc_df)
   grazing_df, grazing_applied_units = RSFieldMeta().apply_units(grazing_df)
3. Merge and pass to export_data_dictionary:
```python
all_applied_units = {**dgo_applied_units, **huc_applied_units, **grazing_applied_units}
export_data_dictionary(all_tables, dict_path, applied_units=all_applied_units)
```
4. Review and expand set_display_unit calls in rpt_data_mart/main.py for any columns that should have user-facing unit labels different from data_unit.
5. Unit abbreviation for PBI column names: The applied_units dict contains pint.Unit objects. The PBIP script formats them with Pint's compact notation (`f"{unit:~P}"` → km, m²) for column display names. The data dictionary stores the full unit name for provenance; abbreviation happens at PBIP generation time.



---

### Relevant Files

| File | Action |
|------|--------|
| `src/util/pandas/RSFieldMeta.py` | Add `theme` to `VALID_COLUMNS`, `FieldMetaValues`, `add_field_meta`, `get_field_meta` |
| `src/util/rme/rme_common_dataprep.py` | Pass `theme` in `apply_all_bins` metadata registration |
| `src/util/metadata_export.py` | Add `theme`, `preferred_format`, `export_unit` to CSV output |
| `scripts/update_pbi_model.py` | **New** — reads data dictionary, generates TMDL files |
| `tests/test_apply_all_bins.py` | Update data dictionary assertions for new columns |
| `tests/test_pbi_format_translation.py` | **New** — format translation tests |
| `src/reports/rpt_data_mart/main.py` | Capture applied_units dicts, call apply_units on all tables, pass to export_data_dictionary |

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

---

### Decisions

- **Fresh generation, not patching**: The script writes a complete PBIP from scratch each time. No need to parse or merge existing TMDL.
- **TMDL via Python**: Cross-platform, stays in the Python ecosystem, no .NET / Tabular Editor dependency.
- **Hand-curated format table**: Covers the ~6 formats actually in use rather than building a general translator.
- **`\r\n` line endings**: Required for Power BI Desktop compatibility.
- **Deterministic UUID v5 lineageTags**: Hash-based GUIDs from stable identifiers (table + raw column name). Idempotent output, free lineageTag preservation across regenerations.
- **Accept duplicate-meta risk for bins**: `add_field_meta` rejects duplicates; accepted since bin columns are generated fresh each run.

---

### Future Enhancements (out of scope)

- **Programmatic DAX measures**: Generate measures from metadata in a future phase.
- **DataMartRoot default value**: Set a sensible default or empty string so PBI always prompts.
- **Relationship auto-generation**: Derive relationships from foreign key conventions in the data dictionary.