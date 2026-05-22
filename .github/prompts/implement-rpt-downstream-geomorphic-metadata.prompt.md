## Prompt: Implement Metadata-Aware Units and Friendly Labels in `rpt_downstream_geomorphic`

Use this prompt when asking an AI coding agent to implement metadata strategy in the downstream geomorphic report.

## Goal

Implement metadata-aware data flow and presentation in `rpt_downstream_geomorphic` so that:

- Metadata comes from Athena definitions.
- Unit conversions are applied consistently.
- Figure labels/titles/axes prefer friendly names with resolved display units.
- Missing metadata degrades safely to current behavior.

## Scope

Primary files:

- `src/reports/rpt_downstream_geomorphic/main.py`
- `src/reports/rpt_downstream_geomorphic/dataprep.py`
- `src/reports/rpt_downstream_geomorphic/figures.py`

Reference patterns:

- Data mart metadata + units orchestration: `src/reports/rpt_data_mart/main.py`
- Shared metadata utilities: `src/util/pandas/RSFieldMeta.py`
- Derived-column metadata pattern: `src/util/rme/rme_common_dataprep.py`

## Required Changes

1. Metadata bootstrap and layer disambiguation

- Keep `define_fields(unit_system)` as the report metadata bootstrap.
- Ensure metadata lookup is unambiguous for report columns by using one consistent layer strategy.
- Set `df.attrs["layer_id"]` as early as possible for report DataFrames used in formatting/presentation.

2. Apply unit conversion in the report pipeline

- In `orchestrate(...)`, apply `RSFieldMeta().apply_units(...)` to the profile source DataFrame before chart generation.
- Preserve `layer_id` context after dataframe transforms that may drop attrs.
- Do not change the report's business logic for filtering/sorting/grouping level paths.

3. Refactor figure labeling to metadata-aware labels

- Replace hardcoded display labels in `figures.py` with metadata-driven labels where practical.
- For y-axis and legend labels, prefer `RSFieldMeta` friendly name + resolved display unit.
- For x-axis (`seg_distance`) use metadata-aware header when available; fallback to existing string.
- Preserve current chart structure and traces unless required for metadata label injection.

4. Safe fallback behavior

- If metadata row is missing, rely on `RSFieldMeta` fallback behavior (`get_friendly_name` / header helpers) so labels remain readable (title-case) without introducing new hardcoded label maps.
- No runtime errors from missing metadata.

5. Tests (Phase 1: minimum required in this implementation task)

- Add only the minimum safety tests needed to protect this change set. For example: 
  - no regression in figure count/key generation.

6. Expanded tests (Phase 2: separate follow-up task)

- Defer broader test expansion to a dedicated follow-up task/PR.
- Follow-up scope can include:
  - fallback label appears when metadata is unavailable;
  - friendly/unit label appears when metadata exists;
  - additional field combinations and chart permutations;
  - SI vs imperial matrix checks;
  - edge-case and snapshot-style validation.

## Acceptance Criteria

- `rpt_downstream_geomorphic` renders charts with metadata-aware labels for configured fields.
- Unit system selection (`SI` vs `imperial`) flows through to chart labels where units are shown.
- Existing report output remains functionally equivalent apart from label/unit improvements.
- Phase 1 minimum safety tests pass.

### Follow-up Acceptance Criteria (separate task)

- Expanded metadata-label test coverage is added and passes.

## Verification Commands

Phase 1 (minimum required now), run from repo root:

```powershell
uv run pytest test_report_smoke.py -k "Downstream"
uv run pytest tests/test_rpt_downstream_geomorphic*.py
```

If no downstream-specific tests exist yet, create the minimum required tests and run:

```powershell
uv run pytest tests/test_rpt_downstream_geomorphic*.py
```

Phase 2 (follow-up test hardening):

```powershell
uv run pytest -k downstream_geomorphic
```

## Guardrails

- Use `Path` over `os.path`.
- Keep typing annotations for all new functions.
- Avoid broad refactors outside listed files.
- Prefer minimal, reviewable diffs.
- Do not change unrelated report behavior or CLI contracts.
- Do not introduce new hardcoded presentation labels when a metadata helper can provide the same fallback.
