# Runtime Unit Resolution Plan (Draft)

## Status

Draft proposal for later implementation.

This document captures architecture direction and concrete signatures without requiring immediate full rollout.

## Problem Statement

Current behavior mixes two concerns:

- Canonical metadata facts (for example, `data_unit`)
- Runtime display choices that depend on context and data magnitude

In practice, this leads to chart-local overrides that mutate shared metadata state and are difficult to reason about for multi-series charts sharing one axis.

## Design Goals

- Keep canonical metadata authoritative and stable.
- Treat display-unit fields as suggestions, not commands.
- Resolve effective units per render call (non-mutating).
- Support shared-axis unit decisions for multi-series charts.
- Keep the first implementation small and testable.

## Terminology

- Canonical unit: source unit of the data value (`data_unit`).
- Suggested unit: metadata-level preference for display (for example current `display_unit` and `display_unit_imperial`).
- Effective unit: final unit used for this specific render/output call.
- Hard override: explicit caller/user choice that pins unit(s), regardless of auto policy.

## Proposed Resolution Precedence

Highest to lowest:

1. User hard override (for example: "show this axis in acres")
2. Call-site pinned unit (for example: this chart always shows square miles)
3. Policy-driven shared-axis unit from data magnitude
4. Suggested metadata unit for active system (SI or imperial)
5. System-default conversion from canonical unit
6. Fallback to canonical unit

This precedence keeps behavior explicit and deterministic.

## Proposed API Shapes

These signatures are intentionally explicit and avoid hidden global mutation.

```python
from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence
import pandas as pd
import pint


@dataclass(frozen=True)
class DisplayContext:
    layer_id: str | None = None
    # Axis id -> tuple of columns that must share a single display unit.
    axis_groups: dict[str, tuple[str, ...]] | None = None
    # Call-site hard pins (applied before auto policy).
    pinned_units: Mapping[str, str | pint.Unit] | None = None
    # User-driven hard overrides (highest precedence).
    user_unit_overrides: Mapping[str, str | pint.Unit] | None = None


@dataclass(frozen=True)
class UnitDecision:
    column: str
    resolved_unit: pint.Unit | None
    source: str  # "user_override" | "pinned" | "policy" | "suggested" | "system_default" | "canonical"
    reason: str
    axis_id: str | None = None


class UnitPresentationPolicy(Protocol):
    def resolve_units(
        self,
        *,
        meta: "RSFieldMeta",
        df: pd.DataFrame,
        columns: Sequence[str],
        context: DisplayContext,
    ) -> dict[str, pint.Unit | None]: ...
```

```python
class RSFieldMeta:
    def resolve_effective_units(
        self,
        df: pd.DataFrame,
        *,
        columns: Sequence[str] | None = None,
        context: DisplayContext | None = None,
        policy: UnitPresentationPolicy | None = None,
    ) -> tuple[dict[str, pint.Unit | None], dict[str, UnitDecision]]:
        ...

    def apply_units(
        self,
        df: pd.DataFrame,
        *,
        layer_id: str | None = None,
        effective_units: Mapping[str, str | pint.Unit] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, pint.Unit | None]]:
        ...

    def bake_units(
        self,
        df: pd.DataFrame,
        *,
        header_units: bool = True,
        context: DisplayContext | None = None,
        policy: UnitPresentationPolicy | None = None,
        effective_units: Mapping[str, str | pint.Unit] | None = None,
    ) -> tuple[pd.DataFrame, list[str], dict[str, pint.Unit | None]]:
        ...
```

## What Happens to `no_convert`

Recommendation: remove it from canonical metadata.

Rationale:

- It is not present in canonical metadata sources.
- It encodes runtime conversion behavior, not a field fact.
- It can be replaced by explicit hard overrides in `DisplayContext`.

Example mapping:

- Previous `no_convert=True, display_unit='hectare'`
- New `DisplayContext(pinned_units={"segment_area": "hectare"})`

## How Unit System, Magnitude Policy, and Hard Overrides Work Together

The following example starts with canonical value `123,456 m2`.

## Value Rendering Policy (Units + Precision + Unit Label Style)

Unit selection and number formatting are separate concerns, but they should be resolved together through one rendering profile so output is understandable.

Suggested pipeline:

1. Resolve effective unit (system, policy, hard overrides).
2. Convert quantity to effective unit.
3. Resolve numeric precision for context (chart, table, prose).
4. Render unit label in compact or full style.

### Why this matters for prose

Narrative sentences are sensitive to precision. A value shown as `0` can be interpreted as none, while `0.4` communicates non-zero presence.

Example sentence pattern:

- "This area is X and includes Y of river length."

For small values, both unit and decimals may need to change.

### Proposed rendering profiles

Keep profile names small and explicit.

- chart:
  - short unit labels
  - fewer decimals
  - prioritize readability at a glance
- table:
  - short or full labels depending on table density
  - moderate decimals
  - stable column formatting
- prose:
  - full labels by default
  - precision that avoids false zero for non-zero values
  - optional dual-format support (human text plus compact in parentheses)

### Proposed precision policy

Use magnitude-aware precision with a non-zero guard.

- If value is exactly zero, show `0`.
- If value is non-zero and abs(value) < 1, show at least 1 decimal in prose.
- If value is non-zero and abs(value) < 0.1, show up to 2 to 3 decimals in prose.
- For values >= 1 and < 100, show 1 decimal in prose.
- For large values, use 0 or 1 decimal depending on profile.

This prevents `0.4` from collapsing to `0` in narrative output.

### Signature sketch for coordinated rendering

```python
from dataclasses import dataclass
from typing import Literal
import pint


@dataclass(frozen=True)
class PrecisionPolicy:
    # Minimum decimals for non-zero values in prose-like output.
    min_nonzero_decimals: int = 1
    # Maximum decimals to prevent noisy text.
    max_decimals: int = 3


@dataclass(frozen=True)
class ValueRenderProfile:
    name: Literal["chart", "table", "prose"]
    unit_label_style: Literal["compact", "full"] = "compact"
    precision: PrecisionPolicy = PrecisionPolicy()


class RSFieldMeta:
    def format_scalar(
    self,
    column_name: str,
    value,
    layer_id: str | None = None,
    *,
    effective_unit: str | pint.Unit | None = None,
    profile: ValueRenderProfile | None = None,
    ) -> str:
    ...
```

Notes:

- `effective_unit` comes from runtime unit resolution.
- `profile` decides decimals and compact versus full unit label style.
- `preferred_format` remains a field-level hint and can be used when profile does not override it.

### Exact Conversion Reference

- m2: `123456`
- sq ft: `1328869.3244053128`
- ha: `12.3456`
- km2: `0.123456`
- acres: `30.506641974410364`
- sq mi: `0.04766662808501619`

### Typical Display Renderings

| Unit | Value | Compact unit style | Full unit style |
|---|---:|---|---|
| m2 | 123,456 | 123,456 m2 | 123,456 square meters |
| sq ft | 1,328,869.32 | 1,328,869.32 ft2 | 1,328,869.32 square feet |
| ha | 12.3456 | 12.3456 ha | 12.3456 hectares |
| km2 | 0.123456 | 0.123456 km2 | 0.123456 square kilometers |
| acres | 30.5066 | 30.5066 ac | 30.5066 acres |
| sq mi | 0.0476666 | 0.0476666 mi2 | 0.0476666 square miles |

Notes:

- Compact vs full string is a formatting concern, not a unit-resolution concern.
- Decimal precision remains separate (`preferred_format` or explicit formatting parameters).

### Small-value prose examples

Assume canonical area value is `0.4 ha` equivalent and report style is prose:

- Poor rendering: `0 hectares`
- Better rendering: `0.4 hectares`
- If unit is changed for readability: `4,000 square meters`

Assume canonical length value resolves to `0.37 km`:

- Compact prose: `0.4 km`
- Full prose: `0.4 kilometers`
- Alternate user-facing phrasing: `about 400 meters`

The right output depends on profile and audience, but the policy should always avoid false-zero non-zero values in prose.

### Scenario Examples

1. SI system, no hard override, magnitude policy enabled:

- Canonical `m2` may auto-resolve to `ha` for readability when totals are large.

2. Imperial system, no hard override, magnitude policy enabled:

- Canonical `m2` may auto-resolve to `acres`.

3. Any system, hard override set to `sq_ft`:

- Effective unit is forced to `sq_ft`.
- Policy and metadata suggestions are ignored for that column.

## Multi-Series Shared-Axis Example

For a grouped chart with `road_segment_area` and `rail_segment_area` on the same x-axis:

```python
context = DisplayContext(
    axis_groups={"x": ("road_segment_area", "rail_segment_area")}
)

baked_df, headers, applied_units = meta.bake_units(
    agg_data,
    context=context,
    policy=scale_policy,
)
```

Expected behavior:

- One effective x-axis unit is chosen for the whole axis group.
- Both series are converted to that same unit.
- Axis label and hover labels remain consistent.

## Phase 1 Spike (Recommended First Implementation)

1. Add optional `effective_units` support to `apply_units` and `bake_units`.
2. Add `DisplayContext` and `resolve_effective_units` scaffolding.
3. Migrate one function end-to-end (`dens_road_rail`) to shared-axis resolution.
4. Add focused tests for:
   - shared-axis same-unit guarantee
   - hard override precedence
   - SI vs imperial auto behavior for area totals

## Out of Scope for Initial Spike

- Renaming all metadata columns immediately.
- Migrating every figure helper in one pass.
- Redesigning all formatting behavior.

## Open Questions (Capture for Later)

- Should suggested unit fields be renamed now or later?
- Should policy threshold rules be metadata-driven or code-driven?
- Should exports always ignore magnitude policy and stay canonical/system-default?
