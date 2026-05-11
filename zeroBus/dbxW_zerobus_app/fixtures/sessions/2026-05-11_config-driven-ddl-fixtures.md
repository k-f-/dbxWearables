# Config-Driven DDL Fixtures & Column Comments

**Date:** 2026-05-11
**Pipeline update IDs:** `86406238`, `355f13cd`, `fcad0b4a`

---

## Problem

Table metadata (column comments, PK constraints, expectations) was hard-coded throughout `silver_healthkit.py`. A previous attempt to apply DDL post-deploy via `ALTER TABLE` failed because Databricks blocks all external schema modifications on pipeline-managed streaming tables and materialized views. Needed a way to externalize DDL metadata AND have it applied at runtime.

## Solution

Config-driven architecture using YAML fixture files loaded at pipeline runtime:

1. **YAML fixtures** (`fixtures/ddl/*.yml`) — define column types, comments, nullable, PK constraints, expectations per table
2. **Python config loader** (`src/pipelines/lib/table_config.py`) — reads YAML, generates schema DDL strings for `dp.create_streaming_table(schema=...)`
3. **SQL materialized views** (`src/pipelines/transformations/gold_and_quarantine.sql`) — `CREATE MATERIALIZED VIEW` with explicit column lists + `COMMENT` clauses (bypasses Python API limitation)

## Key Findings

- `dp.create_streaming_table(schema="DDL string")` accepts column comments AND PK constraints at creation time — unlike ALTER TABLE, this works on pipeline-managed tables
- SQL `CREATE MATERIALIZED VIEW (col TYPE COMMENT '...')` enables column comments on MVs — the Python `@dp.materialized_view` decorator cannot do this
- SCD2 tables must NOT have explicit schemas (AUTO CDC adds `__START_AT`, `__END_AT`, `__IS_DELETED` system columns)
- After `explode()` + multi-select patterns, Spark loses NOT NULL metadata — YAML must declare `nullable: true` for such columns
- SQL literal `60.0` in expressions produces `DECIMAL(27,6)`, not `DOUBLE` — explicit `CAST(... AS DOUBLE)` required

## Changes Made

### Pipeline Channel

- Changed `channel: current` → `channel: preview` in `resources/silver_healthkit.pipeline.yml`

### New Files Created

| File | Purpose |
| --- | --- |
| `fixtures/ddl/bronze_typed_health_samples.yml` | DDL fixture (15 columns, PK, 14 expectations) |
| `fixtures/ddl/bronze_typed_workouts.yml` | DDL fixture (17 columns, PK, 10 expectations) |
| `fixtures/ddl/bronze_typed_sleep_stages.yml` | DDL fixture (12 columns, PK, 8 expectations) |
| `fixtures/ddl/bronze_typed_activity_summaries.yml` | DDL fixture (14 columns, PK, 11 expectations) |
| `fixtures/ddl/bronze_typed_deletes.yml` | DDL fixture (6 columns, PK, 4 expectations) |
| `fixtures/ddl/silver_health_samples.yml` | DDL fixture (14 columns, PK) |
| `fixtures/ddl/silver_workouts.yml` | DDL fixture (16 columns, PK) |
| `fixtures/ddl/silver_sleep_stages.yml` | DDL fixture (11 columns, PK) |
| `fixtures/ddl/silver_activity_summaries.yml` | DDL fixture (13 columns, PK) |
| `fixtures/ddl/gold_sleep_sessions.yml` | DDL fixture (18 columns, documentation + SQL impl note) |
| `fixtures/ddl/README.md` | Directory documentation |
| `src/pipelines/lib/__init__.py` | Package init with re-exports |
| `src/pipelines/lib/table_config.py` | Config loader: `load_table_config`, `build_schema_ddl`, helpers |
| `src/pipelines/transformations/gold_and_quarantine.sql` | SQL MVs with column comments |

### Files Modified

| File | Changes |
| --- | --- |
| `src/pipelines/transformations/silver_healthkit.py` | Full refactor: all bronze_typed + silver tables now config-driven via `dp.create_streaming_table(schema=DDL)` + `@dp.append_flow()`. Gold/quarantine MVs removed (moved to SQL). |
| `src/pipelines/transformations/README.md` | Added “Config-Driven Architecture” section, updated Phase 6 to ✅ Complete, updated code examples |
| `resources/silver_healthkit.pipeline.yml` | Channel → preview, added `gold_and_quarantine.sql` as second library |
| `src/ops/silver-healthkit-constraints` (notebook) | Marked DEPRECATED in cell 1, explains new approach |

## Architecture Summary

```
fixtures/ddl/*.yml     ← Single source of truth for all table metadata
        │
        ├───────────────────────────────────────┐
        │                                       │
        ▼                                       ▼
lib/table_config.py                    gold_and_quarantine.sql
  load_table_config()                    CREATE MATERIALIZED VIEW
  build_schema_ddl()                     (col TYPE COMMENT '...')
        │                                       │
        ▼                                       ▼
silver_healthkit.py                    Pipeline engine
  dp.create_streaming_table(             reads SQL DDL directly
    schema=DDL,
    expect_all_or_drop=...,
    expect_all=...,
  )
  @dp.append_flow(target=...)
```

### What Gets Applied per Layer

| Metadata | Bronze Typed | Silver SCD1 | Silver SCD2 | Gold MV | Quarantine MV |
| --- | --- | --- | --- | --- | --- |
| Table comment | ✅ YAML | ✅ YAML | ✅ Hard-coded | ✅ SQL | ✅ SQL |
| Column comments | ✅ schema DDL | ✅ schema DDL | ❌ (no explicit schema) | ✅ SQL DDL | ✅ SQL DDL |
| PK constraint | ✅ schema DDL | ✅ schema DDL | ❌ | ❌ (MV) | ❌ (MV) |
| Expectations | ✅ expect_all dicts | ❌ (inherits) | ❌ | ❌ | ❌ |
| Table properties | ✅ YAML | ✅ YAML | ✅ Hard-coded | ✅ SQL | ✅ SQL |
| Cluster by | ✅ YAML | ✅ YAML | ✅ Hard-coded | ✅ SQL | ❌ |

## Errors Encountered & Resolved

1. **Schema mismatch: DOUBLE vs DECIMAL(27,6)** — `bronze_typed_sleep_stages.stage_duration_minutes`. SQL literal `/ 60.0` produces DECIMAL. Fixed with `CAST(... AS DOUBLE)` wrapper.
2. **Schema mismatch: NOT NULL vs nullable** — `bronze_typed_sleep_stages.ingested_at` and `stage_uuid`. After `explode()` + two-select, Spark loses NOT NULL metadata. Fixed by setting `nullable: true` in YAML.
3. **Silver nullable mismatch** — Preemptively fixed all silver YAML files to use `nullable: true` everywhere (CDC views produce nullable after `unionByName`).

## Verification

All 14 pipeline tables verified in Unity Catalog:
- ✅ Column comments on all bronze_typed tables (5)
- ✅ Column comments on all silver SCD1 tables (4) 
- ✅ Column comments on gold_sleep_sessions (18 columns)
- ✅ Column comments on quarantine_unmatched_deletes (8 columns)
- ✅ PK constraints on bronze_typed (5) + silver SCD1 (4) = 9 tables
- ✅ Pipeline update `fcad0b4a` completed successfully with all flows

## Remaining Limitations

- **FK constraints** — Cannot be applied because `wearables_zerobus` (the target table) has no declared PK on `record_id`. FK definitions remain in YAML as documentation.
- **SCD2 column comments** — Cannot define explicit schema (AUTO CDC system columns would conflict). Column docs in README only.
- **MV YAML ≠ runtime** — `gold_sleep_sessions.yml` is the canonical reference but the SQL file is the runtime implementation. Changes must be made in both places.
