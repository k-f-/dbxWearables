# DDL Fixture Files

YAML files defining table metadata for the HealthKit SDP pipeline. Loaded at
pipeline runtime by `src/pipelines/lib/table_config.py`.

## What They Define

- **Table comment** — applied via `dp.create_streaming_table(comment=...)`
- **Table properties** — Delta features (CDF, row tracking, variant shredding)
- **Cluster by** — Liquid clustering columns
- **Columns** — name, type, nullable, comment (generates schema DDL string)
- **Constraints** — PK and FK (embedded in schema DDL)
- **Expectations** — `drop` (hard) and `warn` (soft) data quality rules

## How They're Used

```python
from lib.table_config import load_table_config, build_schema_ddl, ...

cfg = load_table_config("bronze_typed_health_samples")

dp.create_streaming_table(
    name="bronze_typed_health_samples",
    schema=build_schema_ddl(cfg),  # → "col1 TYPE COMMENT '...', ..., CONSTRAINT pk PRIMARY KEY (...)"
    expect_all_or_drop=get_expectations_drop(cfg),
    ...
)
```

## Naming Convention

Files match table names exactly: `{table_name}.yml`

## Limitations

- **Materialized views**: `gold_sleep_sessions.yml` is documentation-only for
  column comments (MVs infer schema from the query, no `schema` parameter)
- **SCD2 history tables**: No YAML files — AUTO CDC adds system columns
  (`__START_AT`, `__END_AT`, `__IS_DELETED`) that would conflict with a
  declared schema
- **Nullable**: After `explode()` or `unionByName()`, Spark loses NOT NULL
  metadata. YAML must declare `nullable: true` for such columns even if
  logically required (use expectations for enforcement instead)
