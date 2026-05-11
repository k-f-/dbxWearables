"""HealthKit Pipeline — Bronze Typed → Silver (CDC-Applied).

Continuous-mode Spark Declarative Pipeline that reads from the ZeroBus raw bronze
table (wearables_zerobus) and produces:

1. Bronze Typed Layer — VARIANT → typed column extraction, append-only audit trail.
2. CDC Views — Union typed records (INSERT) with matching deletes (DELETE).
3. Silver Layer — AUTO CDC applied (SCD Type 1 + SCD Type 2 for demo).
4. Gold Layer — Session-level aggregations from stage-level silver data.
5. Quarantine — Unmatched deletes captured for observability.

Delta best practices applied:
- Change Data Feed enabled (downstream incremental processing)
- Row Tracking enabled (row-level lineage)
- Variant Shredding enabled (where VARIANT columns exist)
- Liquid Clustering (per-user + time-series query patterns)
- Data quality expectations (expect / expect_or_drop)

Data Quality Strategy:
- Hard drop (expect_or_drop): Null PKs, null UUIDs (row can't participate in CDC)
- Soft warn (expect): Range checks, temporal consistency, completeness
- Never used (expect_or_fail): Would halt continuous pipeline

Architecture Note:
- Table metadata (comments, column comments, constraints, expectations) is defined
  in YAML fixture files under fixtures/ddl/ and loaded at runtime by the config
  loader (lib.table_config). This keeps DDL concerns separate from transformation
  logic and enables single-source-of-truth documentation.
- SCD2 history tables do NOT use explicit schemas — AUTO CDC adds __START_AT,
  __END_AT, __IS_DELETED system columns automatically.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from lib.table_config import (
    load_table_config,
    build_schema_ddl,
    get_table_comment,
    get_table_properties,
    get_cluster_by,
    get_expectations_drop,
    get_expectations_warn,
)


# Bronze source table — read from pipeline configuration (set in resource YAML)
BRONZE_TABLE = spark.conf.get("bronze_table")

# Type alias for the stages array cast (VARIANT -> typed ARRAY)
STAGES_ARRAY_TYPE = (
    "ARRAY<STRUCT<stage: STRING, start_date: STRING, end_date: STRING, uuid: STRING>>"
)


# #############################################################################
#
#   LAYER 1: BRONZE TYPED (append-only audit trail)
#
#   All bronze typed tables use config-driven definitions:
#   - Schema DDL (with column comments + PK constraints) from YAML
#   - Expectations (drop + warn) from YAML
#   - Table properties and cluster_by from YAML
#   - Transformation logic in @dp.append_flow functions
#
# #############################################################################


# =============================================================================
# bronze_typed_health_samples — typed health measurements (audit trail)
# =============================================================================

_cfg_health_samples = load_table_config("bronze_typed_health_samples")

dp.create_streaming_table(
    name="bronze_typed_health_samples",
    comment=get_table_comment(_cfg_health_samples),
    schema=build_schema_ddl(_cfg_health_samples),
    table_properties=get_table_properties(_cfg_health_samples),
    cluster_by=get_cluster_by(_cfg_health_samples),
    expect_all_or_drop=get_expectations_drop(_cfg_health_samples),
    expect_all=get_expectations_warn(_cfg_health_samples),
)


@dp.append_flow(target="bronze_typed_health_samples")
def bronze_typed_health_samples_flow():
    return (
        spark.readStream.table(BRONZE_TABLE)
        .filter(F.col("record_type") == "samples")
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            F.expr("body:uuid::string").alias("uuid"),
            F.expr("body:type::string").alias("sample_type"),
            F.expr("body:value::double").alias("value"),
            F.expr("body:unit::string").alias("unit"),
            F.expr("body:start_date::timestamp").alias("start_ts"),
            F.expr("body:end_date::timestamp").alias("end_ts"),
            F.expr("body:source_name::string").alias("source_name"),
            F.expr("body:source_bundle_id::string").alias("source_bundle_id"),
            F.expr("body:metadata").alias("metadata"),
            F.to_date(F.expr("body:start_date::timestamp")).alias("sample_date"),
            F.hour(F.expr("body:start_date::timestamp")).alias("sample_hour"),
        )
    )


# =============================================================================
# bronze_typed_workouts — typed workout sessions (audit trail)
# =============================================================================

_cfg_workouts = load_table_config("bronze_typed_workouts")

dp.create_streaming_table(
    name="bronze_typed_workouts",
    comment=get_table_comment(_cfg_workouts),
    schema=build_schema_ddl(_cfg_workouts),
    table_properties=get_table_properties(_cfg_workouts),
    cluster_by=get_cluster_by(_cfg_workouts),
    expect_all_or_drop=get_expectations_drop(_cfg_workouts),
    expect_all=get_expectations_warn(_cfg_workouts),
)


@dp.append_flow(target="bronze_typed_workouts")
def bronze_typed_workouts_flow():
    return (
        spark.readStream.table(BRONZE_TABLE)
        .filter(F.col("record_type") == "workouts")
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            F.expr("body:uuid::string").alias("uuid"),
            F.expr("body:activity_type::string").alias("activity_type"),
            F.expr("body:activity_type_raw::int").alias("activity_type_raw"),
            F.expr("body:duration_seconds::double").alias("duration_seconds"),
            F.expr("body:start_date::timestamp").alias("start_ts"),
            F.expr("body:end_date::timestamp").alias("end_ts"),
            F.expr("body:total_distance_meters::double").alias("total_distance_meters"),
            F.expr("body:total_energy_burned_kcal::double").alias(
                "total_energy_burned_kcal"
            ),
            F.expr("body:source_name::string").alias("source_name"),
            F.expr("body:metadata").alias("metadata"),
            F.to_date(F.expr("body:start_date::timestamp")).alias("workout_date"),
            (F.expr("body:duration_seconds::double") / 60.0).alias("duration_minutes"),
            (F.expr("body:total_distance_meters::double") / 1000.0).alias(
                "distance_km"
            ),
        )
    )


# =============================================================================
# bronze_typed_sleep_stages — exploded sleep stages (one row per stage)
# =============================================================================

_cfg_sleep_stages = load_table_config("bronze_typed_sleep_stages")

dp.create_streaming_table(
    name="bronze_typed_sleep_stages",
    comment=get_table_comment(_cfg_sleep_stages),
    schema=build_schema_ddl(_cfg_sleep_stages),
    table_properties=get_table_properties(_cfg_sleep_stages),
    cluster_by=get_cluster_by(_cfg_sleep_stages),
    expect_all_or_drop=get_expectations_drop(_cfg_sleep_stages),
    expect_all=get_expectations_warn(_cfg_sleep_stages),
)


@dp.append_flow(target="bronze_typed_sleep_stages")
def bronze_typed_sleep_stages_flow():
    stages_cast = f"CAST(body:stages AS {STAGES_ARRAY_TYPE})"
    return (
        spark.readStream.table(BRONZE_TABLE)
        .filter(F.col("record_type") == "sleep")
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            F.expr("body:start_date::timestamp").alias("session_start_ts"),
            F.expr("body:end_date::timestamp").alias("session_end_ts"),
            F.explode(F.expr(stages_cast)).alias("_stage"),
        )
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            F.col("_stage.uuid").alias("stage_uuid"),
            F.col("_stage.stage").alias("stage"),
            F.expr("CAST(_stage.start_date AS TIMESTAMP)").alias("stage_start_ts"),
            F.expr("CAST(_stage.end_date AS TIMESTAMP)").alias("stage_end_ts"),
            F.expr(
                "CAST((CAST(CAST(_stage.end_date AS TIMESTAMP) AS LONG) "
                "- CAST(CAST(_stage.start_date AS TIMESTAMP) AS LONG)) / 60.0 AS DOUBLE)"
            ).alias("stage_duration_minutes"),
            F.col("session_start_ts"),
            F.col("session_end_ts"),
            F.to_date(F.col("session_start_ts")).alias("sleep_date"),
        )
    )


# =============================================================================
# bronze_typed_activity_summaries — daily rings (audit trail)
# =============================================================================

_cfg_activity_summaries = load_table_config("bronze_typed_activity_summaries")

dp.create_streaming_table(
    name="bronze_typed_activity_summaries",
    comment=get_table_comment(_cfg_activity_summaries),
    schema=build_schema_ddl(_cfg_activity_summaries),
    table_properties=get_table_properties(_cfg_activity_summaries),
    cluster_by=get_cluster_by(_cfg_activity_summaries),
    expect_all_or_drop=get_expectations_drop(_cfg_activity_summaries),
    expect_all=get_expectations_warn(_cfg_activity_summaries),
)


@dp.append_flow(target="bronze_typed_activity_summaries")
def bronze_typed_activity_summaries_flow():
    return (
        spark.readStream.table(BRONZE_TABLE)
        .filter(F.col("record_type") == "activity_summaries")
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            # Activity ring values
            F.expr("body:date::date").alias("activity_date"),
            F.expr("body:active_energy_burned_kcal::double").alias(
                "energy_burned_kcal"
            ),
            F.expr("body:active_energy_burned_goal_kcal::double").alias(
                "energy_goal_kcal"
            ),
            F.expr("body:exercise_minutes::int").alias("exercise_minutes"),
            F.expr("body:exercise_minutes_goal::int").alias("exercise_goal_minutes"),
            F.expr("body:stand_hours::int").alias("stand_hours"),
            F.expr("body:stand_hours_goal::int").alias("stand_goal_hours"),
            # Goal attainment ratios (for metric views)
            F.expr(
                "body:active_energy_burned_kcal::double "
                "/ body:active_energy_burned_goal_kcal::double"
            ).alias("energy_goal_pct"),
            F.expr(
                "CAST(body:exercise_minutes::int AS DOUBLE) "
                "/ CAST(body:exercise_minutes_goal::int AS DOUBLE)"
            ).alias("exercise_goal_pct"),
            F.expr(
                "CAST(body:stand_hours::int AS DOUBLE) "
                "/ CAST(body:stand_hours_goal::int AS DOUBLE)"
            ).alias("stand_goal_pct"),
        )
    )


# =============================================================================
# bronze_typed_deletes — deletion event records (audit trail)
# =============================================================================

_cfg_deletes = load_table_config("bronze_typed_deletes")

dp.create_streaming_table(
    name="bronze_typed_deletes",
    comment=get_table_comment(_cfg_deletes),
    schema=build_schema_ddl(_cfg_deletes),
    table_properties=get_table_properties(_cfg_deletes),
    cluster_by=get_cluster_by(_cfg_deletes),
    expect_all_or_drop=get_expectations_drop(_cfg_deletes),
    expect_all=get_expectations_warn(_cfg_deletes),
)


@dp.append_flow(target="bronze_typed_deletes")
def bronze_typed_deletes_flow():
    return (
        spark.readStream.table(BRONZE_TABLE)
        .filter(F.col("record_type") == "deletes")
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            F.expr("body:uuid::string").alias("deleted_uuid"),
            F.expr("body:sample_type::string").alias("sample_type"),
        )
    )


# #############################################################################
#
#   LAYER 2: CDC VIEWS (union typed records + matching deletes)
#
#   Each view produces a unified change feed with an `operation` column:
#   - INSERT rows come from the bronze_typed table (all columns populated)
#   - DELETE rows come from bronze_typed_deletes (only key + ingested_at + operation)
#
#   These views feed into dp.create_auto_cdc_flow() at the silver layer.
#
# #############################################################################


@dp.view(name="cdc_health_samples_v")
def cdc_health_samples_v():
    """CDC feed for health samples: unions sample inserts with HKQuantityType deletes."""
    inserts = (
        spark.readStream.table("bronze_typed_health_samples")
        .withColumn("operation", F.lit("INSERT"))
    )
    deletes = (
        spark.readStream.table("bronze_typed_deletes")
        .filter(
            F.col("sample_type").startswith("HKQuantityType")
            | (F.col("sample_type") == "HKQuantityTypeIdentifierVO2Max")
        )
        .select(
            F.col("deleted_uuid").alias("uuid"),
            F.col("ingested_at"),
            F.lit("DELETE").alias("operation"),
        )
    )
    return inserts.unionByName(deletes, allowMissingColumns=True)


@dp.view(name="cdc_workouts_v")
def cdc_workouts_v():
    """CDC feed for workouts: unions workout inserts with HKWorkoutTypeIdentifier deletes."""
    inserts = (
        spark.readStream.table("bronze_typed_workouts")
        .withColumn("operation", F.lit("INSERT"))
    )
    deletes = (
        spark.readStream.table("bronze_typed_deletes")
        .filter(F.col("sample_type") == "HKWorkoutTypeIdentifier")
        .select(
            F.col("deleted_uuid").alias("uuid"),
            F.col("ingested_at"),
            F.lit("DELETE").alias("operation"),
        )
    )
    return inserts.unionByName(deletes, allowMissingColumns=True)


@dp.view(name="cdc_sleep_stages_v")
def cdc_sleep_stages_v():
    """CDC feed for sleep stages: unions stage inserts with HKCategoryType sleep deletes."""
    inserts = (
        spark.readStream.table("bronze_typed_sleep_stages")
        .withColumn("operation", F.lit("INSERT"))
    )
    deletes = (
        spark.readStream.table("bronze_typed_deletes")
        .filter(F.col("sample_type") == "HKCategoryTypeIdentifierSleepAnalysis")
        .select(
            F.col("deleted_uuid").alias("stage_uuid"),
            F.col("ingested_at"),
            F.lit("DELETE").alias("operation"),
        )
    )
    return inserts.unionByName(deletes, allowMissingColumns=True)


# #############################################################################
#
#   LAYER 3: SILVER (AUTO CDC applied — SCD Type 1 + Type 2)
#
#   SCD1 tables use config-driven schema DDL (column comments + PK constraint).
#   SCD2 history tables do NOT use explicit schemas — AUTO CDC adds system
#   columns (__START_AT, __END_AT, __IS_DELETED) automatically.
#
#   Activity summaries have no deletes — simple streaming passthrough with
#   config-driven schema.
#
# #############################################################################


# =============================================================================
# silver_health_samples — SCD Type 1 (current state, deletes applied)
# =============================================================================

_cfg_silver_health_samples = load_table_config("silver_health_samples")

dp.create_streaming_table(
    name="silver_health_samples",
    comment=get_table_comment(_cfg_silver_health_samples),
    schema=build_schema_ddl(_cfg_silver_health_samples),
    table_properties=get_table_properties(_cfg_silver_health_samples),
    cluster_by=get_cluster_by(_cfg_silver_health_samples),
)

dp.create_auto_cdc_flow(
    target="silver_health_samples",
    source="cdc_health_samples_v",
    keys=["uuid"],
    sequence_by=F.col("ingested_at"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "record_id"],
    stored_as_scd_type=1,
)


# =============================================================================
# silver_health_samples_history — SCD Type 2 (full change history)
# No explicit schema — AUTO CDC adds __START_AT, __END_AT, __IS_DELETED
# =============================================================================

dp.create_streaming_table(
    name="silver_health_samples_history",
    comment=(
        "Full change history for health samples (SCD Type 2). "
        "Includes __START_AT, __END_AT, and __IS_DELETED columns for temporal queries. "
        "Shows when records were active and when they were deleted."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "sample_type", "sample_date"],
)

dp.create_auto_cdc_flow(
    target="silver_health_samples_history",
    source="cdc_health_samples_v",
    keys=["uuid"],
    sequence_by=F.col("ingested_at"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "record_id", "metadata"],
    stored_as_scd_type=2,
)


# =============================================================================
# silver_workouts — SCD Type 1 (current state, deletes applied)
# =============================================================================

_cfg_silver_workouts = load_table_config("silver_workouts")

dp.create_streaming_table(
    name="silver_workouts",
    comment=get_table_comment(_cfg_silver_workouts),
    schema=build_schema_ddl(_cfg_silver_workouts),
    table_properties=get_table_properties(_cfg_silver_workouts),
    cluster_by=get_cluster_by(_cfg_silver_workouts),
)

dp.create_auto_cdc_flow(
    target="silver_workouts",
    source="cdc_workouts_v",
    keys=["uuid"],
    sequence_by=F.col("ingested_at"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "record_id"],
    stored_as_scd_type=1,
)


# =============================================================================
# silver_workouts_history — SCD Type 2 (full change history)
# No explicit schema — AUTO CDC adds __START_AT, __END_AT, __IS_DELETED
# =============================================================================

dp.create_streaming_table(
    name="silver_workouts_history",
    comment=(
        "Full change history for workouts (SCD Type 2). "
        "Tracks when workouts were active and when they were deleted."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "activity_type", "workout_date"],
)

dp.create_auto_cdc_flow(
    target="silver_workouts_history",
    source="cdc_workouts_v",
    keys=["uuid"],
    sequence_by=F.col("ingested_at"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "record_id", "metadata"],
    stored_as_scd_type=2,
)


# =============================================================================
# silver_sleep_stages — SCD Type 1 (current state, deletes applied)
# =============================================================================

_cfg_silver_sleep_stages = load_table_config("silver_sleep_stages")

dp.create_streaming_table(
    name="silver_sleep_stages",
    comment=get_table_comment(_cfg_silver_sleep_stages),
    schema=build_schema_ddl(_cfg_silver_sleep_stages),
    table_properties=get_table_properties(_cfg_silver_sleep_stages),
    cluster_by=get_cluster_by(_cfg_silver_sleep_stages),
)

dp.create_auto_cdc_flow(
    target="silver_sleep_stages",
    source="cdc_sleep_stages_v",
    keys=["stage_uuid"],
    sequence_by=F.col("ingested_at"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "record_id"],
    stored_as_scd_type=1,
)


# =============================================================================
# silver_sleep_stages_history — SCD Type 2 (full change history)
# No explicit schema — AUTO CDC adds __START_AT, __END_AT, __IS_DELETED
# =============================================================================

dp.create_streaming_table(
    name="silver_sleep_stages_history",
    comment=(
        "Full change history for sleep stages (SCD Type 2). "
        "Tracks when individual sleep stages were active and when deleted."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "sleep_date"],
)

dp.create_auto_cdc_flow(
    target="silver_sleep_stages_history",
    source="cdc_sleep_stages_v",
    keys=["stage_uuid"],
    sequence_by=F.col("ingested_at"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "record_id"],
    stored_as_scd_type=2,
)


# =============================================================================
# silver_activity_summaries — streaming passthrough (no deletes exist)
# =============================================================================

_cfg_silver_activity_summaries = load_table_config("silver_activity_summaries")

dp.create_streaming_table(
    name="silver_activity_summaries",
    comment=get_table_comment(_cfg_silver_activity_summaries),
    schema=build_schema_ddl(_cfg_silver_activity_summaries),
    table_properties=get_table_properties(_cfg_silver_activity_summaries),
    cluster_by=get_cluster_by(_cfg_silver_activity_summaries),
)


@dp.append_flow(target="silver_activity_summaries")
def silver_activity_summaries_flow():
    return (
        spark.readStream.table("bronze_typed_activity_summaries")
        .drop("record_id")
    )


# #############################################################################
#
#   LAYER 4: GOLD (session-level aggregations)
#
#   Reconstructs sleep sessions from stage-level silver data.
#   Reads from SCD1 tables (current state with deletes applied) so
#   aggregations reflect the true current picture.
#
#   Note: Materialized views do not support explicit schema DDL —
#   column comments cannot be applied via this mechanism. Table comment
#   and properties are set directly.
#
# #############################################################################


# =============================================================================
# gold_sleep_sessions — session-level sleep aggregation
# =============================================================================

_cfg_gold = load_table_config("gold_sleep_sessions")


@dp.materialized_view(
    name="gold_sleep_sessions",
    comment=get_table_comment(_cfg_gold),
    table_properties=get_table_properties(_cfg_gold),
    cluster_by=get_cluster_by(_cfg_gold),
)
def gold_sleep_sessions():
    return (
        spark.read.table("silver_sleep_stages")
        .groupBy("user_id", "source_platform", "session_start_ts", "session_end_ts", "sleep_date")
        .agg(
            # Per-stage durations
            F.sum(
                F.when(F.col("stage") == "asleepDeep", F.col("stage_duration_minutes"))
            ).alias("deep_sleep_minutes"),
            F.sum(
                F.when(F.col("stage") == "asleepREM", F.col("stage_duration_minutes"))
            ).alias("rem_sleep_minutes"),
            F.sum(
                F.when(F.col("stage") == "asleepCore", F.col("stage_duration_minutes"))
            ).alias("core_sleep_minutes"),
            F.sum(
                F.when(F.col("stage") == "awake", F.col("stage_duration_minutes"))
            ).alias("awake_minutes"),
            # Totals
            F.sum("stage_duration_minutes").alias("total_tracked_minutes"),
            F.count("*").alias("stage_count"),
            # Per-stage counts
            F.count(F.when(F.col("stage") == "asleepDeep", True)).alias("deep_stage_count"),
            F.count(F.when(F.col("stage") == "asleepREM", True)).alias("rem_stage_count"),
            F.count(F.when(F.col("stage") == "asleepCore", True)).alias("core_stage_count"),
            F.count(F.when(F.col("stage") == "awake", True)).alias("awake_stage_count"),
        )
        .withColumn(
            "session_duration_minutes",
            (F.col("session_end_ts").cast("long") - F.col("session_start_ts").cast("long")) / 60.0,
        )
        .withColumn(
            "total_sleep_minutes",
            F.coalesce(F.col("deep_sleep_minutes"), F.lit(0))
            + F.coalesce(F.col("rem_sleep_minutes"), F.lit(0))
            + F.coalesce(F.col("core_sleep_minutes"), F.lit(0)),
        )
        .withColumn(
            "sleep_efficiency",
            F.when(
                F.col("session_duration_minutes") > 0,
                F.col("total_sleep_minutes") / F.col("session_duration_minutes"),
            ),
        )
    )


# #############################################################################
#
#   QUARANTINE: Unmatched deletes (observability)
#
#   Captures delete events whose UUID has no matching record in the target
#   bronze typed table. Expected to be ~99% of sample deletes in dev
#   (historical orphans from HealthKit anchored queries).
#
# #############################################################################


@dp.materialized_view(
    name="quarantine_unmatched_deletes",
    comment=(
        "Delete events with no matching UUID in the target table. "
        "In dev, ~99% of sample deletes are orphans (historical). "
        "In production, non-zero counts may indicate sync ordering issues."
    ),
    table_properties={
        "quality": "quarantine",
    },
)
def quarantine_unmatched_deletes():
    deletes = spark.read.table("bronze_typed_deletes")
    sample_uuids = spark.read.table("bronze_typed_health_samples").select(
        F.col("uuid").alias("existing_uuid")
    )
    workout_uuids = spark.read.table("bronze_typed_workouts").select(
        F.col("uuid").alias("existing_uuid")
    )
    stage_uuids = spark.read.table("bronze_typed_sleep_stages").select(
        F.col("stage_uuid").alias("existing_uuid")
    )
    all_uuids = sample_uuids.union(workout_uuids).union(stage_uuids)

    return (
        deletes.join(
            all_uuids,
            deletes.deleted_uuid == all_uuids.existing_uuid,
            "left_anti",
        )
        .withColumn(
            "target_table",
            F.when(
                F.col("sample_type").startswith("HKQuantityType"),
                F.lit("silver_health_samples"),
            )
            .when(
                F.col("sample_type") == "HKWorkoutTypeIdentifier",
                F.lit("silver_workouts"),
            )
            .when(
                F.col("sample_type") == "HKCategoryTypeIdentifierSleepAnalysis",
                F.lit("silver_sleep_stages"),
            )
            .otherwise(F.lit("unknown")),
        )
        .withColumn("reason", F.lit("no matching uuid in target"))
    )
