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
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


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
# #############################################################################


# =============================================================================
# bronze_typed_health_samples — typed health measurements (audit trail)
# =============================================================================


@dp.table(
    name="bronze_typed_health_samples",
    comment=(
        "Typed health samples extracted from VARIANT bronze data. "
        "Append-only audit trail — no deletes applied at this layer. "
        "Includes heart rate, step count, SpO2, HRV, respiratory rate, "
        "energy burned, flights climbed, and walking distance. "
        "CDC key: uuid."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableVariantShredding": "true",
    },
    cluster_by=["user_id", "sample_type", "sample_date"],
)
# ---- Hard drops (row can't participate in CDC without these) ----
@dp.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
@dp.expect_or_drop("valid_uuid", "uuid IS NOT NULL")
@dp.expect_or_drop("valid_value", "value IS NOT NULL")
# ---- Soft expectations (tracked, row preserved) ----
@dp.expect("valid_user", "user_id IS NOT NULL")
@dp.expect("valid_timestamps", "start_ts IS NOT NULL AND end_ts IS NOT NULL")
@dp.expect("chronological_timestamps", "start_ts <= end_ts")
@dp.expect("valid_unit", "unit IS NOT NULL AND unit != ''")
@dp.expect("known_sample_type", "sample_type LIKE 'HK%'")
@dp.expect("non_negative_value", "value >= 0")
@dp.expect(
    "reasonable_timestamp",
    "start_ts >= '2020-01-01' AND start_ts <= current_timestamp()",
)
@dp.expect(
    "reasonable_heart_rate",
    "sample_type != 'HKQuantityTypeIdentifierHeartRate' OR (value BETWEEN 20 AND 300)",
)
@dp.expect(
    "reasonable_spo2",
    "sample_type != 'HKQuantityTypeIdentifierOxygenSaturation' OR (value BETWEEN 50 AND 100)",
)
@dp.expect(
    "reasonable_hrv",
    "sample_type != 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN' OR (value BETWEEN 0 AND 300)",
)
@dp.expect(
    "reasonable_respiratory_rate",
    "sample_type != 'HKQuantityTypeIdentifierRespiratoryRate' OR (value BETWEEN 4 AND 60)",
)
def bronze_typed_health_samples():
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


@dp.table(
    name="bronze_typed_workouts",
    comment=(
        "Typed workout sessions extracted from VARIANT bronze data. "
        "Append-only audit trail — no deletes applied at this layer. "
        "Includes activity type, duration, distance, and energy burned. "
        "CDC key: uuid."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableVariantShredding": "true",
    },
    cluster_by=["user_id", "activity_type", "workout_date"],
)
# ---- Hard drops ----
@dp.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
@dp.expect_or_drop("valid_uuid", "uuid IS NOT NULL")
# ---- Soft expectations ----
@dp.expect("valid_user", "user_id IS NOT NULL")
@dp.expect("valid_duration", "duration_seconds > 0")
@dp.expect(
    "valid_activity_type", "activity_type IS NOT NULL AND activity_type != ''"
)
@dp.expect(
    "chronological_timestamps",
    "start_ts IS NOT NULL AND end_ts IS NOT NULL AND start_ts <= end_ts",
)
@dp.expect("reasonable_duration", "duration_seconds < 86400")
@dp.expect(
    "non_negative_distance",
    "total_distance_meters IS NULL OR total_distance_meters >= 0",
)
@dp.expect(
    "non_negative_energy",
    "total_energy_burned_kcal IS NULL OR total_energy_burned_kcal >= 0",
)
@dp.expect(
    "reasonable_distance",
    "total_distance_meters IS NULL OR total_distance_meters < 500000",
)
@dp.expect(
    "reasonable_energy",
    "total_energy_burned_kcal IS NULL OR total_energy_burned_kcal < 10000",
)
@dp.expect(
    "reasonable_timestamp",
    "start_ts >= '2020-01-01' AND start_ts <= current_timestamp()",
)
def bronze_typed_workouts():
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


@dp.table(
    name="bronze_typed_sleep_stages",
    comment=(
        "Individual sleep stages exploded from session-level VARIANT data. "
        "One row per stage (asleepDeep, asleepREM, asleepCore, awake). "
        "Append-only audit trail — stage-level deletes applied at silver via AUTO CDC. "
        "CDC key: stage_uuid. Session reconstruction happens at gold layer."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "sleep_date"],
)
# ---- Hard drops (row can't participate in CDC without stage_uuid) ----
@dp.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
@dp.expect_or_drop("valid_stage_uuid", "stage_uuid IS NOT NULL")
# ---- Soft expectations ----
@dp.expect("valid_user", "user_id IS NOT NULL")
@dp.expect(
    "valid_stage_type",
    "stage IN ('asleepDeep', 'asleepREM', 'asleepCore', 'awake')",
)
@dp.expect(
    "valid_stage_timestamps",
    "stage_start_ts IS NOT NULL AND stage_end_ts IS NOT NULL",
)
@dp.expect("chronological_stage", "stage_start_ts < stage_end_ts")
@dp.expect("positive_duration", "stage_duration_minutes > 0")
@dp.expect(
    "reasonable_stage_duration", "stage_duration_minutes <= 720"
)
@dp.expect(
    "reasonable_timestamp",
    "stage_start_ts >= '2020-01-01' AND stage_start_ts <= current_timestamp()",
)
@dp.expect(
    "stage_within_session",
    "stage_start_ts >= session_start_ts AND stage_end_ts <= session_end_ts",
)
def bronze_typed_sleep_stages():
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
                "(CAST(CAST(_stage.end_date AS TIMESTAMP) AS LONG) "
                "- CAST(CAST(_stage.start_date AS TIMESTAMP) AS LONG)) / 60.0"
            ).alias("stage_duration_minutes"),
            F.col("session_start_ts"),
            F.col("session_end_ts"),
            F.to_date(F.col("session_start_ts")).alias("sleep_date"),
        )
    )


# =============================================================================
# bronze_typed_activity_summaries — daily rings (audit trail)
# =============================================================================


@dp.table(
    name="bronze_typed_activity_summaries",
    comment=(
        "Daily Apple Watch activity ring data with goal attainment percentages. "
        "One row per user per day. Append-only — no deletes exist for this type. "
        "Passes through to silver unchanged (no CDC needed)."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "activity_date"],
)
# ---- Hard drops ----
@dp.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
# ---- Soft expectations ----
@dp.expect("valid_user", "user_id IS NOT NULL")
@dp.expect("valid_date", "activity_date IS NOT NULL")
@dp.expect("valid_energy_goal", "energy_goal_kcal > 0")
@dp.expect("valid_exercise_goal", "exercise_goal_minutes > 0")
@dp.expect("valid_stand_goal", "stand_goal_hours > 0")
@dp.expect("non_negative_energy", "energy_burned_kcal >= 0")
@dp.expect(
    "reasonable_exercise", "exercise_minutes >= 0 AND exercise_minutes <= 1440"
)
@dp.expect("reasonable_stand", "stand_hours >= 0 AND stand_hours <= 24")
@dp.expect("reasonable_energy_burned", "energy_burned_kcal < 10000")
@dp.expect(
    "reasonable_date",
    "activity_date >= '2020-01-01' AND activity_date <= current_date()",
)
@dp.expect(
    "goal_attainment_finite",
    "energy_goal_pct IS NOT NULL AND NOT isnan(energy_goal_pct) "
    "AND exercise_goal_pct IS NOT NULL AND NOT isnan(exercise_goal_pct) "
    "AND stand_goal_pct IS NOT NULL AND NOT isnan(stand_goal_pct)",
)
def bronze_typed_activity_summaries():
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


@dp.table(
    name="bronze_typed_deletes",
    comment=(
        "HealthKit deletion event records. Each row is a delete event targeting "
        "a specific UUID + sample_type. Used by CDC views to feed AUTO CDC at silver. "
        "sample_type determines target table: HKQuantityType* -> health_samples, "
        "HKWorkoutTypeIdentifier -> workouts, HKCategoryTypeIdentifierSleepAnalysis -> sleep_stages."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["sample_type", "deleted_uuid"],
)
# ---- Hard drops ----
@dp.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
@dp.expect_or_drop("valid_uuid", "deleted_uuid IS NOT NULL")
# ---- Soft expectations ----
@dp.expect("valid_user", "user_id IS NOT NULL")
@dp.expect("valid_sample_type", "sample_type IS NOT NULL AND sample_type != ''")
@dp.expect("known_sample_type", "sample_type LIKE 'HK%'")
@dp.expect(
    "uuid_format",
    "length(deleted_uuid) = 36 AND deleted_uuid RLIKE '^[0-9A-Fa-f-]{36}$'",
)
def bronze_typed_deletes():
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
#   Each entity gets two target tables:
#   - SCD Type 1: current state only, deletes physically remove rows
#   - SCD Type 2: full history with __START_AT, __END_AT, __IS_DELETED
#
#   Activity summaries have no deletes — simple streaming passthrough.
#
# #############################################################################


# =============================================================================
# silver_health_samples — SCD Type 1 (current state, deletes applied)
# =============================================================================

dp.create_streaming_table(
    name="silver_health_samples",
    comment=(
        "Current-state health samples with deletes physically applied (SCD Type 1). "
        "Records deleted on the source device are removed from this table. "
        "Optimized for dashboards and metric views."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "sample_type", "sample_date"],
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

dp.create_streaming_table(
    name="silver_workouts",
    comment=(
        "Current-state workouts with deletes physically applied (SCD Type 1). "
        "Deleted workouts are removed. Optimized for activity dashboards."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "activity_type", "workout_date"],
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

dp.create_streaming_table(
    name="silver_sleep_stages",
    comment=(
        "Current-state sleep stages with deletes applied (SCD Type 1). "
        "Deleted stages are removed. Use gold_sleep_sessions for session aggregation."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "sleep_date"],
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


@dp.table(
    name="silver_activity_summaries",
    comment=(
        "Activity summaries passed through from bronze (no deletes exist for this type). "
        "One row per user per day with ring values and goal attainment."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "activity_date"],
)
def silver_activity_summaries():
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
# #############################################################################


# =============================================================================
# gold_sleep_sessions — session-level sleep aggregation
# =============================================================================


@dp.materialized_view(
    name="gold_sleep_sessions",
    comment=(
        "Sleep sessions reconstructed from stage-level silver data. "
        "One row per user per session with per-stage duration breakdown, "
        "stage counts, and sleep efficiency. Reads from silver_sleep_stages (SCD1) "
        "so deleted stages are excluded from aggregations."
    ),
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
    cluster_by=["user_id", "sleep_date"],
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
