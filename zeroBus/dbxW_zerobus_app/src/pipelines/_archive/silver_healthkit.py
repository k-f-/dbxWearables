# Databricks notebook source
# DBTITLE 1,Silver Layer — HealthKit Streaming Pipeline
# MAGIC %md
# MAGIC # Silver Layer — HealthKit Streaming Pipeline
# MAGIC
# MAGIC Continuous-mode Spark Declarative Pipeline that reads from the ZeroBus bronze
# MAGIC table (`wearables_zerobus`) and produces typed, deduplicated silver streaming
# MAGIC tables optimized for metric views and KPI computation.
# MAGIC
# MAGIC **Delta best practices applied:**
# MAGIC - Change Data Feed enabled (downstream incremental processing)
# MAGIC - Row Tracking enabled (row-level lineage)
# MAGIC - Variant Shredding enabled (where VARIANT columns exist)
# MAGIC - Liquid Clustering (per-user + time-series query patterns)
# MAGIC - Data quality expectations (expect / expect_or_drop)
# MAGIC
# MAGIC **PK/FK RELY constraints and column comments** are applied post-deploy
# MAGIC via the `src/ops/silver-healthkit-constraints` notebook.
# MAGIC
# MAGIC ## Data Quality Strategy
# MAGIC
# MAGIC | Level | Decorator | Use Case | Continuous-mode behavior |
# MAGIC |-------|-----------|----------|-------------------------|
# MAGIC | **Hard drop** | `expect_or_drop` | Null PKs, null required identifiers, completely useless rows | Row silently dropped, metric tracked |
# MAGIC | **Soft warn** | `expect` | Range checks, temporal consistency, completeness | Row kept, violation counted in DQ dashboard |
# MAGIC | **Never used** | `expect_or_fail` | N/A — would halt continuous pipeline | — |
# MAGIC
# MAGIC **Silver tables:**
# MAGIC | Table | Source record_type | Purpose | Cluster Keys |
# MAGIC |-------|-------------------|--------|-------------|
# MAGIC | `silver_health_samples` | `samples` | Typed health measurements (HR, steps, SpO2, etc.) | user_id, sample_type, sample_date |
# MAGIC | `silver_workouts` | `workouts` | Workout sessions with duration, distance, calories | user_id, activity_type, workout_date |
# MAGIC | `silver_sleep_sessions` | `sleep` | Sleep sessions with per-stage durations | user_id, sleep_date |
# MAGIC | `silver_activity_summaries` | `activity_summaries` | Daily activity ring data with goal attainment | user_id, activity_date |
# MAGIC | `silver_deletes` | `deletes` | Deletion records for downstream SCD handling | sample_type, deleted_uuid |

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
    IntegerType, LongType, ArrayType
)

# Bronze source table — read from pipeline configuration (set in resource YAML)
BRONZE_TABLE = spark.conf.get("bronze_table")

# COMMAND ----------

# DBTITLE 1,silver_health_samples — typed health measurements
@dlt.table(
    name="silver_health_samples",
    comment=(
        "Typed, deduplicated health samples from Apple HealthKit. "
        "Includes heart rate, step count, SpO2, HRV, respiratory rate, "
        "energy burned, flights climbed, and walking distance. "
        "Clustered for per-user time-series queries and live dashboards "
        "(e.g. brickster step counts)."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableVariantShredding": "true",
    },
    cluster_by=["user_id", "sample_type", "sample_date"],
)
# ---- Hard drops (row is useless without these) ----
@dlt.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
@dlt.expect_or_drop("valid_uuid", "uuid IS NOT NULL")
@dlt.expect_or_drop("valid_value", "value IS NOT NULL")
# ---- Soft expectations (tracked, row preserved) ----
@dlt.expect("valid_user", "user_id IS NOT NULL")
@dlt.expect("valid_timestamps", "start_ts IS NOT NULL AND end_ts IS NOT NULL")
@dlt.expect("chronological_timestamps", "start_ts <= end_ts")
@dlt.expect("valid_unit", "unit IS NOT NULL AND unit != ''")
@dlt.expect("known_sample_type", "sample_type LIKE 'HK%'")
@dlt.expect("non_negative_value", "value >= 0")
@dlt.expect(
    "reasonable_timestamp",
    "start_ts >= '2020-01-01' AND start_ts <= current_timestamp()"
)
@dlt.expect(
    "reasonable_heart_rate",
    "sample_type != 'HKQuantityTypeIdentifierHeartRate' OR (value BETWEEN 20 AND 300)"
)
@dlt.expect(
    "reasonable_spo2",
    "sample_type != 'HKQuantityTypeIdentifierOxygenSaturation' OR (value BETWEEN 50 AND 100)"
)
@dlt.expect(
    "reasonable_hrv",
    "sample_type != 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN' OR (value BETWEEN 0 AND 300)"
)
@dlt.expect(
    "reasonable_respiratory_rate",
    "sample_type != 'HKQuantityTypeIdentifierRespiratoryRate' OR (value BETWEEN 4 AND 60)"
)
def silver_health_samples():
    return (
        dlt.readStream(BRONZE_TABLE)
        .filter(F.col("record_type") == "samples")
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            # Extract typed fields from VARIANT body (use SQL :: cast syntax)
            F.expr("body:uuid::string").alias("uuid"),
            F.expr("body:type::string").alias("sample_type"),
            F.expr("body:value::double").alias("value"),
            F.expr("body:unit::string").alias("unit"),
            F.expr("body:start_date::timestamp").alias("start_ts"),
            F.expr("body:end_date::timestamp").alias("end_ts"),
            F.expr("body:source_name::string").alias("source_name"),
            F.expr("body:source_bundle_id::string").alias("source_bundle_id"),
            F.expr("body:metadata").alias("metadata"),
            # Derived columns for metric views and dashboards
            F.to_date(F.expr("body:start_date::timestamp")).alias("sample_date"),
            F.hour(F.expr("body:start_date::timestamp")).alias("sample_hour"),
        )
    )

# COMMAND ----------

# DBTITLE 1,silver_workouts — workout sessions
@dlt.table(
    name="silver_workouts",
    comment=(
        "Typed, deduplicated workout sessions from Apple HealthKit. "
        "Includes activity type, duration, distance, and energy burned. "
        "Clustered for per-user activity analysis and weekly summaries."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableVariantShredding": "true",
    },
    cluster_by=["user_id", "activity_type", "workout_date"],
)
# ---- Hard drops ----
@dlt.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
@dlt.expect_or_drop("valid_uuid", "uuid IS NOT NULL")
# ---- Soft expectations ----
@dlt.expect("valid_user", "user_id IS NOT NULL")
@dlt.expect("valid_duration", "duration_seconds > 0")
@dlt.expect("valid_activity_type", "activity_type IS NOT NULL AND activity_type != ''")
@dlt.expect("chronological_timestamps", "start_ts IS NOT NULL AND end_ts IS NOT NULL AND start_ts <= end_ts")
@dlt.expect(
    "reasonable_duration",
    "duration_seconds < 86400"  # No single workout > 24 hours
)
@dlt.expect(
    "non_negative_distance",
    "total_distance_meters IS NULL OR total_distance_meters >= 0"
)
@dlt.expect(
    "non_negative_energy",
    "total_energy_burned_kcal IS NULL OR total_energy_burned_kcal >= 0"
)
@dlt.expect(
    "reasonable_distance",
    "total_distance_meters IS NULL OR total_distance_meters < 500000"  # < 500km per workout
)
@dlt.expect(
    "reasonable_energy",
    "total_energy_burned_kcal IS NULL OR total_energy_burned_kcal < 10000"  # < 10k kcal
)
@dlt.expect(
    "reasonable_timestamp",
    "start_ts >= '2020-01-01' AND start_ts <= current_timestamp()"
)
def silver_workouts():
    return (
        dlt.readStream(BRONZE_TABLE)
        .filter(F.col("record_type") == "workouts")
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            # Extract typed fields from VARIANT body (use SQL :: cast syntax)
            F.expr("body:uuid::string").alias("uuid"),
            F.expr("body:activity_type::string").alias("activity_type"),
            F.expr("body:activity_type_raw::int").alias("activity_type_raw"),
            F.expr("body:duration_seconds::double").alias("duration_seconds"),
            F.expr("body:start_date::timestamp").alias("start_ts"),
            F.expr("body:end_date::timestamp").alias("end_ts"),
            F.expr("body:total_distance_meters::double").alias("total_distance_meters"),
            F.expr("body:total_energy_burned_kcal::double").alias("total_energy_burned_kcal"),
            F.expr("body:source_name::string").alias("source_name"),
            F.expr("body:metadata").alias("metadata"),
            # Derived columns for metric views
            F.to_date(F.expr("body:start_date::timestamp")).alias("workout_date"),
            (F.expr("body:duration_seconds::double") / 60.0).alias("duration_minutes"),
            (F.expr("body:total_distance_meters::double") / 1000.0).alias("distance_km"),
        )
    )

# COMMAND ----------

# DBTITLE 1,silver_sleep_sessions — sleep with stage durations
# Type alias for the stages array cast (VARIANT -> typed ARRAY)
STAGES_ARRAY_TYPE = "ARRAY<STRUCT<stage: STRING, start_date: STRING, end_date: STRING, uuid: STRING>>"

@dlt.table(
    name="silver_sleep_sessions",
    comment=(
        "Sleep sessions with per-stage duration breakdowns from Apple HealthKit. "
        "Stages include awake, core, deep, and REM. "
        "Clustered for per-user sleep quality trend analysis."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableVariantShredding": "true",
    },
    cluster_by=["user_id", "sleep_date"],
)
# ---- Hard drops ----
@dlt.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
# ---- Soft expectations ----
@dlt.expect("valid_user", "user_id IS NOT NULL")
@dlt.expect("valid_timestamps", "start_ts IS NOT NULL AND end_ts IS NOT NULL")
@dlt.expect("chronological_timestamps", "start_ts < end_ts")
@dlt.expect("positive_duration", "total_duration_minutes > 0")
@dlt.expect(
    "reasonable_sleep_duration",
    "total_duration_minutes BETWEEN 10 AND 1440"  # 10 min to 24 hours
)
@dlt.expect(
    "non_negative_deep",
    "deep_sleep_minutes >= 0"
)
@dlt.expect(
    "non_negative_rem",
    "rem_sleep_minutes >= 0"
)
@dlt.expect(
    "non_negative_core",
    "core_sleep_minutes >= 0"
)
@dlt.expect(
    "non_negative_awake",
    "awake_minutes >= 0"
)
@dlt.expect(
    "stages_within_total",
    "(deep_sleep_minutes + rem_sleep_minutes + core_sleep_minutes + awake_minutes) "
    "<= total_duration_minutes * 1.1"  # 10% tolerance for rounding
)
@dlt.expect(
    "reasonable_timestamp",
    "start_ts >= '2020-01-01' AND start_ts <= current_timestamp()"
)
def silver_sleep_sessions():
    stages_cast = f"CAST(body:stages AS {STAGES_ARRAY_TYPE})"
    return (
        dlt.readStream(BRONZE_TABLE)
        .filter(F.col("record_type") == "sleep")
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            # Session timestamps (VARIANT :: cast syntax)
            F.expr("body:start_date::timestamp").alias("start_ts"),
            F.expr("body:end_date::timestamp").alias("end_ts"),
            # Total duration in minutes
            F.expr(
                "(CAST(body:end_date::timestamp AS LONG) "
                "- CAST(body:start_date::timestamp AS LONG)) / 60.0"
            ).alias("total_duration_minutes"),
            # Stage durations via SQL aggregate on typed array
            F.expr(f"""
                aggregate(
                    filter({stages_cast}, s -> s.stage = 'asleepDeep'),
                    CAST(0.0 AS DOUBLE),
                    (acc, s) -> acc + (
                        CAST(CAST(s.end_date AS TIMESTAMP) AS LONG)
                        - CAST(CAST(s.start_date AS TIMESTAMP) AS LONG)
                    ) / 60.0
                )
            """).alias("deep_sleep_minutes"),
            F.expr(f"""
                aggregate(
                    filter({stages_cast}, s -> s.stage = 'asleepREM'),
                    CAST(0.0 AS DOUBLE),
                    (acc, s) -> acc + (
                        CAST(CAST(s.end_date AS TIMESTAMP) AS LONG)
                        - CAST(CAST(s.start_date AS TIMESTAMP) AS LONG)
                    ) / 60.0
                )
            """).alias("rem_sleep_minutes"),
            F.expr(f"""
                aggregate(
                    filter({stages_cast}, s -> s.stage = 'asleepCore'),
                    CAST(0.0 AS DOUBLE),
                    (acc, s) -> acc + (
                        CAST(CAST(s.end_date AS TIMESTAMP) AS LONG)
                        - CAST(CAST(s.start_date AS TIMESTAMP) AS LONG)
                    ) / 60.0
                )
            """).alias("core_sleep_minutes"),
            F.expr(f"""
                aggregate(
                    filter({stages_cast}, s -> s.stage = 'awake'),
                    CAST(0.0 AS DOUBLE),
                    (acc, s) -> acc + (
                        CAST(CAST(s.end_date AS TIMESTAMP) AS LONG)
                        - CAST(CAST(s.start_date AS TIMESTAMP) AS LONG)
                    ) / 60.0
                )
            """).alias("awake_minutes"),
            # Stages array preserved for detailed analysis (typed)
            F.expr(f"{stages_cast}").alias("stages"),
            # Derived columns for metric views
            F.to_date(F.expr("body:start_date::timestamp")).alias("sleep_date"),
            F.expr(f"size({stages_cast})").alias("num_stages"),
        )
    )

# COMMAND ----------

# DBTITLE 1,silver_activity_summaries — daily rings with goal attainment
@dlt.table(
    name="silver_activity_summaries",
    comment=(
        "Daily Apple Watch activity ring data with goal attainment percentages. "
        "One row per user per day. Supports ring closure rate metrics "
        "and weekly/monthly goal adherence KPIs."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["user_id", "activity_date"],
)
# ---- Hard drops ----
@dlt.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
# ---- Soft expectations ----
@dlt.expect("valid_user", "user_id IS NOT NULL")
@dlt.expect("valid_date", "activity_date IS NOT NULL")
@dlt.expect("valid_energy_goal", "energy_goal_kcal > 0")
@dlt.expect("valid_exercise_goal", "exercise_goal_minutes > 0")
@dlt.expect("valid_stand_goal", "stand_goal_hours > 0")
@dlt.expect("non_negative_energy", "energy_burned_kcal >= 0")
@dlt.expect(
    "reasonable_exercise",
    "exercise_minutes >= 0 AND exercise_minutes <= 1440"  # 0 to 24 hours
)
@dlt.expect(
    "reasonable_stand",
    "stand_hours >= 0 AND stand_hours <= 24"
)
@dlt.expect(
    "reasonable_energy_burned",
    "energy_burned_kcal < 10000"  # < 10k kcal in a single day
)
@dlt.expect(
    "reasonable_date",
    "activity_date >= '2020-01-01' AND activity_date <= current_date()"
)
@dlt.expect(
    "goal_attainment_finite",
    "energy_goal_pct IS NOT NULL AND NOT isnan(energy_goal_pct) "
    "AND exercise_goal_pct IS NOT NULL AND NOT isnan(exercise_goal_pct) "
    "AND stand_goal_pct IS NOT NULL AND NOT isnan(stand_goal_pct)"
)
def silver_activity_summaries():
    return (
        dlt.readStream(BRONZE_TABLE)
        .filter(F.col("record_type") == "activity_summaries")
        .select(
            F.col("record_id"),
            F.col("ingested_at"),
            F.col("user_id"),
            F.col("source_platform"),
            # Activity ring values (VARIANT :: cast syntax)
            F.expr("body:date::date").alias("activity_date"),
            F.expr("body:active_energy_burned_kcal::double").alias("energy_burned_kcal"),
            F.expr("body:active_energy_burned_goal_kcal::double").alias("energy_goal_kcal"),
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

# COMMAND ----------

# DBTITLE 1,silver_deletes — deletion records for downstream SCD
@dlt.table(
    name="silver_deletes",
    comment=(
        "HealthKit deletion records for downstream soft-delete propagation. "
        "Each row represents a sample UUID that was deleted on the source device. "
        "Join to silver_health_samples.uuid for SCD Type 2 handling."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["sample_type", "deleted_uuid"],
)
# ---- Hard drops ----
@dlt.expect_or_drop("valid_record_id", "record_id IS NOT NULL")
@dlt.expect_or_drop("valid_uuid", "deleted_uuid IS NOT NULL")
# ---- Soft expectations ----
@dlt.expect("valid_user", "user_id IS NOT NULL")
@dlt.expect("valid_sample_type", "sample_type IS NOT NULL AND sample_type != ''")
@dlt.expect("known_sample_type", "sample_type LIKE 'HK%'")
@dlt.expect(
    "uuid_format",
    "length(deleted_uuid) = 36 AND deleted_uuid RLIKE '^[0-9A-Fa-f-]{36}$'"
)
def silver_deletes():
    return (
        dlt.readStream(BRONZE_TABLE)
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