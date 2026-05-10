"""Bronze Typed Layer — HealthKit Streaming Pipeline.

Continuous-mode Spark Declarative Pipeline that reads from the ZeroBus raw bronze
table (wearables_zerobus) and produces typed, structured streaming tables that
serve as the append-only audit trail for downstream CDC processing.

This layer performs VARIANT → typed column extraction only. No records are ever
removed. Delete propagation happens at the silver layer via AUTO CDC.

Delta best practices applied:
- Change Data Feed enabled (downstream CDC consumption)
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
        "sample_type determines target table: HKQuantityType* → health_samples, "
        "HKWorkoutTypeIdentifier → workouts, HKCategoryTypeIdentifierSleepAnalysis → sleep_stages."
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
