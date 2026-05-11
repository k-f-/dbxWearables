-- =============================================================================
-- Gold & Quarantine Layer (SQL Materialized Views)
--
-- Defined in SQL to leverage explicit column lists with COMMENT clauses —
-- the Python @dp.materialized_view decorator does not support schema DDL.
--
-- These MVs read from pipeline-internal tables (silver_sleep_stages,
-- bronze_typed_*) and produce session-level aggregations and quarantine
-- observability data.
-- =============================================================================


-- =============================================================================
-- gold_sleep_sessions — session-level sleep aggregation
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_sleep_sessions (
  user_id           STRING    COMMENT 'Authenticated user ID from JWT claims',
  source_platform   STRING    COMMENT 'Data source platform identifier',
  session_start_ts  TIMESTAMP COMMENT 'Sleep session start (UTC, from HealthKit session boundary)',
  session_end_ts    TIMESTAMP COMMENT 'Sleep session end (UTC, from HealthKit session boundary)',
  sleep_date        DATE      COMMENT 'Date of session onset for liquid clustering',
  deep_sleep_minutes    DOUBLE COMMENT 'Total minutes in deep (slow-wave) sleep stage',
  rem_sleep_minutes     DOUBLE COMMENT 'Total minutes in REM sleep stage',
  core_sleep_minutes    DOUBLE COMMENT 'Total minutes in core (light) sleep stage',
  awake_minutes         DOUBLE COMMENT 'Total minutes spent awake within session window',
  total_tracked_minutes DOUBLE COMMENT 'Sum of all stage durations (deep + REM + core + awake)',
  stage_count           BIGINT COMMENT 'Total number of stages in session',
  deep_stage_count      BIGINT COMMENT 'Number of deep sleep stage transitions',
  rem_stage_count       BIGINT COMMENT 'Number of REM stage transitions',
  core_stage_count      BIGINT COMMENT 'Number of core sleep stage transitions',
  awake_stage_count     BIGINT COMMENT 'Number of awake stage transitions',
  session_duration_minutes DOUBLE COMMENT 'Wall-clock time: session_end_ts - session_start_ts in minutes',
  total_sleep_minutes   DOUBLE COMMENT 'Derived: deep + REM + core (excludes awake)',
  sleep_efficiency      DOUBLE COMMENT 'Derived: total_sleep_minutes / session_duration_minutes'
)
COMMENT 'Sleep sessions reconstructed from stage-level silver data. One row per user per session with per-stage duration breakdown, stage counts, and sleep efficiency. Reads from silver_sleep_stages (SCD1) so deleted stages are excluded from aggregations.'
TBLPROPERTIES (
  'quality' = 'gold',
  'delta.enableChangeDataFeed' = 'true'
)
CLUSTER BY (user_id, sleep_date)
AS
SELECT
  user_id,
  source_platform,
  session_start_ts,
  session_end_ts,
  sleep_date,
  -- Per-stage durations
  SUM(CASE WHEN stage = 'asleepDeep' THEN stage_duration_minutes END) AS deep_sleep_minutes,
  SUM(CASE WHEN stage = 'asleepREM'  THEN stage_duration_minutes END) AS rem_sleep_minutes,
  SUM(CASE WHEN stage = 'asleepCore' THEN stage_duration_minutes END) AS core_sleep_minutes,
  SUM(CASE WHEN stage = 'awake'      THEN stage_duration_minutes END) AS awake_minutes,
  -- Totals
  SUM(stage_duration_minutes) AS total_tracked_minutes,
  COUNT(*) AS stage_count,
  -- Per-stage counts
  COUNT(CASE WHEN stage = 'asleepDeep' THEN 1 END) AS deep_stage_count,
  COUNT(CASE WHEN stage = 'asleepREM'  THEN 1 END) AS rem_stage_count,
  COUNT(CASE WHEN stage = 'asleepCore' THEN 1 END) AS core_stage_count,
  COUNT(CASE WHEN stage = 'awake'      THEN 1 END) AS awake_stage_count,
  -- Derived metrics
  CAST(
    (CAST(session_end_ts AS LONG) - CAST(session_start_ts AS LONG)) / 60.0
    AS DOUBLE
  ) AS session_duration_minutes,
  CAST(
    COALESCE(SUM(CASE WHEN stage = 'asleepDeep' THEN stage_duration_minutes END), 0)
    + COALESCE(SUM(CASE WHEN stage = 'asleepREM'  THEN stage_duration_minutes END), 0)
    + COALESCE(SUM(CASE WHEN stage = 'asleepCore' THEN stage_duration_minutes END), 0)
    AS DOUBLE
  ) AS total_sleep_minutes,
  CASE
    WHEN (CAST(session_end_ts AS LONG) - CAST(session_start_ts AS LONG)) > 0
    THEN CAST(
      (
        COALESCE(SUM(CASE WHEN stage = 'asleepDeep' THEN stage_duration_minutes END), 0)
        + COALESCE(SUM(CASE WHEN stage = 'asleepREM'  THEN stage_duration_minutes END), 0)
        + COALESCE(SUM(CASE WHEN stage = 'asleepCore' THEN stage_duration_minutes END), 0)
      ) / (
        (CAST(session_end_ts AS LONG) - CAST(session_start_ts AS LONG)) / 60.0
      ) AS DOUBLE
    )
  END AS sleep_efficiency
FROM silver_sleep_stages
GROUP BY user_id, source_platform, session_start_ts, session_end_ts, sleep_date;


-- =============================================================================
-- quarantine_unmatched_deletes — orphan delete observability
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW quarantine_unmatched_deletes (
  record_id       STRING    COMMENT 'Bronze record GUID of the delete event',
  ingested_at     TIMESTAMP COMMENT 'Server-side ingestion timestamp from ZeroBus',
  user_id         STRING    COMMENT 'Authenticated user ID who issued the delete',
  source_platform STRING    COMMENT 'Data source platform identifier',
  deleted_uuid    STRING    COMMENT 'UUID of the deleted record (no match found in target)',
  sample_type     STRING    COMMENT 'HK type identifier — determines which table was targeted',
  target_table    STRING    COMMENT 'Resolved target table name based on sample_type routing',
  reason          STRING    COMMENT 'Why the record was quarantined (no matching uuid in target)'
)
COMMENT 'Delete events with no matching UUID in the target table. In dev, ~99%% of sample deletes are orphans (historical). In production, non-zero counts may indicate sync ordering issues.'
TBLPROPERTIES (
  'quality' = 'quarantine'
)
AS
SELECT
  d.record_id,
  d.ingested_at,
  d.user_id,
  d.source_platform,
  d.deleted_uuid,
  d.sample_type,
  CASE
    WHEN d.sample_type LIKE 'HKQuantityType%' THEN 'silver_health_samples'
    WHEN d.sample_type = 'HKWorkoutTypeIdentifier' THEN 'silver_workouts'
    WHEN d.sample_type = 'HKCategoryTypeIdentifierSleepAnalysis' THEN 'silver_sleep_stages'
    ELSE 'unknown'
  END AS target_table,
  'no matching uuid in target' AS reason
FROM bronze_typed_deletes d
LEFT ANTI JOIN (
  SELECT uuid AS existing_uuid FROM bronze_typed_health_samples
  UNION ALL
  SELECT uuid AS existing_uuid FROM bronze_typed_workouts
  UNION ALL
  SELECT stage_uuid AS existing_uuid FROM bronze_typed_sleep_stages
) all_uuids
ON d.deleted_uuid = all_uuids.existing_uuid;
