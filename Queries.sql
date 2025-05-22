
-- I. Tranform Data --
-- This query performs a transformation on the raw `user_metrics_kafka.user_metrics_kafka` table 
-- and writes a cleaned version to a new table `user_metrics_kafka.cleaned_user_metrics` in Parquet format 
-- with Snappy compression for optimized storage and query performance.

-- Key Transformations:
-- 1. Converts `stage_timestamp` and `next_stage_timestamp` to Athena-compatible `timestamp` format.
-- 2. Standardizes `device_type` into categories: 'Android', 'iOS', 'Desktop', or 'Other'.
-- 3. Filters out rows where `stage`, `stage_timestamp`, or `time_to_next_stage` is NULL.
-- 4. Excludes rows where `time_to_next_stage` is NaN to ensure numeric integrity.
-- 5. Stores the output in an S3 bucket (`s3://transformed-data-user-funnel/cleaned_user_metrics/`) 
--    using efficient columnar storage (Parquet + Snappy).

CREATE TABLE user_metrics_kafka.cleaned_user_metrics
WITH (
  format = 'PARQUET',
  external_location = 's3://transformed-data-user-funnel/cleaned_user_metrics/',
  write_compression = 'SNAPPY'
) AS
SELECT
  user_id,
  session_id,
  stage,
  cast(stage_timestamp AS timestamp) AS stage_time,
  cast(next_stage_timestamp AS timestamp) AS next_stage_time,
  time_to_next_stage,
  did_drop,
  product_id,
  category,
  CASE 
    WHEN LOWER(device_type) LIKE '%android%' THEN 'Android'
    WHEN LOWER(device_type) LIKE '%iphone%' THEN 'iOS'
    WHEN LOWER(device_type) LIKE '%desktop%' THEN 'Desktop'
    ELSE 'Other'
  END AS device_category,
  location
FROM user_metrics_kafka.user_metrics_kafka
WHERE stage IS NOT NULL
  AND stage_timestamp IS NOT NULL
  AND time_to_next_stage IS NOT NULL
  AND NOT is_nan(time_to_next_stage);

-- II. Ad hoc Exploratory Queries on the dataset --

-- 1. Hourly User Activity Count --
SELECT hour(stage_time) AS hour_of_day,
       COUNT(*) AS user_count
FROM "user_metrics_kafka"."cleaned_user_metrics"
GROUP BY hour(stage_time)
ORDER BY hour_of_day;

-- 2. User Count by Funnel Stage --
SELECT stage,
       COUNT(*) AS user_count
FROM "user_metrics_kafka"."cleaned_user_metrics"
GROUP BY stage
ORDER BY user_count DESC;

-- 3. Top Product Categories --
SELECT category,
       COUNT(*) AS user_count
FROM "user_metrics_kafka"."cleaned_user_metrics"
GROUP BY category
ORDER BY user_count DESC;

-- 4. Average Time to Next Stage --
SELECT stage,
       AVG(CASE 
             WHEN time_to_next_stage IS NOT NULL AND time_to_next_stage = time_to_next_stage 
             THEN time_to_next_stage 
             ELSE NULL 
           END) AS avg_transition_time,
       COUNT(*) AS users
FROM "user_metrics_kafka"."cleaned_user_metrics"
GROUP BY stage
ORDER BY avg_transition_time DESC;


-- 5. Stage Completion Rate (Total % of users reaching each stage) --
WITH total_users AS (
  SELECT COUNT(DISTINCT user_id) AS total
  FROM "user_metrics_kafka"."cleaned_user_metrics"
)
SELECT stage,
       COUNT(DISTINCT user_id) AS users_at_stage,
       ROUND(100.0 * COUNT(DISTINCT user_id) / total, 2) AS stage_completion_rate
FROM "user_metrics_kafka"."cleaned_user_metrics", total_users
GROUP BY stage, total
ORDER BY stage_completion_rate DESC;
