-- Operation log for the upstream DAG orchestration (DAG: test_upstream_dly).
-- Run in Impala. Iceberg format v2 is required for Impala UPDATE.
--
-- Row lifecycle (per dag_name and day of source_insert_dt):
--   READY      inserted by the Spark job test_check_ready_list
--   TRIGGERED  the DAG was triggered by test_upstream_dly   (triggered_ts, updated_ts)
--   SUCCEEDED  the triggered DAG run succeeded               (updated_ts)
--   FAILED     the DAG run failed or could not be triggered  (updated_ts)

CREATE TABLE IF NOT EXISTS dev_stg.upstream_trigger_log (
    dataset_name      STRING    COMMENT 'Source dataset',
    source_tablename  STRING    COMMENT 'Source table',
    dag_name          STRING    COMMENT 'Airflow dag_id to trigger',
    status            STRING    COMMENT 'READY | TRIGGERED | SUCCEEDED | FAILED',
    target_table      STRING,
    source_insert_dt  TIMESTAMP COMMENT 'Source EXECUTION_DATE; defines the day of the row',
    insert_load_id    INT,
    triggered_ts      TIMESTAMP COMMENT 'Set when the DAG is triggered',
    updated_ts        TIMESTAMP COMMENT 'Last status change'
)
STORED AS ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

-- Existing table created as Iceberg v1? Upgrade it (required for UPDATE):
-- ALTER TABLE dev_stg.upstream_trigger_log SET TBLPROPERTIES ('format-version'='2');

-- Re-run a FAILED DAG the same day: set its rows back to READY, e.g.
-- UPDATE dev_stg.upstream_trigger_log SET status = 'READY', updated_ts = from_utc_timestamp(utc_timestamp(), 'Asia/Qatar')
-- WHERE dag_name = '<dag_id>' AND status = 'FAILED'
--   AND source_insert_dt >= trunc(from_utc_timestamp(utc_timestamp(), 'Asia/Qatar'), 'DD');

-- Find duplicate log entries (same DAG, insert_load_id and execution datetime):
-- SELECT dag_name, insert_load_id, source_insert_dt, COUNT(*) AS copies,
--        GROUP_CONCAT(DISTINCT status) AS statuses
-- FROM dev_stg.upstream_trigger_log
-- WHERE source_insert_dt >= trunc(now(), 'DD')
-- GROUP BY dag_name, insert_load_id, source_insert_dt
-- HAVING COUNT(*) > 1;
-- The DAG treats such copies as one row and updates them together.

-- Housekeeping runs daily in DAG test_upstream_dly_maintenance (OPTIMIZE TABLE + expire_snapshots).
