-- Operation log for the upstream DAG orchestration (DAG: test_upstream_dly).
-- Run in Impala. Iceberg format v2 is required for Impala UPDATE.
--
-- Lifecycle of a row (one row per dag_name per run_date):
--   READY      inserted by the Spark job test_check_ready_list
--   TRIGGERED  the DAG was triggered by test_upstream_dly
--   FAILED     the DAG could not be triggered, failed, or timed out

CREATE TABLE IF NOT EXISTS dev_stg.upstream_trigger_log (
    dag_name          STRING    COMMENT 'Airflow dag_id to trigger',
    run_date          DATE      COMMENT 'Business date (Asia/Qatar)',
    status            STRING    COMMENT 'READY | TRIGGERED | FAILED',
    dag_run_id        STRING    COMMENT 'run_id of the triggered DAG run',
    parent_dag_run_id STRING    COMMENT 'run_id of the test_upstream_dly run',
    error_message     STRING    COMMENT 'Failure reason when status = FAILED',
    created_ts        TIMESTAMP COMMENT 'Row inserted (READY)',
    triggered_ts      TIMESTAMP COMMENT 'Status set to TRIGGERED',
    completed_ts      TIMESTAMP COMMENT 'Status set to FAILED',
    updated_ts        TIMESTAMP COMMENT 'Last update'
)
PARTITIONED BY SPEC (MONTH(run_date))
STORED AS ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

-- If the table already exists with only (dag_name, status, ...), add the
-- missing columns instead of recreating it, e.g.:
-- ALTER TABLE dev_stg.upstream_trigger_log ADD COLUMNS (
--     dag_run_id STRING, parent_dag_run_id STRING, error_message STRING,
--     triggered_ts TIMESTAMP, completed_ts TIMESTAMP, updated_ts TIMESTAMP);
-- ALTER TABLE dev_stg.upstream_trigger_log SET TBLPROPERTIES ('format-version'='2');

-- Housekeeping (schedule periodically): merge-on-read UPDATEs create delete
-- files, so compact the table regularly.
-- OPTIMIZE TABLE dev_stg.upstream_trigger_log;
