"""
Daily Iceberg housekeeping for dev_stg.upstream_trigger_log: compacts the data and delete files
written by the 15-minute status UPDATEs and expires old snapshots (Impala 4.3+ / CDW).
"""
from __future__ import annotations

import logging
import random
import time
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.impala.hooks.impala import ImpalaHook

logger = logging.getLogger(__name__)

DAG_ID = "test_upstream_dly_maintenance"
IMPALA_CONN_ID = "vfq_impala"
TRIGGER_LOG_TABLE = "dev_stg.upstream_trigger_log"
SNAPSHOT_RETENTION_DAYS = 7
ATTEMPTS = 5
BACKOFF_SECONDS = 30.0

OPTIMIZE_SQL = f"OPTIMIZE TABLE {TRIGGER_LOG_TABLE}"
EXPIRE_SNAPSHOTS_SQL = (
    f"ALTER TABLE {TRIGGER_LOG_TABLE} EXECUTE expire_snapshots(now() - INTERVAL {SNAPSHOT_RETENTION_DAYS} DAYS)"
)


def run_with_retry(hook, sql: str, attempts: int = ATTEMPTS, sleep=time.sleep) -> None:
    """Run one statement, retrying commit conflicts with the 15-minute status updates."""
    for attempt in range(1, attempts + 1):
        try:
            hook.run(sql)
            logger.info("Done: %s", sql)
            return
        except Exception:
            if attempt == attempts:
                raise
            delay = BACKOFF_SECONDS * attempt + random.uniform(0, BACKOFF_SECONDS)
            logger.warning("Attempt %s/%s of %r failed; retrying in %.0fs", attempt, attempts, sql, delay,
                           exc_info=True)
            sleep(delay)


with DAG(
    dag_id=DAG_ID,
    schedule="45 2 * * *",
    start_date=pendulum.datetime(2026, 10, 6, tz="Asia/Qatar"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "ZahirDin.VFQ", "retries": 1, "retry_delay": timedelta(minutes=10),
                  "execution_timeout": timedelta(hours=1)},
    tags=["test", "dev", "impala", "iceberg", "maintenance"],
) as dag:

    @task
    def optimize_table() -> None:
        """Compact data and delete files."""
        run_with_retry(ImpalaHook(impala_conn_id=IMPALA_CONN_ID), OPTIMIZE_SQL)

    @task
    def expire_snapshots() -> None:
        """Drop snapshots older than the retention period."""
        run_with_retry(ImpalaHook(impala_conn_id=IMPALA_CONN_ID), EXPIRE_SNAPSHOTS_SQL)

    optimize_table() >> expire_snapshots()
