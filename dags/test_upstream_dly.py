"""
Upstream trigger orchestration DAG.

Flow
----
1. ``check_ready_list`` (CDE Spark job) reads the list of DAGs that are ready
   from the source (EDW) and inserts any *new* entry for the current date into
   the Iceberg table ``dev_stg.upstream_trigger_log`` with status ``READY``.
2. ``get_ready_dags`` queries Impala for the DAGs with status ``READY`` and
   ``run_date`` = current date.
3. ``trigger_upstream_dag`` is dynamically mapped over that list. At most
   ``MAX_PARALLEL_DAGS`` (6) child DAGs run at the same time. For each child:
     * the DAG is triggered and the log row is set to ``TRIGGERED``;
     * the child run is awaited; if it fails (or cannot be triggered / times
       out) the log row is set to ``FAILED`` and the task fails.
4. ``end`` succeeds when every child succeeded (or there was nothing to run)
   and fails otherwise, so the parent run reflects child failures.

Airflow configuration required
------------------------------
* Connection ``vfq_impala``   - Impala (impyla) connection.
* Connection ``edw``          - EDW login/password used by the Spark job.
* Connection ``cde_runtime_api`` - CDE virtual cluster API.
* Variable   ``edw_jdbc_url`` - JDBC URL of the EDW source.
* Variable   ``upstream_source_query`` - SQL run on the EDW that returns a
  ``dag_name`` column; ``{curr_date}`` is replaced with the run date.
"""
from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterable
from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.impala.hooks.impala import ImpalaHook
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
DAG_ID = "test_upstream_dly"
LOCAL_TZ = "Asia/Qatar"
IMPALA_CONN_ID = "vfq_impala"
CDE_CONN_ID = "cde_runtime_api"
EDW_CONN_ID = "edw"
TRIGGER_LOG_TABLE = "dev_stg.upstream_trigger_log"

STATUS_READY = "READY"
STATUS_TRIGGERED = "TRIGGERED"
STATUS_FAILED = "FAILED"

MAX_PARALLEL_DAGS = 6
CHILD_POKE_INTERVAL = timedelta(seconds=60)
CHILD_DAG_TIMEOUT = timedelta(hours=6)

# Iceberg uses optimistic concurrency: parallel UPDATEs on the same table can
# fail with a commit conflict, so status updates are retried with backoff.
STATUS_UPDATE_ATTEMPTS = 5
STATUS_UPDATE_BACKOFF_SECONDS = 5.0
ERROR_MESSAGE_MAX_LEN = 1000

# Run date in local (Qatar) time; shared by Spark and Impala so both act on
# the same partition of the log table.
RUN_DATE_TEMPLATE = "{{ data_interval_end.in_timezone('Asia/Qatar').strftime('%Y-%m-%d') }}"

# --------------------------------------------------------------------------- #
# SQL
# --------------------------------------------------------------------------- #
# Parameters use impyla's ``pyformat`` style; impyla quotes/escapes the values.
SELECT_READY_DAGS_SQL = f"""
    SELECT DISTINCT dag_name
    FROM {TRIGGER_LOG_TABLE}
    WHERE status = %(status)s
      AND run_date = CAST(%(run_date)s AS DATE)
    ORDER BY dag_name
"""

MARK_TRIGGERED_SQL = f"""
    UPDATE {TRIGGER_LOG_TABLE}
    SET status = %(status)s,
        dag_run_id = %(dag_run_id)s,
        parent_dag_run_id = %(parent_dag_run_id)s,
        error_message = NULL,
        triggered_ts = now(),
        updated_ts = now()
    WHERE dag_name = %(dag_name)s
      AND run_date = CAST(%(run_date)s AS DATE)
      AND status = '{STATUS_READY}'
"""

MARK_FAILED_SQL = f"""
    UPDATE {TRIGGER_LOG_TABLE}
    SET status = %(status)s,
        dag_run_id = %(dag_run_id)s,
        parent_dag_run_id = %(parent_dag_run_id)s,
        error_message = %(error_message)s,
        completed_ts = now(),
        updated_ts = now()
    WHERE dag_name = %(dag_name)s
      AND run_date = CAST(%(run_date)s AS DATE)
      AND status IN ('{STATUS_READY}', '{STATUS_TRIGGERED}')
"""


# --------------------------------------------------------------------------- #
# Helpers (module level so they can be unit tested)
# --------------------------------------------------------------------------- #
def get_impala_hook() -> ImpalaHook:
    return ImpalaHook(impala_conn_id=IMPALA_CONN_ID)


def fetch_ready_dags(hook: Any, run_date: str, exclude: Iterable[str] = ()) -> list[str]:
    """Return the distinct, non-empty DAG names that are READY for ``run_date``."""
    records = hook.get_records(
        SELECT_READY_DAGS_SQL, parameters={"status": STATUS_READY, "run_date": run_date}
    ) or []
    excluded = set(exclude)
    dag_names: list[str] = []
    for row in records:
        dag_name = (row[0] or "").strip() if row else ""
        if not dag_name:
            logger.warning("Skipping empty dag_name in %s for %s", TRIGGER_LOG_TABLE, run_date)
            continue
        if dag_name in excluded:
            logger.warning("Skipping dag_name=%s: excluded (cannot trigger itself)", dag_name)
            continue
        if dag_name not in dag_names:
            dag_names.append(dag_name)
    return dag_names


def update_status(
    hook: Any,
    *,
    dag_name: str,
    run_date: str,
    status: str,
    dag_run_id: str | None = None,
    parent_dag_run_id: str | None = None,
    error_message: str | None = None,
    attempts: int | None = None,
    backoff_seconds: float | None = None,
    sleep: Callable[[float], None] | None = None,
) -> None:
    """Update the log row of ``dag_name`` / ``run_date``, retrying commit conflicts."""
    attempts = attempts or STATUS_UPDATE_ATTEMPTS
    backoff_seconds = STATUS_UPDATE_BACKOFF_SECONDS if backoff_seconds is None else backoff_seconds
    sleep = sleep or time.sleep
    if status == STATUS_TRIGGERED:
        sql = MARK_TRIGGERED_SQL
    elif status == STATUS_FAILED:
        sql = MARK_FAILED_SQL
    else:
        raise ValueError(f"Unsupported status: {status!r}")

    parameters = {
        "status": status,
        "dag_name": dag_name,
        "run_date": run_date,
        "dag_run_id": dag_run_id,
        "parent_dag_run_id": parent_dag_run_id,
    }
    if status == STATUS_FAILED:
        parameters["error_message"] = (error_message or "")[:ERROR_MESSAGE_MAX_LEN] or None

    for attempt in range(1, attempts + 1):
        try:
            hook.run(sql, parameters=parameters)
            logger.info("Set %s.%s for %s to %s", TRIGGER_LOG_TABLE, dag_name, run_date, status)
            return
        except Exception:
            if attempt == attempts:
                logger.exception(
                    "Giving up setting %s to %s after %s attempts", dag_name, status, attempts
                )
                raise
            delay = backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0, backoff_seconds)
            logger.warning(
                "Attempt %s/%s to set %s to %s failed; retrying in %.1fs",
                attempt, attempts, dag_name, status, delay, exc_info=True,
            )
            sleep(delay)


def build_child_run_id(parent_dag_id: str, parent_run_id: str) -> str:
    """Deterministic run_id so a retried task re-attaches to the same child run."""
    return f"triggered__{parent_dag_id}__{parent_run_id}"


def trigger_child_dag(dag_id: str, run_id: str, conf: dict | None = None) -> str:
    """Trigger ``dag_id`` with ``run_id`` and return the run_id.

    Raises if the DAG does not exist or is paused (a paused DAG's run would sit
    in the queue until the timeout). If a run with ``run_id`` already exists
    (e.g. the task was retried) it is reused instead of creating a new one.
    """
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists
    from airflow.models import DagModel, DagRun

    dag_model = DagModel.get_dagmodel(dag_id)
    if dag_model is None:
        raise AirflowException(f"DAG {dag_id!r} does not exist")
    if dag_model.is_paused:
        raise AirflowException(f"DAG {dag_id!r} is paused")

    if DagRun.find(dag_id=dag_id, run_id=run_id):
        logger.info("DAG run %s.%s already exists; re-attaching", dag_id, run_id)
        return run_id
    try:
        dag_run = trigger_dag(dag_id=dag_id, run_id=run_id, conf=conf, replace_microseconds=False)
    except DagRunAlreadyExists:
        logger.info("DAG run %s.%s already exists; re-attaching", dag_id, run_id)
        return run_id
    if dag_run is None:
        raise AirflowException(f"Failed to trigger DAG {dag_id!r}")
    logger.info("Triggered %s with run_id=%s", dag_id, dag_run.run_id)
    return dag_run.run_id


def wait_for_dag_run(
    dag_id: str,
    run_id: str,
    poke_interval: float = CHILD_POKE_INTERVAL.total_seconds(),
    timeout: float = CHILD_DAG_TIMEOUT.total_seconds(),
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> str:
    """Block until the DAG run finishes and return its final state."""
    from airflow.models import DagRun

    finished = {DagRunState.SUCCESS, DagRunState.FAILED}
    deadline = clock() + timeout
    while True:
        runs = DagRun.find(dag_id=dag_id, run_id=run_id)
        if not runs:
            raise AirflowException(f"DAG run {dag_id}.{run_id} not found")
        state = runs[0].state
        logger.info("DAG run %s.%s is %s", dag_id, run_id, state)
        if state in finished:
            return DagRunState(state).value
        if clock() >= deadline:
            raise AirflowException(
                f"Timed out after {timeout:.0f}s waiting for {dag_id}.{run_id} (state={state})"
            )
        sleep(poke_interval)


def run_upstream_dag(
    *,
    dag_name: str,
    run_date: str,
    parent_dag_id: str,
    parent_run_id: str,
    hook: Any,
    trigger: Callable[..., str] | None = None,
    wait: Callable[..., str] | None = None,
) -> str:
    """Trigger one upstream DAG, keep its log row in sync and wait for it."""
    trigger = trigger or trigger_child_dag
    wait = wait or wait_for_dag_run
    child_run_id = build_child_run_id(parent_dag_id, parent_run_id)
    triggered_run_id: str | None = None
    try:
        triggered_run_id = trigger(
            dag_name,
            child_run_id,
            conf={"run_date": run_date, "parent_dag_id": parent_dag_id, "parent_run_id": parent_run_id},
        )
        update_status(
            hook,
            dag_name=dag_name,
            run_date=run_date,
            status=STATUS_TRIGGERED,
            dag_run_id=triggered_run_id,
            parent_dag_run_id=parent_run_id,
        )
        state = wait(dag_name, triggered_run_id)
        if state != DagRunState.SUCCESS.value:
            raise AirflowException(f"DAG {dag_name} run {triggered_run_id} finished as {state}")
        logger.info("DAG %s run %s succeeded", dag_name, triggered_run_id)
        return state
    except BaseException as exc:
        # BaseException so task timeouts / SIGTERM also leave a FAILED row.
        try:
            update_status(
                hook,
                dag_name=dag_name,
                run_date=run_date,
                status=STATUS_FAILED,
                dag_run_id=triggered_run_id,
                parent_dag_run_id=parent_run_id,
                error_message=f"{type(exc).__name__}: {exc}",
            )
        except Exception:
            logger.exception("Could not mark %s as %s", dag_name, STATUS_FAILED)
        raise


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
default_args = {
    "owner": "ABC",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    start_date=pendulum.datetime(2026, 10, 6, tz=LOCAL_TZ),
    catchup=False,
    # A single active run so two runs never race on the same log rows.
    max_active_runs=1,
    tags=["test", "dev", "spark", "impala", "iceberg"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Credentials are resolved at run time through Jinja (not at DAG parse time)
    # and are masked in the Airflow UI/logs.
    check_ready_list = CDEJobRunOperator(
        task_id="check_ready_list",
        job_name="test_check_ready_list",
        connection_id=CDE_CONN_ID,
        overrides={
            "spark": {
                "conf": {
                    "spark.edw_jdbc_url": "{{ var.value.edw_jdbc_url }}",
                    "spark.edw_username": "{{ conn.get('" + EDW_CONN_ID + "').login }}",
                    "spark.edw_password": "{{ conn.get('" + EDW_CONN_ID + "').password }}",
                    "spark.source_query": "{{ var.value.upstream_source_query }}",
                    "spark.target_table": TRIGGER_LOG_TABLE,
                    "spark.curr_date": RUN_DATE_TEMPLATE,
                }
            }
        },
    )

    @task
    def get_ready_dags(run_date: str) -> list[str]:
        dag_names = fetch_ready_dags(get_impala_hook(), run_date, exclude=[DAG_ID])
        logger.info("%s DAG(s) %s for %s: %s", len(dag_names), STATUS_READY, run_date, dag_names)
        return dag_names

    @task(
        max_active_tis_per_dagrun=MAX_PARALLEL_DAGS,
        execution_timeout=CHILD_DAG_TIMEOUT + timedelta(minutes=15),
    )
    def trigger_upstream_dag(dag_name: str, run_date: str, **context) -> str:
        return run_upstream_dag(
            dag_name=dag_name,
            run_date=run_date,
            parent_dag_id=context["dag"].dag_id,
            parent_run_id=context["run_id"],
            hook=get_impala_hook(),
        )

    ready_dags = get_ready_dags(run_date=RUN_DATE_TEMPLATE)
    triggered = trigger_upstream_dag.partial(run_date=RUN_DATE_TEMPLATE).expand(dag_name=ready_dags)

    # NONE_FAILED: success when there was nothing to trigger (mapped task is
    # skipped), failure when any upstream DAG failed.
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    start >> check_ready_list >> ready_dags >> triggered >> end
