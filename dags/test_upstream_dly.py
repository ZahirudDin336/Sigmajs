"""
Upstream trigger orchestration DAG (master DAG).

Runs every 15 minutes (Asia/Qatar). Every run is short: it never waits for the
DAGs it triggers, so it does not keep running. One run does:

1. ``reconcile_triggered``  - for every DAG marked ``TRIGGERED`` today, look up
   its Airflow run and set the log row to ``SUCCEEDED`` / ``FAILED`` (with
   ``updated_ts``) once that run has finished.
2. ``check_day_pending``    - if all expected DAGs (Variable
   ``upstream_expected_dag_count``, default 6) have finished today, skip the
   rest of this run and every later run today; the next day starts afresh.
3. ``validate_edw_connection`` + ``check_ready_list`` (CDE Spark job) - load
   new READY rows for today from the source into ``dev_stg.upstream_trigger_log``.
4. ``get_dags_to_trigger``  - READY DAGs of today that have not run yet, limited
   so that at most ``MAX_PARALLEL_DAGS`` (6) child DAGs run at the same time.
5. ``trigger_upstream_dag`` - triggers them in parallel and marks them
   ``TRIGGERED`` (``triggered_ts``, ``updated_ts``); a DAG that cannot be
   triggered is marked ``FAILED``.

Log row lifecycle: READY -> TRIGGERED -> SUCCEEDED | FAILED. Each DAG is run
once per day. To re-run a FAILED DAG the same day, set its rows back to READY.

Airflow configuration
---------------------
* Connection ``vfq_impala``        - Impala.
* Connection ``oracle-conn-edwdb`` - EDW (login, password, extra ``jdbc_url``).
* Connection ``cde_runtime_api``   - CDE virtual cluster API.
* Variable ``upstream_expected_dag_count`` (optional, default 6).
"""
from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
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
SCHEDULE = "*/15 * * * *"
IMPALA_CONN_ID = "vfq_impala"
CDE_CONN_ID = "cde_runtime_api"
EDW_CONN_ID = "oracle-conn-edwdb"
CDE_JOB_NAME = "test_check_ready_list"
TRIGGER_LOG_TABLE = "dev_stg.upstream_trigger_log"

EXPECTED_DAGS_VARIABLE = "upstream_expected_dag_count"
DEFAULT_EXPECTED_DAG_COUNT = 6
MAX_PARALLEL_DAGS = 6

STATUS_READY = "READY"
STATUS_TRIGGERED = "TRIGGERED"
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"
FINISHED_STATUSES = (STATUS_SUCCEEDED, STATUS_FAILED)

CHILD_RUN_ID_PREFIX = "upstream"
CDE_JOB_TIMEOUT_SECONDS = 60 * 60
TASK_TIMEOUT = timedelta(minutes=10)
DAGRUN_TIMEOUT = timedelta(hours=2)

# Iceberg uses optimistic concurrency: parallel UPDATEs on the same table can
# fail with a commit conflict, so Impala statements are retried with backoff.
IMPALA_ATTEMPTS = 5
IMPALA_BACKOFF_SECONDS = 5.0

# Run date in local (Qatar) time.
RUN_DATE_TEMPLATE = "{{ data_interval_end.in_timezone('Asia/Qatar').strftime('%Y-%m-%d') }}"

# --------------------------------------------------------------------------- #
# SQL (impyla "pyformat" parameters; impyla quotes/escapes the values)
# --------------------------------------------------------------------------- #
TODAY_FILTER = (
    "source_insert_dt >= CAST(%(day_start)s AS TIMESTAMP) "
    "AND source_insert_dt < CAST(%(day_end)s AS TIMESTAMP)"
)

REFRESH_SQL = f"REFRESH {TRIGGER_LOG_TABLE}"

SELECT_DAY_STATE_SQL = f"""
    SELECT dag_name, status, MIN(source_insert_dt) AS first_insert_dt
    FROM {TRIGGER_LOG_TABLE}
    WHERE {TODAY_FILTER}
      AND dag_name IS NOT NULL
    GROUP BY dag_name, status
"""

UPDATE_STATUS_SQL = f"""
    UPDATE {TRIGGER_LOG_TABLE}
    SET status = %(status)s,{{extra_set}}
        updated_ts = now()
    WHERE dag_name = %(dag_name)s
      AND status IN ({{from_statuses}})
      AND {TODAY_FILTER}
"""


def _sql_in_list(values: Iterable[str]) -> str:
    # Only used with the STATUS_* constants above, never with user input.
    return ", ".join(f"'{v}'" for v in values)


# --------------------------------------------------------------------------- #
# Helpers (module level so they can be unit tested)
# --------------------------------------------------------------------------- #
def get_impala_hook() -> ImpalaHook:
    return ImpalaHook(impala_conn_id=IMPALA_CONN_ID)


def day_bounds(run_date: str) -> dict[str, str]:
    """``[day_start, day_end)`` timestamps for ``run_date`` (YYYY-MM-DD)."""
    day = date.fromisoformat(run_date)
    return {
        "day_start": f"{day.isoformat()} 00:00:00",
        "day_end": f"{(day + timedelta(days=1)).isoformat()} 00:00:00",
    }


def run_impala(
    hook: Any,
    sql: str,
    parameters: dict | None = None,
    *,
    fetch: bool = False,
    attempts: int | None = None,
    backoff_seconds: float | None = None,
    sleep: Callable[[float], None] | None = None,
):
    """Run one Impala statement, retrying failures (e.g. Iceberg commit conflicts)."""
    attempts = attempts or IMPALA_ATTEMPTS
    backoff_seconds = IMPALA_BACKOFF_SECONDS if backoff_seconds is None else backoff_seconds
    sleep = sleep or time.sleep
    for attempt in range(1, attempts + 1):
        try:
            if fetch:
                return hook.get_records(sql, parameters=parameters) or []
            return hook.run(sql, parameters=parameters)
        except Exception:
            if attempt == attempts:
                logger.exception("Impala statement failed after %s attempts", attempts)
                raise
            delay = backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0, backoff_seconds)
            logger.warning(
                "Impala statement failed (attempt %s/%s); retrying in %.1fs",
                attempt, attempts, delay, exc_info=True,
            )
            sleep(delay)


@dataclass
class DayState:
    """Per-DAG state of today's log rows."""

    ready: list[str] = field(default_factory=list)  # oldest first
    running: list[str] = field(default_factory=list)
    succeeded: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    @property
    def finished(self) -> list[str]:
        return self.succeeded + self.failed


def fetch_day_state(hook: Any, run_date: str, exclude: Iterable[str] = ()) -> DayState:
    """Classify today's DAGs. A DAG with several rows takes its most advanced status
    (TRIGGERED > FAILED > SUCCEEDED > READY), so it is triggered at most once a day."""
    run_impala(hook, REFRESH_SQL)
    records = run_impala(hook, SELECT_DAY_STATE_SQL, day_bounds(run_date), fetch=True)

    excluded = set(exclude)
    statuses: dict[str, set[str]] = {}
    first_seen: dict[str, Any] = {}
    for row in records:
        dag_name = (row[0] or "").strip()
        status = (row[1] or "").strip().upper()
        if not dag_name or dag_name in excluded:
            continue
        statuses.setdefault(dag_name, set()).add(status)
        if row[2] is not None and (dag_name not in first_seen or str(row[2]) < str(first_seen[dag_name])):
            first_seen[dag_name] = row[2]

    state = DayState()
    for dag_name in sorted(statuses, key=lambda d: (str(first_seen.get(d, "")), d)):
        dag_statuses = statuses[dag_name]
        if STATUS_TRIGGERED in dag_statuses:
            state.running.append(dag_name)
        elif STATUS_FAILED in dag_statuses:
            state.failed.append(dag_name)
        elif STATUS_SUCCEEDED in dag_statuses:
            state.succeeded.append(dag_name)
        elif STATUS_READY in dag_statuses:
            state.ready.append(dag_name)
        else:
            logger.warning("Ignoring %s with unknown status(es) %s", dag_name, sorted(dag_statuses))
    return state


def get_expected_dag_count() -> int:
    from airflow.models import Variable

    raw = Variable.get(EXPECTED_DAGS_VARIABLE, default_var=DEFAULT_EXPECTED_DAG_COUNT)
    try:
        count = int(raw)
    except (TypeError, ValueError):
        raise AirflowException(f"Variable {EXPECTED_DAGS_VARIABLE} must be an integer, got {raw!r}") from None
    if count < 1:
        raise AirflowException(f"Variable {EXPECTED_DAGS_VARIABLE} must be >= 1, got {count}")
    return count


def is_day_complete(state: DayState, expected: int) -> bool:
    """True when ``expected`` DAGs finished today and nothing is ready or running."""
    return len(state.finished) >= expected and not state.running and not state.ready


def select_dags_to_trigger(state: DayState, max_parallel: int = MAX_PARALLEL_DAGS) -> list[str]:
    """READY DAGs (oldest first) that fit in the free parallel slots."""
    free_slots = max(0, max_parallel - len(state.running))
    return state.ready[:free_slots]


def update_status(
    hook: Any,
    *,
    dag_name: str,
    run_date: str,
    status: str,
    from_statuses: Iterable[str],
    set_triggered_ts: bool = False,
    **retry_kwargs,
) -> None:
    """Set today's rows of ``dag_name`` that are in ``from_statuses`` to ``status``."""
    sql = UPDATE_STATUS_SQL.format(
        extra_set="\n        triggered_ts = now()," if set_triggered_ts else "",
        from_statuses=_sql_in_list(from_statuses),
    )
    run_impala(hook, sql, {"status": status, "dag_name": dag_name, **day_bounds(run_date)}, **retry_kwargs)
    logger.info("%s: %s -> %s (%s)", dag_name, "/".join(from_statuses), status, run_date)


def child_run_id_prefix(run_date: str) -> str:
    return f"{CHILD_RUN_ID_PREFIX}__{run_date}__"


def build_child_run_id(run_date: str, parent_run_id: str) -> str:
    """Unique per master run, so a retried task re-attaches to the same child run."""
    return f"{child_run_id_prefix(run_date)}{parent_run_id}"


def trigger_child_dag(dag_id: str, run_id: str, conf: dict | None = None) -> str:
    """Trigger ``dag_id`` without waiting for it; returns the run_id.

    Raises if the DAG does not exist or is paused. An existing run with the same
    run_id (task retry) is reused instead of creating a new one.
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
        logger.info("DAG run %s.%s already exists; re-using it", dag_id, run_id)
        return run_id
    try:
        dag_run = trigger_dag(dag_id=dag_id, run_id=run_id, conf=conf, replace_microseconds=False)
    except DagRunAlreadyExists:
        logger.info("DAG run %s.%s already exists; re-using it", dag_id, run_id)
        return run_id
    if dag_run is None:
        raise AirflowException(f"Failed to trigger DAG {dag_id!r}")
    logger.info("Triggered %s with run_id=%s", dag_id, dag_run.run_id)
    return dag_run.run_id


def find_latest_child_run(dag_id: str, run_date: str) -> tuple[str, str] | None:
    """``(run_id, state)`` of the newest run of ``dag_id`` triggered for ``run_date``."""
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    prefix = child_run_id_prefix(run_date)
    escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    with create_session() as session:
        dag_run = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id, DagRun.run_id.like(f"{escaped}%", escape="\\"))
            .order_by(DagRun.execution_date.desc())
            .first()
        )
        return (dag_run.run_id, dag_run.state) if dag_run else None


def reconcile_triggered_dags(
    hook: Any, run_date: str, exclude: Iterable[str] = (), find_run: Callable | None = None
) -> dict[str, list[str]]:
    """Move today's TRIGGERED DAGs whose run has finished to SUCCEEDED / FAILED."""
    find_run = find_run or find_latest_child_run
    state = fetch_day_state(hook, run_date, exclude=exclude)
    summary: dict[str, list[str]] = {"succeeded": [], "failed": [], "running": [], "errors": []}

    for dag_name in state.running:
        try:
            found = find_run(dag_name, run_date)
            if found is None:
                logger.error("No Airflow run of %s found for %s; marking %s", dag_name, run_date, STATUS_FAILED)
                new_status = STATUS_FAILED
            else:
                run_id, run_state = found
                if run_state == DagRunState.SUCCESS:
                    new_status = STATUS_SUCCEEDED
                elif run_state == DagRunState.FAILED:
                    logger.error("DAG %s run %s failed", dag_name, run_id)
                    new_status = STATUS_FAILED
                else:
                    logger.info("DAG %s run %s is still %s", dag_name, run_id, run_state)
                    summary["running"].append(dag_name)
                    continue
            update_status(hook, dag_name=dag_name, run_date=run_date, status=new_status,
                          from_statuses=[STATUS_TRIGGERED])
            summary["succeeded" if new_status == STATUS_SUCCEEDED else "failed"].append(dag_name)
        except Exception:
            # Keep reconciling the other DAGs; fail the task at the end.
            logger.exception("Could not reconcile %s", dag_name)
            summary["errors"].append(dag_name)

    logger.info("Reconcile summary for %s: %s", run_date, summary)
    if summary["errors"]:
        raise AirflowException(f"Could not update the status of: {', '.join(summary['errors'])}")
    return summary


def trigger_upstream(
    *,
    dag_name: str,
    run_date: str,
    parent_dag_id: str,
    parent_run_id: str,
    hook: Any,
    trigger: Callable[..., str] | None = None,
) -> str:
    """Trigger one upstream DAG and mark it TRIGGERED; mark FAILED if that fails."""
    trigger = trigger or trigger_child_dag
    try:
        run_id = trigger(
            dag_name,
            build_child_run_id(run_date, parent_run_id),
            conf={"run_date": run_date, "parent_dag_id": parent_dag_id, "parent_run_id": parent_run_id},
        )
        update_status(hook, dag_name=dag_name, run_date=run_date, status=STATUS_TRIGGERED,
                      from_statuses=[STATUS_READY], set_triggered_ts=True)
        return run_id
    except Exception as exc:
        logger.error("Could not trigger %s: %s", dag_name, exc)
        try:
            update_status(hook, dag_name=dag_name, run_date=run_date, status=STATUS_FAILED,
                          from_statuses=[STATUS_READY, STATUS_TRIGGERED])
        except Exception:
            logger.exception("Could not mark %s as %s", dag_name, STATUS_FAILED)
        raise


def validate_edw_connection_config(conn: Any) -> None:
    missing = [name for name, value in (("host", conn.host), ("port", conn.port), ("login", conn.login),
                                        ("password", conn.password),
                                        ("extra.jdbc_url", conn.extra_dejson.get("jdbc_url"))) if not value]
    if missing:
        raise AirflowException(f"Connection {EDW_CONN_ID!r} is missing: {', '.join(missing)}")


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
default_args = {
    "owner": "ZahirDin.VFQ",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": TASK_TIMEOUT,
}

_edw = f"conn.get('{EDW_CONN_ID}')"

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2026, 10, 6, tz=LOCAL_TZ),
    catchup=False,
    # One run at a time: runs never overlap on the log table.
    max_active_runs=1,
    dagrun_timeout=DAGRUN_TIMEOUT,
    tags=["test", "dev", "spark", "impala", "iceberg"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task
    def reconcile_triggered(run_date: str) -> dict[str, list[str]]:
        return reconcile_triggered_dags(get_impala_hook(), run_date, exclude=[DAG_ID])

    @task.short_circuit
    def check_day_pending(run_date: str) -> bool:
        expected = get_expected_dag_count()
        state = fetch_day_state(get_impala_hook(), run_date, exclude=[DAG_ID])
        if is_day_complete(state, expected):
            logger.info(
                "All %s expected DAG(s) finished for %s (succeeded=%s, failed=%s); "
                "skipping until tomorrow", expected, run_date, state.succeeded, state.failed,
            )
            return False
        logger.info("%s/%s DAG(s) finished for %s; continuing", len(state.finished), expected, run_date)
        return True

    @task
    def validate_edw_connection() -> None:
        validate_edw_connection_config(BaseHook.get_connection(EDW_CONN_ID))

    # Credentials are resolved at run time through Jinja (masked in the UI/logs).
    check_ready_list = CDEJobRunOperator(
        task_id="check_ready_list",
        job_name=CDE_JOB_NAME,
        connection_id=CDE_CONN_ID,
        timeout=CDE_JOB_TIMEOUT_SECONDS,
        execution_timeout=timedelta(seconds=CDE_JOB_TIMEOUT_SECONDS + 300),
        overrides={
            "spark": {
                "conf": {
                    "spark.edw_jdbc_url": "{{ " + _edw + ".extra_dejson.jdbc_url }}",
                    "spark.edw_username": "{{ " + _edw + ".login }}",
                    "spark.edw_password": "{{ " + _edw + ".password }}",
                    "spark.curr_date": RUN_DATE_TEMPLATE,
                }
            }
        },
    )

    @task
    def get_dags_to_trigger(run_date: str) -> list[str]:
        state = fetch_day_state(get_impala_hook(), run_date, exclude=[DAG_ID])
        to_trigger = select_dags_to_trigger(state)
        logger.info(
            "%s: running=%s ready=%s -> triggering %s (max %s in parallel)",
            run_date, state.running, state.ready, to_trigger, MAX_PARALLEL_DAGS,
        )
        return to_trigger

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_DAGS)
    def trigger_upstream_dag(dag_name: str, run_date: str, **context) -> str:
        return trigger_upstream(
            dag_name=dag_name,
            run_date=run_date,
            parent_dag_id=context["dag"].dag_id,
            parent_run_id=context["run_id"],
            hook=get_impala_hook(),
        )

    reconciled = reconcile_triggered(run_date=RUN_DATE_TEMPLATE)
    pending = check_day_pending(run_date=RUN_DATE_TEMPLATE)
    validated = validate_edw_connection()
    to_trigger = get_dags_to_trigger(run_date=RUN_DATE_TEMPLATE)
    triggered = trigger_upstream_dag.partial(run_date=RUN_DATE_TEMPLATE).expand(dag_name=to_trigger)

    # NONE_FAILED: succeeds when nothing had to be triggered.
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    start >> reconciled >> pending >> validated >> check_ready_list >> to_trigger >> triggered >> end
