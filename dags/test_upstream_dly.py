"""
Upstream trigger orchestration DAG (master DAG).

Runs every 15 minutes (Asia/Qatar). Each run:

1. ``reconcile_triggered``  - safety net: any DAG still ``TRIGGERED`` whose
   Airflow run already finished (e.g. its master task was killed) gets its log
   row set to ``SUCCEEDED`` / ``FAILED``.
2. ``check_day_pending``    - if all expected DAGs (Variable
   ``upstream_expected_dag_count``, default 6) finished today, skip the rest of
   this run and every later run today; the next day starts afresh.
3. ``validate_edw_connection`` + ``check_ready_list`` (CDE Spark job) - load
   new READY rows for today into ``dev_stg.upstream_trigger_log``.
4. ``get_dags_to_trigger``  - READY DAGs of today that have not run yet.
5. ``trigger_upstream_dag`` - for each DAG, in parallel: trigger it, set the row
   to ``TRIGGERED``, WAIT for the DAG run to finish, then set ``SUCCEEDED`` or
   ``FAILED`` (``updated_ts`` on every change). At most ``MAX_PARALLEL_DAGS`` (6)
   run at the same time across all master runs.

Only one master run is active at a time (``max_active_runs=1``). A run waits
for its DAGs, so it can last hours; the next 15-minute run starts when it ends
(missed intervals are not caught up). A DAG is never triggered twice for the
same day.

Log row lifecycle: READY -> TRIGGERED -> SUCCEEDED | FAILED. To re-run a FAILED
DAG the same day, set its rows back to READY.

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
from airflow.exceptions import AirflowException, AirflowFailException
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
CHILD_POKE_INTERVAL = timedelta(seconds=60)
CHILD_DAG_TIMEOUT = timedelta(hours=6)
CDE_JOB_TIMEOUT_SECONDS = 60 * 60
TASK_TIMEOUT = timedelta(minutes=10)
DAGRUN_TIMEOUT = CHILD_DAG_TIMEOUT + timedelta(hours=2)
# Only one master run at a time.
MAX_ACTIVE_RUNS = 1

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

# Duplicate log entries CAN occur in the Impala log table, e.g. when:
#   * the CDE job test_check_ready_list runs twice at the same time (a manual run
#     in CDE next to the scheduled one): both see the row as new and insert it;
#   * the dates differ around midnight: the source filters on Oracle's SYSDATE,
#     the Spark job on its own current_date() (Spark session time zone) and the
#     DAG on the Qatar date, so one side's "today" can miss the other's rows;
#   * rows were inserted by hand or by an older version of the job.
# A duplicate is a row with the same dag_name, insert_load_id and
# source_insert_dt (the source EXECUTION_DATE). They are collapsed here (GROUP
# BY) so a DAG is still triggered once and counted once against the expected
# number of DAGs. All copies share the insert_load_id, so every status UPDATE
# changes all copies together and they never drift apart.
SELECT_DAY_STATE_SQL = f"""
    SELECT dag_name, status, insert_load_id, source_insert_dt, COUNT(*) AS copies
    FROM {TRIGGER_LOG_TABLE}
    WHERE {TODAY_FILTER}
      AND dag_name IS NOT NULL
    GROUP BY dag_name, status, insert_load_id, source_insert_dt
"""

# Rows are addressed by insert_load_id, so every update touches exactly the
# rows that were read (dag_name and the day filter are extra safety).
UPDATE_STATUS_SQL = f"""
    UPDATE {TRIGGER_LOG_TABLE}
    SET status = %(status)s,{{extra_set}}
        updated_ts = now()
    WHERE insert_load_id IN ({{load_ids}})
      AND dag_name = %(dag_name)s
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


class ImpalaPermissionError(AirflowFailException):
    """The Impala user is not authorised; retrying cannot help."""


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
        except Exception as exc:
            if "AuthorizationException" in str(exc):
                # A missing privilege never fixes itself: fail at once, no retries.
                raise ImpalaPermissionError(
                    f"Impala user of connection {IMPALA_CONN_ID!r} lacks privileges on {TRIGGER_LOG_TABLE} "
                    f"(needs SELECT, REFRESH and UPDATE; grant them in Ranger). Impala said: {exc}"
                ) from exc
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
    # dag_name -> status -> sorted insert_load_ids of today's rows
    load_ids: dict[str, dict[str, list[int]]] = field(default_factory=dict)

    @property
    def finished(self) -> list[str]:
        return self.succeeded + self.failed

    def ids(self, dag_name: str, *statuses: str) -> list[int]:
        by_status = self.load_ids.get(dag_name, {})
        return sorted({i for status in statuses for i in by_status.get(status, [])})


def fetch_day_state(hook: Any, run_date: str, exclude: Iterable[str] = ()) -> DayState:
    """Classify today's DAGs. A DAG with several rows takes its most advanced status
    (TRIGGERED > FAILED > SUCCEEDED > READY), so it is triggered at most once a day."""
    run_impala(hook, REFRESH_SQL)
    records = run_impala(hook, SELECT_DAY_STATE_SQL, day_bounds(run_date), fetch=True)

    excluded = set(exclude)
    statuses: dict[str, set[str]] = {}
    first_seen: dict[str, Any] = {}
    state = DayState()
    for dag_name, status, load_id, insert_dt, copies in records:
        dag_name = (dag_name or "").strip()
        status = (status or "").strip().upper()
        if not dag_name or dag_name in excluded:
            continue
        if copies and int(copies) > 1:
            logger.warning("%s: %s duplicate %s rows (insert_load_id=%s, source_insert_dt=%s); treated as one",
                           dag_name, copies, status, load_id, insert_dt)
        if load_id is None:
            logger.warning("Ignoring a %s row of %s without insert_load_id (cannot be updated uniquely)",
                           status, dag_name)
            continue
        statuses.setdefault(dag_name, set()).add(status)
        state.load_ids.setdefault(dag_name, {}).setdefault(status, []).append(int(load_id))
        if insert_dt is not None and (dag_name not in first_seen or str(insert_dt) < str(first_seen[dag_name])):
            first_seen[dag_name] = insert_dt

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


def select_dags_to_trigger(state: DayState, max_parallel: int = MAX_PARALLEL_DAGS) -> list[dict]:
    """READY DAGs (oldest first) that fit in the free parallel slots, each with the
    insert_load_ids of its READY rows: ``{"dag_name": ..., "insert_load_ids": [...]}``."""
    free_slots = max(0, max_parallel - len(state.running))
    return [{"dag_name": d, "insert_load_ids": state.ids(d, STATUS_READY)} for d in state.ready[:free_slots]]


def update_status(
    hook: Any,
    *,
    dag_name: str,
    run_date: str,
    status: str,
    from_statuses: Iterable[str],
    insert_load_ids: Iterable[int],
    set_triggered_ts: bool = False,
    **retry_kwargs,
) -> None:
    """Set the rows ``insert_load_ids`` of ``dag_name`` that are in ``from_statuses`` to ``status``."""
    load_ids = sorted({int(i) for i in insert_load_ids})
    if not load_ids:
        raise ValueError(f"No insert_load_id given for {dag_name}; refusing an unbounded update")
    from_statuses = list(from_statuses)
    parameters = {"status": status, "dag_name": dag_name, **day_bounds(run_date)}
    parameters.update({f"load_id_{n}": load_id for n, load_id in enumerate(load_ids)})
    sql = UPDATE_STATUS_SQL.format(
        extra_set="\n        triggered_ts = now()," if set_triggered_ts else "",
        load_ids=", ".join(f"%(load_id_{n})s" for n in range(len(load_ids))),
        from_statuses=_sql_in_list(from_statuses),
    )
    run_impala(hook, sql, parameters, **retry_kwargs)
    logger.info("%s insert_load_id=%s: %s -> %s (%s)", dag_name, load_ids, "/".join(from_statuses), status,
                run_date)


class ChildRunAlreadyExists(AirflowException):
    """The child run id is taken: another master run triggered this DAG."""


class ChildRunTimeout(AirflowException):
    """The child run did not finish in time; it may still be running."""


def child_run_id_prefix(run_date: str) -> str:
    return f"{CHILD_RUN_ID_PREFIX}__{run_date}__"


def build_child_run_id(run_date: str, attempt: int) -> str:
    """``upstream__<run_date>__<n>``: the n-th run of a DAG for that day. The
    unique (dag_id, run_id) key stops two master runs triggering it twice."""
    return f"{child_run_id_prefix(run_date)}{attempt}"


def trigger_child_dag(dag_id: str, run_id: str, conf: dict | None = None) -> str:
    """Trigger ``dag_id`` with ``run_id`` and return the run_id.

    Raises ChildRunAlreadyExists if ``run_id`` exists, AirflowException if the
    DAG does not exist or is paused.
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
        raise ChildRunAlreadyExists(f"DAG run {dag_id}.{run_id} already exists")
    try:
        dag_run = trigger_dag(dag_id=dag_id, run_id=run_id, conf=conf, replace_microseconds=False)
    except DagRunAlreadyExists as exc:
        raise ChildRunAlreadyExists(str(exc)) from exc
    if dag_run is None:
        raise AirflowException(f"Failed to trigger DAG {dag_id!r}")
    logger.info("Triggered %s with run_id=%s", dag_id, dag_run.run_id)
    return dag_run.run_id


@dataclass(frozen=True)
class ChildRun:
    run_id: str
    state: str
    parent_run_id: str | None


def list_child_runs(dag_id: str, run_date: str) -> list[ChildRun]:
    """Runs of ``dag_id`` triggered by this DAG for ``run_date``, newest first."""
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    prefix = child_run_id_prefix(run_date)
    escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    with create_session() as session:
        runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id, DagRun.run_id.like(f"{escaped}%", escape="\\"))
            .order_by(DagRun.execution_date.desc())
            .all()
        )
        return [ChildRun(r.run_id, r.state, (r.conf or {}).get("parent_run_id")) for r in runs]


def find_latest_child_run(dag_id: str, run_date: str) -> tuple[str, str] | None:
    """``(run_id, state)`` of the newest run of ``dag_id`` triggered for ``run_date``."""
    runs = list_child_runs(dag_id, run_date)
    return (runs[0].run_id, runs[0].state) if runs else None


def wait_for_dag_run(
    dag_id: str,
    run_id: str,
    poke_interval: float = CHILD_POKE_INTERVAL.total_seconds(),
    timeout: float = CHILD_DAG_TIMEOUT.total_seconds(),
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> str:
    """Block until the DAG run is success/failed and return that state."""
    from airflow.models import DagRun

    deadline = clock() + timeout
    while True:
        runs = DagRun.find(dag_id=dag_id, run_id=run_id)
        if not runs:
            raise AirflowException(f"DAG run {dag_id}.{run_id} not found")
        state = runs[0].state
        if state in (DagRunState.SUCCESS, DagRunState.FAILED):
            logger.info("DAG run %s.%s finished: %s", dag_id, run_id, state)
            return DagRunState(state).value
        if clock() >= deadline:
            raise ChildRunTimeout(f"{dag_id}.{run_id} still {state} after {timeout:.0f}s")
        logger.info("DAG run %s.%s is %s; checking again in %.0fs", dag_id, run_id, state, poke_interval)
        sleep(poke_interval)


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
                    summary["running"].append(dag_name)
                    continue
            update_status(hook, dag_name=dag_name, run_date=run_date, status=new_status,
                          from_statuses=[STATUS_TRIGGERED], insert_load_ids=state.ids(dag_name, STATUS_TRIGGERED))
            summary["succeeded" if new_status == STATUS_SUCCEEDED else "failed"].append(dag_name)
        except Exception:
            # Keep reconciling the other DAGs; fail the task at the end.
            logger.exception("Could not reconcile %s", dag_name)
            summary["errors"].append(dag_name)

    logger.info("Reconcile summary for %s: %s", run_date, summary)
    if summary["errors"]:
        raise AirflowException(f"Could not update the status of: {', '.join(summary['errors'])}")
    return summary


def _mark_final(hook: Any, dag_name: str, run_date: str, status: str, load_ids: list[int]) -> None:
    # READY too: the DAG ran even if marking it TRIGGERED failed earlier.
    update_status(hook, dag_name=dag_name, run_date=run_date, status=status,
                  from_statuses=[STATUS_READY, STATUS_TRIGGERED], insert_load_ids=load_ids)


def trigger_and_wait(
    *,
    dag_name: str,
    insert_load_ids: list[int],
    run_date: str,
    parent_dag_id: str,
    parent_run_id: str,
    hook: Any,
    trigger: Callable[..., str] | None = None,
    wait: Callable[..., str] | None = None,
    list_runs: Callable[..., list[ChildRun]] | None = None,
) -> str:
    """Trigger one upstream DAG, wait for it and keep its log rows in sync.

    Only the rows ``insert_load_ids`` (the READY rows that were selected) are
    updated. Returns the final state ("success"), or "skipped" when another
    master run already owns today's run of this DAG. Raises when the DAG fails.
    """
    load_ids = sorted({int(i) for i in insert_load_ids})
    if not load_ids:
        raise AirflowException(f"No insert_load_id for {dag_name}; nothing to trigger")
    trigger = trigger or trigger_child_dag
    wait = wait or wait_for_dag_run
    list_runs = list_runs or list_child_runs

    # 1. Find or create today's run of the DAG.
    existing = list_runs(dag_name, run_date)
    mine = [r for r in existing if r.parent_run_id == parent_run_id]
    if mine:
        run_id = mine[0].run_id  # this task is being retried
        logger.info("Re-attaching to %s.%s", dag_name, run_id)
    elif any(r.state not in (DagRunState.SUCCESS, DagRunState.FAILED) for r in existing):
        logger.info("%s already has an active run for %s from another master run; skipping", dag_name, run_date)
        return "skipped"
    elif not set(load_ids) & set(fetch_day_state(hook, run_date).ids(dag_name, STATUS_READY)):
        # The selected rows are no longer READY: another master run handled the DAG.
        logger.info("%s rows %s are no longer %s; skipping", dag_name, load_ids, STATUS_READY)
        return "skipped"
    else:
        run_id = None
    try:
        if run_id is None:
            run_id = trigger(
                dag_name,
                build_child_run_id(run_date, len(existing) + 1),
                conf={"run_date": run_date, "parent_dag_id": parent_dag_id, "parent_run_id": parent_run_id,
                  "insert_load_ids": load_ids},
            )
    except ChildRunAlreadyExists:
        logger.info("%s was just triggered by another master run; skipping", dag_name)
        return "skipped"
    except Exception as exc:
        logger.error("Could not trigger %s: %s", dag_name, exc)
        try:
            _mark_final(hook, dag_name, run_date, STATUS_FAILED, load_ids)
        except Exception:
            logger.exception("Could not mark %s as %s", dag_name, STATUS_FAILED)
        raise

    # 2. Mark it TRIGGERED. If that fails keep going: the final update fixes the row.
    try:
        update_status(hook, dag_name=dag_name, run_date=run_date, status=STATUS_TRIGGERED,
                      from_statuses=[STATUS_READY], insert_load_ids=load_ids, set_triggered_ts=True)
    except Exception:
        logger.exception("Could not mark %s as %s; continuing", dag_name, STATUS_TRIGGERED)

    # 3. Wait for it. A timeout or kill leaves the row TRIGGERED; a later run's
    #    reconcile_triggered step sets the final status when the DAG finishes.
    state = wait(dag_name, run_id)

    # 4. Final status.
    if state == DagRunState.SUCCESS.value:
        _mark_final(hook, dag_name, run_date, STATUS_SUCCEEDED, load_ids)
        return state
    _mark_final(hook, dag_name, run_date, STATUS_FAILED, load_ids)
    raise AirflowException(f"DAG {dag_name} run {run_id} finished as {state}")


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
    max_active_runs=MAX_ACTIVE_RUNS,
    dagrun_timeout=DAGRUN_TIMEOUT,
    tags=["test", "dev", "spark", "impala", "iceberg"],
) as dag:
    start = EmptyOperator(task_id="start")

    # max_active_tis_per_dag=1: extra guard, these steps never run twice at once.
    @task(max_active_tis_per_dag=1)
    def reconcile_triggered(run_date: str) -> dict[str, list[str]]:
        return reconcile_triggered_dags(get_impala_hook(), run_date, exclude=[DAG_ID])

    @task.short_circuit(max_active_tis_per_dag=1)
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
        max_active_tis_per_dag=1,
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

    @task(max_active_tis_per_dag=1)
    def get_dags_to_trigger(run_date: str) -> list[dict]:
        state = fetch_day_state(get_impala_hook(), run_date, exclude=[DAG_ID])
        to_trigger = select_dags_to_trigger(state)
        logger.info(
            "%s: running=%s ready=%s -> triggering %s (max %s in parallel)",
            run_date, state.running, state.ready, [e["dag_name"] for e in to_trigger], MAX_PARALLEL_DAGS,
        )
        return to_trigger

    # At most 6 child DAGs run (and are waited on) at the same time.
    @task(
        max_active_tis_per_dag=MAX_PARALLEL_DAGS,
        execution_timeout=CHILD_DAG_TIMEOUT + timedelta(minutes=15),
        # Show the DAG name instead of 0, 1, 2... in the UI.
        map_index_template="{{ task.op_kwargs['entry']['dag_name'] }}",
    )
    def trigger_upstream_dag(entry: dict, run_date: str, **context) -> str:
        return trigger_and_wait(
            dag_name=entry["dag_name"],
            insert_load_ids=entry["insert_load_ids"],
            run_date=run_date,
            parent_dag_id=context["dag"].dag_id,
            parent_run_id=context["run_id"],
            hook=get_impala_hook(),
        )

    reconciled = reconcile_triggered(run_date=RUN_DATE_TEMPLATE)
    pending = check_day_pending(run_date=RUN_DATE_TEMPLATE)
    validated = validate_edw_connection()
    to_trigger = get_dags_to_trigger(run_date=RUN_DATE_TEMPLATE)
    triggered = trigger_upstream_dag.partial(run_date=RUN_DATE_TEMPLATE).expand(entry=to_trigger)

    # NONE_FAILED: succeeds when every DAG succeeded or nothing had to run;
    # fails when any DAG failed.
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    start >> reconciled >> pending >> validated >> check_ready_list >> to_trigger >> triggered >> end
