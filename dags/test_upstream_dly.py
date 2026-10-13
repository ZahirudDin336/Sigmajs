"""
Master DAG: loads READY upstream DAGs from the EDW, triggers them (max 6 in parallel)
and keeps their status in the Impala Iceberg table dev_stg.upstream_trigger_log.

Runs every 15 minutes (Asia/Qatar), one run at a time:
  reconcile_triggered -> check_day_pending -> validate_edw_connection -> check_ready_list
  -> get_dags_to_trigger -> run_upstream_dag (mapped sensor, one per DAG) -> end

Status flow: READY -> TRIGGERED -> SUCCEEDED | FAILED (updated_ts on every change).

Airflow configuration:
  Connections: vfq_impala, oracle-conn-edwdb (extra "jdbc_url"), cde_runtime_api
  Variables:   upstream_dag_list           production list of DAGs to run each day
               upstream_expected_dag_count testing only, used when the list is not set (default 2)
               upstream_alert_emails       optional, e-mail on any task failure
               upstream_edw_password_file  optional, password file path in the Spark job instead of the password
"""
from __future__ import annotations

import json
import logging
import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow import __version__ as AIRFLOW_VERSION
from airflow.decorators import task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.impala.hooks.impala import ImpalaHook
from airflow.sensors.base import PokeReturnValue
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from packaging.version import Version

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------- configuration
DAG_ID = "test_upstream_dly"
LOCAL_TZ = "Asia/Qatar"
SCHEDULE = "*/15 * * * *"
IMPALA_CONN_ID = "vfq_impala"
CDE_CONN_ID = "cde_runtime_api"
EDW_CONN_ID = "oracle-conn-edwdb"
CDE_JOB_NAME = "test_check_ready_list"
TRIGGER_LOG_TABLE = "dev_stg.upstream_trigger_log"

DAG_LIST_VARIABLE = "upstream_dag_list"
EXPECTED_DAGS_VARIABLE = "upstream_expected_dag_count"
DEFAULT_EXPECTED_DAG_COUNT = 2
ALERT_EMAILS_VARIABLE = "upstream_alert_emails"
EDW_PASSWORD_FILE_VARIABLE = "upstream_edw_password_file"
MAX_PARALLEL_DAGS = 6

STATUS_READY = "READY"
STATUS_TRIGGERED = "TRIGGERED"
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"

CHILD_RUN_ID_PREFIX = "upstream"
CHILD_POKE_INTERVAL = timedelta(seconds=60)
CHILD_DAG_TIMEOUT = timedelta(hours=6)
CDE_JOB_TIMEOUT_SECONDS = 60 * 60
CDE_POLL_SECONDS = 30
TASK_TIMEOUT = timedelta(minutes=10)
DAGRUN_TIMEOUT = CHILD_DAG_TIMEOUT + timedelta(hours=2)
MAX_ACTIVE_RUNS = 1

IMPALA_ATTEMPTS = 5
IMPALA_BACKOFF_SECONDS = 5.0

# Spark resources of the CDE job: the extract returns a few rows every 15 minutes.
SPARK_RESOURCES = {"driverCores": 1, "driverMemory": "2g", "executorCores": 1, "executorMemory": "2g",
                   "numExecutors": 1}

RUN_DATE_TEMPLATE = "{{ data_interval_end.in_timezone('Asia/Qatar').strftime('%Y-%m-%d') }}"
# map_index_template (DAG name per mapped task in the UI) exists from Airflow 2.9.
MAP_INDEX_KWARGS = (
    {"map_index_template": "{{ task.op_kwargs['entry']['dag_name'] }}"}
    if Version(AIRFLOW_VERSION).release >= (2, 9) else {}
)

# ----------------------------------------------------------------------------- SQL
TODAY_FILTER = (
    "source_insert_dt >= CAST(%(day_start)s AS TIMESTAMP) "
    "AND source_insert_dt < CAST(%(day_end)s AS TIMESTAMP)"
)
NOW_QATAR = f"from_utc_timestamp(utc_timestamp(), '{LOCAL_TZ}')"

REFRESH_SQL = f"REFRESH {TRIGGER_LOG_TABLE}"

# Duplicate rows (same dag_name, insert_load_id, source_insert_dt) are collapsed into one.
SELECT_DAY_STATE_SQL = f"""
    SELECT dag_name, status, insert_load_id, source_insert_dt, COUNT(*) AS copies
    FROM {TRIGGER_LOG_TABLE}
    WHERE {TODAY_FILTER}
      AND dag_name IS NOT NULL
    GROUP BY dag_name, status, insert_load_id, source_insert_dt
"""

# Rows are addressed by insert_load_id; dag_name and the day filter are extra guards.
UPDATE_STATUS_SQL = f"""
    UPDATE {TRIGGER_LOG_TABLE}
    SET status = %(status)s,{{extra_set}}
        updated_ts = {NOW_QATAR}
    WHERE insert_load_id IN ({{load_ids}})
      AND dag_name = %(dag_name)s
      AND status IN ({{from_statuses}})
      AND {TODAY_FILTER}
"""


def _sql_in_list(values: Iterable[str]) -> str:
    """SQL IN-list of the STATUS_* constants (never user input)."""
    return ", ".join(f"'{v}'" for v in values)


# ----------------------------------------------------------------------------- Impala
class ImpalaPermissionError(AirflowFailException):
    """Impala user lacks Ranger privileges; not retried."""


def get_impala_hook() -> ImpalaHook:
    return ImpalaHook(impala_conn_id=IMPALA_CONN_ID)


def day_bounds(run_date: str) -> dict[str, str]:
    """[day_start, day_end) timestamps of run_date."""
    day = date.fromisoformat(run_date)
    return {"day_start": f"{day} 00:00:00", "day_end": f"{day + timedelta(days=1)} 00:00:00"}


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
    """Run one Impala statement; retries commit conflicts with backoff, fails fast on authorization errors."""
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
                raise ImpalaPermissionError(
                    f"Impala user of connection {IMPALA_CONN_ID!r} lacks privileges on {TRIGGER_LOG_TABLE} "
                    f"(needs SELECT, REFRESH and UPDATE; grant them in Ranger). Impala said: {exc}"
                ) from exc
            if attempt == attempts:
                logger.exception("Impala statement failed after %s attempts", attempts)
                raise
            delay = backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0, backoff_seconds)
            logger.warning("Impala statement failed (attempt %s/%s); retrying in %.1fs",
                           attempt, attempts, delay, exc_info=True)
            sleep(delay)


@dataclass
class DayState:
    """Today's DAGs by effective status, with the insert_load_ids of their rows."""

    ready: list[str] = field(default_factory=list)
    running: list[str] = field(default_factory=list)
    succeeded: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    load_ids: dict[str, dict[str, list[int]]] = field(default_factory=dict)

    @property
    def finished(self) -> list[str]:
        return self.succeeded + self.failed

    def ids(self, dag_name: str, *statuses: str) -> list[int]:
        by_status = self.load_ids.get(dag_name, {})
        return sorted({i for status in statuses for i in by_status.get(status, [])})


def fetch_day_state(hook: Any, run_date: str, exclude: Iterable[str] = ()) -> DayState:
    """Read today's rows; a DAG takes its most advanced status (TRIGGERED > FAILED > SUCCEEDED > READY)."""
    run_impala(hook, REFRESH_SQL)
    records = run_impala(hook, SELECT_DAY_STATE_SQL, day_bounds(run_date), fetch=True)

    excluded = set(exclude)
    statuses: dict[str, set[str]] = {}
    first_seen: dict[str, str] = {}
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
            logger.warning("Ignoring a %s row of %s without insert_load_id", status, dag_name)
            continue
        statuses.setdefault(dag_name, set()).add(status)
        state.load_ids.setdefault(dag_name, {}).setdefault(status, []).append(int(load_id))
        if insert_dt is not None and (dag_name not in first_seen or str(insert_dt) < first_seen[dag_name]):
            first_seen[dag_name] = str(insert_dt)

    buckets = ((STATUS_TRIGGERED, state.running), (STATUS_FAILED, state.failed),
               (STATUS_SUCCEEDED, state.succeeded), (STATUS_READY, state.ready))
    for dag_name in sorted(statuses, key=lambda d: (first_seen.get(d, ""), d)):
        bucket = next((b for s, b in buckets if s in statuses[dag_name]), None)
        if bucket is None:
            logger.warning("Ignoring %s with unknown status(es) %s", dag_name, sorted(statuses[dag_name]))
        else:
            bucket.append(dag_name)
    return state


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
    """Set the given insert_load_id rows of dag_name from from_statuses to status (with updated_ts)."""
    load_ids = sorted({int(i) for i in insert_load_ids})
    if not load_ids:
        raise ValueError(f"No insert_load_id given for {dag_name}; refusing an unbounded update")
    from_statuses = list(from_statuses)
    parameters = {"status": status, "dag_name": dag_name, **day_bounds(run_date)}
    parameters.update({f"load_id_{n}": load_id for n, load_id in enumerate(load_ids)})
    sql = UPDATE_STATUS_SQL.format(
        extra_set=f"\n        triggered_ts = {NOW_QATAR}," if set_triggered_ts else "",
        load_ids=", ".join(f"%(load_id_{n})s" for n in range(len(load_ids))),
        from_statuses=_sql_in_list(from_statuses),
    )
    run_impala(hook, sql, parameters, **retry_kwargs)
    logger.info("%s insert_load_id=%s: %s -> %s (%s)", dag_name, load_ids, "/".join(from_statuses), status,
                run_date)


# ----------------------------------------------------------------------------- day target
def get_expected_dag_count() -> int:
    """Testing target: Variable upstream_expected_dag_count (default 2)."""
    from airflow.models import Variable

    raw = Variable.get(EXPECTED_DAGS_VARIABLE, default_var=DEFAULT_EXPECTED_DAG_COUNT)
    try:
        count = int(raw)
    except (TypeError, ValueError):
        raise AirflowException(f"Variable {EXPECTED_DAGS_VARIABLE} must be an integer, got {raw!r}") from None
    if count < 1:
        raise AirflowException(f"Variable {EXPECTED_DAGS_VARIABLE} must be >= 1, got {count}")
    return count


def parse_name_list(raw: Any, variable: str = DAG_LIST_VARIABLE) -> list[str]:
    """Distinct names from a JSON list or a comma/newline separated string."""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        items = raw
    else:
        text = str(raw).strip()
        if text.startswith(("[", "{")):
            try:
                items = json.loads(text)
            except ValueError as exc:
                raise AirflowException(f"Variable {variable} is not a valid JSON list: {exc}") from None
            if not isinstance(items, list):
                raise AirflowException(f"Variable {variable} must be a list")
        else:
            items = text.replace("\n", ",").split(",")
    names: list[str] = []
    for item in items:
        name = str(item).strip()
        if name and name not in names:
            names.append(name)
    return names



def get_upstream_dag_list() -> list[str]:
    """Production target: Variable upstream_dag_list; empty when not set."""
    from airflow.models import Variable

    names = parse_name_list(Variable.get(DAG_LIST_VARIABLE, default_var=None))
    if DAG_ID in names:
        raise AirflowException(f"Variable {DAG_LIST_VARIABLE} must not contain {DAG_ID} itself")
    return names


def get_day_target() -> list[str] | int:
    """The DAG list when set (production), else the expected count (testing)."""
    dag_list = get_upstream_dag_list()
    if dag_list:
        return dag_list
    logger.info("Variable %s not set; using %s (testing mode)", DAG_LIST_VARIABLE, EXPECTED_DAGS_VARIABLE)
    return get_expected_dag_count()


def pending_dags(state: DayState, target: list[str] | int) -> list[str]:
    """Listed DAGs not yet SUCCEEDED/FAILED today (always empty in count mode)."""
    if isinstance(target, int):
        return []
    finished = set(state.finished)
    return [d for d in target if d not in finished]


def is_day_complete(state: DayState, target: list[str] | int) -> bool:
    """List: every listed DAG executed. Count: enough DAGs executed and nothing ready or running."""
    if isinstance(target, int):
        return len(state.finished) >= target and not state.running and not state.ready
    return not pending_dags(state, target)


def select_dags_to_trigger(
    state: DayState, max_parallel: int = MAX_PARALLEL_DAGS, allowed: Iterable[str] | None = None
) -> list[dict]:
    """Oldest READY DAGs (only allowed ones when given) that fit in the free parallel slots."""
    ready = state.ready
    if allowed is not None:
        allowed = set(allowed)
        skipped = [d for d in ready if d not in allowed]
        if skipped:
            logger.warning("READY but not in %s, not triggered: %s", DAG_LIST_VARIABLE, skipped)
        ready = [d for d in ready if d in allowed]
    free_slots = max(0, max_parallel - len(state.running))
    return [{"dag_name": d, "insert_load_ids": state.ids(d, STATUS_READY)} for d in ready[:free_slots]]


# ----------------------------------------------------------------------------- child DAG runs
class ChildRunAlreadyExists(AirflowException):
    """The child run id is taken (already triggered)."""


@dataclass(frozen=True)
class ChildRun:
    run_id: str
    state: str
    parent_run_id: str | None


def child_run_id_prefix(run_date: str) -> str:
    return f"{CHILD_RUN_ID_PREFIX}__{run_date}__"


def build_child_run_id(run_date: str, attempt: int) -> str:
    """upstream__<run_date>__<n>: n-th run of a DAG that day; unique per DAG, so never triggered twice."""
    return f"{child_run_id_prefix(run_date)}{attempt}"


def _like_prefix(prefix: str) -> str:
    return prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def trigger_child_dag(dag_id: str, run_id: str, conf: dict | None = None) -> str:
    """Trigger dag_id (must exist in this Airflow and be unpaused) with run_id."""
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists
    from airflow.models import DagModel, DagRun

    dag_model = DagModel.get_dagmodel(dag_id)
    if dag_model is None:
        raise AirflowException(f"DAG {dag_id!r} does not exist in this Airflow (same CDE virtual cluster?)")
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


def list_child_runs(dag_id: str, run_date: str) -> list[ChildRun]:
    """Runs of dag_id triggered by this DAG for run_date, newest first."""
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    with create_session() as session:
        runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id,
                    DagRun.run_id.like(_like_prefix(child_run_id_prefix(run_date)), escape="\\"))
            .order_by(DagRun.execution_date.desc())
            .all()
        )
        return [ChildRun(r.run_id, r.state, (r.conf or {}).get("parent_run_id")) for r in runs]


def find_latest_child_run(dag_id: str, run_date: str) -> tuple[str, str] | None:
    """(run_id, state) of the newest run of dag_id triggered for run_date."""
    runs = list_child_runs(dag_id, run_date)
    return (runs[0].run_id, runs[0].state) if runs else None


def count_active_child_runs() -> int:
    """Queued or running DAG runs triggered by this DAG (any day)."""
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    with create_session() as session:
        return (
            session.query(DagRun)
            .filter(DagRun.run_id.like(_like_prefix(f"{CHILD_RUN_ID_PREFIX}__"), escape="\\"),
                    DagRun.state.in_([DagRunState.QUEUED, DagRunState.RUNNING]))
            .count()
        )


def reconcile_triggered_dags(
    hook: Any, run_date: str, exclude: Iterable[str] = (), find_run: Callable | None = None
) -> dict[str, list[str]]:
    """Set SUCCEEDED/FAILED for TRIGGERED rows whose DAG run already finished (recovery after a timeout/kill)."""
    find_run = find_run or find_latest_child_run
    state = fetch_day_state(hook, run_date, exclude=exclude)
    summary: dict[str, list[str]] = {"succeeded": [], "failed": [], "running": [], "errors": []}

    for dag_name in state.running:
        try:
            found = find_run(dag_name, run_date)
            if found is None:
                logger.error("No Airflow run of %s found for %s; marking %s", dag_name, run_date, STATUS_FAILED)
                new_status = STATUS_FAILED
            elif found[1] == DagRunState.SUCCESS:
                new_status = STATUS_SUCCEEDED
            elif found[1] == DagRunState.FAILED:
                logger.error("DAG %s run %s failed", dag_name, found[0])
                new_status = STATUS_FAILED
            else:
                summary["running"].append(dag_name)
                continue
            update_status(hook, dag_name=dag_name, run_date=run_date, status=new_status,
                          from_statuses=[STATUS_TRIGGERED], insert_load_ids=state.ids(dag_name, STATUS_TRIGGERED))
            summary["succeeded" if new_status == STATUS_SUCCEEDED else "failed"].append(dag_name)
        except Exception:
            logger.exception("Could not reconcile %s", dag_name)
            summary["errors"].append(dag_name)

    logger.info("Reconcile summary for %s: %s", run_date, summary)
    if summary["errors"]:
        raise AirflowException(f"Could not update the status of: {', '.join(summary['errors'])}")
    return summary


def _mark_final(hook: Any, dag_name: str, run_date: str, status: str, load_ids: list[int]) -> None:
    """Final status; READY included in case marking TRIGGERED had failed."""
    update_status(hook, dag_name=dag_name, run_date=run_date, status=status,
                  from_statuses=[STATUS_READY, STATUS_TRIGGERED], insert_load_ids=load_ids)


def poke_upstream_dag(
    *,
    dag_name: str,
    insert_load_ids: list[int],
    run_date: str,
    parent_dag_id: str,
    parent_run_id: str,
    hook: Any,
    trigger: Callable[..., str] | None = None,
    list_runs: Callable[..., list[ChildRun]] | None = None,
    count_active: Callable[[], int] | None = None,
) -> str | None:
    """One sensor check for a DAG: trigger it when a slot is free, then record its outcome.

    Returns None to check again later, "success" when the DAG succeeded; raises
    AirflowSkipException when another run handles it and AirflowException when it failed.
    """
    load_ids = sorted({int(i) for i in insert_load_ids})
    if not load_ids:
        raise AirflowException(f"No insert_load_id for {dag_name}; nothing to trigger")
    trigger = trigger or trigger_child_dag
    list_runs = list_runs or list_child_runs
    count_active = count_active or count_active_child_runs

    runs = list_runs(dag_name, run_date)
    mine = next((r for r in runs if r.parent_run_id == parent_run_id), None)

    if mine is None:
        if any(r.state not in (DagRunState.SUCCESS, DagRunState.FAILED) for r in runs):
            raise AirflowSkipException(f"{dag_name} already has an active run for {run_date}")
        if not set(load_ids) & set(fetch_day_state(hook, run_date).ids(dag_name, STATUS_READY)):
            raise AirflowSkipException(f"{dag_name} rows {load_ids} are no longer {STATUS_READY}")
        active = count_active()
        if active >= MAX_PARALLEL_DAGS:
            logger.info("%s waits for a free slot (%s/%s DAGs running)", dag_name, active, MAX_PARALLEL_DAGS)
            return None
        try:
            run_id = trigger(dag_name, build_child_run_id(run_date, len(runs) + 1),
                             conf={"run_date": run_date, "parent_dag_id": parent_dag_id,
                                   "parent_run_id": parent_run_id, "insert_load_ids": load_ids})
        except ChildRunAlreadyExists as exc:
            raise AirflowSkipException(f"{dag_name} was triggered by another run") from exc
        except Exception as exc:
            logger.error("Could not trigger %s: %s", dag_name, exc)
            try:
                _mark_final(hook, dag_name, run_date, STATUS_FAILED, load_ids)
            except Exception:
                logger.exception("Could not mark %s as %s", dag_name, STATUS_FAILED)
            raise
        try:
            update_status(hook, dag_name=dag_name, run_date=run_date, status=STATUS_TRIGGERED,
                          from_statuses=[STATUS_READY], insert_load_ids=load_ids, set_triggered_ts=True)
        except Exception:
            logger.exception("Could not mark %s as %s; the final update covers it", dag_name, STATUS_TRIGGERED)
        logger.info("%s triggered as %s; waiting", dag_name, run_id)
        return None

    if mine.state == DagRunState.SUCCESS:
        _mark_final(hook, dag_name, run_date, STATUS_SUCCEEDED, load_ids)
        return DagRunState.SUCCESS.value
    if mine.state == DagRunState.FAILED:
        _mark_final(hook, dag_name, run_date, STATUS_FAILED, load_ids)
        raise AirflowException(f"DAG {dag_name} run {mine.run_id} failed")
    logger.info("%s run %s is %s; checking again in %ss", dag_name, mine.run_id, mine.state,
                int(CHILD_POKE_INTERVAL.total_seconds()))
    return None


# ----------------------------------------------------------------------------- checks and alerts
def validate_edw_connection_config(conn: Any) -> None:
    """EDW connection has host, port, login, password and extra jdbc_url."""
    missing = [name for name, value in (("host", conn.host), ("port", conn.port), ("login", conn.login),
                                        ("password", conn.password),
                                        ("extra.jdbc_url", conn.extra_dejson.get("jdbc_url"))) if not value]
    if missing:
        raise AirflowException(f"Connection {EDW_CONN_ID!r} is missing: {', '.join(missing)}")


def notify_failure(context: dict) -> None:
    """on_failure_callback: log the failure and e-mail Variable upstream_alert_emails when set."""
    from airflow.models import Variable

    ti = context["task_instance"]
    subject = f"[Airflow] {ti.dag_id}.{ti.task_id} failed ({context.get('run_id')})"
    body = f"{subject}<br>map_index={ti.map_index}<br>error: {context.get('exception')}<br>log: {ti.log_url}"
    logger.error(subject)
    try:
        recipients = parse_name_list(Variable.get(ALERT_EMAILS_VARIABLE, default_var=None), ALERT_EMAILS_VARIABLE)
        if recipients:
            from airflow.utils.email import send_email

            send_email(recipients, subject, body)
    except Exception:
        logger.exception("Could not send the failure alert")


# ----------------------------------------------------------------------------- DAG
default_args = {
    "owner": "ZahirDin.VFQ",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": TASK_TIMEOUT,
    "on_failure_callback": notify_failure,
}

_edw = f"conn.get('{EDW_CONN_ID}')"
_pw_file = f"var.value.get('{EDW_PASSWORD_FILE_VARIABLE}', '')"

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

    @task(max_active_tis_per_dag=1)
    def reconcile_triggered(run_date: str) -> dict[str, list[str]]:
        """Finalise TRIGGERED rows whose DAG run already ended."""
        return reconcile_triggered_dags(get_impala_hook(), run_date, exclude=[DAG_ID])

    @task.short_circuit(max_active_tis_per_dag=1)
    def check_day_pending(run_date: str) -> bool:
        """Skip the rest of the run (and the day) once today's target is executed."""
        target = get_day_target()
        state = fetch_day_state(get_impala_hook(), run_date, exclude=[DAG_ID])
        if is_day_complete(state, target):
            logger.info("All expected DAG(s) executed for %s (%s; succeeded=%s, failed=%s); skipping until tomorrow",
                        run_date, target, state.succeeded, state.failed)
            return False
        if isinstance(target, int):
            logger.info("%s/%s DAG(s) executed for %s; continuing", len(state.finished), target, run_date)
        else:
            logger.info("%s: executed=%s, still to execute=%s; continuing",
                        run_date, [d for d in target if d in state.finished], pending_dags(state, target))
        return True

    @task
    def validate_edw_connection() -> None:
        """Fail early when the EDW connection is incomplete."""
        validate_edw_connection_config(BaseHook.get_connection(EDW_CONN_ID))

    check_ready_list = CDEJobRunOperator(
        task_id="check_ready_list",
        job_name=CDE_JOB_NAME,
        connection_id=CDE_CONN_ID,
        timeout=CDE_JOB_TIMEOUT_SECONDS,
        job_poll_interval=CDE_POLL_SECONDS,
        execution_timeout=timedelta(seconds=CDE_JOB_TIMEOUT_SECONDS + 300),
        max_active_tis_per_dag=1,
        overrides={
            "spark": {
                **SPARK_RESOURCES,
                "conf": {
                    "spark.dynamicAllocation.maxExecutors": "2",
                    "spark.edw_jdbc_url": "{{ " + _edw + ".extra_dejson.jdbc_url }}",
                    "spark.edw_username": "{{ " + _edw + ".login }}",
                    # Password only when no password file is configured.
                    "spark.edw_password": "{{ '' if " + _pw_file + " else " + _edw + ".password }}",
                    "spark.edw_password_file": "{{ " + _pw_file + " }}",
                    "spark.curr_date": RUN_DATE_TEMPLATE,
                },
            }
        },
    )

    @task(max_active_tis_per_dag=1)
    def get_dags_to_trigger(run_date: str) -> list[dict]:
        """READY DAGs to run now, with the insert_load_ids of their rows."""
        state = fetch_day_state(get_impala_hook(), run_date, exclude=[DAG_ID])
        to_trigger = select_dags_to_trigger(state, allowed=get_upstream_dag_list() or None)
        logger.info("%s: running=%s ready=%s -> triggering %s (max %s in parallel)",
                    run_date, state.running, state.ready, [e["dag_name"] for e in to_trigger], MAX_PARALLEL_DAGS)
        return to_trigger

    @task.sensor(
        mode="reschedule",
        poke_interval=CHILD_POKE_INTERVAL.total_seconds(),
        timeout=CHILD_DAG_TIMEOUT.total_seconds(),
        execution_timeout=TASK_TIMEOUT,
        max_active_tis_per_dag=1,
        **MAP_INDEX_KWARGS,
    )
    def run_upstream_dag(entry: dict, run_date: str, **context) -> PokeReturnValue:
        """Trigger one DAG and wait for it without holding a worker slot; record SUCCEEDED/FAILED."""
        result = poke_upstream_dag(
            dag_name=entry["dag_name"],
            insert_load_ids=entry["insert_load_ids"],
            run_date=run_date,
            parent_dag_id=context["dag"].dag_id,
            parent_run_id=context["run_id"],
            hook=get_impala_hook(),
        )
        return PokeReturnValue(is_done=result is not None, xcom_value=result)

    reconciled = reconcile_triggered(run_date=RUN_DATE_TEMPLATE)
    pending = check_day_pending(run_date=RUN_DATE_TEMPLATE)
    validated = validate_edw_connection()
    to_trigger = get_dags_to_trigger(run_date=RUN_DATE_TEMPLATE)
    triggered = run_upstream_dag.partial(run_date=RUN_DATE_TEMPLATE).expand(entry=to_trigger)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    start >> reconciled >> pending >> validated >> check_ready_list >> to_trigger >> triggered >> end
