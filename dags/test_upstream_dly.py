"""
Trigger every downstream DAG that is flagged READY in the upstream trigger log.

Flow:
    start -> validate_edw_connection -> check_ready_list (CDE Spark)
          -> get_ready_dag_names (Impala)
          -> trigger_ready_dag[dag_name]  (mapped, at most MAX_PARALLEL_TRIGGERS at a time)
          -> mark_dags_completed (marks only the DAGs that succeeded)
          -> end
                                          \\-> run_summary (always runs; troubleshooting report)

Troubleshooting:
    Every log line from this DAG is prefixed with "[test_upstream_dly]" so it is
    easy to grep. If trigger_ready_dag is SKIPPED, it means get_ready_dag_names
    returned zero DAGs; open the run_summary log (or get_ready_dag_names log) to
    see the reason for each READY row that was not triggered.
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import DagModel
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.impala.hooks.impala import ImpalaHook
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

logger = logging.getLogger(__name__)

DAG_ID = 'test_upstream_dly'
LOCAL_TZ = 'Asia/Qatar'
LOG_PREFIX = f'[{DAG_ID}]'

EDW_CONN_ID = 'oracle-conn-edwdb'
IMPALA_CONN_ID = 'vfq_impala'
CDE_CONN_ID = 'cde_runtime_api'

# Single place to switch schema between environments (dev_stg -> prd_stg, ...).
TRIGGER_LOG_TABLE = 'dev_stg.upstream_trigger_log'

# Max downstream DAGs running at the same time. The rest wait in the queue
# and start as slots free up, so every READY DAG still runs in this run.
MAX_PARALLEL_TRIGGERS = 6
TRIGGER_TASK_ID = 'trigger_ready_dag'
GET_READY_TASK_ID = 'get_ready_dag_names'
SKIPPED_XCOM_KEY = 'skipped_dags'

RUN_DATE = f"{{{{ data_interval_end.in_timezone('{LOCAL_TZ}').strftime('%Y-%m-%d') }}}}"


def _log_task_event(context, event: str) -> None:
    """Callback: one line per task outcome, with what is needed to find it."""
    ti = context['task_instance']
    logger.log(
        logging.ERROR if event == 'FAILED' else logging.INFO,
        '%s task=%s map_index=%s try=%s event=%s run_id=%s error=%r',
        LOG_PREFIX, ti.task_id, ti.map_index, ti.try_number, event,
        context['run_id'], context.get('exception'),
    )


default_args = {
    'owner': 'ZahirDin.VFQ',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=2),
    'on_success_callback': lambda ctx: _log_task_event(ctx, 'SUCCESS'),
    'on_failure_callback': lambda ctx: _log_task_event(ctx, 'FAILED'),
    'on_retry_callback': lambda ctx: _log_task_event(ctx, 'UP_FOR_RETRY'),
}


class LoggedCDEJobRunOperator(CDEJobRunOperator):
    """CDEJobRunOperator that logs what it submits and how it ended."""

    def execute(self, context):
        conf = self.overrides.get('spark', {}).get('conf', {})
        logger.info(
            '%s Submitting CDE job=%s via connection=%s curr_date=%s conf_keys=%s',
            LOG_PREFIX, self.job_name, CDE_CONN_ID, conf.get('spark.curr_date'), sorted(conf),
        )
        try:
            result = super().execute(context)
        except Exception:
            logger.exception('%s CDE job=%s FAILED', LOG_PREFIX, self.job_name)
            raise
        logger.info('%s CDE job=%s finished successfully, result=%r', LOG_PREFIX, self.job_name, result)
        return result


class LoggedTriggerDagRunOperator(TriggerDagRunOperator):
    """TriggerDagRunOperator that logs the target, run_id and final outcome."""

    def execute(self, context):
        ti = context['task_instance']
        logger.info(
            '%s map_index=%s triggering dag=%s run_id=%s wait_for_completion=%s poke_interval=%ss',
            LOG_PREFIX, ti.map_index, self.trigger_dag_id, self.trigger_run_id,
            self.wait_for_completion, self.poke_interval,
        )
        try:
            result = super().execute(context)
        except Exception:
            logger.exception(
                '%s map_index=%s dag=%s run_id=%s did NOT succeed',
                LOG_PREFIX, ti.map_index, self.trigger_dag_id, self.trigger_run_id,
            )
            raise
        logger.info(
            '%s map_index=%s dag=%s run_id=%s SUCCEEDED',
            LOG_PREFIX, ti.map_index, self.trigger_dag_id, self.trigger_run_id,
        )
        return result


def build_cde_job(task_id: str, job_name: str, extra_conf: dict | None = None) -> CDEJobRunOperator:
    """Create a CDE Spark job task. Call it inside a `with DAG(...)` block.

    EDW credentials are resolved through Jinja at run time, never at parse time.
    Airflow masks the rendered password in logs / the Rendered tab, and Spark
    redacts any conf key containing "password" in its UI by default.
    """
    edw = f"conn.get('{EDW_CONN_ID}')"
    conf = {
        'spark.edw_jdbc_url': f"{{{{ {edw}.extra_dejson.jdbc_url }}}}",
        'spark.edw_username': f"{{{{ {edw}.login }}}}",
        'spark.edw_password': f"{{{{ {edw}.password }}}}",
        'spark.curr_date': RUN_DATE,
        **(extra_conf or {}),
    }
    return LoggedCDEJobRunOperator(
        task_id=task_id,
        job_name=job_name,
        connection_id=CDE_CONN_ID,
        overrides={'spark': {'conf': conf}},
    )


@task(retries=0)
def validate_edw_connection() -> None:
    """Fail fast if the EDW connection lacks what the Spark job needs.

    Retries are pointless here: a misconfigured connection will not fix itself.
    """
    logger.info('%s Validating connection=%s', LOG_PREFIX, EDW_CONN_ID)
    conn = BaseHook.get_connection(EDW_CONN_ID)
    fields = {
        'login': conn.login,
        'password': conn.password,
        'extra.jdbc_url': conn.extra_dejson.get('jdbc_url'),
    }
    # Log presence only, never values.
    logger.info(
        '%s connection=%s conn_type=%s fields present: %s',
        LOG_PREFIX, EDW_CONN_ID, conn.conn_type,
        {name: bool(value) for name, value in fields.items()},
    )
    missing = [name for name, value in fields.items() if not value]
    if missing:
        raise ValueError(f"Connection '{EDW_CONN_ID}' is missing: {', '.join(missing)}")
    logger.info('%s connection=%s is valid', LOG_PREFIX, EDW_CONN_ID)


def _skip_reason(dag_name: str) -> str | None:
    """Why a READY dag_name cannot be triggered, or None if it can."""
    if dag_name != dag_name.strip():
        return f'name has leading/trailing whitespace ({dag_name!r}); fix the row in {TRIGGER_LOG_TABLE}'
    if dag_name == DAG_ID:
        return 'a DAG cannot trigger itself'
    dag_model = DagModel.get_dagmodel(dag_name)
    if dag_model is None:
        return 'DAG id not found in Airflow (check spelling/case, or the DAG file failed to import)'
    if not dag_model.is_active:
        return 'DAG is inactive in Airflow (its file was removed or is not being parsed)'
    if dag_model.is_paused:
        return 'DAG is paused in Airflow (unpause it to let it be triggered)'
    return None


@task(task_id=GET_READY_TASK_ID)
def get_ready_dag_names(ti=None) -> list[str]:
    """Return the distinct READY DAG ids that can actually be triggered.

    DAGs that can't be triggered are skipped with a logged reason and stay
    READY: a paused DAG would hold one of the parallel slots until the timeout,
    a missing one would just fail, and self would loop.
    """
    hook = ImpalaHook(impala_conn_id=IMPALA_CONN_ID)

    # The Spark job writes this table outside Impala, so Impala's cached
    # metadata can be stale; refresh it before reading.
    logger.info('%s REFRESH %s', LOG_PREFIX, TRIGGER_LOG_TABLE)
    hook.run(f'REFRESH {TRIGGER_LOG_TABLE}')

    # Overview of the whole table, so an empty READY list can be explained
    # (e.g. rows exist but with status 'ready', 'Ready ' or NULL).
    status_counts = hook.get_records(f"""
        SELECT status, COUNT(*) FROM {TRIGGER_LOG_TABLE} GROUP BY status ORDER BY status
    """)
    logger.info(
        '%s %s row count by status: %s',
        LOG_PREFIX, TRIGGER_LOG_TABLE, {repr(status): count for status, count in status_counts} or 'TABLE IS EMPTY',
    )
    near_ready = {
        status: count for status, count in status_counts
        if status != 'READY' and str(status).strip().upper() == 'READY'
    }
    if near_ready:
        logger.warning(
            "%s %d row(s) look READY but are not exactly 'READY' (case/whitespace) and will NOT be picked up: %s",
            LOG_PREFIX, sum(near_ready.values()), {repr(k): v for k, v in near_ready.items()},
        )

    records = hook.get_records(f"""
        SELECT DISTINCT dag_name
        FROM {TRIGGER_LOG_TABLE}
        WHERE status = 'READY'
          AND dag_name IS NOT NULL
        ORDER BY dag_name
    """)
    logger.info('%s READY rows returned by Impala (%d): %s', LOG_PREFIX, len(records), [r[0] for r in records])

    dag_names: list[str] = []
    skipped: dict[str, str] = {}
    for (dag_name,) in records:
        reason = _skip_reason(dag_name)
        if reason:
            skipped[dag_name] = reason
            logger.warning('%s SKIP dag=%r: %s', LOG_PREFIX, dag_name, reason)
        else:
            dag_names.append(dag_name)
            logger.info('%s OK   dag=%r will be triggered', LOG_PREFIX, dag_name)

    ti.xcom_push(key=SKIPPED_XCOM_KEY, value=skipped)

    if not dag_names:
        logger.warning(
            '%s NOTHING TO TRIGGER: %d READY row(s), %d skipped. %s will be SKIPPED by Airflow '
            '(a mapped task over an empty list never starts). Reasons: %s',
            LOG_PREFIX, len(records), len(skipped), TRIGGER_TASK_ID,
            skipped or f"no rows with status = 'READY' in {TRIGGER_LOG_TABLE}",
        )
    else:
        logger.info(
            '%s Will trigger %d DAG(s), max %d at a time: %s',
            LOG_PREFIX, len(dag_names), MAX_PARALLEL_TRIGGERS, dag_names,
        )
    return dag_names


@task(trigger_rule=TriggerRule.ALL_DONE)
def mark_dags_completed(dag_names: list[str] | None, dag_run=None) -> None:
    """Mark COMPLETED only the DAGs whose triggered run succeeded.

    Runs even when some triggers failed (ALL_DONE), so successful DAGs are
    always recorded; failed ones stay READY for the next run. The task then
    fails itself so the overall DAG run still shows the failure.
    """
    if dag_names is None:
        # An earlier step failed. Fail here too (without retrying): this task
        # runs on ALL_DONE, so succeeding would let `end` succeed and the whole
        # run would wrongly show SUCCESS.
        raise AirflowFailException(
            f'{LOG_PREFIX} No output from {GET_READY_TASK_ID} because an earlier task failed; '
            'nothing was triggered or updated. See run_summary for the failing task.'
        )
    if not dag_names:
        logger.info('%s No DAGs were triggered; nothing to update.', LOG_PREFIX)
        return

    # Map index i of the trigger task corresponds to dag_names[i].
    states = {
        ti.map_index: ti.state
        for ti in dag_run.get_task_instances()
        if ti.task_id == TRIGGER_TASK_ID
    }
    succeeded, not_succeeded = [], {}
    for i, name in enumerate(dag_names):
        state = states.get(i)
        logger.info('%s map_index=%d dag=%s trigger_state=%s', LOG_PREFIX, i, name, state)
        if state == TaskInstanceState.SUCCESS:
            succeeded.append(name)
        else:
            not_succeeded[name] = str(state)

    if succeeded:
        placeholders = ', '.join(f'%(d{i})s' for i in range(len(succeeded)))
        logger.info('%s UPDATE %s SET status=COMPLETED for: %s', LOG_PREFIX, TRIGGER_LOG_TABLE, succeeded)
        ImpalaHook(impala_conn_id=IMPALA_CONN_ID).run(
            f"""
            UPDATE {TRIGGER_LOG_TABLE}
            SET status = 'COMPLETED'
            WHERE status = 'READY'
              AND dag_name IN ({placeholders})
            """,
            parameters={f'd{i}': name for i, name in enumerate(succeeded)},
        )
    logger.info('%s Marked COMPLETED (%d): %s', LOG_PREFIX, len(succeeded), succeeded)

    if not_succeeded:
        logger.error('%s Left as READY (%d), with trigger state: %s', LOG_PREFIX, len(not_succeeded), not_succeeded)
        raise AirflowException(
            f'{len(not_succeeded)} triggered DAG(s) did not succeed and remain READY: {not_succeeded}'
        )


@task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
def run_summary(dag_run=None, ti=None) -> None:
    """Log the state of every task in this run and explain skips/failures.

    Always runs, and is a separate leaf, so it never changes the run's result.
    """
    lines = []
    trigger_states = []
    for task_ti in sorted(dag_run.get_task_instances(), key=lambda t: (t.task_id, t.map_index)):
        if task_ti.task_id == ti.task_id:
            continue
        lines.append(
            f'  {task_ti.task_id:<28} map_index={task_ti.map_index:<3} state={str(task_ti.state):<16} '
            f'try={task_ti.try_number} duration=' + (f'{task_ti.duration:.0f}s' if task_ti.duration is not None else '-')
        )
        if task_ti.task_id == TRIGGER_TASK_ID:
            trigger_states.append(task_ti.state)
    logger.info('%s Task states for run_id=%s:\n%s', LOG_PREFIX, dag_run.run_id, '\n'.join(lines))

    dag_names = ti.xcom_pull(task_ids=GET_READY_TASK_ID)
    skipped = ti.xcom_pull(task_ids=GET_READY_TASK_ID, key=SKIPPED_XCOM_KEY) or {}
    logger.info('%s DAGs selected for trigger: %s', LOG_PREFIX, dag_names)
    if skipped:
        logger.warning('%s READY rows NOT triggered, with reasons:', LOG_PREFIX)
        for name, reason in skipped.items():
            logger.warning('%s   %r: %s', LOG_PREFIX, name, reason)

    if dag_names is None:
        logger.error(
            '%s %s produced no list (state above shows why). %s did not run.',
            LOG_PREFIX, GET_READY_TASK_ID, TRIGGER_TASK_ID,
        )
    elif not dag_names:
        logger.warning(
            '%s %s was SKIPPED because %s returned 0 DAGs. Reason: %s',
            LOG_PREFIX, TRIGGER_TASK_ID, GET_READY_TASK_ID,
            'every READY row was skipped (see reasons above)' if skipped
            else f"no rows with status = 'READY' in {TRIGGER_LOG_TABLE} "
                 f'(see row count by status in the {GET_READY_TASK_ID} log)',
        )
    else:
        counts: dict[str, int] = {}
        for state in trigger_states:
            counts[str(state)] = counts.get(str(state), 0) + 1
        logger.info('%s %s outcome over %d DAG(s): %s', LOG_PREFIX, TRIGGER_TASK_ID, len(dag_names), counts)


with DAG(
    dag_id=DAG_ID,
    description='Triggers downstream DAGs flagged READY in the upstream trigger log.',
    doc_md=__doc__,
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 6, tz=LOCAL_TZ),
    catchup=False,
    # Concurrent runs would read the same READY rows and trigger DAGs twice.
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    tags=['test', 'dev', 'spark', 'impala', 'iceberg'],
) as dag:
    start = EmptyOperator(task_id='start')
    # Succeed even when nothing was READY (mapped triggers are skipped).
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED)

    check_ready_list = build_cde_job('check_ready_list', 'test_check_ready_list')
    dag_names = get_ready_dag_names()

    # Classic operator mapping (.partial/.expand). Do not put this operator in a
    # mapped @task_group: DAG serialization fails there with
    # "unhashable type: 'MappedArgument'".
    trigger_ready_dags = LoggedTriggerDagRunOperator.partial(
        task_id=TRIGGER_TASK_ID,
        # Deterministic run_id + reset_dag_run make retries idempotent:
        # a retry re-runs the same downstream run instead of starting a new one.
        trigger_run_id=f'triggered_by__{DAG_ID}__{{{{ run_id }}}}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
        # Set deferrable=True if a triggerer is running, to free worker slots while waiting.
        retries=0,
        execution_timeout=timedelta(hours=6),
        # At most MAX_PARALLEL_TRIGGERS downstream DAGs run at once; with
        # max_active_runs=1 this cap is global. The rest queue and start as
        # slots free up.
        max_active_tis_per_dag=MAX_PARALLEL_TRIGGERS,
    ).expand(trigger_dag_id=dag_names)

    update_dag_status = mark_dags_completed(dag_names)

    start >> validate_edw_connection() >> check_ready_list >> dag_names
    trigger_ready_dags >> update_dag_status >> end
    # Separate leaf so its success never hides a failure in the run result.
    update_dag_status >> run_summary()
