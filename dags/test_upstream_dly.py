"""
Trigger every downstream DAG that is flagged READY in the upstream trigger log.

Flow:
    start -> validate_edw_connection -> check_ready_list (CDE Spark)
          -> get_ready_dag_names (Impala)
          -> trigger_ready_dag[dag_name]  (mapped, at most MAX_PARALLEL_TRIGGERS at a time)
          -> mark_dags_completed (marks only the DAGs that succeeded)
          -> end
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
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

EDW_CONN_ID = 'oracle-conn-edwdb'
IMPALA_CONN_ID = 'vfq_impala'
CDE_CONN_ID = 'cde_runtime_api'

# Single place to switch schema between environments (dev_stg -> prd_stg, ...).
TRIGGER_LOG_TABLE = 'dev_stg.upstream_trigger_log'

# Max downstream DAGs running at the same time. The rest wait in the queue
# and start as slots free up, so every READY DAG still runs in this run.
MAX_PARALLEL_TRIGGERS = 6
TRIGGER_TASK_ID = 'trigger_ready_dag'

RUN_DATE = f"{{{{ data_interval_end.in_timezone('{LOCAL_TZ}').strftime('%Y-%m-%d') }}}}"

default_args = {
    'owner': 'ZahirDin.VFQ',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=2),
}


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
    return CDEJobRunOperator(
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
    conn = BaseHook.get_connection(EDW_CONN_ID)
    missing = [
        name
        for name, value in (
            ('login', conn.login),
            ('password', conn.password),
            ('extra.jdbc_url', conn.extra_dejson.get('jdbc_url')),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Connection '{EDW_CONN_ID}' is missing: {', '.join(missing)}")


@task
def get_ready_dag_names() -> list[str]:
    """Return the distinct READY DAG ids that can actually be triggered.

    DAGs that don't exist, are paused, or are this DAG itself are skipped
    (and stay READY): a paused DAG would hold one of the parallel slots until
    the timeout, a missing one would just fail, and self would loop.
    """
    hook = ImpalaHook(impala_conn_id=IMPALA_CONN_ID)
    # The Spark job writes this table outside Impala, so Impala's cached
    # metadata can be stale; refresh it before reading.
    hook.run(f'REFRESH {TRIGGER_LOG_TABLE}')
    records = hook.get_records(f"""
        SELECT DISTINCT dag_name
        FROM {TRIGGER_LOG_TABLE}
        WHERE status = 'READY'
          AND dag_name IS NOT NULL
        ORDER BY dag_name
    """)
    dag_names = []
    for (dag_name,) in records:
        dag_model = DagModel.get_dagmodel(dag_name)
        if dag_name == DAG_ID:
            logger.warning('Skipping %s: a DAG cannot trigger itself', dag_name)
        elif dag_model is None:
            logger.warning('Skipping %s: DAG not found in Airflow', dag_name)
        elif dag_model.is_paused:
            logger.warning('Skipping %s: DAG is paused', dag_name)
        else:
            dag_names.append(dag_name)
    logger.info(
        'DAGs to trigger (%d, max %d at a time): %s',
        len(dag_names), MAX_PARALLEL_TRIGGERS, dag_names,
    )
    return dag_names


@task(trigger_rule=TriggerRule.ALL_DONE)
def mark_dags_completed(dag_names: list[str] | None, dag_run=None) -> None:
    """Mark COMPLETED only the DAGs whose triggered run succeeded.

    Runs even when some triggers failed (ALL_DONE), so successful DAGs are
    always recorded; failed ones stay READY for the next run. The task then
    fails itself so the overall DAG run still shows the failure.
    """
    if not dag_names:
        logger.info('No DAGs were triggered; nothing to update.')
        return

    # Map index i of the trigger task corresponds to dag_names[i].
    states = {
        ti.map_index: ti.state
        for ti in dag_run.get_task_instances()
        if ti.task_id == TRIGGER_TASK_ID
    }
    succeeded = [n for i, n in enumerate(dag_names) if states.get(i) == TaskInstanceState.SUCCESS]
    not_succeeded = [n for i, n in enumerate(dag_names) if states.get(i) != TaskInstanceState.SUCCESS]

    if succeeded:
        placeholders = ', '.join(f'%(d{i})s' for i in range(len(succeeded)))
        ImpalaHook(impala_conn_id=IMPALA_CONN_ID).run(
            f"""
            UPDATE {TRIGGER_LOG_TABLE}
            SET status = 'COMPLETED'
            WHERE status = 'READY'
              AND dag_name IN ({placeholders})
            """,
            parameters={f'd{i}': name for i, name in enumerate(succeeded)},
        )
    logger.info('Marked COMPLETED (%d): %s', len(succeeded), succeeded)

    if not_succeeded:
        raise AirflowException(
            f'{len(not_succeeded)} triggered DAG(s) did not succeed and remain READY: {not_succeeded}'
        )


with DAG(
    dag_id=DAG_ID,
    description='Triggers downstream DAGs flagged READY in the upstream trigger log.',
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
    trigger_ready_dags = TriggerDagRunOperator.partial(
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
