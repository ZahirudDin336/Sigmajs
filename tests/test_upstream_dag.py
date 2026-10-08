from __future__ import annotations

import itertools
import os
from unittest import mock

import pendulum
import pytest
import test_upstream_dly as m
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from fakes import FakeImpalaHook

RUN_DATE = "2026-10-08"


@pytest.fixture
def no_sleep():
    return lambda _seconds: None


# --------------------------------------------------------------------------- #
# DAG structure
# --------------------------------------------------------------------------- #
def test_dagbag_imports_without_errors():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=os.environ["AIRFLOW__CORE__DAGS_FOLDER"], include_examples=False)
    assert bag.import_errors == {}
    assert m.DAG_ID in bag.dags


def test_dag_structure():
    dag = m.dag
    assert dag.schedule_interval is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert set(dag.task_ids) == {"start", "check_ready_list", "get_ready_dags", "trigger_upstream_dag", "end"}

    chain = ["start", "check_ready_list", "get_ready_dags", "trigger_upstream_dag", "end"]
    for upstream, downstream in itertools.pairwise(chain):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}


def test_trigger_task_is_mapped_with_max_six_in_parallel():
    from airflow.models.mappedoperator import MappedOperator

    trigger = m.dag.get_task("trigger_upstream_dag")
    assert isinstance(trigger, MappedOperator)
    assert trigger.partial_kwargs["max_active_tis_per_dagrun"] == 6
    assert trigger.partial_kwargs["execution_timeout"] > m.CHILD_DAG_TIMEOUT


def test_end_fails_on_failure_but_not_on_empty_list():
    assert m.dag.get_task("end").trigger_rule == TriggerRule.NONE_FAILED


def test_cde_overrides_render(airflow_db, monkeypatch):
    monkeypatch.setenv("AIRFLOW_VAR_EDW_JDBC_URL", "jdbc:oracle:thin:@edw:1521/EDW")
    monkeypatch.setenv("AIRFLOW_VAR_UPSTREAM_SOURCE_QUERY", "SELECT dag_name FROM src WHERE d='{curr_date}'")
    monkeypatch.setenv("AIRFLOW_CONN_EDW", "generic://edw_user:s3cret@edw-host")
    from airflow.models import TaskInstance

    task = m.dag.get_task("check_ready_list")
    dag_run = m.dag.create_dagrun(
        run_id="render_test",
        state=DagRunState.RUNNING,
        execution_date=pendulum.datetime(2026, 10, 7, 22, 30, tz="UTC"),  # 01:30 on the 8th in Qatar
        data_interval=(pendulum.datetime(2026, 10, 7, 22, 30, tz="UTC"),) * 2,
    )
    ti = TaskInstance(task, run_id=dag_run.run_id)
    ti.dag_run = dag_run
    ti.render_templates()

    conf = ti.task.overrides["spark"]["conf"]
    assert conf == {
        "spark.edw_jdbc_url": "jdbc:oracle:thin:@edw:1521/EDW",
        "spark.edw_username": "edw_user",
        "spark.edw_password": "s3cret",
        "spark.source_query": "SELECT dag_name FROM src WHERE d='{curr_date}'",
        "spark.target_table": "dev_stg.upstream_trigger_log",
        "spark.curr_date": "2026-10-08",
    }
    assert ti.task.job_name == "test_check_ready_list"


# --------------------------------------------------------------------------- #
# fetch_ready_dags
# --------------------------------------------------------------------------- #
def test_fetch_ready_dags_filters_by_ready_and_run_date():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_b", "dag_a"])
    hook.rows[("dag_c", RUN_DATE)] = {"dag_name": "dag_c", "run_date": RUN_DATE, "status": m.STATUS_TRIGGERED}
    hook.rows[("dag_d", "2026-10-07")] = {"dag_name": "dag_d", "run_date": "2026-10-07", "status": m.STATUS_READY}

    assert m.fetch_ready_dags(hook, RUN_DATE) == ["dag_a", "dag_b"]
    sql = " ".join(hook.executed[0].split())
    assert "FROM dev_stg.upstream_trigger_log" in sql
    assert "WHERE status = 'READY' AND run_date = CAST('2026-10-08' AS DATE)" in sql


def test_fetch_ready_dags_cleans_rows():
    hook = mock.Mock()
    hook.get_records.return_value = [(" dag_a ",), ("dag_a",), (None,), ("",), (), (m.DAG_ID,), ("dag_b",)]
    assert m.fetch_ready_dags(hook, RUN_DATE, exclude=[m.DAG_ID]) == ["dag_a", "dag_b"]


def test_fetch_ready_dags_handles_no_rows():
    hook = mock.Mock()
    hook.get_records.return_value = None
    assert m.fetch_ready_dags(hook, RUN_DATE) == []


# --------------------------------------------------------------------------- #
# update_status
# --------------------------------------------------------------------------- #
def test_mark_triggered_sql():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"])
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                    dag_run_id="run_1", parent_dag_run_id="parent_1")
    sql = " ".join(hook.executed[0].split())
    assert sql.startswith("UPDATE dev_stg.upstream_trigger_log SET status = 'TRIGGERED', dag_run_id = 'run_1'")
    assert "triggered_ts = now()" in sql
    assert "WHERE dag_name = 'dag_a' AND run_date = CAST('2026-10-08' AS DATE) AND status = 'READY'" in sql
    assert hook.status("dag_a", RUN_DATE) == m.STATUS_TRIGGERED


def test_mark_failed_sql_escapes_error_message():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"])
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_FAILED,
                    error_message="it's broken'; DROP TABLE x; --")
    sql = " ".join(hook.executed[0].split())
    assert "error_message = 'it\\'s broken\\'; DROP TABLE x; --'" in sql
    assert "dag_run_id = NULL" in sql
    assert "AND status IN ('READY', 'TRIGGERED')" in sql
    assert hook.status("dag_a", RUN_DATE) == m.STATUS_FAILED


def test_mark_failed_truncates_error_message():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"])
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_FAILED, error_message="x" * 5000)
    assert len(hook.rows[("dag_a", RUN_DATE)]["error_message"]) == m.ERROR_MESSAGE_MAX_LEN


def test_triggered_does_not_overwrite_failed_row():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"])
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_FAILED, error_message="boom")
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED)
    assert hook.status("dag_a", RUN_DATE) == m.STATUS_FAILED


def test_update_status_retries_commit_conflicts(no_sleep):
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"], fail_runs=2)
    sleeps = []
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                    backoff_seconds=1, sleep=sleeps.append)
    assert hook.status("dag_a", RUN_DATE) == m.STATUS_TRIGGERED
    assert len(sleeps) == 2
    assert 1 <= sleeps[0] <= 2 and 2 <= sleeps[1] <= 3  # exponential backoff + jitter


def test_update_status_gives_up_after_max_attempts(no_sleep):
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"], fail_runs=10)
    with pytest.raises(RuntimeError, match="CommitFailedException"):
        m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                        attempts=3, sleep=no_sleep)
    assert hook.fail_runs == 7
    assert hook.status("dag_a", RUN_DATE) == m.STATUS_READY


def test_update_status_rejects_unknown_status():
    with pytest.raises(ValueError):
        m.update_status(mock.Mock(), dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_READY)


# --------------------------------------------------------------------------- #
# run_upstream_dag
# --------------------------------------------------------------------------- #
def _run(hook, trigger, wait):
    return m.run_upstream_dag(dag_name="dag_a", run_date=RUN_DATE, parent_dag_id=m.DAG_ID,
                              parent_run_id="manual__1", hook=hook, trigger=trigger, wait=wait)


def test_run_upstream_dag_success():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"])
    trigger = mock.Mock(return_value="child_run")
    wait = mock.Mock(return_value="success")

    assert _run(hook, trigger, wait) == "success"

    trigger.assert_called_once_with(
        "dag_a", f"triggered__{m.DAG_ID}__manual__1",
        conf={"run_date": RUN_DATE, "parent_dag_id": m.DAG_ID, "parent_run_id": "manual__1"},
    )
    wait.assert_called_once_with("dag_a", "child_run")
    row = hook.rows[("dag_a", RUN_DATE)]
    assert row["status"] == m.STATUS_TRIGGERED
    assert row["dag_run_id"] == "child_run"
    assert row["parent_dag_run_id"] == "manual__1"


def test_run_upstream_dag_marks_triggered_before_waiting():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"])
    seen = []
    _run(hook, mock.Mock(return_value="r"), lambda *_: seen.append(hook.status("dag_a", RUN_DATE)) or "success")
    assert seen == [m.STATUS_TRIGGERED]


def test_run_upstream_dag_child_failure_marks_failed():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"])
    with pytest.raises(AirflowException, match="finished as failed"):
        _run(hook, mock.Mock(return_value="child_run"), mock.Mock(return_value="failed"))
    row = hook.rows[("dag_a", RUN_DATE)]
    assert row["status"] == m.STATUS_FAILED
    assert row["dag_run_id"] == "child_run"
    assert "finished as failed" in row["error_message"]
    assert len(hook.executed) == 2  # TRIGGERED then FAILED


def test_run_upstream_dag_trigger_error_marks_failed():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"])
    wait = mock.Mock()
    with pytest.raises(AirflowException, match="does not exist"):
        _run(hook, mock.Mock(side_effect=AirflowException("DAG 'dag_a' does not exist")), wait)
    wait.assert_not_called()
    row = hook.rows[("dag_a", RUN_DATE)]
    assert row["status"] == m.STATUS_FAILED
    assert row["dag_run_id"] is None


def test_run_upstream_dag_task_timeout_marks_failed():
    hook = FakeImpalaHook.with_ready(RUN_DATE, ["dag_a"])
    with pytest.raises(AirflowTaskTimeout):
        _run(hook, mock.Mock(return_value="r"), mock.Mock(side_effect=AirflowTaskTimeout("Timeout")))
    assert hook.status("dag_a", RUN_DATE) == m.STATUS_FAILED


def test_run_upstream_dag_failed_update_does_not_hide_original_error(monkeypatch):
    monkeypatch.setattr(m, "STATUS_UPDATE_ATTEMPTS", 1)
    hook = mock.Mock()
    hook.run.side_effect = [None, RuntimeError("impala down")]
    with pytest.raises(AirflowException, match="finished as failed"):
        m.run_upstream_dag(dag_name="dag_a", run_date=RUN_DATE, parent_dag_id=m.DAG_ID, parent_run_id="p",
                           hook=hook, trigger=mock.Mock(return_value="r"), wait=mock.Mock(return_value="failed"))


# --------------------------------------------------------------------------- #
# trigger_child_dag / wait_for_dag_run against a real Airflow metadata DB
# --------------------------------------------------------------------------- #
@pytest.fixture
def child_dag(airflow_db):
    from airflow import DAG
    from airflow.models import DagModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.session import create_session

    with DAG("child_dag", schedule=None, start_date=pendulum.datetime(2026, 1, 1)) as child:
        EmptyOperator(task_id="noop")
    child.sync_to_db()
    SerializedDagModel.write_dag(child)
    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == "child_dag").update({"is_paused": False})
    yield child
    from airflow.models import DagRun
    with create_session() as session:
        session.query(DagRun).filter(DagRun.dag_id == "child_dag").delete()


def _set_state(dag_id, run_id, state):
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    with create_session() as session:
        session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id).update({"state": state})


def test_trigger_child_dag_creates_queued_run(child_dag):
    from airflow.models import DagRun

    run_id = m.trigger_child_dag("child_dag", "triggered__x__1", conf={"run_date": RUN_DATE})
    assert run_id == "triggered__x__1"
    (dag_run,) = DagRun.find(dag_id="child_dag", run_id=run_id)
    assert dag_run.state == DagRunState.QUEUED
    assert dag_run.conf == {"run_date": RUN_DATE}
    assert dag_run.run_type == "manual"


def test_trigger_child_dag_reattaches_on_retry(child_dag):
    from airflow.models import DagRun

    m.trigger_child_dag("child_dag", "triggered__x__2")
    assert m.trigger_child_dag("child_dag", "triggered__x__2") == "triggered__x__2"
    assert len(DagRun.find(dag_id="child_dag")) == 1


def test_trigger_child_dag_unknown_dag(airflow_db):
    with pytest.raises(AirflowException, match="does not exist"):
        m.trigger_child_dag("no_such_dag", "r1")


def test_trigger_child_dag_paused(child_dag):
    from airflow.models import DagModel
    from airflow.utils.session import create_session

    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == "child_dag").update({"is_paused": True})
    with pytest.raises(AirflowException, match="is paused"):
        m.trigger_child_dag("child_dag", "r1")


@pytest.mark.parametrize("final_state", [DagRunState.SUCCESS, DagRunState.FAILED])
def test_wait_for_dag_run_returns_final_state(child_dag, final_state):
    run_id = m.trigger_child_dag("child_dag", f"wait_{final_state}")
    polls = []

    def fake_sleep(_seconds):
        polls.append(1)
        _set_state("child_dag", run_id, DagRunState.RUNNING if len(polls) == 1 else final_state)

    assert m.wait_for_dag_run("child_dag", run_id, poke_interval=0, sleep=fake_sleep) == final_state.value
    assert len(polls) == 2


def test_wait_for_dag_run_times_out(child_dag):
    run_id = m.trigger_child_dag("child_dag", "wait_timeout")
    now = [0.0]

    def fake_sleep(seconds):
        now[0] += seconds

    with pytest.raises(AirflowException, match="Timed out"):
        m.wait_for_dag_run("child_dag", run_id, poke_interval=60, timeout=300,
                           sleep=fake_sleep, clock=lambda: now[0])
    assert now[0] == 300


def test_wait_for_dag_run_missing_run(airflow_db):
    with pytest.raises(AirflowException, match="not found"):
        m.wait_for_dag_run("child_dag", "missing")


# --------------------------------------------------------------------------- #
# End to end: run the whole DAG with dag.test()
# --------------------------------------------------------------------------- #
def _run_dag(monkeypatch, hook, child_results):
    monkeypatch.setenv("AIRFLOW_VAR_EDW_JDBC_URL", "jdbc:x")
    monkeypatch.setenv("AIRFLOW_VAR_UPSTREAM_SOURCE_QUERY", "SELECT 1")
    monkeypatch.setenv("AIRFLOW_CONN_EDW", "generic://u:p@h")
    monkeypatch.setattr(m, "get_impala_hook", lambda: hook)
    triggered = []

    def fake_trigger(dag_id, run_id, conf=None):
        triggered.append((dag_id, run_id, conf))
        return run_id

    monkeypatch.setattr(m, "trigger_child_dag", fake_trigger)
    monkeypatch.setattr(m, "wait_for_dag_run", lambda dag_id, run_id: child_results[dag_id])
    cde_runs = []

    def execute(self, context, **_):  # stands in for the CDE API call
        cde_runs.append(self.overrides["spark"]["conf"]["spark.curr_date"])

    monkeypatch.setattr(m.CDEJobRunOperator, "execute", execute)
    dag_run = m.dag.test(execution_date=pendulum.datetime(2026, 10, 8, 3, 0, tz="Asia/Qatar"))
    assert cde_runs == [RUN_DATE]
    states = {(ti.task_id, ti.map_index): ti.state for ti in dag_run.get_task_instances()}
    return dag_run, states, triggered


def test_end_to_end_one_child_fails(airflow_db, monkeypatch):
    names = [f"dag_{i}" for i in range(7)]
    hook = FakeImpalaHook.with_ready(RUN_DATE, names)
    hook.rows[("old_dag", "2026-10-07")] = {"dag_name": "old_dag", "run_date": "2026-10-07", "status": m.STATUS_READY}
    results = {name: "success" for name in names}
    results["dag_3"] = "failed"

    dag_run, states, triggered = _run_dag(monkeypatch, hook, results)

    assert sorted(d for d, _, _ in triggered) == names
    assert all(conf["run_date"] == RUN_DATE for _, _, conf in triggered)
    for name in names:
        expected = m.STATUS_FAILED if name == "dag_3" else m.STATUS_TRIGGERED
        assert hook.status(name, RUN_DATE) == expected
    assert hook.status("old_dag", "2026-10-07") == m.STATUS_READY

    mapped = {k: v for k, v in states.items() if k[0] == "trigger_upstream_dag"}
    assert len(mapped) == 7
    assert list(mapped.values()).count(TaskInstanceState.FAILED) == 1
    assert states[("end", -1)] == TaskInstanceState.UPSTREAM_FAILED
    assert dag_run.state == DagRunState.FAILED


def test_end_to_end_no_ready_dags(airflow_db, monkeypatch):
    hook = FakeImpalaHook.with_ready("2026-10-07", ["yesterday_dag"])
    dag_run, states, triggered = _run_dag(monkeypatch, hook, {})

    assert triggered == []
    assert states[("get_ready_dags", -1)] == TaskInstanceState.SUCCESS
    assert states[("end", -1)] == TaskInstanceState.SUCCESS
    assert dag_run.state == DagRunState.SUCCESS
