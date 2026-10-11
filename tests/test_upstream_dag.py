from __future__ import annotations

import itertools
import os
from unittest import mock

import pendulum
import pytest
import test_upstream_dly as m
from airflow.exceptions import AirflowException
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from fakes import NOW, FakeImpalaHook

RUN_DATE = "2026-10-11"
row = FakeImpalaHook.row

EDW_CONN = (
    '{"conn_type": "oracle", "host": "edw", "port": 1521, "login": "edw_user", "password": "s3cret",'
    ' "extra": {"jdbc_url": "jdbc:oracle:thin:@edw:1521/EDW"}}'
)


@pytest.fixture
def edw_conn(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_ORACLE-CONN-EDWDB", EDW_CONN)


def no_sleep(_seconds):
    return None


# --------------------------------------------------------------------------- #
# DAG structure
# --------------------------------------------------------------------------- #
def test_dagbag_imports_without_errors():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=os.environ["AIRFLOW__CORE__DAGS_FOLDER"], include_examples=False)
    assert bag.import_errors == {}
    assert m.DAG_ID in bag.dags


def test_dag_runs_every_15_minutes_in_qatar_time():
    dag = m.dag
    assert dag.timetable.summary == "*/15 * * * *"
    assert str(dag.timezone.name) == "Asia/Qatar"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout == m.DAGRUN_TIMEOUT
    first = dag.next_dagrun_info(None)
    assert first.data_interval.end - first.data_interval.start == pendulum.duration(minutes=15)


def test_task_order():
    chain = ["start", "reconcile_triggered", "check_day_pending", "validate_edw_connection",
             "check_ready_list", "get_dags_to_trigger", "trigger_upstream_dag", "end"]
    assert set(m.dag.task_ids) == set(chain)
    for upstream, downstream in itertools.pairwise(chain):
        assert m.dag.get_task(upstream).downstream_task_ids == {downstream}


def test_trigger_task_is_mapped_with_max_six_in_parallel():
    from airflow.models.mappedoperator import MappedOperator

    trigger = m.dag.get_task("trigger_upstream_dag")
    assert isinstance(trigger, MappedOperator)
    assert trigger.partial_kwargs["max_active_tis_per_dagrun"] == 6


def test_timeouts_and_trigger_rules():
    assert m.dag.get_task("end").trigger_rule == TriggerRule.NONE_FAILED
    for task in m.dag.tasks:
        timeout = task.partial_kwargs.get("execution_timeout") if task.task_id == "trigger_upstream_dag" \
            else task.execution_timeout
        assert timeout is not None, task.task_id
    cde = m.dag.get_task("check_ready_list")
    assert cde.timeout == m.CDE_JOB_TIMEOUT_SECONDS
    assert cde.execution_timeout.total_seconds() > m.CDE_JOB_TIMEOUT_SECONDS


def test_cde_overrides_render(airflow_db, edw_conn):
    from airflow.models import TaskInstance

    task = m.dag.get_task("check_ready_list")
    start = pendulum.datetime(2026, 10, 10, 20, 45, tz="UTC")  # 23:45 Qatar
    dag_run = m.dag.create_dagrun(run_id="render_test", state=DagRunState.RUNNING, execution_date=start,
                                  data_interval=(start, start.add(minutes=15)))
    ti = TaskInstance(task, run_id=dag_run.run_id)
    ti.dag_run = dag_run
    ti.render_templates()

    assert ti.task.overrides["spark"]["conf"] == {
        "spark.edw_jdbc_url": "jdbc:oracle:thin:@edw:1521/EDW",
        "spark.edw_username": "edw_user",
        "spark.edw_password": "s3cret",
        "spark.curr_date": RUN_DATE,  # interval ends at 00:00 Qatar on the 11th
    }
    assert ti.task.job_name == "test_check_ready_list"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def test_day_bounds():
    assert m.day_bounds("2026-12-31") == {"day_start": "2026-12-31 00:00:00", "day_end": "2027-01-01 00:00:00"}
    with pytest.raises(ValueError):
        m.day_bounds("31-12-2026")


def test_fetch_day_state_classifies_and_orders():
    hook = FakeImpalaHook.with_rows(
        row("dag_late", time="09:00:00"),
        row("dag_early", time="07:00:00"),
        row("dag_run", m.STATUS_TRIGGERED),
        row("dag_ok", m.STATUS_SUCCEEDED),
        row("dag_bad", m.STATUS_FAILED),
        row("dag_ok", m.STATUS_READY, time="10:00:00", dataset="late_dataset"),  # already ran today
        row("dag_mixed", m.STATUS_TRIGGERED), row("dag_mixed", m.STATUS_READY, dataset="other"),
        row("dag_yesterday", day="2026-10-10"),
        row(m.DAG_ID),
    )
    state = m.fetch_day_state(hook, RUN_DATE, exclude=[m.DAG_ID])

    assert state.ready == ["dag_early", "dag_late"]
    assert sorted(state.running) == ["dag_mixed", "dag_run"]
    assert state.succeeded == ["dag_ok"]
    assert state.failed == ["dag_bad"]
    assert hook.executed[0] == "REFRESH dev_stg.upstream_trigger_log"
    select = " ".join(hook.executed[1].split())
    assert ("WHERE source_insert_dt >= CAST('2026-10-11 00:00:00' AS TIMESTAMP) "
            "AND source_insert_dt < CAST('2026-10-12 00:00:00' AS TIMESTAMP)") in select


@pytest.mark.parametrize("running, ready, expected", [
    (0, 8, 6), (2, 8, 4), (6, 3, 0), (7, 3, 0), (0, 2, 2),
])
def test_select_dags_to_trigger_respects_max_parallel(running, ready, expected):
    state = m.DayState(ready=[f"r{i}" for i in range(ready)], running=[f"x{i}" for i in range(running)])
    selected = m.select_dags_to_trigger(state)
    assert selected == state.ready[:expected]


@pytest.mark.parametrize("succeeded, failed, running, ready, complete", [
    (6, 0, 0, 0, True),
    (4, 2, 0, 0, True),   # failed DAGs count as executed
    (5, 0, 0, 0, False),
    (5, 0, 1, 0, False),
    (6, 0, 0, 1, False),  # a 7th DAG became ready
    (6, 0, 1, 0, False),
    (7, 0, 0, 0, True),
])
def test_is_day_complete(succeeded, failed, running, ready, complete):
    state = m.DayState(ready=["r"] * ready, running=["x"] * running,
                       succeeded=["s"] * succeeded, failed=["f"] * failed)
    assert m.is_day_complete(state, 6) is complete


def test_expected_dag_count_default_and_variable(airflow_db, monkeypatch):
    assert m.get_expected_dag_count() == 6
    monkeypatch.setenv("AIRFLOW_VAR_UPSTREAM_EXPECTED_DAG_COUNT", "8")
    assert m.get_expected_dag_count() == 8
    for bad in ("six", "0", "-1"):
        monkeypatch.setenv("AIRFLOW_VAR_UPSTREAM_EXPECTED_DAG_COUNT", bad)
        with pytest.raises(AirflowException, match="upstream_expected_dag_count"):
            m.get_expected_dag_count()


def test_update_status_sql_and_effect():
    hook = FakeImpalaHook.with_rows(row("dag_a"), row("dag_a", dataset="ds2"), row("dag_a", day="2026-10-10"))
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                    from_statuses=[m.STATUS_READY], set_triggered_ts=True)
    sql = " ".join(hook.executed[0].split())
    assert sql.startswith("UPDATE dev_stg.upstream_trigger_log SET status = 'TRIGGERED', "
                          "triggered_ts = now(), updated_ts = now() WHERE dag_name = 'dag_a' "
                          "AND status IN ('READY') AND source_insert_dt >= CAST('2026-10-11 00:00:00' AS TIMESTAMP)")
    assert hook.statuses("dag_a") == [m.STATUS_TRIGGERED, m.STATUS_TRIGGERED]
    assert all(r["triggered_ts"] == NOW and r["updated_ts"] == NOW for r in hook.rows[:2])
    assert hook.status("dag_a", "2026-10-10") == m.STATUS_READY  # other days untouched


def test_final_status_update_sets_updated_ts_only():
    hook = FakeImpalaHook.with_rows(row("dag_a", m.STATUS_TRIGGERED))
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_SUCCEEDED,
                    from_statuses=[m.STATUS_TRIGGERED])
    sql = " ".join(hook.executed[0].split())
    assert "SET status = 'SUCCEEDED', updated_ts = now()" in sql
    assert "triggered_ts" not in sql
    assert hook.rows[0]["updated_ts"] == NOW and hook.rows[0]["triggered_ts"] is None


def test_update_status_escapes_values():
    hook = FakeImpalaHook()
    m.update_status(hook, dag_name="x'; DROP TABLE t; --", run_date=RUN_DATE, status=m.STATUS_FAILED,
                    from_statuses=[m.STATUS_READY])
    assert "dag_name = 'x\\'; DROP TABLE t; --'" in hook.executed[0]


def test_impala_retries_commit_conflicts():
    hook = FakeImpalaHook.with_rows(row("dag_a"), fail_runs=2)
    sleeps = []
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                    from_statuses=[m.STATUS_READY], backoff_seconds=1, sleep=sleeps.append)
    assert hook.status("dag_a") == m.STATUS_TRIGGERED
    assert len(sleeps) == 2 and 1 <= sleeps[0] <= 2 and 2 <= sleeps[1] <= 3


def test_impala_gives_up_after_max_attempts():
    hook = FakeImpalaHook.with_rows(row("dag_a"), fail_runs=10)
    with pytest.raises(RuntimeError, match="CommitFailedException"):
        m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                        from_statuses=[m.STATUS_READY], attempts=3, sleep=no_sleep)
    assert hook.fail_runs == 7
    assert hook.status("dag_a") == m.STATUS_READY


def test_select_retries_too():
    hook = FakeImpalaHook.with_rows(row("dag_a"), fail_selects=1)
    with mock.patch.object(m.time, "sleep") as sleep:
        assert m.fetch_day_state(hook, RUN_DATE).ready == ["dag_a"]
    sleep.assert_called_once()


def test_validate_edw_connection_config():
    from airflow.models import Connection

    m.validate_edw_connection_config(Connection.from_json(EDW_CONN, conn_id=m.EDW_CONN_ID))
    bad = Connection(conn_id=m.EDW_CONN_ID, conn_type="oracle", host="edw", login="u")
    with pytest.raises(AirflowException, match="port, password, extra.jdbc_url"):
        m.validate_edw_connection_config(bad)


# --------------------------------------------------------------------------- #
# trigger_upstream
# --------------------------------------------------------------------------- #
def _trigger(hook, trigger):
    return m.trigger_upstream(dag_name="dag_a", run_date=RUN_DATE, parent_dag_id=m.DAG_ID,
                              parent_run_id="scheduled__1", hook=hook, trigger=trigger)


def test_trigger_upstream_marks_triggered_without_waiting():
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    trigger = mock.Mock(return_value="child_run")
    assert _trigger(hook, trigger) == "child_run"
    trigger.assert_called_once_with(
        "dag_a", f"upstream__{RUN_DATE}__scheduled__1",
        conf={"run_date": RUN_DATE, "parent_dag_id": m.DAG_ID, "parent_run_id": "scheduled__1"},
    )
    assert hook.status("dag_a") == m.STATUS_TRIGGERED
    assert hook.rows[0]["triggered_ts"] == NOW


def test_trigger_upstream_failure_marks_failed():
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    with pytest.raises(AirflowException, match="is paused"):
        _trigger(hook, mock.Mock(side_effect=AirflowException("DAG 'dag_a' is paused")))
    assert hook.status("dag_a") == m.STATUS_FAILED
    assert hook.rows[0]["updated_ts"] == NOW and hook.rows[0]["triggered_ts"] is None


def test_trigger_upstream_status_update_failure_marks_failed(monkeypatch):
    monkeypatch.setattr(m, "IMPALA_ATTEMPTS", 1)
    hook = FakeImpalaHook.with_rows(row("dag_a"), fail_runs=1)
    with pytest.raises(RuntimeError):
        _trigger(hook, mock.Mock(return_value="r"))
    assert hook.status("dag_a") == m.STATUS_FAILED


def test_trigger_upstream_keeps_original_error_if_failed_update_fails(monkeypatch):
    monkeypatch.setattr(m, "IMPALA_ATTEMPTS", 1)
    hook = FakeImpalaHook.with_rows(row("dag_a"), fail_runs=5)
    with pytest.raises(AirflowException, match="does not exist"):
        _trigger(hook, mock.Mock(side_effect=AirflowException("DAG 'dag_a' does not exist")))


# --------------------------------------------------------------------------- #
# reconcile_triggered_dags
# --------------------------------------------------------------------------- #
def test_reconcile_updates_finished_runs():
    hook = FakeImpalaHook.with_rows(
        row("dag_ok", m.STATUS_TRIGGERED), row("dag_bad", m.STATUS_TRIGGERED),
        row("dag_busy", m.STATUS_TRIGGERED), row("dag_queued", m.STATUS_TRIGGERED),
        row("dag_lost", m.STATUS_TRIGGERED), row("dag_ready"),
    )
    runs = {"dag_ok": ("r1", "success"), "dag_bad": ("r2", "failed"),
            "dag_busy": ("r3", "running"), "dag_queued": ("r4", "queued")}

    summary = m.reconcile_triggered_dags(hook, RUN_DATE, find_run=lambda d, _: runs.get(d))

    assert summary == {"succeeded": ["dag_ok"], "failed": ["dag_bad", "dag_lost"],
                       "running": ["dag_busy", "dag_queued"], "errors": []}
    assert hook.status("dag_ok") == m.STATUS_SUCCEEDED
    assert hook.status("dag_bad") == m.STATUS_FAILED
    assert hook.status("dag_lost") == m.STATUS_FAILED
    assert hook.status("dag_busy") == m.STATUS_TRIGGERED
    assert hook.status("dag_ready") == m.STATUS_READY
    assert hook.rows[0]["updated_ts"] == NOW


def test_reconcile_continues_after_an_error_then_fails(monkeypatch):
    monkeypatch.setattr(m, "IMPALA_ATTEMPTS", 1)
    hook = FakeImpalaHook.with_rows(row("dag_a", m.STATUS_TRIGGERED), row("dag_b", m.STATUS_TRIGGERED))

    def find_run(dag_name, _):
        if dag_name == "dag_a":
            raise RuntimeError("metadata db down")
        return ("r", "success")

    with pytest.raises(AirflowException, match="dag_a"):
        m.reconcile_triggered_dags(hook, RUN_DATE, find_run=find_run)
    assert hook.status("dag_b") == m.STATUS_SUCCEEDED
    assert hook.status("dag_a") == m.STATUS_TRIGGERED


# --------------------------------------------------------------------------- #
# Airflow metadata DB: triggering and finding child runs
# --------------------------------------------------------------------------- #
@pytest.fixture
def child_dag(airflow_db):
    from airflow import DAG
    from airflow.models import DagModel, DagRun
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
    with create_session() as session:
        session.query(DagRun).filter(DagRun.dag_id == "child_dag").delete()


def _set_state(run_id, state):
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    with create_session() as session:
        session.query(DagRun).filter(DagRun.dag_id == "child_dag", DagRun.run_id == run_id).update({"state": state})


def test_trigger_child_dag_creates_queued_run(child_dag):
    from airflow.models import DagRun

    run_id = m.trigger_child_dag("child_dag", m.build_child_run_id(RUN_DATE, "p1"), conf={"run_date": RUN_DATE})
    (dag_run,) = DagRun.find(dag_id="child_dag", run_id=run_id)
    assert run_id == "upstream__2026-10-11__p1"
    assert dag_run.state == DagRunState.QUEUED
    assert dag_run.conf == {"run_date": RUN_DATE}


def test_trigger_child_dag_reuses_run_on_retry(child_dag):
    from airflow.models import DagRun

    m.trigger_child_dag("child_dag", "upstream__x__1")
    assert m.trigger_child_dag("child_dag", "upstream__x__1") == "upstream__x__1"
    assert len(DagRun.find(dag_id="child_dag")) == 1


def test_trigger_child_dag_unknown_or_paused(child_dag):
    from airflow.models import DagModel
    from airflow.utils.session import create_session

    with pytest.raises(AirflowException, match="does not exist"):
        m.trigger_child_dag("no_such_dag", "r1")
    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == "child_dag").update({"is_paused": True})
    with pytest.raises(AirflowException, match="is paused"):
        m.trigger_child_dag("child_dag", "r1")


def test_find_latest_child_run(child_dag):
    assert m.find_latest_child_run("child_dag", RUN_DATE) is None

    first = m.trigger_child_dag("child_dag", m.build_child_run_id(RUN_DATE, "p1"))
    _set_state(first, DagRunState.FAILED)
    m.trigger_child_dag("child_dag", m.build_child_run_id("2026-10-12", "p9"))  # another day
    m.trigger_child_dag("child_dag", "manual__something")                       # not ours
    assert m.find_latest_child_run("child_dag", RUN_DATE) == (first, "failed")

    # Re-run after a FAILED row was reset to READY: the newest run wins.
    second = m.trigger_child_dag("child_dag", m.build_child_run_id(RUN_DATE, "p2"))
    assert m.find_latest_child_run("child_dag", RUN_DATE) == (second, "queued")
    _set_state(second, DagRunState.SUCCESS)
    assert m.find_latest_child_run("child_dag", RUN_DATE) == (second, "success")


def test_find_latest_child_run_escapes_like_wildcards(child_dag):
    m.trigger_child_dag("child_dag", "upstreamX_2026-10-11__p1")
    assert m.find_latest_child_run("child_dag", RUN_DATE) is None


def test_reconcile_against_real_child_run(child_dag):
    run_id = m.trigger_child_dag("child_dag", m.build_child_run_id(RUN_DATE, "p1"))
    hook = FakeImpalaHook.with_rows(row("child_dag", m.STATUS_TRIGGERED))
    assert m.reconcile_triggered_dags(hook, RUN_DATE)["running"] == ["child_dag"]
    _set_state(run_id, DagRunState.SUCCESS)
    assert m.reconcile_triggered_dags(hook, RUN_DATE)["succeeded"] == ["child_dag"]
    assert hook.status("child_dag") == m.STATUS_SUCCEEDED


# --------------------------------------------------------------------------- #
# End to end: several 15-minute runs of the whole DAG with dag.test()
# --------------------------------------------------------------------------- #
class World:
    """Child DAG runs as the master DAG sees them through the metadata DB."""

    def __init__(self, monkeypatch, hook):
        self.hook = hook
        self.child_state: dict[str, str] = {}
        self.triggered: list[str] = []
        self.cde_runs = 0
        monkeypatch.setenv("AIRFLOW_CONN_ORACLE-CONN-EDWDB", EDW_CONN)
        monkeypatch.setattr(m, "get_impala_hook", lambda: hook)
        monkeypatch.setattr(m, "trigger_child_dag", self.trigger)
        monkeypatch.setattr(m, "find_latest_child_run", self.find)

        def execute(op, context, **_):  # stands in for the CDE API call
            self.cde_runs += 1

        monkeypatch.setattr(m.CDEJobRunOperator, "execute", execute)

    def trigger(self, dag_id, run_id, conf=None):
        if dag_id == "missing_dag":
            raise AirflowException("DAG 'missing_dag' does not exist")
        self.triggered.append(dag_id)
        self.child_state[dag_id] = "queued"
        return run_id

    def find(self, dag_id, run_date):
        return (f"run_{dag_id}", self.child_state[dag_id]) if dag_id in self.child_state else None

    def run(self, hhmm: str):
        hour, minute = map(int, hhmm.split(":"))
        dag_run = m.dag.test(execution_date=pendulum.datetime(2026, 10, 11, hour, minute, tz="Asia/Qatar"))
        states = {(ti.task_id, ti.map_index): ti.state for ti in dag_run.get_task_instances()}
        return dag_run, states


def test_end_to_end_day(airflow_db, monkeypatch):
    names = [f"dag_{i}" for i in range(6)]
    hook = FakeImpalaHook.with_rows(*[row(n, time=f"0{i}:00:00") for i, n in enumerate(names[:5])],
                                    row("dag_old", day="2026-10-10"))
    world = World(monkeypatch, hook)

    # 08:00 - five DAGs READY: all triggered in parallel, run finishes at once.
    dag_run, states = world.run("08:00")
    assert dag_run.state == DagRunState.SUCCESS
    assert sorted(world.triggered) == names[:5]
    assert all(hook.status(n) == m.STATUS_TRIGGERED for n in names[:5])
    assert len([k for k in states if k[0] == "trigger_upstream_dag"]) == 5
    assert hook.status("dag_old", "2026-10-10") == m.STATUS_READY

    # 08:15 - three finished (2 ok, 1 failed), the sixth DAG became READY.
    world.child_state.update(dag_0="success", dag_1="success", dag_2="failed", dag_3="running")
    hook.rows.append(row("dag_5", time="08:10:00"))
    dag_run, states = world.run("08:15")
    assert dag_run.state == DagRunState.SUCCESS
    assert [hook.status(n) for n in names] == [m.STATUS_SUCCEEDED, m.STATUS_SUCCEEDED, m.STATUS_FAILED,
                                               m.STATUS_TRIGGERED, m.STATUS_TRIGGERED, m.STATUS_TRIGGERED]
    assert world.triggered.count("dag_5") == 1 and world.triggered.count("dag_0") == 1

    # 08:30 - nothing new: no trigger, the run still ends.
    dag_run, states = world.run("08:30")
    assert dag_run.state == DagRunState.SUCCESS
    assert states[("trigger_upstream_dag", -1)] == TaskInstanceState.SKIPPED
    assert len(world.triggered) == 6

    # 08:45 - all six finished: statuses updated, rest of the run skipped.
    world.child_state.update(dag_3="success", dag_4="failed", dag_5="success")
    cde_runs = world.cde_runs
    dag_run, states = world.run("08:45")
    assert [hook.status(n) for n in names] == [m.STATUS_SUCCEEDED, m.STATUS_SUCCEEDED, m.STATUS_FAILED,
                                               m.STATUS_SUCCEEDED, m.STATUS_FAILED, m.STATUS_SUCCEEDED]
    assert states[("check_day_pending", -1)] == TaskInstanceState.SUCCESS
    assert states[("check_ready_list", -1)] == TaskInstanceState.SKIPPED
    assert states[("end", -1)] == TaskInstanceState.SKIPPED
    assert world.cde_runs == cde_runs  # no Spark job once the day is complete

    # 09:00 - stays stopped for the rest of the day.
    dag_run, states = world.run("09:00")
    assert states[("check_ready_list", -1)] == TaskInstanceState.SKIPPED
    assert world.cde_runs == cde_runs


def test_end_to_end_max_six_running(airflow_db, monkeypatch):
    names = [f"dag_{i}" for i in range(9)]
    monkeypatch.setenv("AIRFLOW_VAR_UPSTREAM_EXPECTED_DAG_COUNT", "9")
    hook = FakeImpalaHook.with_rows(*[row(n, time=f"0{i}:00:00") for i, n in enumerate(names)])
    world = World(monkeypatch, hook)

    world.run("10:00")
    assert sorted(world.triggered) == names[:6]  # oldest first, capped at 6
    assert [hook.status(n) for n in names[6:]] == [m.STATUS_READY] * 3

    world.child_state.update(dag_0="success", dag_1="failed")
    world.run("10:15")
    assert sorted(world.triggered) == names[:8]  # two slots freed
    assert hook.status("dag_8") == m.STATUS_READY


def test_end_to_end_trigger_failure_is_logged_and_others_continue(airflow_db, monkeypatch):
    hook = FakeImpalaHook.with_rows(row("dag_a"), row("missing_dag"), row("dag_b"))
    world = World(monkeypatch, hook)

    dag_run, states = world.run("11:00")

    assert sorted(world.triggered) == ["dag_a", "dag_b"]
    assert hook.status("missing_dag") == m.STATUS_FAILED
    assert hook.status("dag_a") == m.STATUS_TRIGGERED
    mapped = [s for (task_id, _), s in states.items() if task_id == "trigger_upstream_dag"]
    assert sorted(mapped) == sorted([TaskInstanceState.SUCCESS, TaskInstanceState.SUCCESS,
                                     TaskInstanceState.FAILED])
    assert dag_run.state == DagRunState.FAILED  # visible in the UI / alerts


def test_end_to_end_impala_down_fails_run(airflow_db, monkeypatch):
    monkeypatch.setattr(m, "IMPALA_ATTEMPTS", 2)
    monkeypatch.setattr(m, "IMPALA_BACKOFF_SECONDS", 0)
    hook = FakeImpalaHook.with_rows(row("dag_a"), fail_runs=100)
    world = World(monkeypatch, hook)

    dag_run, states = world.run("12:00")

    assert states[("reconcile_triggered", -1)] == TaskInstanceState.FAILED
    assert states[("check_ready_list", -1)] == TaskInstanceState.UPSTREAM_FAILED
    assert world.triggered == []
    assert dag_run.state == DagRunState.FAILED
