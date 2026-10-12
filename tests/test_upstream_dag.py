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
    assert dag.max_active_runs == 1  # only one master run at a time
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
    # Across ALL master runs, not just one.
    assert trigger.partial_kwargs["max_active_tis_per_dag"] == 6
    assert trigger.partial_kwargs["execution_timeout"] > m.CHILD_DAG_TIMEOUT


def test_steps_never_run_twice_at_once():
    for task_id in ("reconcile_triggered", "check_day_pending", "check_ready_list", "get_dags_to_trigger"):
        assert m.dag.get_task(task_id).max_active_tis_per_dag == 1, task_id


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
    state = m.DayState(ready=[f"r{i}" for i in range(ready)], running=[f"x{i}" for i in range(running)],
                       load_ids={f"r{i}": {m.STATUS_READY: [i]} for i in range(ready)})
    selected = m.select_dags_to_trigger(state)
    assert selected == [{"dag_name": f"r{i}", "insert_load_ids": [i]} for i in range(expected)]


def test_fetch_day_state_collects_insert_load_ids():
    hook = FakeImpalaHook.with_rows(
        row("dag_a", load_id=11), row("dag_a", load_id=12, dataset="ds2"),
        row("dag_b", m.STATUS_TRIGGERED, load_id=21), row("dag_b", m.STATUS_READY, load_id=22, dataset="late"),
        row("dag_c", load_id=None), row("dag_d", load_id=None), row("dag_d", load_id=41, dataset="ds2"),
    )
    hook.rows[4]["insert_load_id"] = hook.rows[5]["insert_load_id"] = None
    state = m.fetch_day_state(hook, RUN_DATE)
    assert state.ready == ["dag_a", "dag_d"]  # dag_c has no usable row
    assert state.ids("dag_a", m.STATUS_READY) == [11, 12]
    assert state.ids("dag_b", m.STATUS_TRIGGERED) == [21]
    assert state.ids("dag_b", m.STATUS_READY, m.STATUS_TRIGGERED) == [21, 22]
    assert state.ids("dag_d", m.STATUS_READY) == [41]
    assert m.select_dags_to_trigger(state) == [{"dag_name": "dag_a", "insert_load_ids": [11, 12]},
                                               {"dag_name": "dag_d", "insert_load_ids": [41]}]


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


def test_day_summary_counts_distinct_dags_despite_duplicates():
    hook = FakeImpalaHook.with_rows(
        row("dag_1", m.STATUS_SUCCEEDED, load_id=1), row("dag_1", m.STATUS_SUCCEEDED, load_id=1),  # exact dup
        row("dag_2", m.STATUS_SUCCEEDED, load_id=2), row("dag_2", m.STATUS_SUCCEEDED, load_id=3, dataset="b"),
        row("dag_3", m.STATUS_FAILED, load_id=4), row("dag_3", m.STATUS_READY, load_id=5, dataset="late"),
        row("dag_4", " succeeded ", load_id=6),                                                     # dirty status
        row("dag_5 ", m.STATUS_SUCCEEDED, load_id=7), row("dag_5", m.STATUS_SUCCEEDED, load_id=8),   # stray space
        row("dag_6", m.STATUS_FAILED, load_id=9),
        row("dag_7", m.STATUS_SUCCEEDED, day="2026-10-10", load_id=10),                             # yesterday
        row(m.DAG_ID, m.STATUS_SUCCEEDED, load_id=11),                                              # itself
    )
    assert len([r for r in hook.rows if r["source_insert_dt"].startswith(RUN_DATE)]) == 11

    state = m.fetch_day_summary(hook, RUN_DATE, exclude=[m.DAG_ID])

    assert state.succeeded == ["dag_1", "dag_2", "dag_4", "dag_5"]
    assert state.failed == ["dag_3", "dag_6"]
    assert state.ready == [] and state.running == []
    assert len(state.finished) == 6
    assert m.is_day_complete(state, 6)
    sql = " ".join(hook.executed[-1].split())
    assert "GROUP BY dag_name" in sql
    assert "source_insert_dt >= CAST('2026-10-11 00:00:00' AS TIMESTAMP)" in sql


def test_day_summary_running_duplicate_keeps_day_open():
    hook = FakeImpalaHook.with_rows(
        *[row(f"dag_{i}", m.STATUS_SUCCEEDED) for i in range(6)],
        row("dag_0", m.STATUS_TRIGGERED, dataset="rerun"),  # dag_0 is running again
    )
    state = m.fetch_day_summary(hook, RUN_DATE)
    assert state.running == ["dag_0"]
    assert len(state.finished) == 5
    assert not m.is_day_complete(state, 6)


def test_day_state_reports_duplicate_rows(caplog):
    hook = FakeImpalaHook.with_rows(row("dag_a", load_id=1), row("dag_a", load_id=1))
    state = m.fetch_day_state(hook, RUN_DATE)
    assert state.ids("dag_a", m.STATUS_READY) == [1]
    assert "2 duplicate READY rows with insert_load_id=1" in caplog.text


def test_expected_dag_count_default_and_variable(airflow_db, monkeypatch):
    assert m.get_expected_dag_count() == 6
    monkeypatch.setenv("AIRFLOW_VAR_UPSTREAM_EXPECTED_DAG_COUNT", "8")
    assert m.get_expected_dag_count() == 8
    for bad in ("six", "0", "-1"):
        monkeypatch.setenv("AIRFLOW_VAR_UPSTREAM_EXPECTED_DAG_COUNT", bad)
        with pytest.raises(AirflowException, match="upstream_expected_dag_count"):
            m.get_expected_dag_count()


def test_update_status_sql_and_effect():
    hook = FakeImpalaHook.with_rows(row("dag_a", load_id=11), row("dag_a", load_id=12, dataset="ds2"),
                                    row("dag_a", load_id=13, dataset="late"), row("dag_a", day="2026-10-10"))
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                    from_statuses=[m.STATUS_READY], insert_load_ids=[12, 11, 12], set_triggered_ts=True)
    sql = " ".join(hook.executed[0].split())
    assert sql == ("UPDATE dev_stg.upstream_trigger_log SET status = 'TRIGGERED', "
                   "triggered_ts = now(), updated_ts = now() WHERE insert_load_id IN (11, 12) "
                   "AND dag_name = 'dag_a' AND status IN ('READY') "
                   "AND source_insert_dt >= CAST('2026-10-11 00:00:00' AS TIMESTAMP) "
                   "AND source_insert_dt < CAST('2026-10-12 00:00:00' AS TIMESTAMP)")
    # Only the given insert_load_ids change.
    assert hook.statuses("dag_a") == [m.STATUS_TRIGGERED, m.STATUS_TRIGGERED, m.STATUS_READY]
    assert all(r["triggered_ts"] == NOW and r["updated_ts"] == NOW for r in hook.rows[:2])
    assert hook.rows[2]["updated_ts"] is None
    assert hook.status("dag_a", "2026-10-10") == m.STATUS_READY  # other days untouched


def test_update_status_requires_insert_load_ids():
    hook = FakeImpalaHook()
    with pytest.raises(ValueError, match="insert_load_id"):
        m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_FAILED,
                        from_statuses=[m.STATUS_READY], insert_load_ids=[])
    assert hook.executed == []


def test_final_status_update_sets_updated_ts_only():
    hook = FakeImpalaHook.with_rows(row("dag_a", m.STATUS_TRIGGERED))
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_SUCCEEDED,
                    from_statuses=[m.STATUS_TRIGGERED], insert_load_ids=hook.load_ids("dag_a"))
    sql = " ".join(hook.executed[0].split())
    assert "SET status = 'SUCCEEDED', updated_ts = now()" in sql
    assert "triggered_ts" not in sql
    assert hook.rows[0]["updated_ts"] == NOW and hook.rows[0]["triggered_ts"] is None


def test_update_status_escapes_values():
    hook = FakeImpalaHook()
    m.update_status(hook, dag_name="x'; DROP TABLE t; --", run_date=RUN_DATE, status=m.STATUS_FAILED,
                    from_statuses=[m.STATUS_READY], insert_load_ids=[1])
    assert "dag_name = 'x\\'; DROP TABLE t; --'" in hook.executed[0]


def test_impala_retries_commit_conflicts():
    hook = FakeImpalaHook.with_rows(row("dag_a"), fail_runs=2)
    sleeps = []
    m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                    from_statuses=[m.STATUS_READY], insert_load_ids=hook.load_ids("dag_a"),
                    backoff_seconds=1, sleep=sleeps.append)
    assert hook.status("dag_a") == m.STATUS_TRIGGERED
    assert len(sleeps) == 2 and 1 <= sleeps[0] <= 2 and 2 <= sleeps[1] <= 3


def test_impala_gives_up_after_max_attempts():
    hook = FakeImpalaHook.with_rows(row("dag_a"), fail_runs=10)
    with pytest.raises(RuntimeError, match="CommitFailedException"):
        m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                        from_statuses=[m.STATUS_READY], insert_load_ids=hook.load_ids("dag_a"),
                        attempts=3, sleep=no_sleep)
    assert hook.fail_runs == 7
    assert hook.status("dag_a") == m.STATUS_READY


AUTH_ERROR = ("Query ca41a0f356623ce7:d2e2cb1d00000000 failed:\nAuthorizationException: User "
              "'USER@INTERNAL.COMP.US' does not have privileges to access: dev_stg.upstream_trigger_log")


def test_authorization_error_fails_at_once_without_retries():
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    hook.run = mock.Mock(side_effect=RuntimeError(AUTH_ERROR))
    sleep = mock.Mock()
    with pytest.raises(m.ImpalaPermissionError, match="lacks privileges on dev_stg.upstream_trigger_log"):
        m.update_status(hook, dag_name="dag_a", run_date=RUN_DATE, status=m.STATUS_TRIGGERED,
                        from_statuses=[m.STATUS_READY], insert_load_ids=[1], sleep=sleep)
    hook.run.assert_called_once()
    sleep.assert_not_called()


def test_authorization_error_on_read_fails_before_anything_is_triggered(airflow_db, monkeypatch):
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    hook.run = mock.Mock(side_effect=RuntimeError(AUTH_ERROR))  # REFRESH / UPDATE refused
    world = World(monkeypatch, hook)
    dag_run, states = world.run("13:00")
    assert states[("reconcile_triggered", -1)] == TaskInstanceState.FAILED
    assert states[("trigger_upstream_dag", -1)] == TaskInstanceState.UPSTREAM_FAILED
    assert world.triggered == []
    assert world.cde_runs == 0


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
# trigger_and_wait
# --------------------------------------------------------------------------- #
CHILD_RUN_1 = f"upstream__{RUN_DATE}__1"


def _run(hook, trigger=None, wait=None, existing=(), parent_run_id="scheduled__1", load_ids=None):
    return m.trigger_and_wait(
        dag_name="dag_a", insert_load_ids=hook.load_ids("dag_a") if load_ids is None else load_ids,
        run_date=RUN_DATE, parent_dag_id=m.DAG_ID, parent_run_id=parent_run_id, hook=hook,
        trigger=trigger or mock.Mock(return_value=CHILD_RUN_1),
        wait=wait or mock.Mock(return_value="success"),
        list_runs=lambda *_: list(existing),
    )


def test_success_waits_then_marks_succeeded():
    hook = FakeImpalaHook.with_rows(row("dag_a"), row("dag_a", dataset="ds2"))
    trigger = mock.Mock(return_value=CHILD_RUN_1)
    seen = []

    def wait(dag_id, run_id):
        seen.append((run_id, hook.statuses("dag_a")))
        return "success"

    assert _run(hook, trigger, wait) == "success"
    trigger.assert_called_once_with(
        "dag_a", CHILD_RUN_1,
        conf={"run_date": RUN_DATE, "parent_dag_id": m.DAG_ID, "parent_run_id": "scheduled__1",
              "insert_load_ids": hook.load_ids("dag_a")},
    )
    # TRIGGERED while waiting, SUCCEEDED after.
    assert seen == [(CHILD_RUN_1, [m.STATUS_TRIGGERED, m.STATUS_TRIGGERED])]
    assert hook.statuses("dag_a") == [m.STATUS_SUCCEEDED, m.STATUS_SUCCEEDED]
    assert all(r["triggered_ts"] == NOW and r["updated_ts"] == NOW for r in hook.rows)
    final_sql = " ".join(hook.executed[-1].split())
    assert "SET status = 'SUCCEEDED', updated_ts = now()" in final_sql


def test_child_failure_marks_failed_and_raises():
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    with pytest.raises(AirflowException, match="finished as failed"):
        _run(hook, wait=mock.Mock(return_value="failed"))
    assert hook.status("dag_a") == m.STATUS_FAILED
    assert hook.rows[0]["updated_ts"] == NOW
    assert "SET status = 'FAILED', updated_ts = now()" in " ".join(hook.executed[-1].split())


@pytest.mark.parametrize("error", ["DAG 'dag_a' does not exist", "DAG 'dag_a' is paused"])
def test_trigger_error_marks_failed(error):
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    wait = mock.Mock()
    with pytest.raises(AirflowException, match=error):
        _run(hook, trigger=mock.Mock(side_effect=AirflowException(error)), wait=wait)
    wait.assert_not_called()
    assert hook.status("dag_a") == m.STATUS_FAILED
    assert hook.rows[0]["updated_ts"] == NOW and hook.rows[0]["triggered_ts"] is None


def test_trigger_error_kept_when_failed_update_also_fails(monkeypatch):
    monkeypatch.setattr(m, "IMPALA_ATTEMPTS", 1)
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    hook.fail_updates = 5
    with pytest.raises(AirflowException, match="does not exist"):
        _run(hook, trigger=mock.Mock(side_effect=AirflowException("DAG 'dag_a' does not exist")))


def test_wait_timeout_leaves_triggered_for_reconcile():
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    with pytest.raises(m.ChildRunTimeout):
        _run(hook, wait=mock.Mock(side_effect=m.ChildRunTimeout("still running")))
    assert hook.status("dag_a") == m.STATUS_TRIGGERED


def test_triggered_update_failure_does_not_stop_waiting(monkeypatch):
    monkeypatch.setattr(m, "IMPALA_ATTEMPTS", 1)
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    hook.fail_updates = 1  # the TRIGGERED update
    assert _run(hook) == "success"
    assert hook.status("dag_a") == m.STATUS_SUCCEEDED  # final update also covers READY rows


def test_retry_reattaches_to_own_run():
    hook = FakeImpalaHook.with_rows(row("dag_a", m.STATUS_TRIGGERED))
    trigger, wait = mock.Mock(), mock.Mock(return_value="success")
    own = m.ChildRun(CHILD_RUN_1, "running", "scheduled__1")
    assert _run(hook, trigger, wait, existing=[own]) == "success"
    trigger.assert_not_called()
    wait.assert_called_once_with("dag_a", CHILD_RUN_1)
    assert hook.status("dag_a") == m.STATUS_SUCCEEDED


def test_skips_dag_owned_by_another_master_run():
    hook = FakeImpalaHook.with_rows(row("dag_a", m.STATUS_TRIGGERED))
    trigger, wait = mock.Mock(), mock.Mock()
    other = m.ChildRun(CHILD_RUN_1, "running", "scheduled__0")
    assert _run(hook, trigger, wait, existing=[other]) == "skipped"
    trigger.assert_not_called()
    wait.assert_not_called()
    assert hook.status("dag_a") == m.STATUS_TRIGGERED


def test_skips_dag_finished_while_waiting_for_a_slot():
    hook = FakeImpalaHook.with_rows(row("dag_a", m.STATUS_SUCCEEDED))
    trigger = mock.Mock()
    done = m.ChildRun(CHILD_RUN_1, "success", "scheduled__0")
    assert _run(hook, trigger, existing=[done]) == "skipped"
    trigger.assert_not_called()
    assert hook.status("dag_a") == m.STATUS_SUCCEEDED


def test_skips_when_another_master_run_wins_the_trigger_race():
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    trigger = mock.Mock(side_effect=m.ChildRunAlreadyExists("taken"))
    assert _run(hook, trigger) == "skipped"
    assert hook.status("dag_a") == m.STATUS_READY


def test_rerun_after_reset_to_ready_uses_next_attempt():
    hook = FakeImpalaHook.with_rows(row("dag_a"))  # FAILED row was reset to READY
    trigger = mock.Mock(return_value=f"upstream__{RUN_DATE}__2")
    failed_before = m.ChildRun(CHILD_RUN_1, "failed", "scheduled__0")
    assert _run(hook, trigger, existing=[failed_before]) == "success"
    assert trigger.call_args.args[1] == f"upstream__{RUN_DATE}__2"
    assert hook.status("dag_a") == m.STATUS_SUCCEEDED


def test_only_selected_rows_are_updated():
    # A row for the same DAG that became READY after selection keeps its status.
    hook = FakeImpalaHook.with_rows(row("dag_a", load_id=11), row("dag_a", load_id=12, dataset="late"))
    assert _run(hook, load_ids=[11]) == "success"
    assert {r["insert_load_id"]: r["status"] for r in hook.rows} == {11: m.STATUS_SUCCEEDED, 12: m.STATUS_READY}
    assert all("insert_load_id IN (11)" in sql for sql in hook.executed if sql.lstrip().startswith("UPDATE"))


def test_failure_updates_only_selected_rows():
    hook = FakeImpalaHook.with_rows(row("dag_a", load_id=11), row("dag_a", load_id=12, dataset="late"))
    with pytest.raises(AirflowException):
        _run(hook, wait=mock.Mock(return_value="failed"), load_ids=[11])
    assert {r["insert_load_id"]: r["status"] for r in hook.rows} == {11: m.STATUS_FAILED, 12: m.STATUS_READY}


def test_no_insert_load_ids_is_rejected():
    hook = FakeImpalaHook.with_rows(row("dag_a"))
    trigger = mock.Mock()
    with pytest.raises(AirflowException, match="insert_load_id"):
        _run(hook, trigger, load_ids=[])
    trigger.assert_not_called()


def test_skips_when_selected_rows_are_no_longer_ready():
    hook = FakeImpalaHook.with_rows(row("dag_a", m.STATUS_SUCCEEDED, load_id=11),
                                    row("dag_a", load_id=12, dataset="late"))
    trigger = mock.Mock()
    assert _run(hook, trigger, load_ids=[11]) == "skipped"
    trigger.assert_not_called()


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

    run_id = m.trigger_child_dag("child_dag", m.build_child_run_id(RUN_DATE, 1),
                                 conf={"run_date": RUN_DATE, "parent_run_id": "p1"})
    (dag_run,) = DagRun.find(dag_id="child_dag", run_id=run_id)
    assert run_id == "upstream__2026-10-11__1"
    assert dag_run.state == DagRunState.QUEUED
    assert dag_run.conf == {"run_date": RUN_DATE, "parent_run_id": "p1"}


def test_trigger_child_dag_same_run_id_twice_is_rejected(child_dag):
    from airflow.models import DagRun

    m.trigger_child_dag("child_dag", "upstream__x__1")
    with pytest.raises(m.ChildRunAlreadyExists):
        m.trigger_child_dag("child_dag", "upstream__x__1")
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


def test_list_and_find_child_runs(child_dag):
    assert m.find_latest_child_run("child_dag", RUN_DATE) is None

    first = m.trigger_child_dag("child_dag", m.build_child_run_id(RUN_DATE, 1), conf={"parent_run_id": "p1"})
    _set_state(first, DagRunState.FAILED)
    m.trigger_child_dag("child_dag", m.build_child_run_id("2026-10-12", 1))  # another day
    m.trigger_child_dag("child_dag", "manual__something")                    # not ours
    assert m.find_latest_child_run("child_dag", RUN_DATE) == (first, "failed")

    second = m.trigger_child_dag("child_dag", m.build_child_run_id(RUN_DATE, 2), conf={"parent_run_id": "p2"})
    assert m.list_child_runs("child_dag", RUN_DATE) == [
        m.ChildRun(second, "queued", "p2"), m.ChildRun(first, "failed", "p1")]
    _set_state(second, DagRunState.SUCCESS)
    assert m.find_latest_child_run("child_dag", RUN_DATE) == (second, "success")


def test_find_latest_child_run_escapes_like_wildcards(child_dag):
    m.trigger_child_dag("child_dag", "upstreamX_2026-10-11__1")
    assert m.find_latest_child_run("child_dag", RUN_DATE) is None


@pytest.mark.parametrize("final_state", [DagRunState.SUCCESS, DagRunState.FAILED])
def test_wait_for_dag_run_returns_final_state(child_dag, final_state):
    run_id = m.trigger_child_dag("child_dag", f"wait_{final_state}")
    polls = []

    def fake_sleep(_seconds):
        polls.append(1)
        _set_state(run_id, DagRunState.RUNNING if len(polls) == 1 else final_state)

    assert m.wait_for_dag_run("child_dag", run_id, poke_interval=0, sleep=fake_sleep) == final_state.value
    assert len(polls) == 2


def test_wait_for_dag_run_times_out(child_dag):
    run_id = m.trigger_child_dag("child_dag", "wait_timeout")
    now = [0.0]

    def fake_sleep(seconds):
        now[0] += seconds

    with pytest.raises(m.ChildRunTimeout):
        m.wait_for_dag_run("child_dag", run_id, poke_interval=60, timeout=300, sleep=fake_sleep,
                           clock=lambda: now[0])
    assert now[0] == 300


def test_wait_for_dag_run_missing_run(airflow_db):
    with pytest.raises(AirflowException, match="not found"):
        m.wait_for_dag_run("child_dag", "missing")


def test_trigger_and_wait_against_real_metadata_db(child_dag):
    hook = FakeImpalaHook.with_rows(row("child_dag"))

    def finish(dag_id, run_id):
        _set_state(run_id, DagRunState.SUCCESS)
        return m.wait_for_dag_run(dag_id, run_id, poke_interval=0)

    ids = hook.load_ids("child_dag")
    assert m.trigger_and_wait(dag_name="child_dag", insert_load_ids=ids, run_date=RUN_DATE,
                              parent_dag_id=m.DAG_ID, parent_run_id="p1", hook=hook, wait=finish) == "success"
    assert hook.status("child_dag") == m.STATUS_SUCCEEDED
    # A second master run for the same day does not trigger it again.
    assert m.trigger_and_wait(dag_name="child_dag", insert_load_ids=ids, run_date=RUN_DATE,
                              parent_dag_id=m.DAG_ID, parent_run_id="p2", hook=hook) == "skipped"
    assert len(m.list_child_runs("child_dag", RUN_DATE)) == 1


def test_reconcile_against_real_child_run(child_dag):
    run_id = m.trigger_child_dag("child_dag", m.build_child_run_id(RUN_DATE, 1))
    hook = FakeImpalaHook.with_rows(row("child_dag", m.STATUS_TRIGGERED))
    assert m.reconcile_triggered_dags(hook, RUN_DATE)["running"] == ["child_dag"]
    _set_state(run_id, DagRunState.SUCCESS)
    assert m.reconcile_triggered_dags(hook, RUN_DATE)["succeeded"] == ["child_dag"]
    assert hook.status("child_dag") == m.STATUS_SUCCEEDED


# --------------------------------------------------------------------------- #
# End to end: several 15-minute runs of the whole DAG with dag.test()
# --------------------------------------------------------------------------- #
class World:
    """Child DAGs as the master DAG sees them: ``outcome`` is how each one ends."""

    def __init__(self, monkeypatch, hook):
        self.hook = hook
        self.outcome: dict[str, str] = {}
        self.runs: dict[str, list[m.ChildRun]] = {}
        self.cde_runs = 0
        self.seen_while_waiting: dict[str, str] = {}
        monkeypatch.setenv("AIRFLOW_CONN_ORACLE-CONN-EDWDB", EDW_CONN)
        monkeypatch.setattr(m, "get_impala_hook", lambda: hook)
        monkeypatch.setattr(m, "trigger_child_dag", self.trigger)
        monkeypatch.setattr(m, "list_child_runs", lambda dag_id, _: list(reversed(self.runs.get(dag_id, []))))
        monkeypatch.setattr(m, "find_latest_child_run",
                            lambda dag_id, _: (lambda r: (r[-1].run_id, r[-1].state) if r else None)(
                                self.runs.get(dag_id, [])))
        monkeypatch.setattr(m, "wait_for_dag_run", self.wait)

        def execute(op, context, **_):  # stands in for the CDE API call
            self.cde_runs += 1

        monkeypatch.setattr(m.CDEJobRunOperator, "execute", execute)

    @property
    def triggered(self) -> list[str]:
        return [dag for dag, runs in self.runs.items() for _ in runs]

    def trigger(self, dag_id, run_id, conf=None):
        if dag_id == "missing_dag":
            raise AirflowException("DAG 'missing_dag' does not exist")
        self.runs.setdefault(dag_id, []).append(m.ChildRun(run_id, "queued", conf["parent_run_id"]))
        return run_id

    def wait(self, dag_id, run_id):
        self.seen_while_waiting[dag_id] = self.hook.status(dag_id)
        state = self.outcome.get(dag_id, "success")
        self.runs[dag_id][-1] = m.ChildRun(run_id, state, self.runs[dag_id][-1].parent_run_id)
        return state

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
    world.outcome.update(dag_2="failed")

    # 08:00 - five DAGs READY: triggered in parallel, each waited for and finalised.
    dag_run, states = world.run("08:00")
    assert sorted(world.triggered) == names[:5]
    assert world.seen_while_waiting == {n: m.STATUS_TRIGGERED for n in names[:5]}
    assert [hook.status(n) for n in names[:5]] == [m.STATUS_SUCCEEDED, m.STATUS_SUCCEEDED, m.STATUS_FAILED,
                                                   m.STATUS_SUCCEEDED, m.STATUS_SUCCEEDED]
    assert all(r["updated_ts"] == NOW for r in hook.rows if r["dag_name"] in names)
    assert hook.status("dag_old", "2026-10-10") == m.STATUS_READY
    mapped = [s for (task_id, _), s in states.items() if task_id == "trigger_upstream_dag"]
    assert sorted(mapped) == sorted([TaskInstanceState.SUCCESS] * 4 + [TaskInstanceState.FAILED])
    labels = {ti.rendered_map_index for ti in dag_run.get_task_instances() if ti.task_id == "trigger_upstream_dag"}
    assert labels == set(names[:5])  # the UI shows the DAG name per mapped task
    assert states[("end", -1)] == TaskInstanceState.UPSTREAM_FAILED
    assert dag_run.state == DagRunState.FAILED  # a DAG failed: visible in the UI / alerts

    # 08:15 - nothing new: nothing triggered again.
    dag_run, states = world.run("08:15")
    assert dag_run.state == DagRunState.SUCCESS
    assert states[("trigger_upstream_dag", -1)] == TaskInstanceState.SKIPPED
    assert len(world.triggered) == 5

    # 08:30 - the sixth DAG becomes READY and succeeds.
    hook.rows.append(row("dag_5", time="08:20:00"))
    dag_run, states = world.run("08:30")
    assert dag_run.state == DagRunState.SUCCESS
    assert hook.status("dag_5") == m.STATUS_SUCCEEDED
    assert sorted(world.triggered) == names

    # 08:45 - all six finished (plus duplicate rows): run stops early, no Spark job, until tomorrow.
    hook.rows.append(dict(hook.rows[0]))
    hook.rows.append(row("dag_1", m.STATUS_SUCCEEDED, dataset="second_dataset"))
    cde_runs = world.cde_runs
    for hhmm in ("08:45", "09:00"):
        dag_run, states = world.run(hhmm)
        assert states[("check_day_pending", -1)] == TaskInstanceState.SUCCESS
        assert states[("check_ready_list", -1)] == TaskInstanceState.SKIPPED
        assert states[("end", -1)] == TaskInstanceState.SKIPPED
    assert world.cde_runs == cde_runs
    assert len(world.triggered) == 6


def test_end_to_end_at_most_six_selected(airflow_db, monkeypatch):
    names = [f"dag_{i}" for i in range(9)]
    monkeypatch.setenv("AIRFLOW_VAR_UPSTREAM_EXPECTED_DAG_COUNT", "9")
    hook = FakeImpalaHook.with_rows(*[row(n, time=f"0{i}:00:00") for i, n in enumerate(names)])
    world = World(monkeypatch, hook)

    world.run("10:00")
    assert sorted(world.triggered) == names[:6]  # oldest first, capped at 6
    assert [hook.status(n) for n in names[6:]] == [m.STATUS_READY] * 3

    world.run("10:15")
    assert sorted(world.triggered) == names
    assert all(hook.status(n) == m.STATUS_SUCCEEDED for n in names)


def test_end_to_end_running_dags_count_against_the_limit(airflow_db, monkeypatch):
    # Four DAGs are still running from an earlier master run.
    hook = FakeImpalaHook.with_rows(*[row(f"busy_{i}", m.STATUS_TRIGGERED) for i in range(4)],
                                    *[row(f"new_{i}", time=f"0{i}:00:00") for i in range(4)])
    world = World(monkeypatch, hook)
    for i in range(4):
        world.runs[f"busy_{i}"] = [m.ChildRun(f"upstream__{RUN_DATE}__1", "running", "earlier_run")]

    world.run("11:00")
    assert sorted(d for d in world.triggered if d.startswith("new_")) == ["new_0", "new_1"]
    assert [hook.status(f"busy_{i}") for i in range(4)] == [m.STATUS_TRIGGERED] * 4


def test_end_to_end_trigger_failure_marks_failed_and_others_continue(airflow_db, monkeypatch):
    hook = FakeImpalaHook.with_rows(row("dag_a"), row("missing_dag"), row("dag_b"))
    world = World(monkeypatch, hook)

    dag_run, states = world.run("11:00")

    assert sorted(world.triggered) == ["dag_a", "dag_b"]
    assert hook.status("missing_dag") == m.STATUS_FAILED
    assert hook.status("dag_a") == m.STATUS_SUCCEEDED
    assert dag_run.state == DagRunState.FAILED


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
