from __future__ import annotations

from unittest import mock

import pytest
import test_upstream_dly_maintenance as mt


def test_dag_definition():
    dag = mt.dag
    assert dag.timetable.summary == "45 2 * * *"
    assert str(dag.timezone.name) == "Asia/Qatar"
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert dag.get_task("optimize_table").downstream_task_ids == {"expire_snapshots"}


def test_statements():
    assert mt.OPTIMIZE_SQL == "OPTIMIZE TABLE dev_stg.upstream_trigger_log"
    assert mt.EXPIRE_SNAPSHOTS_SQL == (
        "ALTER TABLE dev_stg.upstream_trigger_log EXECUTE expire_snapshots(now() - INTERVAL 7 DAYS)"
    )


def test_run_with_retry_retries_commit_conflicts():
    hook = mock.Mock()
    hook.run.side_effect = [RuntimeError("CommitFailedException"), None]
    sleeps = []
    mt.run_with_retry(hook, mt.OPTIMIZE_SQL, sleep=sleeps.append)
    assert hook.run.call_count == 2 and len(sleeps) == 1


def test_run_with_retry_gives_up():
    hook = mock.Mock()
    hook.run.side_effect = RuntimeError("down")
    with pytest.raises(RuntimeError):
        mt.run_with_retry(hook, mt.OPTIMIZE_SQL, attempts=3, sleep=lambda _: None)
    assert hook.run.call_count == 3


def test_tasks_run_the_statements(airflow_db, monkeypatch):
    hook = mock.Mock()
    monkeypatch.setattr(mt, "ImpalaHook", lambda **_: hook)
    dag_run = mt.dag.test()
    assert dag_run.state == "success"
    assert [c.args[0] for c in hook.run.call_args_list] == [mt.OPTIMIZE_SQL, mt.EXPIRE_SNAPSHOTS_SQL]
