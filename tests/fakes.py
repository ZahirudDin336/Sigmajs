"""In-memory stand-in for the Impala hook, emulating dev_stg.upstream_trigger_log."""
from __future__ import annotations

import test_upstream_dly as m
from impala.interface import _bind_parameters


class FakeImpalaHook:
    """Keeps rows keyed by (dag_name, run_date) and applies the DAG's statements.

    Every statement is also rendered with impyla's own parameter binding so the
    tests see exactly the SQL text that would reach Impala.
    """

    def __init__(self, rows: dict[tuple[str, str], dict] | None = None, fail_runs: int = 0):
        self.rows = rows or {}
        self.fail_runs = fail_runs
        self.executed: list[str] = []

    @classmethod
    def with_ready(cls, run_date: str, dag_names, **kwargs) -> FakeImpalaHook:
        rows = {(d, run_date): {"dag_name": d, "run_date": run_date, "status": m.STATUS_READY} for d in dag_names}
        return cls(rows, **kwargs)

    def status(self, dag_name: str, run_date: str) -> str:
        return self.rows[(dag_name, run_date)]["status"]

    def get_records(self, sql, parameters=None):
        self.executed.append(_bind_parameters(sql, parameters))
        assert sql == m.SELECT_READY_DAGS_SQL
        return [
            (row["dag_name"],)
            for row in sorted(self.rows.values(), key=lambda r: r["dag_name"])
            if row["status"] == parameters["status"] and row["run_date"] == parameters["run_date"]
        ]

    def run(self, sql, parameters=None):
        if self.fail_runs:
            self.fail_runs -= 1
            raise RuntimeError("CommitFailedException: conflicting delete files")
        self.executed.append(_bind_parameters(sql, parameters))
        if sql == m.MARK_TRIGGERED_SQL:
            allowed = {m.STATUS_READY}
        elif sql == m.MARK_FAILED_SQL:
            allowed = {m.STATUS_READY, m.STATUS_TRIGGERED}
        else:
            raise AssertionError(f"unexpected SQL: {sql}")
        row = self.rows.get((parameters["dag_name"], parameters["run_date"]))
        if row and row["status"] in allowed:
            row.update({k: v for k, v in parameters.items() if k not in ("dag_name", "run_date")})
