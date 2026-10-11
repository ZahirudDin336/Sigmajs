"""In-memory stand-in for the Impala hook, emulating dev_stg.upstream_trigger_log."""
from __future__ import annotations

import re

import test_upstream_dly as m
from impala.interface import _bind_parameters

NOW = "2026-10-11 12:00:00"


class FakeImpalaHook:
    """Keeps log rows in a list and applies the DAG's statements to them.

    Every statement is also rendered with impyla's own parameter binding so the
    tests see exactly the SQL text that would reach Impala.
    """

    def __init__(self, rows: list[dict] | None = None, fail_runs: int = 0, fail_selects: int = 0):
        self.rows = rows or []
        self.fail_runs = fail_runs
        self.fail_selects = fail_selects
        self.executed: list[str] = []

    @staticmethod
    def row(dag_name, status=m.STATUS_READY, day="2026-10-11", time="08:00:00", dataset=None):
        return {"dataset_name": dataset or f"ds_{dag_name}", "dag_name": dag_name, "status": status,
                "source_insert_dt": f"{day} {time}", "triggered_ts": None, "updated_ts": None}

    @classmethod
    def with_rows(cls, *rows, **kwargs) -> FakeImpalaHook:
        return cls(list(rows), **kwargs)

    def statuses(self, dag_name: str, day: str = "2026-10-11") -> list[str]:
        return [r["status"] for r in self.rows if r["dag_name"] == dag_name and r["source_insert_dt"].startswith(day)]

    def status(self, dag_name: str, day: str = "2026-10-11") -> str:
        (status,) = set(self.statuses(dag_name, day))
        return status

    @staticmethod
    def _in_day(row, p):
        return p["day_start"] <= row["source_insert_dt"] < p["day_end"]

    def get_records(self, sql, parameters=None):
        if self.fail_selects:
            self.fail_selects -= 1
            raise RuntimeError("impala unavailable")
        self.executed.append(_bind_parameters(sql, parameters))
        assert sql == m.SELECT_DAY_STATE_SQL
        groups: dict[tuple, str] = {}
        for row in self.rows:
            if row["dag_name"] is not None and self._in_day(row, parameters):
                key = (row["dag_name"], row["status"])
                groups[key] = min(groups.get(key, row["source_insert_dt"]), row["source_insert_dt"])
        return [(dag, status, ts) for (dag, status), ts in groups.items()]

    def run(self, sql, parameters=None):
        if self.fail_runs:
            self.fail_runs -= 1
            raise RuntimeError("CommitFailedException: conflicting delete files")
        self.executed.append(_bind_parameters(sql, parameters) if parameters else sql)
        if sql == m.REFRESH_SQL:
            return
        match = re.search(r"status IN \(([^)]*)\)", sql)
        assert sql.lstrip().startswith(f"UPDATE {m.TRIGGER_LOG_TABLE}") and match, sql
        from_statuses = {s.strip().strip("'") for s in match.group(1).split(",")}
        for row in self.rows:
            if (row["dag_name"] == parameters["dag_name"] and row["status"] in from_statuses
                    and self._in_day(row, parameters)):
                row["status"] = parameters["status"]
                row["updated_ts"] = NOW
                if "triggered_ts = now()" in sql:
                    row["triggered_ts"] = NOW
