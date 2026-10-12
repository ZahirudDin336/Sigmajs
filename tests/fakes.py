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

    def __init__(self, rows: list[dict] | None = None, fail_runs: int = 0, fail_selects: int = 0,
                 fail_updates: int = 0):
        self.rows = rows or []
        self.fail_runs = fail_runs
        self.fail_updates = fail_updates
        self.fail_selects = fail_selects
        self.executed: list[str] = []

    _next_load_id = 1000

    @classmethod
    def row(cls, dag_name, status=m.STATUS_READY, day="2026-10-11", time="08:00:00", dataset=None,
            load_id=None):
        if load_id is None:
            cls._next_load_id += 1
            load_id = cls._next_load_id
        return {"dataset_name": dataset or f"ds_{dag_name}", "dag_name": dag_name, "status": status,
                "insert_load_id": load_id, "source_insert_dt": f"{day} {time}",
                "triggered_ts": None, "updated_ts": None}

    def load_ids(self, dag_name: str, day: str = "2026-10-11") -> list[int]:
        return sorted(r["insert_load_id"] for r in self.rows
                      if r["dag_name"] == dag_name and r["source_insert_dt"].startswith(day))

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
        rows = [r for r in self.rows
                if r["dag_name"] is not None and r["dag_name"].strip() and self._in_day(r, parameters)]
        if sql == m.SELECT_DAY_STATE_SQL:
            groups: dict[tuple, list] = {}
            for r in rows:
                key = (r["dag_name"].strip(), r["status"].strip().upper(), r["insert_load_id"])
                groups.setdefault(key, []).append(r["source_insert_dt"])
            return [(*key, min(dts), len(dts)) for key, dts in groups.items()]
        assert sql == m.SELECT_DAY_SUMMARY_SQL, sql
        rank = {m.STATUS_TRIGGERED: 4, m.STATUS_FAILED: 3, m.STATUS_SUCCEEDED: 2, m.STATUS_READY: 1}
        names = {4: m.STATUS_TRIGGERED, 3: m.STATUS_FAILED, 2: m.STATUS_SUCCEEDED, 1: m.STATUS_READY}
        summary: dict[str, list] = {}
        for r in rows:
            summary.setdefault(r["dag_name"].strip(), []).append(rank.get(r["status"].strip().upper(), 0))
        return [(dag, names.get(max(ranks), "UNKNOWN"), len(ranks)) for dag, ranks in summary.items()]

    def run(self, sql, parameters=None):
        if self.fail_runs:
            self.fail_runs -= 1
            raise RuntimeError("CommitFailedException: conflicting delete files")
        if self.fail_updates and sql.lstrip().startswith("UPDATE"):
            self.fail_updates -= 1
            raise RuntimeError("CommitFailedException: conflicting delete files")
        self.executed.append(_bind_parameters(sql, parameters) if parameters else sql)
        if sql == m.REFRESH_SQL:
            return
        match = re.search(r"status IN \(([^)]*)\)", sql)
        assert sql.lstrip().startswith(f"UPDATE {m.TRIGGER_LOG_TABLE}") and match, sql
        from_statuses = {s.strip().strip("'") for s in match.group(1).split(",")}
        for row in self.rows:
            load_ids = {v for k, v in parameters.items() if k.startswith("load_id_")}
            if (row["insert_load_id"] in load_ids
                    and row["dag_name"] == parameters["dag_name"] and row["status"] in from_statuses
                    and self._in_day(row, parameters)):
                row["status"] = parameters["status"]
                row["updated_ts"] = NOW
                if "triggered_ts = now()" in sql:
                    row["triggered_ts"] = NOW
