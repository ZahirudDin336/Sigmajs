"""Run the DAG's Impala queries, exactly as impyla sends them, on local Spark SQL.

Spark understands the same SQL (TRIM, UPPER, CASE, CAST AS TIMESTAMP, GROUP BY,
sub-queries), so this checks the real query text and the duplicate handling
against real rows rather than against a Python emulation.
"""
from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")
import test_upstream_dly as m  # noqa: E402
from impala.interface import _bind_parameters  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

RUN_DATE = "2026-10-11"
T = datetime(2026, 10, 11, 8, 0)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = (
        SparkSession.builder.appName("test_impala_sql").master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
        .getOrCreate()
    )
    session.sql("CREATE DATABASE IF NOT EXISTS dev_stg")
    yield session
    session.sql("DROP DATABASE IF EXISTS dev_stg CASCADE")
    session.stop()


@pytest.fixture
def log_table(spark):
    def load(rows):
        spark.sql(f"DROP TABLE IF EXISTS {m.TRIGGER_LOG_TABLE}")
        spark.createDataFrame(
            rows, "dag_name string, status string, insert_load_id int, source_insert_dt timestamp"
        ).write.saveAsTable(m.TRIGGER_LOG_TABLE)
    yield load
    spark.sql(f"DROP TABLE IF EXISTS {m.TRIGGER_LOG_TABLE}")


def run_query(spark, sql):
    return [tuple(r) for r in spark.sql(_bind_parameters(sql, m.day_bounds(RUN_DATE))).collect()]


DUPLICATED_DAY = [
    ("dag_1", "SUCCEEDED", 1, T), ("dag_1", "SUCCEEDED", 1, T),                   # exact duplicate
    ("dag_2", "SUCCEEDED", 2, T), ("dag_2", "SUCCEEDED", 3, T),                   # two datasets
    ("dag_3", "FAILED", 4, T), ("dag_3", "READY", 5, T),                          # late row after failure
    ("dag_4", " succeeded ", 6, T),                                               # dirty status
    ("dag_5 ", "SUCCEEDED", 7, T), ("dag_5", "SUCCEEDED", 8, T),                  # stray space
    ("dag_6", "TRIGGERED", 9, T), ("dag_6", "FAILED", 10, T),                     # running again
    ("dag_7", "SUCCEEDED", 11, datetime(2026, 10, 10, 23, 59)),                   # yesterday
    ("dag_8", "SUCCEEDED", 12, datetime(2026, 10, 12, 0, 0)),                     # tomorrow
    (None, "READY", 13, T), ("  ", "READY", 14, T),                               # no DAG
]


def test_summary_query_returns_one_row_per_distinct_dag(spark, log_table):
    log_table(DUPLICATED_DAY)
    result = sorted(run_query(spark, m.SELECT_DAY_SUMMARY_SQL))
    assert result == [
        ("dag_1", "SUCCEEDED", 2),
        ("dag_2", "SUCCEEDED", 2),
        ("dag_3", "FAILED", 2),
        ("dag_4", "SUCCEEDED", 1),
        ("dag_5", "SUCCEEDED", 2),
        ("dag_6", "TRIGGERED", 2),
    ]


def test_summary_drives_day_complete(spark, log_table):
    log_table(DUPLICATED_DAY)

    class Hook:  # runs the statements on Spark
        def run(self, sql, parameters=None):
            assert sql == m.REFRESH_SQL

        def get_records(self, sql, parameters=None):
            return [tuple(r) for r in spark.sql(_bind_parameters(sql, parameters)).collect()]

    state = m.fetch_day_summary(Hook(), RUN_DATE)
    assert len(state.finished) == 5 and state.running == ["dag_6"]
    assert not m.is_day_complete(state, 5)  # dag_6 still running

    log_table([r for r in DUPLICATED_DAY if r[0] != "dag_6"] + [("dag_6", "SUCCEEDED", 9, T)])
    state = m.fetch_day_summary(Hook(), RUN_DATE)
    assert len(state.finished) == 6
    assert m.is_day_complete(state, 6)


def test_state_query_collapses_duplicate_rows(spark, log_table):
    log_table(DUPLICATED_DAY)
    result = sorted(run_query(spark, m.SELECT_DAY_STATE_SQL), key=lambda r: (r[0], r[2]))
    assert [(d, s, i, c) for d, s, i, _, c in result] == [
        ("dag_1", "SUCCEEDED", 1, 2),
        ("dag_2", "SUCCEEDED", 2, 1), ("dag_2", "SUCCEEDED", 3, 1),
        ("dag_3", "FAILED", 4, 1), ("dag_3", "READY", 5, 1),
        ("dag_4", "SUCCEEDED", 6, 1),
        ("dag_5", "SUCCEEDED", 7, 1), ("dag_5", "SUCCEEDED", 8, 1),
        ("dag_6", "TRIGGERED", 9, 1), ("dag_6", "FAILED", 10, 1),
    ]
