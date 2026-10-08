"""Tests for the CDE Spark job, run on local Spark with a real Iceberg catalog.

The Iceberg Spark runtime jar is taken from $ICEBERG_SPARK_JAR or downloaded
once from Maven Central into ~/.cache. The EDW source is emulated with an
Apache Derby database through Spark's JDBC reader (Derby ships with Spark).
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import urllib.request
from datetime import date
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
import check_ready_list as job  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
ICEBERG_VERSION = "1.6.1"
ICEBERG_ARTIFACT = f"iceberg-spark-runtime-3.5_2.12-{ICEBERG_VERSION}.jar"
CURR_DATE = "2026-10-08"
TABLE = "local.dev_stg.upstream_trigger_log"

# Mirrors sql/upstream_trigger_log.sql: Impala TIMESTAMP is Iceberg
# "timestamp without time zone", i.e. Spark TIMESTAMP_NTZ.
CREATE_TABLE = f"""
    CREATE TABLE {TABLE} (
        dag_name STRING, run_date DATE, status STRING, dag_run_id STRING,
        parent_dag_run_id STRING, error_message STRING, created_ts TIMESTAMP_NTZ,
        triggered_ts TIMESTAMP_NTZ, completed_ts TIMESTAMP_NTZ, updated_ts TIMESTAMP_NTZ)
    USING iceberg PARTITIONED BY (months(run_date))
    TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='merge-on-read')
"""


def iceberg_jar() -> str:
    if os.environ.get("ICEBERG_SPARK_JAR"):
        return os.environ["ICEBERG_SPARK_JAR"]
    path = Path.home() / ".cache" / "iceberg" / ICEBERG_ARTIFACT
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        url = (
            "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/"
            f"{ICEBERG_VERSION}/{ICEBERG_ARTIFACT}"
        )
        urllib.request.urlretrieve(url, path.with_suffix(".part"))
        path.with_suffix(".part").rename(path)
    return str(path)


def spark_conf(warehouse: Path) -> dict[str, str]:
    return {
        "spark.master": "local[2]",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "2",
        "spark.jars": iceberg_jar(),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": str(warehouse),
    }


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    return tmp_path_factory.mktemp("warehouse")


@pytest.fixture(scope="module")
def spark(warehouse):
    builder = SparkSession.builder.appName("test_check_ready_list")
    for key, value in spark_conf(warehouse).items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def log_table(spark):
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(CREATE_TABLE)
    yield TABLE
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")


def make_cfg(**overrides) -> job.JobConfig:
    values = dict(jdbc_url="jdbc:x", username="u", password="p", source_query="SELECT 1",
                  curr_date=CURR_DATE, target_table=TABLE)
    values.update(overrides)
    return job.JobConfig(**values)


def rows(spark, table=TABLE):
    return {
        (r.dag_name, r.run_date.isoformat()): r
        for r in spark.table(table).collect()
    }


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
FULL_CONF = {
    "spark.edw_jdbc_url": "jdbc:oracle:thin:@edw:1521/EDW",
    "spark.edw_username": "user",
    "spark.edw_password": "secret",
    "spark.source_query": "SELECT dag_name FROM t WHERE d = '{curr_date}'",
    "spark.curr_date": CURR_DATE,
}


def test_parse_config_defaults():
    cfg = job.parse_config(FULL_CONF)
    assert cfg.target_table == "dev_stg.upstream_trigger_log"
    assert cfg.driver is None
    assert cfg.curr_date == CURR_DATE


def test_parse_config_overrides():
    cfg = job.parse_config({**FULL_CONF, "spark.target_table": " a.b ", "spark.edw_driver": "oracle.jdbc.OracleDriver"})
    assert cfg.target_table == "a.b"
    assert cfg.driver == "oracle.jdbc.OracleDriver"


@pytest.mark.parametrize("key", sorted(FULL_CONF))
def test_parse_config_requires_every_setting(key):
    conf = {**FULL_CONF, key: "  "}
    with pytest.raises(ValueError, match=key):
        job.parse_config(conf)


@pytest.mark.parametrize("bad", ["08-10-2026", "2026-13-01", "{{ ds }}"])
def test_parse_config_rejects_bad_date(bad):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        job.parse_config({**FULL_CONF, "spark.curr_date": bad})


def test_render_source_query():
    assert job.render_source_query(" SELECT a FROM t WHERE d='{curr_date}'; ", CURR_DATE) == (
        "SELECT a FROM t WHERE d='2026-10-08'"
    )


# --------------------------------------------------------------------------- #
# Transformations
# --------------------------------------------------------------------------- #
def test_build_ready_entries_normalises(spark):
    src = spark.createDataFrame([(" dag_a ",), ("dag_a",), (None,), ("",), ("  ",), ("dag_b",)], "DAG_NAME string")
    out = job.build_ready_entries(src, CURR_DATE)
    assert out.columns == ["dag_name", "run_date", "status"]
    assert sorted((r.dag_name, r.run_date, r.status) for r in out.collect()) == [
        ("dag_a", date(2026, 10, 8), "READY"),
        ("dag_b", date(2026, 10, 8), "READY"),
    ]


def test_build_ready_entries_requires_dag_name(spark):
    with pytest.raises(ValueError, match="dag_name"):
        job.build_ready_entries(spark.createDataFrame([("x",)], "name string"), CURR_DATE)


def test_filter_new_entries_only_same_run_date(spark):
    entries = job.build_ready_entries(spark.createDataFrame([("a",), ("b",), ("c",)], "dag_name string"), CURR_DATE)
    existing = spark.createDataFrame(
        [("a", date(2026, 10, 8)), ("b", date(2026, 10, 7))], "dag_name string, run_date date"
    )
    assert sorted(r.dag_name for r in job.filter_new_entries(entries, existing, CURR_DATE).collect()) == ["b", "c"]


# --------------------------------------------------------------------------- #
# run() against a real Iceberg table
# --------------------------------------------------------------------------- #
def test_run_inserts_only_new_ready_rows(spark, log_table):
    spark.sql(f"""
        INSERT INTO {log_table} VALUES
        ('dag_a', DATE'{CURR_DATE}', 'TRIGGERED', 'run_1', 'p1', NULL,
         TIMESTAMP_NTZ'2026-10-08 01:00:00', TIMESTAMP_NTZ'2026-10-08 02:00:00', NULL, NULL),
        ('dag_b', DATE'2026-10-07', 'FAILED', 'run_0', 'p0', 'boom',
         TIMESTAMP_NTZ'2026-10-07 01:00:00', NULL, TIMESTAMP_NTZ'2026-10-07 03:00:00', NULL)
    """)
    src = spark.createDataFrame([("dag_a",), ("dag_b",), ("dag_c",), (" dag_c",), (None,)], "dag_name string")

    assert job.run(spark, make_cfg(), source_df=src) == 2

    result = rows(spark, log_table)
    assert set(result) == {("dag_a", CURR_DATE), ("dag_b", "2026-10-07"), ("dag_b", CURR_DATE), ("dag_c", CURR_DATE)}
    assert result[("dag_a", CURR_DATE)].status == "TRIGGERED"  # existing row untouched
    assert result[("dag_a", CURR_DATE)].dag_run_id == "run_1"
    assert result[("dag_b", "2026-10-07")].status == "FAILED"
    for key in [("dag_b", CURR_DATE), ("dag_c", CURR_DATE)]:
        row = result[key]
        assert row.status == "READY"
        assert row.created_ts is not None and row.updated_ts is not None
        assert row.dag_run_id is None and row.triggered_ts is None

    # Re-running for the same date is a no-op.
    assert job.run(spark, make_cfg(), source_df=src) == 0
    assert spark.table(log_table).count() == 4


def test_run_with_empty_source(spark, log_table):
    src = spark.createDataFrame([], "dag_name string")
    assert job.run(spark, make_cfg(), source_df=src) == 0
    assert spark.table(log_table).count() == 0


def test_read_source_over_jdbc(spark):
    url = "jdbc:derby:memory:edw_read;create=true"
    spark.createDataFrame(
        [("dag_a", "2026-10-08"), ("dag_b", "2026-10-07")], "DAG_NAME string, LOAD_DATE string"
    ).write.option("createTableColumnTypes", "DAG_NAME VARCHAR(128), LOAD_DATE VARCHAR(10)").jdbc(
        url, "SRC_DAGS", mode="overwrite")

    cfg = make_cfg(jdbc_url=url, username="app", password="app",
                   source_query="SELECT DAG_NAME FROM SRC_DAGS WHERE LOAD_DATE = '{curr_date}';",
                   driver="org.apache.derby.jdbc.EmbeddedDriver")
    assert [r[0] for r in job.read_source(spark, cfg).collect()] == ["dag_a"]


# --------------------------------------------------------------------------- #
# main(): the job exactly as CDE runs it, in its own Spark application
# --------------------------------------------------------------------------- #
def _spark_python(code: str, env: dict) -> None:
    result = subprocess.run([sys.executable, "-c", textwrap.dedent(code)], env=env,
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr[-4000:]


def _submit_args(conf: dict[str, str]) -> str:
    import shlex
    return " ".join(f"--conf {shlex.quote(f'{k}={v}')}" for k, v in conf.items()) + " pyspark-shell"


def test_main_end_to_end(tmp_path):
    warehouse = tmp_path / "warehouse"
    derby_url = f"jdbc:derby:{tmp_path / 'edw'};create=true"
    base_conf = spark_conf(warehouse)
    env = {**os.environ, "PYSPARK_SUBMIT_ARGS": _submit_args(base_conf)}

    # Seed the source database and the (empty) target table.
    _spark_python(f"""
        import os
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        src = spark.createDataFrame(
            [("dag_a", "{CURR_DATE}"), ("dag_b", "{CURR_DATE}"), ("dag_x", "2026-10-07")],
            "DAG_NAME string, LOAD_DATE string")
        (src.write.option("createTableColumnTypes", "DAG_NAME VARCHAR(128), LOAD_DATE VARCHAR(10)")
            .jdbc("{derby_url}", "SRC_DAGS"))
        spark.sql(os.environ["CREATE_TABLE_SQL"])
        spark.stop()
    """, {**env, "CREATE_TABLE_SQL": CREATE_TABLE})

    job_conf = {
        **base_conf,
        "spark.edw_jdbc_url": derby_url,
        "spark.edw_username": "app",
        "spark.edw_password": "app",
        "spark.edw_driver": "org.apache.derby.jdbc.EmbeddedDriver",
        "spark.source_query": "SELECT DAG_NAME FROM SRC_DAGS WHERE LOAD_DATE = '{curr_date}'",
        "spark.curr_date": CURR_DATE,
        "spark.target_table": TABLE,
    }
    result = subprocess.run([sys.executable, str(ROOT / "spark" / "check_ready_list.py")],
                            env={**os.environ, "PYSPARK_SUBMIT_ARGS": _submit_args(job_conf)},
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr[-4000:]
    assert "Inserted 2 new READY row(s)" in result.stderr

    _spark_python(f"""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        got = sorted((r.dag_name, str(r.run_date), r.status) for r in spark.table("{TABLE}").collect())
        assert got == [("dag_a", "{CURR_DATE}", "READY"), ("dag_b", "{CURR_DATE}", "READY")], got
        spark.stop()
    """, env)


def test_main_fails_on_missing_conf(tmp_path):
    conf = {**spark_conf(tmp_path / "warehouse"), "spark.curr_date": CURR_DATE}
    result = subprocess.run([sys.executable, str(ROOT / "spark" / "check_ready_list.py")],
                            env={**os.environ, "PYSPARK_SUBMIT_ARGS": _submit_args(conf)},
                            capture_output=True, text=True, timeout=300)
    assert result.returncode != 0
    assert "Missing required Spark conf: spark.edw_jdbc_url" in result.stderr
