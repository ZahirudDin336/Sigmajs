"""Tests for the CDE Spark job, on local Spark with a Hive metastore.

The EDW (Oracle) source is emulated with an Apache Derby database read through
Spark's JDBC reader (Derby ships with Spark).
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")
import check_ready_list as job  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

DERBY_URL = "jdbc:derby:memory:edw;create=true"
DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver"
# Spark cannot write NULL strings to Derby, so "" stands for a missing DAG mapping.
SOURCE_QUERY = (
    "(SELECT DATASET_NAME, TABLENAME, NULLIF(AIRFLOW_DAG_NAME, '') AS AIRFLOW_DAG_NAME, STATUS, "
    "EXECUTION_DATE, INSERT_DT, INSERT_LOAD_ID FROM SRC_LOG) src"
)
SOURCE_COLUMNS = (
    "DATASET_NAME VARCHAR(64), TABLENAME VARCHAR(64), AIRFLOW_DAG_NAME VARCHAR(64), STATUS VARCHAR(16), "
    "EXECUTION_DATE TIMESTAMP, INSERT_DT TIMESTAMP, INSERT_LOAD_ID INTEGER"
)
NOW = datetime.now().replace(microsecond=0)
TODAY = NOW.replace(hour=0, minute=0, second=0)
YESTERDAY = TODAY - timedelta(days=1)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    base = tmp_path_factory.mktemp("hive")
    session = (
        SparkSession.builder.appName("test_check_ready_list")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(base / "warehouse"))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={base / 'metastore_db'};create=true")
        .enableHiveSupport()
        .getOrCreate()
    )
    session.sql("CREATE DATABASE IF NOT EXISTS dev_stg")
    yield session
    session.stop()


@pytest.fixture
def tables(spark):
    spark.sql(f"DROP TABLE IF EXISTS {job.UPSTREAM_TRIGGER_LOG}")
    spark.sql(f"DROP TABLE IF EXISTS {job.BD_test_upstream_dly}")
    spark.sql(f"""
        CREATE TABLE {job.UPSTREAM_TRIGGER_LOG} (
            dataset_name STRING, source_tablename STRING, dag_name STRING, status STRING,
            target_table STRING, source_insert_dt TIMESTAMP, insert_load_id INT,
            triggered_ts TIMESTAMP, updated_ts TIMESTAMP)
        STORED AS PARQUET
    """)
    spark.conf.set("spark.edw_jdbc_url", DERBY_URL)
    spark.conf.set("spark.edw_username", "app")
    spark.conf.set("spark.edw_password", "app")
    yield
    for key in ("spark.edw_jdbc_url", "spark.edw_username", "spark.edw_password"):
        spark.conf.unset(key)


def write_source(spark, rows):
    df = spark.createDataFrame(
        rows,
        "DATASET_NAME string, TABLENAME string, AIRFLOW_DAG_NAME string, STATUS string, "
        "EXECUTION_DATE timestamp, INSERT_DT timestamp, INSERT_LOAD_ID int",
    )
    (df.write.option("createTableColumnTypes", SOURCE_COLUMNS)
       .jdbc(DERBY_URL, "SRC_LOG", mode="overwrite"))


def add_log_row(spark, dataset, table, dag, status, ts, load_id=1):
    spark.sql(f"""
        INSERT INTO {job.UPSTREAM_TRIGGER_LOG} VALUES
        ('{dataset}', '{table}', '{dag}', '{status}', NULL, TIMESTAMP'{ts}', {load_id}, NULL, NULL)
    """)


def log_rows(spark):
    return sorted(spark.table(job.UPSTREAM_TRIGGER_LOG).collect(),
                  key=lambda r: (r.dataset_name, r.source_insert_dt))


def run_job(spark):
    job.etl(spark_session=spark, source_query=SOURCE_QUERY, driver=DERBY_DRIVER)


def test_inserts_one_ready_row_per_new_dataset(spark, tables):
    write_source(spark, [
        ("ds1", "t1", "dag_a", "SUCCESS", TODAY + timedelta(hours=1), NOW, 1),
        ("ds1", "t1", "dag_a", "SUCCESS", TODAY + timedelta(hours=2), NOW, 2),   # latest load wins
        ("ds2", "t2", "", "SUCCESS", TODAY + timedelta(hours=1), NOW, 3),      # no DAG mapped
        ("ds3", "t3", "dag_b", "SUCCESS", TODAY + timedelta(hours=1), NOW, 4),
        ("ds4", "t4", "dag_c", "SUCCESS", TODAY + timedelta(hours=1), NOW, 5),   # already logged today
        ("ds5", "t5", "dag_d", "SUCCESS", TODAY + timedelta(hours=1), NOW, None),  # no insert_load_id
    ])
    add_log_row(spark, "ds4", "t4", "dag_c", "TRIGGERED", TODAY + timedelta(minutes=5))
    add_log_row(spark, "ds3", "t3", "dag_b", "SUCCEEDED", YESTERDAY + timedelta(hours=1))

    run_job(spark)

    rows = log_rows(spark)
    new = [r for r in rows if r.status == "READY"]
    assert [(r.dataset_name, r.source_tablename, r.dag_name) for r in new] == [
        ("ds1", "t1", "dag_a"), ("ds3", "t3", "dag_b")]
    ds1 = new[0]
    assert ds1.insert_load_id == 2
    assert ds1.source_insert_dt == TODAY + timedelta(hours=2)
    assert all(r.triggered_ts is None and r.updated_ts is not None and r.target_table is None for r in new)
    # Existing rows untouched.
    assert [(r.dataset_name, r.status) for r in rows if r.status != "READY"] == [
        ("ds3", "SUCCEEDED"), ("ds4", "TRIGGERED")]
    # Staging table holds the full source extract.
    assert spark.table(job.BD_test_upstream_dly).count() == 6


def test_same_load_and_execution_datetime_is_not_inserted_twice(spark, tables):
    # A source row dated just before Spark's current_date() (Oracle's "today" and
    # Spark's disagree around midnight) is invisible to the "already logged today"
    # check; the insert_load_id + execution datetime check still finds it.
    exec_dt = YESTERDAY + timedelta(hours=23, minutes=30)
    write_source(spark, [
        ("ds1", "t1", "dag_a", "SUCCESS", exec_dt, NOW, 7),
        ("ds1", "t1", "dag_a", "SUCCESS", exec_dt, NOW, 7),  # exact duplicate in the source
    ])
    add_log_row(spark, "ds1", "t1", "dag_a", "SUCCEEDED", exec_dt, load_id=7)

    run_job(spark)
    run_job(spark)

    assert [(r.dataset_name, r.insert_load_id, r.status) for r in log_rows(spark)] == [("ds1", 7, "SUCCEEDED")]


def test_load_id_check_skips_only_the_same_load(spark, tables):
    exec_dt = YESTERDAY + timedelta(hours=23, minutes=30)
    write_source(spark, [
        ("ds1", "t1", "dag_a", "SUCCESS", exec_dt, NOW, 8),  # new load of a logged dataset
        ("ds2", "t2", "dag_b", "SUCCESS", exec_dt, NOW, 7),  # same load id, other dataset
    ])
    add_log_row(spark, "ds1", "t1", "dag_a", "SUCCEEDED", exec_dt, load_id=7)

    run_job(spark)

    assert sorted((r.dataset_name, r.insert_load_id, r.status) for r in log_rows(spark)) == [
        ("ds1", 7, "SUCCEEDED"), ("ds1", 8, "READY"), ("ds2", 7, "READY")]


def test_rerun_every_15_minutes_inserts_nothing_new(spark, tables):
    write_source(spark, [("ds1", "t1", "dag_a", "SUCCESS", TODAY + timedelta(hours=1), NOW, 1)])
    run_job(spark)
    run_job(spark)
    assert len(log_rows(spark)) == 1

    # A new dataset appears in the source later in the day.
    write_source(spark, [("ds1", "t1", "dag_a", "SUCCESS", TODAY + timedelta(hours=1), NOW, 1),
                         ("ds9", "t9", "dag_z", "SUCCESS", TODAY + timedelta(hours=3), NOW, 2)])
    run_job(spark)
    assert [(r.dataset_name, r.status) for r in log_rows(spark)] == [("ds1", "READY"), ("ds9", "READY")]


def write_edw_table(spark, name, schema, column_types, rows):
    (spark.createDataFrame(rows, schema).write.option("createTableColumnTypes", column_types)
       .jdbc(DERBY_URL, name, mode="overwrite"))


def test_production_source_query(spark, tables):
    """Runs job.ready_list_query itself on Derby (only TRUNC(SYSDATE) is swapped for Derby's syntax)."""
    write_edw_table(
        spark, "UP_EDW.HLP_ADS_DATASETS_LOG_TEST",
        "DATASET_NAME string, TABLENAME string, STATUS string, EXECUTION_DATE timestamp, INSERT_DT timestamp, "
        "INSERT_LOAD_ID int",
        "DATASET_NAME VARCHAR(64), TABLENAME VARCHAR(64), STATUS VARCHAR(16), EXECUTION_DATE TIMESTAMP, "
        "INSERT_DT TIMESTAMP, INSERT_LOAD_ID INTEGER",
        [
            ("ds1", "t1", "READY", TODAY + timedelta(hours=1), NOW, 1),       # match
            ("ds2", "t2", "READY", TODAY + timedelta(hours=1), NOW, 2),       # dataset INACTIVE
            ("ds3", "t3", "READY", TODAY + timedelta(hours=1), NOW, 3),       # no mapping at all
            ("ds4", "t4", "RUNNING", TODAY + timedelta(hours=1), NOW, 4),     # not READY
            ("ds5", "t5", "READY", YESTERDAY + timedelta(hours=1), NOW, 5),   # yesterday
            ("ds6", "t6", "READY", TODAY + timedelta(hours=2), NOW, 6),       # match, other DAG
            ("ds1", "tX", "READY", TODAY + timedelta(hours=1), NOW, 7),       # same dataset, other table
        ],
    )
    write_edw_table(
        spark, "UP_EDW.HLP_ADS_DATASETS",
        "DATASET_NAME string, SOURCE_TABLENAME string, AIRFLOW_DAG_NAME string, STATUS string",
        "DATASET_NAME VARCHAR(64), SOURCE_TABLENAME VARCHAR(64), AIRFLOW_DAG_NAME VARCHAR(64), STATUS VARCHAR(16)",
        [
            ("ds1", "t1", "dag_a", "ACTIVE"),
            ("ds1", "t1", "dag_old", "INACTIVE"),  # old mapping of the same table: must not duplicate
            ("ds2", "t2", "dag_b", "INACTIVE"),
            ("ds4", "t4", "dag_d", "ACTIVE"),
            ("ds5", "t5", "dag_e", "ACTIVE"),
            ("ds6", "t6", "dag_f", "ACTIVE"),
        ],
    )
    derby_query = job.ready_list_query.replace("TRUNC(SYSDATE)", "TIMESTAMP(CURRENT_DATE, TIME('00:00:00'))")
    assert derby_query != job.ready_list_query

    extract = spark.read.jdbc(DERBY_URL, derby_query, properties={"driver": DERBY_DRIVER})
    assert sorted((r.DATASET_NAME, r.TABLENAME, r.AIRFLOW_DAG_NAME) for r in extract.collect()) == [
        ("ds1", "t1", "dag_a"), ("ds6", "t6", "dag_f")]

    job.etl(spark_session=spark, source_query=derby_query, driver=DERBY_DRIVER)
    assert [(r.dataset_name, r.dag_name, r.status, r.insert_load_id) for r in log_rows(spark)] == [
        ("ds1", "dag_a", "READY", 1), ("ds6", "dag_f", "READY", 6)]


def test_empty_source(spark, tables):
    write_source(spark, [])
    run_job(spark)
    assert log_rows(spark) == []


@pytest.mark.parametrize("key", ["spark.edw_jdbc_url", "spark.edw_username", "spark.edw_password"])
def test_missing_conf_fails_before_any_write(spark, tables, key):
    spark.conf.unset(key)
    with pytest.raises(ValueError, match=key):
        run_job(spark)
    assert not spark.catalog.tableExists(job.BD_test_upstream_dly)


def test_source_error_is_raised(spark, tables):
    with pytest.raises(Exception, match="NO_SUCH_TABLE|does not exist"):
        job.etl(spark_session=spark, source_query="(SELECT * FROM NO_SUCH_TABLE) src", driver=DERBY_DRIVER)


def test_session_builder_sets_timestamp_flag():
    import inspect

    assert "spark.sql.iceberg.handle-timestamp-without-timezone" in inspect.getsource(job.build_spark_session)
