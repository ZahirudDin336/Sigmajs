"""
CDE Spark job ``test_check_ready_list``.

Reads the list of DAGs that are ready from the source (EDW over JDBC) and
inserts a ``READY`` row into the Iceberg table ``dev_stg.upstream_trigger_log``
for every DAG that has no row yet for the run date. Existing rows (whatever
their status) are never touched, so the job is safe to re-run.

Spark conf (passed by the Airflow DAG through CDE overrides):
    spark.edw_jdbc_url   JDBC URL of the source
    spark.edw_username   source user
    spark.edw_password   source password (redacted in the Spark UI)
    spark.source_query   SQL returning a ``dag_name`` column; ``{curr_date}``
                         is replaced with the run date
    spark.curr_date      run date, YYYY-MM-DD
    spark.target_table   optional, default dev_stg.upstream_trigger_log
    spark.edw_driver     optional JDBC driver class
"""
from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("check_ready_list")

DEFAULT_TARGET_TABLE = "dev_stg.upstream_trigger_log"
STATUS_READY = "READY"
SOURCE_VIEW = "upstream_new_ready_dags"


@dataclass(frozen=True)
class JobConfig:
    jdbc_url: str
    username: str
    password: str
    source_query: str
    curr_date: str
    target_table: str = DEFAULT_TARGET_TABLE
    driver: str | None = None


def parse_config(conf: dict[str, str]) -> JobConfig:
    """Validate the ``spark.*`` settings and build a :class:`JobConfig`."""
    required = {
        "jdbc_url": "spark.edw_jdbc_url",
        "username": "spark.edw_username",
        "password": "spark.edw_password",
        "source_query": "spark.source_query",
        "curr_date": "spark.curr_date",
    }
    values = {field: (conf.get(key) or "").strip() for field, key in required.items()}
    missing = [required[field] for field, value in values.items() if not value]
    if missing:
        raise ValueError(f"Missing required Spark conf: {', '.join(missing)}")
    try:
        date.fromisoformat(values["curr_date"])
    except ValueError as exc:
        raise ValueError(f"spark.curr_date must be YYYY-MM-DD, got {values['curr_date']!r}") from exc

    return JobConfig(
        **values,
        target_table=(conf.get("spark.target_table") or DEFAULT_TARGET_TABLE).strip(),
        driver=(conf.get("spark.edw_driver") or "").strip() or None,
    )


def render_source_query(source_query: str, curr_date: str) -> str:
    return source_query.replace("{curr_date}", curr_date).strip().rstrip(";")


def read_source(spark: SparkSession, cfg: JobConfig) -> DataFrame:
    reader = (
        spark.read.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("user", cfg.username)
        .option("password", cfg.password)
        .option("query", render_source_query(cfg.source_query, cfg.curr_date))
    )
    if cfg.driver:
        reader = reader.option("driver", cfg.driver)
    return reader.load()


def build_ready_entries(source_df: DataFrame, curr_date: str) -> DataFrame:
    """Normalise the source list into distinct ``(dag_name, run_date, status)`` rows."""
    columns = {c.lower(): c for c in source_df.columns}
    if "dag_name" not in columns:
        raise ValueError(f"Source query must return a dag_name column, got {source_df.columns}")
    return (
        source_df.select(F.trim(F.col(columns["dag_name"]).cast("string")).alias("dag_name"))
        .where(F.col("dag_name").isNotNull() & (F.col("dag_name") != ""))
        .distinct()
        .withColumn("run_date", F.to_date(F.lit(curr_date)))
        .withColumn("status", F.lit(STATUS_READY))
    )


def filter_new_entries(entries_df: DataFrame, existing_df: DataFrame, curr_date: str) -> DataFrame:
    """Drop the DAGs that already have a row for ``curr_date``."""
    existing = existing_df.where(F.col("run_date") == F.to_date(F.lit(curr_date))).select("dag_name")
    return entries_df.join(existing, on="dag_name", how="left_anti")


def merge_new_entries(spark: SparkSession, new_df: DataFrame, target_table: str) -> None:
    """Insert ``new_df`` atomically; MERGE keeps it idempotent if rows appeared meanwhile."""
    new_df.createOrReplaceTempView(SOURCE_VIEW)
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING {SOURCE_VIEW} s
        ON t.dag_name = s.dag_name AND t.run_date = s.run_date
        WHEN NOT MATCHED THEN INSERT (dag_name, run_date, status, created_ts, updated_ts)
        VALUES (s.dag_name, s.run_date, s.status, current_timestamp(), current_timestamp())
        """
    )


def run(spark: SparkSession, cfg: JobConfig, source_df: DataFrame | None = None) -> int:
    """Run the job and return the number of new READY rows inserted."""
    if source_df is None:
        source_df = read_source(spark, cfg)

    entries = build_ready_entries(source_df, cfg.curr_date)
    new_entries = filter_new_entries(entries, spark.table(cfg.target_table), cfg.curr_date).cache()
    try:
        new_count = new_entries.count()
        logger.info("%s new DAG(s) to insert for %s", new_count, cfg.curr_date)
        if new_count:
            merge_new_entries(spark, new_entries, cfg.target_table)
        return new_count
    finally:
        new_entries.unpersist()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = SparkSession.builder.appName("test_check_ready_list").getOrCreate()
    # Impala creates Iceberg TIMESTAMP columns as "timestamp without time zone";
    # older Spark/Iceberg versions need this to write them.
    spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    try:
        cfg = parse_config(dict(spark.sparkContext.getConf().getAll()))
        logger.info("Loading READY DAGs for %s into %s", cfg.curr_date, cfg.target_table)
        inserted = run(spark, cfg)
        logger.info("Inserted %s new READY row(s) into %s", inserted, cfg.target_table)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
