import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_logger(spark_session):
    log4j = spark_session._jvm.org.apache.log4j
    return log4j.LogManager.getLogger(__name__)


APP_NAME = "test_upstream_dly"
BD_test_upstream_dly = "dev_stg.tmp_upstream_list_dly"
BD_upstream_trigger_log = "dev_stg.upstream_trigger_log"

# On CDP/CDE, a plain "CREATE TABLE ... AS SELECT" from Spark can land as a
# managed (insert-only ACID) Hive table. Spark writes its parquet files straight
# into the table directory, but Hive/Impala only read ACID base_/delta_ dirs, so
# the table looks empty. Forcing a non-transactional (external) table fixes that.
TMP_TABLE_PROPS = "TBLPROPERTIES ('transactional'='false', 'external.table.purge'='true')"


ready_list_query = """
(   select
    logs.DATASET_NAME,
    logs.TABLENAME,
    lists.AIRFLOW_DAG_NAME,
    logs.STATUS,
    logs.EXECUTION_DATE,
    logs.INSERT_DT,
    logs.INSERT_LOAD_ID
    from
    UP_EDW.HLP_ADS_DATASETS_LOG_TEST logs
    LEFT JOIN UP_EDW.HLP_ADS_DATASETS lists
    on lists.DATASET_NAME=logs.DATASET_NAME and lists.SOURCE_TABLENAME=logs.TABLENAME
    and lists.STATUS='ACTIVE'
    where logs.EXECUTION_DATE>=trunc(SYSDATE)
) src
"""


def etl():
    spark_session = None
    logger = None
    try:
        spark_session = (
            SparkSession.builder.appName(APP_NAME)
            .config("spark.executor.memory", "10g")
            .config("spark.driver.memory", "10g")
            .config("spark.dynamicAllocation.maxExecutors", "5")
            .config("spark.executor.cores", "5")
            .config("spark.yarn.executor.memoryOverhead", "8g")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
            .config("spark.jars", "/app/mount/common/ojdbc8.jar")
            # Impala-compatible parquet timestamps
            .config("spark.sql.parquet.outputTimestampType", "INT96")
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .enableHiveSupport()
            .getOrCreate()
        )

        logger = get_logger(spark_session)
        spark_session.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
        spark_session.sql("SET hive.exec.dynamic.partition = true")

        logger.info(f"Starting application: '{APP_NAME}'")

        driver = "oracle.jdbc.driver.OracleDriver"

        edw_jdbc_url = spark_session.conf.get("spark.edw_jdbc_url")
        edw_username = spark_session.conf.get("spark.edw_username")
        edw_password = spark_session.conf.get("spark.edw_password")

        df_list = (
            spark_session.read.format("jdbc")
            .option("url", edw_jdbc_url)
            .option("dbtable", ready_list_query)
            .option("user", edw_username)
            .option("password", edw_password)
            .option("driver", driver)
            .option("fetchsize", "10000")
            .load()
        )

        # Oracle returns upper-case names and unbounded NUMBER -> decimal(38,10);
        # normalise so Hive/Impala and the INSERT below see stable types.
        df_list = df_list.select(
            F.col("DATASET_NAME").cast("string").alias("dataset_name"),
            F.col("TABLENAME").cast("string").alias("tablename"),
            F.col("AIRFLOW_DAG_NAME").cast("string").alias("airflow_dag_name"),
            F.col("STATUS").cast("string").alias("status"),
            F.col("EXECUTION_DATE").cast("timestamp").alias("execution_date"),
            F.col("INSERT_DT").cast("timestamp").alias("insert_dt"),
            F.col("INSERT_LOAD_ID").cast("bigint").alias("insert_load_id"),
        ).cache()

        # Materialise once: the JDBC source is read a single time and we know
        # up front whether Oracle actually returned anything.
        src_count = df_list.count()
        logger.info(f"Rows fetched from Oracle: {src_count}")
        if src_count == 0:
            logger.warn(
                "Oracle query returned 0 rows (EXECUTION_DATE >= trunc(SYSDATE) on the "
                "Oracle server clock); tmp table will be empty and nothing will be inserted"
            )

        df_list.createOrReplaceTempView("test_upstream_dly")

        spark_session.sql(f"DROP TABLE IF EXISTS {BD_test_upstream_dly} PURGE")
        spark_session.sql(f"""
            CREATE TABLE {BD_test_upstream_dly}
            STORED AS PARQUET
            {TMP_TABLE_PROPS}
            AS SELECT * FROM test_upstream_dly
        """)

        # Spark caches table file listings; make sure we read what we just wrote.
        spark_session.catalog.refreshTable(BD_test_upstream_dly)
        tmp_count = spark_session.table(BD_test_upstream_dly).count()
        logger.info(f"Data successfully written to table '{BD_test_upstream_dly}': {tmp_count} rows")
        if tmp_count != src_count:
            raise RuntimeError(
                f"{BD_test_upstream_dly} has {tmp_count} rows but Oracle returned {src_count}"
            )

        # Only insert rows not already logged today (anti join instead of the
        # LEFT JOIN + IS NULL filter, same result, no null-column pitfalls).
        spark_session.sql(f"""
        INSERT INTO {BD_upstream_trigger_log}
        (
            dataset_name,
            source_tablename,
            dag_name,
            status,
            target_table,
            source_insert_dt,
            insert_load_id,
            triggered_ts,
            updated_ts
        )
        SELECT
            o.dataset_name                      AS dataset_name,
            o.tablename                         AS source_tablename,
            o.airflow_dag_name                  AS dag_name,
            o.status                            AS status,
            CAST(NULL AS STRING)                AS target_table,
            o.execution_date                    AS source_insert_dt,
            CAST(o.insert_load_id AS INT)       AS insert_load_id,
            current_timestamp()                 AS triggered_ts,
            CAST(NULL AS TIMESTAMP)             AS updated_ts
        FROM {BD_test_upstream_dly} o
        LEFT ANTI JOIN (
            SELECT dataset_name, source_tablename
            FROM {BD_upstream_trigger_log}
            WHERE source_insert_dt >= current_date()
        ) l
        ON  o.dataset_name = l.dataset_name
        AND o.tablename    = l.source_tablename
        """)

        spark_session.catalog.refreshTable(BD_upstream_trigger_log)
        logger.info(f"Data successfully written to table {BD_upstream_trigger_log}")

        # NOTE: Impala caches HMS metadata. After this job, run in Impala:
        #   INVALIDATE METADATA dev_stg.tmp_upstream_list_dly;   -- table was dropped/recreated
        #   REFRESH dev_stg.upstream_trigger_log;                -- new files added
        # (or rely on HMS event sync if hms_event_polling_interval_s is enabled).

    except Exception as err:
        if logger:
            logger.error("An error occured during application execution")
            logger.error(f"Error Type: {type(err).__name__}")
            logger.error(f"Error Message: {str(err)}")
        else:
            print(f"Application failed (before logger initialization): {str(err)}", file=sys.stderr)
        raise
    finally:
        if spark_session:
            if logger:
                logger.info("Stopping Spark Session")
            spark_session.stop()


if __name__ == "__main__":
    etl()
