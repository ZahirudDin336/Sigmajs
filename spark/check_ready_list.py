"""
CDE Spark job ``test_check_ready_list``.

Reads today's dataset load log from the EDW (Oracle), stages it in
``dev_stg.tmp_upstream_list_dly`` and inserts one ``READY`` row per new
(dataset, table) into ``dev_stg.upstream_trigger_log``. Rows already present for
today are never inserted again, so the job can run every 15 minutes.

Spark conf (passed by the Airflow DAG):
    spark.edw_jdbc_url, spark.edw_username, spark.edw_password
"""
import sys

from pyspark.sql import SparkSession

APP_NAME = "test_upstream_dly"
BD_test_upstream_dly = "dev_stg.tmp_upstream_list_dly"
UPSTREAM_TRIGGER_LOG = "dev_stg.upstream_trigger_log"
STATUS_READY = "READY"
ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver"

ready_list_query = """
(	select
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

# One READY row per (dataset, table) and day: the latest load of the source
# wins, rows without a mapped DAG or insert_load_id (the key the Airflow DAG
# updates by) are skipped, rows already logged today are skipped. triggered_ts stays NULL until the Airflow DAG triggers the DAG.
INSERT_NEW_READY_SQL = f"""
INSERT INTO {UPSTREAM_TRIGGER_LOG}
(
dataset_name,
source_tablename,
dag_name,
status, target_table ,
source_insert_dt,
insert_load_id,
triggered_ts ,
updated_ts
)
SELECT
o.dataset_name as dataset_name,
o.tablename as source_tablename,
o.airflow_dag_name as dag_name,
'{STATUS_READY}' as status, null as target_table ,
o.execution_date as source_insert_dt,
cast(o.insert_load_id as int) as insert_load_id,
NULL as triggered_ts,
current_timestamp() AS updated_ts
FROM (
    SELECT s.*,
           row_number() OVER (PARTITION BY s.dataset_name, s.tablename
                              ORDER BY s.execution_date DESC, s.insert_load_id DESC) AS rn
    FROM {BD_test_upstream_dly} s
    WHERE s.airflow_dag_name IS NOT NULL
      AND s.insert_load_id IS NOT NULL
) o
LEFT JOIN (
    SELECT DISTINCT dataset_name, source_tablename
    FROM {UPSTREAM_TRIGGER_LOG}
    WHERE source_insert_dt >= current_date()
) l
ON o.dataset_name = l.dataset_name
and o.tablename = l.source_tablename
where o.rn = 1
and l.dataset_name IS NULL
"""


def get_logger(spark_session):
    log4j = spark_session._jvm.org.apache.log4j
    return log4j.LogManager.getLogger(__name__)


def build_spark_session():
    return (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.executor.memory", "10g")
        .config("spark.driver.memory", "10g")
        .config("spark.dynamicAllocation.maxExecutors", "5")
        .config("spark.executor.cores", "5")
        .config("spark.cores.max", "5")
        .config("spark.yarn.executor.memoryOverhead", "8g")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
        .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
        .config("spark.jars", "/app/mount/common/ojdbc8.jar")
        # Impala creates Iceberg TIMESTAMP columns "without time zone";
        # Spark < 3.4 can only read/write them with this flag.
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_edw_config(spark_session):
    keys = ("spark.edw_jdbc_url", "spark.edw_username", "spark.edw_password")
    values = {key: (spark_session.conf.get(key, "") or "").strip() for key in keys}
    missing = [key for key, value in values.items() if not value]
    if missing:
        raise ValueError(f"Missing required Spark conf: {', '.join(missing)}")
    return values["spark.edw_jdbc_url"], values["spark.edw_username"], values["spark.edw_password"]


def etl(spark_session=None, source_query=ready_list_query, driver=ORACLE_DRIVER):
    own_session = spark_session is None
    logger = None
    try:
        if own_session:
            spark_session = build_spark_session()

        logger = get_logger(spark_session)
        spark_session.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
        spark_session.sql("SET hive.exec.dynamic.partition = true")

        logger.info(f"Starting application: '{APP_NAME}'")

        edw_jdbc_url, edw_username, edw_password = get_edw_config(spark_session)

        df_list = spark_session.read.jdbc(
            edw_jdbc_url, source_query,
            properties={"user": edw_username, "password": edw_password, "driver": driver},
        )
        df_list.createOrReplaceTempView("test_upstream_dly")

        spark_session.sql(f"DROP TABLE IF EXISTS {BD_test_upstream_dly}")
        spark_session.sql(f""" CREATE TABLE {BD_test_upstream_dly} STORED AS PARQUET AS
                                SELECT * FROM test_upstream_dly """)
        logger.info(f"Data successfully written to table '{BD_test_upstream_dly}'")

        spark_session.sql(INSERT_NEW_READY_SQL)
        logger.info(f"Data successfully written to table {UPSTREAM_TRIGGER_LOG}")

    except Exception as err:
        if logger:
            logger.error("An error occured during application execution")
            logger.error(f"Error Type: {type(err).__name__}")
            logger.error(f"Error Message: {str(err)}")
        else:
            print(f"Application failed (before logger initialization): {str(err)}", file=sys.stderr)
        raise
    finally:
        if own_session and spark_session:
            spark_session.stop()
            if logger:
                logger.info("Spark Session stopped")


if __name__ == "__main__":
    etl()
