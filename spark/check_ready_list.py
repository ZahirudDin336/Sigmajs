"""
CDE Spark job test_check_ready_list: inserts one READY row per new (dataset, table) load of today
from the EDW into dev_stg.upstream_trigger_log. Safe to run every 15 minutes.

Spark conf (set by the Airflow DAG): spark.edw_jdbc_url, spark.edw_username and
spark.edw_password or spark.edw_password_file.
"""
import sys

from pyspark.sql import SparkSession

APP_NAME = "test_upstream_dly"
SOURCE_VIEW = "upstream_ready_list"
UPSTREAM_TRIGGER_LOG = "dev_stg.upstream_trigger_log"
STATUS_READY = "READY"
LOCAL_TZ = "Asia/Qatar"
ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver"
JDBC_FETCH_SIZE = "1000"

# Today's READY loads of ACTIVE datasets (one ACTIVE mapping per dataset/table), filtered in Oracle.
ready_list_query = """
(
    SELECT logs.DATASET_NAME,
           logs.TABLENAME,
           lists.AIRFLOW_DAG_NAME,
           logs.STATUS,
           logs.EXECUTION_DATE,
           logs.INSERT_DT,
           logs.INSERT_LOAD_ID
    FROM UP_EDW.HLP_ADS_DATASETS_LOG_TEST logs
    JOIN UP_EDW.HLP_ADS_DATASETS lists
      ON  lists.DATASET_NAME     = logs.DATASET_NAME
      AND lists.SOURCE_TABLENAME = logs.TABLENAME
    WHERE lists.STATUS = 'ACTIVE'
      AND logs.STATUS  = 'READY'
      AND logs.EXECUTION_DATE >= TRUNC(SYSDATE)
      AND lists.AIRFLOW_DAG_NAME IS NOT NULL
      AND logs.INSERT_LOAD_ID IS NOT NULL
) src
"""

# Qatar wall-clock date/time, independent of the Spark and JVM time zone (UTC).
QATAR_NOW = f"from_utc_timestamp(current_timestamp(), '{LOCAL_TZ}')"
QATAR_TODAY = f"to_date({QATAR_NOW})"

# New loads only: latest load per (dataset, table); skip what is already logged today, and the same
# load (insert_load_id + execution datetime) logged since yesterday (covers the midnight boundary).
INSERT_NEW_READY_SQL = f"""
INSERT INTO {UPSTREAM_TRIGGER_LOG}
(dataset_name, source_tablename, dag_name, status, target_table,
 source_insert_dt, insert_load_id, triggered_ts, updated_ts)
SELECT o.dataset_name,
       o.tablename,
       o.airflow_dag_name,
       '{STATUS_READY}',
       NULL,
       o.execution_date,
       CAST(o.insert_load_id AS INT),
       NULL,
       {QATAR_NOW}
FROM (
    SELECT s.*,
           row_number() OVER (PARTITION BY s.dataset_name, s.tablename
                              ORDER BY s.execution_date DESC, s.insert_load_id DESC) AS rn
    FROM {SOURCE_VIEW} s
    WHERE s.airflow_dag_name IS NOT NULL
      AND s.insert_load_id IS NOT NULL
) o
LEFT JOIN (
    SELECT DISTINCT dataset_name, source_tablename
    FROM {UPSTREAM_TRIGGER_LOG}
    WHERE source_insert_dt >= {QATAR_TODAY}
) l
  ON o.dataset_name = l.dataset_name
 AND o.tablename = l.source_tablename
LEFT JOIN (
    SELECT DISTINCT dataset_name, source_tablename, insert_load_id, source_insert_dt
    FROM {UPSTREAM_TRIGGER_LOG}
    WHERE source_insert_dt >= date_sub({QATAR_TODAY}, 1)
      AND insert_load_id IS NOT NULL
) k
  ON o.dataset_name = k.dataset_name
 AND o.tablename = k.source_tablename
 AND CAST(o.insert_load_id AS INT) = k.insert_load_id
 AND o.execution_date = k.source_insert_dt
WHERE o.rn = 1
  AND l.dataset_name IS NULL
  AND k.insert_load_id IS NULL
"""


def get_logger(spark_session):
    """Spark (log4j) logger, so messages appear in the CDE driver log."""
    return spark_session._jvm.org.apache.log4j.LogManager.getLogger(APP_NAME)


def build_spark_session():
    """Spark session; resources come from the CDE job / Airflow overrides."""
    return (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.jars", "/app/mount/common/ojdbc8.jar")
        # Impala's Iceberg TIMESTAMP columns are "without time zone"; Spark < 3.4 needs this flag.
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        # Timestamps pass through unchanged as wall-clock values; Qatar time is derived in SQL.
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )


def read_password_file(path):
    """Password from a file (e.g. a mounted CDE secret), first line, without the newline."""
    with open(path, encoding="utf-8") as handle:
        return handle.readline().strip()


def get_edw_config(spark_session):
    """(jdbc_url, user, password) from the Spark conf; the password may come from a file."""
    conf = {key: (spark_session.conf.get(key, "") or "").strip()
            for key in ("spark.edw_jdbc_url", "spark.edw_username", "spark.edw_password", "spark.edw_password_file")}
    if conf["spark.edw_password_file"]:
        conf["spark.edw_password"] = read_password_file(conf["spark.edw_password_file"])
    missing = [key for key in ("spark.edw_jdbc_url", "spark.edw_username", "spark.edw_password") if not conf[key]]
    if missing:
        raise ValueError(f"Missing required Spark conf: {', '.join(missing)}")
    return conf["spark.edw_jdbc_url"], conf["spark.edw_username"], conf["spark.edw_password"]


def etl(spark_session=None, source_query=ready_list_query, driver=ORACLE_DRIVER):
    """Extract today's READY loads and insert the new ones as READY rows."""
    own_session = spark_session is None
    logger = None
    try:
        if own_session:
            spark_session = build_spark_session()
        logger = get_logger(spark_session)
        logger.info(f"Starting application: '{APP_NAME}'")

        edw_jdbc_url, edw_username, edw_password = get_edw_config(spark_session)
        (spark_session.read.jdbc(
            edw_jdbc_url, source_query,
            properties={"user": edw_username, "password": edw_password, "driver": driver,
                        "fetchsize": JDBC_FETCH_SIZE},
        ).createOrReplaceTempView(SOURCE_VIEW))

        spark_session.sql(INSERT_NEW_READY_SQL)
        logger.info(f"New READY rows written to {UPSTREAM_TRIGGER_LOG}")
    except Exception as err:
        if logger:
            logger.error(f"Application failed: {type(err).__name__}: {err}")
        else:
            print(f"Application failed (before logger initialization): {err}", file=sys.stderr)
        raise
    finally:
        if own_session and spark_session:
            spark_session.stop()


if __name__ == "__main__":
    etl()
