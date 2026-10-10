import sys
from pyspark.sql import SparkSession
# CDP: geospatial imports below are unused by this job. They are commented out because
# `fiona.path` no longer exists in Fiona >= 1.10, and shapely/geopandas/fiona are often
# missing from the CDE/YARN Python env -> ImportError before Spark even starts.
# from shapely.geometry import Point
# import geopandas as gpd
# import fiona.path
import pandas as pd
import decimal
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


def get_logger(spark_session):
	log4j = spark_session._jvm.org.apache.log4j
	return log4j.LogManager.getLogger(__name__)


APP_NAME = "test_upstream_dly"
BD_test_upstream_dly="dev_stg.tmp_upstream_list_dly"


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

def etl():
    spark_session = None
    logger = None
    try:
        spark_session = (
            SparkSession.builder.appName(APP_NAME)
            # NOTE (CDP/CDE): driver memory and spark.jars must also be passed at submit time
            # (CDE job config / spark-submit --driver-memory --jars); set here they are too late
            # for the already-running driver JVM.
            .config("spark.executor.memory", "10g")
            .config("spark.driver.memory", "10g")
            .config("spark.dynamicAllocation.maxExecutors", "5")
            .config("spark.executor.cores", "5")
            .config("spark.cores.max", "5")
            # CDP: spark.yarn.executor.memoryOverhead is deprecated (ignored on CDE/Kubernetes)
            .config("spark.executor.memoryOverhead", "8g")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
            .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
            .config("mapreduce.input.fileinputformat.input.dir.recursive", "true") 
            .config("spark.jars", "/app/mount/common/ojdbc8.jar")
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .enableHiveSupport()
            .getOrCreate()
        )

        logger = get_logger(spark_session)
        spark_session.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
        spark_session.sql("SET hive.exec.dynamic.partition = true")

        logger.info(f"Starting application: '{APP_NAME}'")
        
        driver   = "oracle.jdbc.driver.OracleDriver"

        edw_jdbc_url = spark_session.conf.get("spark.edw_jdbc_url")
        edw_username = spark_session.conf.get("spark.edw_username")
        edw_password = spark_session.conf.get("spark.edw_password")

        # fetchsize: Oracle JDBC default is 10 rows per round trip
        df_list = spark_session.read.jdbc(edw_jdbc_url,ready_list_query,properties={"user": edw_username, "password": edw_password, "driver": driver, "fetchsize": "10000"})

        df_list.createOrReplaceTempView("test_upstream_dly")

        # CDP: DROP TABLE + CTAS fails on re-run with LOCATION_ALREADY_EXISTS when the HMS
        # (managed->external translation) drops the table but leaves its folder on HDFS/S3.
        # Create the table once, then overwrite its data on every run.
        # external.table.purge: a manual DROP TABLE will also delete the files.
        spark_session.sql(f""" CREATE TABLE IF NOT EXISTS {BD_test_upstream_dly} STORED AS PARQUET
                                TBLPROPERTIES ('external.table.purge'='true') AS
                                SELECT * FROM test_upstream_dly WHERE 1=0 """)
        spark_session.sql(f""" INSERT OVERWRITE TABLE {BD_test_upstream_dly}
                                SELECT * FROM test_upstream_dly """)

        logger.info(f"Data successfully written to table '{BD_test_upstream_dly}'")

#df_upstream = spark.sql("""SELECT * FROM  dev_stg.upstream_trigger_log""")
        spark_session.sql("""
        INSERT INTO dev_stg.upstream_trigger_log
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
        o.status as status, null as target_table ,
        o.execution_date as source_insert_dt, 
        cast(o.insert_load_id as int) as insert_load_id,
        current_timestamp() as triggered_ts,
        NULL AS updated_ts
        FROM dev_stg.tmp_upstream_list_dly o
        LEFT JOIN  dev_stg.upstream_trigger_log l
        ON o.dataset_name = l.dataset_name 
        and  o.tablename = l.source_tablename 
        and l.source_insert_dt >= current_date()
        where l.INSERT_LOAD_ID is null
        and l.dataset_name IS NULL
        AND l.source_tablename IS NULL
        """)

        logger.info(f"Data successfully written to table dev_stg.upstream_trigger_log")

    except Exception as err:
        if logger:
            logger.error(f"An error occured during application execution")
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
