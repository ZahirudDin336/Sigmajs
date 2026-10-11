"""Runs travel_analysis_daily_load.sql + DQ checks on Spark (Hive dialect) with test data.

Usage: python run_test.py   (needs pyspark + Java)
"""
import os
import re
import sys
import tempfile
from datetime import datetime, timezone

from pyspark.sql import SparkSession

HERE = os.path.dirname(os.path.abspath(__file__))
LOAD_SQL = os.path.join(HERE, "..", "travel_analysis_daily_load.sql")
DQ_SQL = os.path.join(HERE, "..", "travel_analysis_dq_checks.sql")

work = tempfile.mkdtemp()
spark = (SparkSession.builder.master("local[2]")
         .config("spark.sql.warehouse.dir", os.path.join(work, "wh"))
         .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={work}/ms;create=true")
         .config("spark.sql.session.timeZone", "UTC")
         # Hive-style write (staging dir + move) so the target can be read while overwritten
         .config("spark.sql.hive.convertMetastoreParquet", "false")
         .config("spark.sql.hive.convertInsertingNonPartitionedTable", "false")
         .enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def ms(s):
    return int(datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def statements(path):
    sql = re.sub(r"--[^\n]*", "", open(path).read())
    return [s.strip() for s in sql.split(";") if s.strip()]


def run_load():
    for st in statements(LOAD_SQL):
        if st.upper().startswith("SET "):
            continue
        # landing table is created by the test itself
        if "dev_stg.travel_analysis(" in st:
            continue
        spark.sql(st)


for db in ("dev_stg", "dev_ads"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

T1, T2 = ms("2026-09-01"), ms("2026-09-10")
D1, D2, D3 = ms("2026-10-01"), ms("2026-10-02"), ms("2026-10-03")
OPEN = 6854

# --- existing target ---------------------------------------------------
spark.sql("""
CREATE TABLE dev_ads.travel_analysis(vf_number string, cust_num string, out_of_country_dt timestamp,
  return_to_country_dt timestamp, travelling_country string, roaming_sp_name string, extract_dt int)
STORED AS PARQUET""")
spark.sql("""INSERT INTO dev_ads.travel_analysis VALUES
 ('K1','C1',TIMESTAMP'2026-09-01 00:00:00',TIMESTAMP'2999-12-31 00:00:00','India, UAE','Ooredoo',20260930),
 ('K3','C3',TIMESTAMP'2026-09-01 00:00:00',TIMESTAMP'2026-09-20 00:00:00','UK','A',20260930),
 ('K5','C5',TIMESTAMP'2026-09-01 00:00:00',TIMESTAMP'2026-09-05 00:00:00','Spain, Italy','Z',20260930),
 ('K6',NULL,TIMESTAMP'2026-09-01 00:00:00',TIMESTAMP'2999-12-31 00:00:00','Oman','R',20260930),
 ('K7','C7',TIMESTAMP'2026-09-01 00:00:00',TIMESTAMP'2999-12-31 00:00:00','India,UAE',NULL,20260930),
 ('K2','C2',TIMESTAMP'2026-09-01 00:00:00',TIMESTAMP'2999-12-31 00:00:00',NULL,NULL,20260930),
 ('K9','C9',TIMESTAMP'2026-09-01 00:00:00',TIMESTAMP'2999-12-31 00:00:00',' Oman ',NULL,20260930)
""")

# --- landing (stage) ----------------------------------------------------
stage = [
    # extract, vf, cust, out, return, country, roaming
    (D1, "K1", "C1", T1, ms("2026-09-25"), "INDIA", "Vodafone"),   # existing country, other case
    (D2, "K1", "C1", T1, OPEN, "Oman", "Vodafone"),                # new country
    (D2, "K1", "C1", T1, OPEN, "QATAR", None),                     # qatar -> skip
    (D1, "K2", "C2", T1, OPEN, "qatar", None),
    (D2, "K2", "C2", T1, OPEN, "Kenya", "X"),
    (D3, "K2", "C2", T1, OPEN, "Kenya", "X"),                      # same country again
    (D1, "K3", "C3", T1, OPEN, " Qatar ", "A"),                    # only qatar -> unchanged list
    (D2, "K4", "C4", T2, OPEN, "France", None),                    # new key, later extract
    (D1, "K4", "C4", T2, OPEN, "Spain", None),                     # new key, earlier extract
    (D1, "K4", "C4", T2, OPEN, "", None),                          # blank -> skip
    (D1, "K4", "C4", T2, OPEN, "Spain", None),                     # exact dup row
    (D1, "K6", None, T1, OPEN, "Oman", None),                      # null cust_num, existing country
    (D2, "K6", None, T1, OPEN, "Yemen", None),
    (D1, "K7", "C7", T1, OPEN, "UAE", None),                       # list without space
    (D1, "K8", "C8", T1, OPEN, "QATAR", None),                     # new key, only qatar
    (D1, "K9", "C9", T1, OPEN, "  oman  ", None),                  # padded existing country
    (D2, "K9", "C9", T1, OPEN, " Yemen ", None),                   # padded new country
    (D1, None, "C9", T1, OPEN, "Peru", None),                      # null vf -> rejected
]
cols = "extract_dt bigint, vf_number string, cust_num string, out_of_country_dt bigint, " \
       "return_to_country_dt bigint, travelling_country string, roaming_sp_name string"
spark.createDataFrame(stage, cols).write.mode("overwrite").format("parquet").saveAsTable("dev_stg.travel_analysis")

expected = {
    # vf: (cust, return, countries, roaming, extract)
    "K1": ("C1", "2026-09-25 00:00:00", "India, UAE, Oman", "Vodafone|||Ooredoo", 20261002),
    "K2": ("C2", "2999-12-31 00:00:00", "Kenya", "X", 20261003),
    "K3": ("C3", "2026-09-20 00:00:00", "UK", "A", 20261001),
    "K4": ("C4", "2999-12-31 00:00:00", "Spain, France", None, 20261002),
    "K5": ("C5", "2026-09-05 00:00:00", "Spain, Italy", "Z", 20260930),
    "K6": (None, "2999-12-31 00:00:00", "Oman, Yemen", "R", 20261002),
    "K7": ("C7", "2999-12-31 00:00:00", "India, UAE", None, 20261001),
    "K8": ("C8", "2999-12-31 00:00:00", None, None, 20261001),
    "K9": ("C9", "2999-12-31 00:00:00", "Oman, Yemen", None, 20261002),
}


def check(run):
    rows = spark.sql("""SELECT vf_number, cust_num, CAST(return_to_country_dt AS string) r,
                               travelling_country, roaming_sp_name, extract_dt
                        FROM dev_ads.travel_analysis ORDER BY vf_number""").collect()
    ok = True
    got = {}
    for r in rows:
        if r.vf_number in got:
            print(f"  FAIL duplicate key {r.vf_number}")
            ok = False
        got[r.vf_number] = (r.cust_num, r.r, r.travelling_country, r.roaming_sp_name, r.extract_dt)
    for k, exp in expected.items():
        status = "ok  " if got.get(k) == exp else "FAIL"
        ok &= got.get(k) == exp
        print(f"  {status} {k}: {got.get(k)}" + ("" if got.get(k) == exp else f"  expected {exp}"))
    if set(got) - set(expected):
        print(f"  FAIL unexpected keys {set(got) - set(expected)}")
        ok = False
    for i, st in enumerate(statements(DQ_SQL), 1):
        v = spark.sql(st).collect()[0][0]
        print(f"  {'ok  ' if v == 0 else 'FAIL'} DQ check {i} = {v}")
        ok &= v == 0
    print(f"run {run}: {'PASS' if ok else 'FAIL'}")
    return ok


run_load()
r1 = check(1)
run_load()   # re-run same delta: must be idempotent
r2 = check(2)
sys.exit(0 if r1 and r2 else 1)
