"""Tests subscr_travel_analysis_daily_load.sql on Spark (Hive dialect, Hive serde tables).

Day 1 file -> load -> check, day 2 file -> load -> check, day 2 rerun -> check (idempotent).

Usage:
  python run_test.py              # new script, all scenarios + DQ checks
  python run_test.py --original   # current production script on the same data (shows its issues)
Needs pyspark + Java.
"""
import os
import re
import sys
import tempfile
from datetime import datetime, timezone

from pyspark.sql import SparkSession

HERE = os.path.dirname(os.path.abspath(__file__))
ORIGINAL = "--original" in sys.argv
LOAD_SQL = os.path.join(HERE, "original_load.sql") if ORIGINAL \
    else os.path.join(HERE, "..", "subscr_travel_analysis_daily_load.sql")
DQ_SQL = os.path.join(HERE, "..", "subscr_travel_analysis_dq_checks.sql")

work = tempfile.mkdtemp()
spark = (SparkSession.builder.master("local[2]")
         .config("spark.sql.warehouse.dir", os.path.join(work, "wh"))
         .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                 f"jdbc:derby:;databaseName={os.path.join(work, 'metastore_db')};create=true")
         .config("spark.sql.session.timeZone", "UTC")
         # Hive serde write path (staging dir + move), like Hive itself
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
        # landing table (external, ofs://) is created by the test itself
        if re.search(r"stg\.subscr_travel_analysis(\(|$)", st):
            continue
        spark.sql(st)


LANDING_COLS = ("extract_dt bigint, vf_number string, cust_num string, out_of_country_dt bigint, "
                "return_to_country_dt bigint, travelling_country string, roaming_sp_name string")


def land(rows):
    spark.createDataFrame(rows, LANDING_COLS).write.mode("overwrite").format("parquet") \
        .saveAsTable("stg.subscr_travel_analysis")


for db in ("stg", "ads"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

T1 = ms("2026-09-01")
D1, D2, D3 = ms("2026-10-01"), ms("2026-10-02"), ms("2026-10-03")
OPEN = "2999-12-31 00:00:00"

# --- ads before day 1 -------------------------------------------------
spark.sql("""
CREATE TABLE ads.subscr_travel_analysis(vf_number string, cust_num string, out_of_country_dt timestamp,
  return_to_country_dt timestamp, travelling_country string, roaming_sp_name string, extract_dt int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'""")
spark.sql(f"""INSERT INTO ads.subscr_travel_analysis VALUES
 ('A1', 'C1', TIMESTAMP'2026-09-01 00:00:00', TIMESTAMP'{OPEN}', 'India, UAE', 'Ooredoo', 20260930),
 ('A2', 'C2', TIMESTAMP'2026-09-01 00:00:00', TIMESTAMP'{OPEN}', NULL, NULL, 20260930),
 ('A3', 'C3', TIMESTAMP'2026-09-01 00:00:00', TIMESTAMP'2026-09-20 00:00:00', 'UK', 'A', 20260930),
 ('A5', 'C5', TIMESTAMP'2026-09-01 00:00:00', NULL, 'Spain, Italy', 'Z', 20260930),
 ('A6', NULL, TIMESTAMP'2026-09-01 00:00:00', TIMESTAMP'{OPEN}', 'Oman', 'R', 20260930),
 ('A7', 'C7', TIMESTAMP'2026-09-01 00:00:00', TIMESTAMP'{OPEN}', 'India,UAE', NULL, 20260930),
 ('A9', 'C9', TIMESTAMP'2026-09-01 00:00:00', TIMESTAMP'{OPEN}', ' Oman ', NULL, 20260930),
 ('A10','C10',TIMESTAMP'2026-09-01 00:00:00', TIMESTAMP'{OPEN}', 'India', NULL, 20260930),
 ('A13','C13',TIMESTAMP'2026-09-01 00:00:00', TIMESTAMP'{OPEN}', 'Iran', 'P ||| Q', 20260930)
""")

# extract, vf, cust, out, return, country, roaming
DAY1 = [
    (D1, "A1", "C1", T1, ms("2026-09-25"), "INDIA", "Vodafone"),  # already in list (other case) + real return
    (D1, "A1", "C1", T1, None, "QATAR", None),                    # QATAR ignored
    (D1, "A2", "C2", T1, None, "qatar", None),                    # only QATAR, empty list stays empty
    (D1, "A3", "C3", T1, None, " Qatar ", "A"),                   # padded QATAR, roaming already present
    (D1, "A4", "C4", T1, None, "Spain", None),                    # new key
    (D1, "A4", "C4", T1, None, "", None),                         # blank ignored
    (D1, "A4", "C4", T1, None, "Spain", None),                    # exact duplicate row
    (D1, "A6", None, T1, None, "Oman", None),                     # NULL cust_num, existing country
    (D1, "A6", None, T1, None, "Yemen", None),                    # NULL cust_num, new country
    (D1, "A7", "C7", T1, None, "UAE", None),                      # list stored without space
    (D1, "A8", "C8", T1, None, "QATAR", None),                    # new key with only QATAR
    (D1, "A9", "C9", T1, None, "  oman  ", None),                 # padded existing country
    (D1, "A9", "C9", T1, None, " Yemen ", None),                  # padded new country
    (D1, "A10", "C10", T1, None, "Oman", None),                   # two new countries, same extract_dt
    (D1, "A10", "C10", T1, None, "Kenya", None),
    (D1, "A11", "C11", T1, None, "Germany", None),                # new key, two countries same extract_dt
    (D1, "A11", "C11", T1, None, "France", None),
    (D2, "A12", "C12", T1, None, "Peru", None),                   # new key, later extract_dt first in file
    (D1, "A12", "C12", T1, None, "Chile", None),
    (D1, "A13", "C13", T1, None, "Iran", "Q"),                    # provider already in list (spaced '|||')
    (D1, "A13", "C13", T1, None, "Iran", "T"),                    # two new providers, same extract_dt
    (D1, "A13", "C13", T1, None, "Iran", "S"),
    (D1, None, "C99", T1, None, "Peru", None),                    # NULL vf_number rejected
]
DAY2 = [
    (D2, "A1", "C1", T1, None, "Oman", "Vodafone"),               # new country; roaming already present
    (D2, "A1", "C1", T1, None, "QATAR", None),
    (D2, "A1", "C1", T1, None, "UAE", None),                      # already in list
    (D2, "A2", "C2", T1, None, "Kenya", "X"),                     # first country; roaming into NULL
    (D3, "A2", "C2", T1, None, "Kenya", "X"),                     # same country, later day
    (D2, "A4", "C4", T1, None, "France", None),
    (D2, "A4", "C4", T1, None, "spain", None),
    (D2, "A10", "C10", T1, None, "india", None),
    (D2, "A10", "C10", T1, None, "Brazil", None),
    (D2, "A11", "C11", T1, None, "germany", None),                # nothing new
    (D2, "A13", "C13", T1, None, "Iran", "U"),                    # new provider appended at the end
    (D2, "A13", "C13", T1, None, "Iran", "P"),
]

EXP_DAY1 = {
    # vf: (cust, return, travelling_country, roaming, extract_dt)
    "A1":  ("C1", "2026-09-25 00:00:00", "India, UAE", "Ooredoo|||Vodafone", 20261001),
    "A2":  ("C2", OPEN, None, None, 20261001),
    "A3":  ("C3", "2026-09-20 00:00:00", "UK", "A", 20261001),
    "A4":  ("C4", OPEN, "Spain", None, 20261001),
    "A5":  ("C5", None, "Spain, Italy", "Z", 20260930),
    "A6":  (None, OPEN, "Oman, Yemen", "R", 20261001),
    "A7":  ("C7", OPEN, "India, UAE", None, 20261001),
    "A8":  ("C8", OPEN, None, None, 20261001),
    "A9":  ("C9", OPEN, "Oman, Yemen", None, 20261001),
    "A10": ("C10", OPEN, "India, Kenya, Oman", None, 20261001),
    "A11": ("C11", OPEN, "France, Germany", None, 20261001),
    "A12": ("C12", OPEN, "Chile, Peru", None, 20261002),
    "A13": ("C13", OPEN, "Iran", "P|||Q|||S|||T", 20261001),
}
EXP_DAY2 = dict(EXP_DAY1, **{
    "A1":  ("C1", "2026-09-25 00:00:00", "India, UAE, Oman", "Ooredoo|||Vodafone", 20261002),
    "A2":  ("C2", OPEN, "Kenya", "X", 20261003),
    "A4":  ("C4", OPEN, "Spain, France", None, 20261002),
    "A10": ("C10", OPEN, "India, Kenya, Oman, Brazil", None, 20261002),
    "A11": ("C11", OPEN, "France, Germany", None, 20261002),
    "A13": ("C13", OPEN, "Iran", "P|||Q|||S|||T|||U", 20261002),
})


def check(name, expected):
    rows = spark.sql("""SELECT vf_number, cust_num, CAST(return_to_country_dt AS string) r,
                               travelling_country, roaming_sp_name, extract_dt
                        FROM ads.subscr_travel_analysis""").collect()
    got, ok = {}, True
    for r in rows:
        v = (r.cust_num, r.r, r.travelling_country, r.roaming_sp_name, r.extract_dt)
        if r.vf_number in got:
            print(f"  FAIL {r.vf_number}: duplicate row {v}")
            ok = False
            continue
        got[r.vf_number] = v
    for k in sorted(expected, key=lambda x: int(x[1:])):
        good = got.get(k) == expected[k]
        ok &= good
        print(f"  {'ok  ' if good else 'FAIL'} {k:4} {got.get(k)}" + ("" if good else f"\n       expected {expected[k]}"))
    extra = set(got) - set(expected)
    if extra:
        print(f"  FAIL unexpected keys {extra}")
        ok = False
    if not ORIGINAL:
        for i, st in enumerate(statements(DQ_SQL), 1):
            v = spark.sql(st).collect()[0][0]
            print(f"  {'ok  ' if v == 0 else 'FAIL'} DQ check {i} = {v}")
            ok &= v == 0
    print(f"{name}: {'PASS' if ok else 'FAIL'}\n")
    return ok


results = []
for name, data, exp in (("day 1", DAY1, EXP_DAY1), ("day 2", DAY2, EXP_DAY2), ("day 2 rerun", DAY2, EXP_DAY2)):
    land(data)
    run_load()
    results.append(check(name, exp))
sys.exit(0 if all(results) else 1)
