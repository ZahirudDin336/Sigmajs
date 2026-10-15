"""Checks the 2-year rebuild against the daily load.

The same history is (a) replayed day by day through subscr_travel_analysis_daily_load.sql
and (b) rebuilt in one go from a snapshot. Both results must be identical.

Usage:
  python run_rebuild_test.py              # fixed rebuild (subscr_travel_analysis_2y_rebuild.sql)
  python run_rebuild_test.py --original   # rebuild as received (test/user_rebuild_2y.sql)
Needs pyspark + Java.
"""
import os
import re
import sys
import tempfile
from datetime import datetime, timezone

from pyspark.sql import SparkSession

HERE = os.path.dirname(os.path.abspath(__file__))
DAILY_SQL = os.path.join(HERE, "..", "subscr_travel_analysis_daily_load.sql")
REBUILD_SQL = os.path.join(HERE, "user_rebuild_2y.sql") if "--original" in sys.argv \
    else os.path.join(HERE, "..", "subscr_travel_analysis_2y_rebuild.sql")

work = tempfile.mkdtemp()
spark = (SparkSession.builder.master("local[2]")
         .config("spark.sql.warehouse.dir", os.path.join(work, "wh"))
         .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                 f"jdbc:derby:;databaseName={os.path.join(work, 'metastore_db')};create=true")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.hive.convertMetastoreParquet", "false")
         .config("spark.sql.hive.convertInsertingNonPartitionedTable", "false")
         .enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def statements(path):
    sql = re.sub(r"--[^\n]*", "", open(path).read())
    return [s.strip() for s in sql.split(";") if s.strip()]


def run(path):
    for st in statements(path):
        if st.upper().startswith("SET ") or re.search(r"stg\.subscr_travel_analysis(\(|$)", st):
            continue
        spark.sql(st)


def dt(s):
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


for db in ("stg", "ads", "dev_stg", "dev_raw"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

OUT = "2026-01-01"
# extract_dt, vf, cust, out, return, country, roaming
HIST = [
    # T1: same country other case, QATAR, padded, return set on last day, roaming changes
    ("2026-01-01", "T1", "C1", OUT, None, "India", "Ooredoo"),
    ("2026-01-02", "T1", "C1", OUT, None, "INDIA", None),
    ("2026-01-02", "T1", "C1", OUT, None, "UAE", None),
    ("2026-01-03", "T1", "C1", OUT, None, "QATAR", None),
    ("2026-01-04", "T1", "C1", OUT, None, "Oman ", "Vodafone"),
    ("2026-01-05", "T1", "C1", OUT, "2026-01-20", "india", "Ooredoo"),
    # T2: NULL cust_num, QATAR and NULL country first
    ("2026-01-01", "T2", None, OUT, None, "QATAR", None),
    ("2026-01-02", "T2", None, OUT, None, None, None),
    ("2026-01-03", "T2", None, OUT, None, "Kenya", "X"),
    # T3: empty cust_num
    ("2026-01-01", "T3", "", OUT, None, "Spain", None),
    ("2026-01-02", "T3", " ", OUT, None, "France", None),
    # T4: only QATAR / blank -> trip must still exist
    ("2026-01-01", "T4", "C4", OUT, None, "QATAR", "Y"),
    ("2026-01-02", "T4", "C4", OUT, "2026-01-03", "", None),
    # T5: two countries on the same extract_dt
    ("2026-01-01", "T5", "C5", OUT, None, "Germany", None),
    ("2026-01-01", "T5", "C5", OUT, None, "France", None),
    ("2026-01-02", "T5", "C5", OUT, None, "Italy", None),
    # T6: return date corrected to an earlier date later on
    ("2026-01-01", "T6", "C6", OUT, "2026-02-10", "Peru", None),
    ("2026-01-02", "T6", "C6", OUT, "2026-02-05", "Peru", None),
    # T7: same country on later days -> extract_dt must move forward
    ("2026-01-01", "T7", "C7", OUT, None, "Oman", None),
    ("2026-01-04", "T7", "C7", OUT, None, "Oman", "Z"),
]

# (a) daily replay ------------------------------------------------------
ms = lambda s: None if s is None else int(dt(s).timestamp() * 1000)
landing_cols = ("extract_dt bigint, vf_number string, cust_num string, out_of_country_dt bigint, "
                "return_to_country_dt bigint, travelling_country string, roaming_sp_name string")
for day in sorted({h[0] for h in HIST}):
    rows = [(ms(e), v, c, ms(o), ms(r), t, rs) for e, v, c, o, r, t, rs in HIST if e == day]
    spark.createDataFrame(rows, landing_cols).write.mode("overwrite").format("parquet") \
        .saveAsTable("stg.subscr_travel_analysis")
    run(DAILY_SQL)

# (b) rebuild from snapshot --------------------------------------------
snap = [(dt(e), v, c, dt(o), dt(r) if r else None, t, rs) for e, v, c, o, r, t, rs in HIST]
spark.createDataFrame(snap, "extract_dt timestamp, vf_number string, cust_num string, "
                            "out_of_country_dt timestamp, return_to_country_dt timestamp, "
                            "travelling_country string, roaming_sp_name string") \
    .write.mode("overwrite").format("parquet").saveAsTable("dev_raw.edw_subscr_travel_analysis_snapshot_2y_20261003")
run(REBUILD_SQL)

q = """SELECT vf_number, cust_num, CAST(out_of_country_dt AS string) o, CAST(return_to_country_dt AS string) r,
              travelling_country, roaming_sp_name, extract_dt FROM {}"""
daily = {(r.vf_number, r.cust_num): r for r in spark.sql(q.format("ads.subscr_travel_analysis")).collect()}
rebuild = {}
for r in spark.sql(q.format("dev_stg.travel_analysis_fix_20261004_ads_rebuild")).collect():
    rebuild.setdefault(r.vf_number, []).append(r)

ok = True
for (vf, cust), d in sorted(daily.items()):
    rb = rebuild.get(vf, [])
    dv = tuple(d)[1:]
    if len(rb) == 1 and tuple(rb[0])[1:] == dv:
        print(f"  ok   {vf} {dv}")
        continue
    ok = False
    print(f"  FAIL {vf}\n       daily   {dv}")
    if not rb:
        print("       rebuild <missing>")
    for r in rb:
        print(f"       rebuild {tuple(r)[1:]}")
print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
