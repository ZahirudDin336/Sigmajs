-- =====================================================================
-- One-off rebuild of ads.subscr_travel_analysis from the 2-year snapshot
-- (CDP / Hive 3). Produces exactly what the daily load would produce if
-- it had been run day by day over the same history:
--
--   travelling_country : first appearance order (extract_dt, then name),
--                        trimmed, case-insensitive distinct, QATAR / NULL /
--                        '' ignored, separator ', '
--   roaming_sp_name    : distinct, oldest first appearance first, new ones
--                        appended at the end, separator '|||'
--   return_to_country_dt: latest real return date by extract_dt, else 2999
--   extract_dt         : latest extract_dt of the trip
--   trips with only QATAR / NULL countries are kept (country NULL)
--
-- Snapshot columns assumed: extract_dt timestamp / 'yyyy-MM-dd HH:mm:ss'
-- string; out_of_country_dt and return_to_country_dt castable to timestamp.
-- =====================================================================

SET hive.vectorized.execution.enabled=true;
SET hive.map.aggr=true;
SET hive.optimize.reducededuplication=true;

-- 1. cleansed snapshot (same rules as the daily load)
DROP TABLE IF EXISTS dev_stg.travel_analysis_fix_20261004_src;
CREATE TABLE dev_stg.travel_analysis_fix_20261004_src STORED AS PARQUET AS
SELECT TRIM(vf_number)                                                     AS vf_number,
       COALESCE(NULLIF(TRIM(cust_num), ''), '#NULL#')                      AS cust_key,
       CAST(out_of_country_dt AS timestamp)                                AS out_of_country_dt,
       CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(extract_dt), 'yyyyMMdd') AS int)  AS extract_dt,
       CAST(return_to_country_dt AS timestamp)                             AS return_to_country_dt,
       CASE WHEN UPPER(TRIM(travelling_country)) = 'QATAR' THEN NULL
            ELSE NULLIF(TRIM(travelling_country), '')
       END                                                                 AS country,
       NULLIF(TRIM(roaming_sp_name), '')                                   AS roaming
FROM   DEV_RAW.EDW_SUBSCR_TRAVEL_ANALYSIS_SNAPSHOT_2Y_20261003
WHERE  NULLIF(TRIM(vf_number), '') IS NOT NULL
  AND  out_of_country_dt IS NOT NULL
  AND  extract_dt IS NOT NULL;

-- 2. one row per trip + item (C = country, R = roaming provider):
--    first appearance as 'yyyyMMdd#value'
DROP TABLE IF EXISTS dev_stg.travel_analysis_fix_20261004_item;
CREATE TABLE dev_stg.travel_analysis_fix_20261004_item STORED AS PARQUET AS
SELECT vf_number, cust_key, out_of_country_dt, i.kind,
       MIN(CONCAT(CAST(extract_dt AS string), '#', i.val))                 AS first_item,
       MAX(extract_dt)                                                     AS extract_dt,
       MAX(IF(return_to_country_dt < CAST('2999-12-31 00:00:00' AS timestamp),
              NAMED_STRUCT('e', extract_dt, 'v', return_to_country_dt), NULL)) AS last_return
FROM   dev_stg.travel_analysis_fix_20261004_src
LATERAL VIEW EXPLODE(MAP('C', country, 'R', roaming)) i AS kind, val
GROUP  BY vf_number, cust_key, out_of_country_dt, i.kind,
          IF(i.kind = 'C', UPPER(i.val), i.val);

-- 3. one row per trip
DROP TABLE IF EXISTS dev_stg.travel_analysis_fix_20261004_ads_rebuild;
CREATE TABLE dev_stg.travel_analysis_fix_20261004_ads_rebuild STORED AS PARQUET AS
SELECT vf_number,
       NULLIF(cust_key, '#NULL#')                                          AS cust_num,
       out_of_country_dt,
       COALESCE(MAX(last_return).v, CAST('2999-12-31 00:00:00' AS timestamp)) AS return_to_country_dt,
       -- countries: oldest first
       NULLIF(REGEXP_REPLACE(CONCAT_WS(', ',
                SORT_ARRAY(COLLECT_LIST(IF(kind = 'C', first_item, NULL)))),
              '[0-9]{8}#', ''), '')                                        AS travelling_country,
       -- roaming: oldest first, new ones at the end
       NULLIF(REGEXP_REPLACE(CONCAT_WS('|||',
                SORT_ARRAY(COLLECT_LIST(IF(kind = 'R', first_item, NULL)))),
              '[0-9]{8}#', ''), '')                                        AS roaming_sp_name,
       MAX(extract_dt)                                                     AS extract_dt
FROM   dev_stg.travel_analysis_fix_20261004_item
GROUP  BY vf_number, cust_key, out_of_country_dt;

-- 4. after review: load it
-- INSERT OVERWRITE TABLE ads.subscr_travel_analysis
-- SELECT vf_number, cust_num, out_of_country_dt, return_to_country_dt,
--        travelling_country, roaming_sp_name, extract_dt
-- FROM   dev_stg.travel_analysis_fix_20261004_ads_rebuild;
