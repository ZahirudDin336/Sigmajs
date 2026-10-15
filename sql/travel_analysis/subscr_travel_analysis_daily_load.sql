-- =====================================================================
-- Daily load : ads.subscr_travel_analysis  (CDP / Hive 3, CDE job)
--
-- Key : vf_number + cust_num + out_of_country_dt
--
-- travelling_country rules
--   * country already in the ads list            -> not concatenated
--   * country not in the list                    -> appended, ordered by extract_dt
--   * several new countries on the same extract_dt -> all appended
--     (alphabetical within that extract_dt)
--   * QATAR / NULL / ''                          -> ignored
--   * matching is trimmed + case-insensitive; list separator is ', '
--
-- roaming_sp_name rules
--   * oldest first appearance first, new providers appended at the end
--     (ordered by extract_dt, alphabetical within one extract_dt)
--   * provider already in the list -> not concatenated (trimmed, exact match)
--   * NULL / '' ignored; separator is '|||'
--
-- Only trips present in today's file are re-computed; all other ads rows
-- are copied unchanged by a map-only pass (broadcast join on today's
-- small file). Requires ads to be in the normalised form this job writes
-- (true after the 2-year rebuild or any earlier run of this job).
--
-- Steps:
--   1. stg.subscr_travel_analysis_delta : today's landing rows, cleansed
--   2. stg.subscr_travel_analysis_key   : today's trips merged with their ads rows
--   3. INSERT OVERWRITE ads = untouched ads rows + step 2
-- =====================================================================

SET hive.vectorized.execution.enabled=true;
SET hive.map.aggr=true;
SET hive.optimize.reducededuplication=true;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.optimize.cte.materialize.threshold=2;
SET hive.stats.autogather=true;

-- ---------------------------------------------------------------------
-- 0. Landing (external) and target (created on first run)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg.subscr_travel_analysis;
CREATE EXTERNAL TABLE stg.subscr_travel_analysis(
  extract_dt           bigint,
  vf_number            string,
  cust_num             string,
  out_of_country_dt    bigint,
  return_to_country_dt bigint,
  travelling_country   string,
  roaming_sp_name      string)
STORED AS PARQUET
LOCATION 'ofs://appr/vfqbigdata/upstream/edw/TRAVEL_ANALYSIS/';

CREATE TABLE IF NOT EXISTS ads.subscr_travel_analysis(
  vf_number            string,
  cust_num             string,
  out_of_country_dt    timestamp,
  return_to_country_dt timestamp,
  travelling_country   string,
  roaming_sp_name      string,
  extract_dt           int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

-- ---------------------------------------------------------------------
-- 1. today's landing rows, cleansed (QATAR / blank -> NULL, NULL return -> 2999)
--    ord = 15-digit sort key: 1 + extract_dt(8) + 000000
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg.subscr_travel_analysis_delta;
CREATE TABLE stg.subscr_travel_analysis_delta STORED AS PARQUET AS
SELECT TRIM(vf_number)                                                     AS vf_number,
       COALESCE(NULLIF(TRIM(cust_num), ''), '#NULL#')                      AS cust_key,
       CAST(FROM_UNIXTIME(CAST(out_of_country_dt / 1000 AS bigint)) AS timestamp) AS out_of_country_dt,
       CAST(FROM_UNIXTIME(CAST(extract_dt / 1000 AS bigint), 'yyyyMMdd') AS int)  AS extract_dt,
       CASE WHEN return_to_country_dt IS NULL
            THEN CAST('2999-12-31 00:00:00' AS timestamp)
            ELSE CAST(FROM_UNIXTIME(CAST(return_to_country_dt / 1000 AS bigint)) AS timestamp)
       END                                                                 AS return_to_country_dt,
       CONCAT('1', FROM_UNIXTIME(CAST(extract_dt / 1000 AS bigint), 'yyyyMMdd'), '000000') AS ord,
       CASE WHEN UPPER(TRIM(travelling_country)) = 'QATAR' THEN NULL
            ELSE NULLIF(TRIM(travelling_country), '')
       END                                                                 AS country,
       NULLIF(TRIM(roaming_sp_name), '')                                   AS roaming
FROM   stg.subscr_travel_analysis
WHERE  NULLIF(TRIM(vf_number), '') IS NOT NULL
  AND  out_of_country_dt IS NOT NULL
  AND  extract_dt IS NOT NULL;

-- ---------------------------------------------------------------------
-- 2. today's trips merged with their current ads rows, one row per trip
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg.subscr_travel_analysis_key;
CREATE TABLE stg.subscr_travel_analysis_key STORED AS PARQUET AS
WITH
-- current ads rows of today's trips (map-side semi join; materialised once)
hit AS (
  SELECT a.vf_number,
         COALESCE(a.cust_num, '#NULL#')                                    AS cust_key,
         a.out_of_country_dt, a.return_to_country_dt,
         a.travelling_country, a.roaming_sp_name, a.extract_dt
  FROM   ads.subscr_travel_analysis a
  LEFT   SEMI JOIN stg.subscr_travel_analysis_delta d
    ON   a.vf_number = d.vf_number
   AND   COALESCE(a.cust_num, '#NULL#') = d.cust_key
   AND   a.out_of_country_dt = d.out_of_country_dt
),
-- one row per item: kind C = country, R = roaming provider
--   ads items keep their list position: ord = 0 + 00000000 + position(6)
src AS (
  SELECT h.vf_number, h.cust_key, h.out_of_country_dt, 0 AS is_stg, h.extract_dt,
         h.return_to_country_dt, 'C' AS kind,
         CONCAT('000000000', LPAD(CAST(c.pos AS string), 6, '0'))          AS ord,
         NULLIF(TRIM(c.val), '')                                           AS val
  FROM   hit h
  LATERAL VIEW OUTER POSEXPLODE(SPLIT(h.travelling_country, ',')) c AS pos, val
  UNION ALL
  SELECT h.vf_number, h.cust_key, h.out_of_country_dt, 0, h.extract_dt,
         h.return_to_country_dt, 'R',
         CONCAT('000000000', LPAD(CAST(r.pos AS string), 6, '0')),
         NULLIF(TRIM(r.val), '')
  FROM   hit h
  LATERAL VIEW POSEXPLODE(SPLIT(h.roaming_sp_name, '\\|\\|\\|')) r AS pos, val
  UNION ALL
  SELECT d.vf_number, d.cust_key, d.out_of_country_dt, 1, d.extract_dt,
         d.return_to_country_dt, i.kind, d.ord, i.val
  FROM   stg.subscr_travel_analysis_delta d
  LATERAL VIEW EXPLODE(MAP('C', d.country, 'R', d.roaming)) i AS kind, val
),
-- one row per trip + item: first occurrence wins -> no duplicates
--   countries match case-insensitive, providers match exactly
item AS (
  SELECT vf_number, cust_key, out_of_country_dt, kind,
         MIN(CONCAT(ord, '#', val))                                        AS first_item,
         MAX(extract_dt)                                                   AS extract_dt,
         MAX(IF(is_stg = 0, return_to_country_dt, NULL))                   AS ads_return,
         -- latest real (not open) return date from landing
         MAX(IF(is_stg = 1 AND return_to_country_dt < CAST('2999-12-31 00:00:00' AS timestamp),
                NAMED_STRUCT('e', extract_dt, 'v', return_to_country_dt), NULL)) AS stg_return
  FROM   src
  GROUP  BY vf_number, cust_key, out_of_country_dt, kind,
            IF(kind = 'C', UPPER(val), val)
)
SELECT vf_number,
       NULLIF(cust_key, '#NULL#')                                          AS cust_num,
       out_of_country_dt,
       -- latest real landing date, else ads date, else open
       COALESCE(MAX(stg_return).v, MAX(ads_return),
                CAST('2999-12-31 00:00:00' AS timestamp))                  AS return_to_country_dt,
       NULLIF(REGEXP_REPLACE(CONCAT_WS(', ',
                SORT_ARRAY(COLLECT_LIST(IF(kind = 'C', first_item, NULL)))),
              '[0-9]{15}#', ''), '')                                       AS travelling_country,
       NULLIF(REGEXP_REPLACE(CONCAT_WS('|||',
                SORT_ARRAY(COLLECT_LIST(IF(kind = 'R', first_item, NULL)))),
              '[0-9]{15}#', ''), '')                                       AS roaming_sp_name,
       MAX(extract_dt)                                                     AS extract_dt
FROM   item
GROUP  BY vf_number, cust_key, out_of_country_dt;

-- ---------------------------------------------------------------------
-- 3. final load: untouched rows (map-only anti join) + today's trips
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE ads.subscr_travel_analysis
SELECT a.vf_number, a.cust_num, a.out_of_country_dt, a.return_to_country_dt,
       a.travelling_country, a.roaming_sp_name, a.extract_dt
FROM   ads.subscr_travel_analysis a
LEFT   JOIN stg.subscr_travel_analysis_key k
  ON   a.vf_number = k.vf_number
 AND   COALESCE(a.cust_num, '#NULL#') = COALESCE(k.cust_num, '#NULL#')
 AND   a.out_of_country_dt = k.out_of_country_dt
WHERE  k.vf_number IS NULL

UNION ALL

SELECT vf_number, cust_num, out_of_country_dt, return_to_country_dt,
       travelling_country, roaming_sp_name, extract_dt
FROM   stg.subscr_travel_analysis_key;
