-- =====================================================================
-- Daily load : dev_ads.travel_analysis  (CDP / Hive 3)
--
-- Business key : vf_number + cust_num + out_of_country_dt
--
-- travelling_country rules
--   * Comma separated list (", "), ordered by first appearance:
--       existing target list first (original order kept), then new
--       countries from stage ordered by extract_dt ascending.
--   * No duplicate country (case-insensitive, trimmed).
--   * Stage value NULL / '' / 'QATAR'  -> treated as NULL -> skipped.
--   * Stage country already in the list -> skipped.
--   * Stage can carry many rows per key (same or new country, several
--     extract_dt) -> collapsed to ONE row per key before merging.
--
-- Fixes vs. previous version
--   1. Several stage rows per key produced several output rows.
--   2. LIKE '%,X,%' never matched lists built with ", " (comma+space),
--      so already present countries were appended again.
--   3. cust_num NULL never matched in the join -> row duplicated
--      (once from stage, once as "target only").
--   4. New key with open trip (6854) got NULL return_to_country_dt
--      instead of 2999-12-31.
--   5. New roaming_sp_name was dropped when target value was NULL and
--      was re-appended every day when already present.
--   6. Typo PARQURT, snapshot table name mismatch.
-- =====================================================================

SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;
SET hive.auto.convert.join=true;
SET hive.vectorized.execution.enabled=true;

-- ---------------------------------------------------------------------
-- 1. Landing (external, parquet)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dev_stg.travel_analysis;
CREATE EXTERNAL TABLE dev_stg.travel_analysis(
  extract_dt           bigint,
  vf_number            string,
  cust_num             string,
  out_of_country_dt    bigint,
  return_to_country_dt bigint,
  travelling_country   string,
  roaming_sp_name      string)
STORED AS PARQUET
LOCATION 'ofs://appr/bigdata/upstream-landing/travel_analysis/';

-- ---------------------------------------------------------------------
-- 2. Delta : typed + cleansed (QATAR / blank -> NULL), exact dups removed
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dev_stg.delta_travel_analysis;
CREATE TABLE dev_stg.delta_travel_analysis STORED AS PARQUET AS
SELECT DISTINCT
       TRIM(vf_number)                                                   AS vf_number,
       NULLIF(TRIM(cust_num), '')                                        AS cust_num,
       CAST(FROM_UNIXTIME(CAST(out_of_country_dt / 1000 AS bigint)) AS timestamp) AS out_of_country_dt,
       CASE WHEN return_to_country_dt = 6854
            THEN CAST('2999-12-31 00:00:00' AS timestamp)
            ELSE CAST(FROM_UNIXTIME(CAST(return_to_country_dt / 1000 AS bigint)) AS timestamp)
       END                                                               AS return_to_country_dt,
       CASE WHEN travelling_country IS NULL
              OR TRIM(travelling_country) = ''
              OR UPPER(TRIM(travelling_country)) = 'QATAR'
            THEN NULL
            ELSE TRIM(travelling_country)
       END                                                               AS travelling_country,
       NULLIF(TRIM(roaming_sp_name), '')                                 AS roaming_sp_name,
       CAST(FROM_UNIXTIME(CAST(extract_dt / 1000 AS bigint), 'yyyyMMdd') AS int) AS extract_dt
FROM   dev_stg.travel_analysis
WHERE  vf_number IS NOT NULL
  AND  TRIM(vf_number) <> ''
  AND  out_of_country_dt IS NOT NULL
  AND  extract_dt IS NOT NULL;

-- ---------------------------------------------------------------------
-- 3. Delta collapsed to one row per business key
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dev_stg.delta_travel_analysis_key;
CREATE TABLE dev_stg.delta_travel_analysis_key STORED AS PARQUET AS
SELECT vf_number,
       cust_num,
       out_of_country_dt,
       -- latest real (not open) return date in this delta
       MAX(CASE WHEN return_to_country_dt IS NOT NULL
                 AND return_to_country_dt <> CAST('2999-12-31 00:00:00' AS timestamp)
                THEN NAMED_STRUCT('e', extract_dt, 'v', return_to_country_dt)
           END).v                                                        AS return_to_country_dt,
       MAX(CASE WHEN return_to_country_dt = CAST('2999-12-31 00:00:00' AS timestamp)
                THEN 1 ELSE 0 END)                                       AS has_open_trip,
       -- latest non-null roaming provider in this delta
       MAX(CASE WHEN roaming_sp_name IS NOT NULL
                THEN NAMED_STRUCT('e', extract_dt, 'v', roaming_sp_name)
           END).v                                                        AS roaming_sp_name,
       MAX(extract_dt)                                                   AS extract_dt
FROM   dev_stg.delta_travel_analysis
GROUP  BY vf_number, cust_num, out_of_country_dt;

-- ---------------------------------------------------------------------
-- 4. Target (create on first run) + snapshot, one row per key
--    (snapshot is required: INSERT OVERWRITE cannot read its own target)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev_ads.travel_analysis(
  vf_number            string,
  cust_num             string,
  out_of_country_dt    timestamp,
  return_to_country_dt timestamp,
  travelling_country   string,
  roaming_sp_name      string,
  extract_dt           int)
STORED AS PARQUET;

DROP TABLE IF EXISTS dev_stg.travel_analysis_dev_ads;
CREATE TABLE dev_stg.travel_analysis_dev_ads STORED AS PARQUET AS
SELECT vf_number, cust_num, out_of_country_dt, return_to_country_dt,
       travelling_country, roaming_sp_name, extract_dt
FROM  (SELECT t.*,
              ROW_NUMBER() OVER (PARTITION BY vf_number, COALESCE(cust_num, '#NULL#'), out_of_country_dt
                                 ORDER BY extract_dt DESC, return_to_country_dt DESC) AS rn
       FROM   dev_ads.travel_analysis t) x
WHERE  rn = 1;

-- ---------------------------------------------------------------------
-- 5. Country list per key : existing list + new stage countries
--    ord prefix (15 digits) = src(1) + extract_dt(8) + position(6)
--      src 0 = existing target token (keeps original position)
--      src 1 = stage token          (ordered by extract_dt)
--    MIN(ord) per country keeps the first occurrence -> no duplicates.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS dev_stg.travel_analysis_country;
CREATE TABLE dev_stg.travel_analysis_country STORED AS PARQUET AS
SELECT vf_number,
       cust_key,
       out_of_country_dt,
       REGEXP_REPLACE(CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(ord_country))),
                      '[0-9]{15}#', '')                                  AS travelling_country
FROM  (SELECT vf_number, cust_key, out_of_country_dt, country_key,
              MIN(ord_country) AS ord_country
       FROM  (-- existing target tokens, only for keys present in today's delta
              SELECT a.vf_number,
                     COALESCE(a.cust_num, '#NULL#')                        AS cust_key,
                     a.out_of_country_dt,
                     UPPER(TRIM(t.country))                                AS country_key,
                     CONCAT('0', '00000000', LPAD(CAST(t.pos AS string), 6, '0'),
                            '#', TRIM(t.country))                          AS ord_country
              FROM   dev_stg.travel_analysis_dev_ads a
              JOIN   dev_stg.delta_travel_analysis_key k
                ON   a.vf_number = k.vf_number
               AND   COALESCE(a.cust_num, '#NULL#') = COALESCE(k.cust_num, '#NULL#')
               AND   a.out_of_country_dt = k.out_of_country_dt
              LATERAL VIEW POSEXPLODE(SPLIT(a.travelling_country, ',')) t AS pos, country
              WHERE  TRIM(t.country) <> ''

              UNION ALL

              -- stage tokens (QATAR / blank already NULL)
              SELECT s.vf_number,
                     COALESCE(s.cust_num, '#NULL#')                        AS cust_key,
                     s.out_of_country_dt,
                     UPPER(s.travelling_country)                           AS country_key,
                     CONCAT('1', LPAD(CAST(s.extract_dt AS string), 8, '0'), '000000',
                            '#', s.travelling_country)                     AS ord_country
              FROM   dev_stg.delta_travel_analysis s
              WHERE  s.travelling_country IS NOT NULL
             ) tok
       GROUP  BY vf_number, cust_key, out_of_country_dt, country_key
      ) dedup
GROUP  BY vf_number, cust_key, out_of_country_dt;

-- ---------------------------------------------------------------------
-- 6. Final merge
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE dev_ads.travel_analysis
-- keys in today's delta (new or updated)
SELECT s.vf_number,
       s.cust_num,
       s.out_of_country_dt,
       COALESCE(s.return_to_country_dt,
                a.return_to_country_dt,
                CASE WHEN s.has_open_trip = 1
                     THEN CAST('2999-12-31 00:00:00' AS timestamp) END)  AS return_to_country_dt,
       c.travelling_country,
       CASE WHEN s.roaming_sp_name IS NULL
              THEN a.roaming_sp_name
            WHEN a.roaming_sp_name IS NULL OR TRIM(a.roaming_sp_name) = ''
              THEN s.roaming_sp_name
            WHEN INSTR(CONCAT('|||', a.roaming_sp_name, '|||'),
                       CONCAT('|||', s.roaming_sp_name, '|||')) > 0
              THEN a.roaming_sp_name
            ELSE CONCAT(s.roaming_sp_name, '|||', a.roaming_sp_name)
       END                                                               AS roaming_sp_name,
       s.extract_dt
FROM   dev_stg.delta_travel_analysis_key s
LEFT   JOIN dev_stg.travel_analysis_dev_ads a
  ON   s.vf_number = a.vf_number
 AND   COALESCE(s.cust_num, '#NULL#') = COALESCE(a.cust_num, '#NULL#')
 AND   s.out_of_country_dt = a.out_of_country_dt
LEFT   JOIN dev_stg.travel_analysis_country c
  ON   s.vf_number = c.vf_number
 AND   COALESCE(s.cust_num, '#NULL#') = c.cust_key
 AND   s.out_of_country_dt = c.out_of_country_dt

UNION ALL

-- keys only in target (untouched)
SELECT a.vf_number,
       a.cust_num,
       a.out_of_country_dt,
       a.return_to_country_dt,
       a.travelling_country,
       a.roaming_sp_name,
       a.extract_dt
FROM   dev_stg.travel_analysis_dev_ads a
LEFT   JOIN dev_stg.delta_travel_analysis_key s
  ON   s.vf_number = a.vf_number
 AND   COALESCE(s.cust_num, '#NULL#') = COALESCE(a.cust_num, '#NULL#')
 AND   s.out_of_country_dt = a.out_of_country_dt
WHERE  s.vf_number IS NULL;
