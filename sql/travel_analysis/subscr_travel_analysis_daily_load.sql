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
-- Steps (no joins, each step is a stg work table):
--   1. stg.subscr_travel_analysis_src     : ads rows + landing rows, one row per country
--   2. stg.subscr_travel_analysis_country : one row per key + country (first occurrence)
--   3. stg.subscr_travel_analysis_key     : one row per key (merged)
--   4. INSERT OVERWRITE ads.subscr_travel_analysis from step 3
-- =====================================================================

SET hive.vectorized.execution.enabled=true;
SET hive.map.aggr=true;
SET hive.optimize.reducededuplication=true;
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
-- 1. ads + landing, one row per country
--    ord = 15-digit sort key: src(1) + extract_dt(8) + position(6)
--      ads     : 0 + 00000000 + position in the existing list
--      landing : 1 + extract_dt + 000000
--    This table is also the snapshot of ads, so step 4 never reads the
--    table it overwrites.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg.subscr_travel_analysis_src;
CREATE TABLE stg.subscr_travel_analysis_src STORED AS PARQUET AS
  -- existing ads list, exploded with its position
  SELECT t.vf_number,
         COALESCE(t.cust_num, '#NULL#')                                    AS cust_key,
         t.out_of_country_dt,
         0                                                                 AS is_stg,
         t.extract_dt,
         t.return_to_country_dt,
         t.roaming_sp_name,
         CONCAT('000000000', LPAD(CAST(c.pos AS string), 6, '0'))          AS ord,
         NULLIF(TRIM(c.country), '')                                       AS country
  FROM   ads.subscr_travel_analysis t
  LATERAL VIEW OUTER POSEXPLODE(SPLIT(t.travelling_country, ',')) c AS pos, country

  UNION ALL

  -- landing: QATAR / blank -> NULL, NULL return -> 2999-12-31
  SELECT TRIM(vf_number),
         COALESCE(NULLIF(TRIM(cust_num), ''), '#NULL#'),
         CAST(FROM_UNIXTIME(CAST(out_of_country_dt / 1000 AS bigint)) AS timestamp),
         1,
         CAST(FROM_UNIXTIME(CAST(extract_dt / 1000 AS bigint), 'yyyyMMdd') AS int),
         CASE WHEN return_to_country_dt IS NULL
              THEN CAST('2999-12-31 00:00:00' AS timestamp)
              ELSE CAST(FROM_UNIXTIME(CAST(return_to_country_dt / 1000 AS bigint)) AS timestamp)
         END,
         NULLIF(TRIM(roaming_sp_name), ''),
         CONCAT('1', FROM_UNIXTIME(CAST(extract_dt / 1000 AS bigint), 'yyyyMMdd'), '000000'),
         CASE WHEN UPPER(TRIM(travelling_country)) = 'QATAR' THEN NULL
              ELSE NULLIF(TRIM(travelling_country), '')
         END
  FROM   stg.subscr_travel_analysis
  WHERE  NULLIF(TRIM(vf_number), '') IS NOT NULL
    AND  out_of_country_dt IS NOT NULL
    AND  extract_dt IS NOT NULL;

-- ---------------------------------------------------------------------
-- 2. one row per key + country: first occurrence wins -> no duplicates
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg.subscr_travel_analysis_country;
CREATE TABLE stg.subscr_travel_analysis_country STORED AS PARQUET AS
  SELECT vf_number, cust_key, out_of_country_dt,
         MIN(CONCAT(ord, '#', country))                                    AS ord_country,
         MAX(is_stg)                                                       AS has_stg,
         MAX(extract_dt)                                                   AS extract_dt,
         MAX(IF(is_stg = 0, return_to_country_dt, NULL))                   AS ads_return,
         -- latest real (not open) return date from landing
         MAX(IF(is_stg = 1 AND return_to_country_dt < CAST('2999-12-31 00:00:00' AS timestamp),
                NAMED_STRUCT('e', extract_dt, 'v', return_to_country_dt), NULL)) AS stg_return,
         MAX(IF(is_stg = 0, roaming_sp_name, NULL))                        AS ads_roaming,
         -- latest non-null roaming provider from landing
         MAX(IF(is_stg = 1 AND roaming_sp_name IS NOT NULL,
                NAMED_STRUCT('e', extract_dt, 'v', roaming_sp_name), NULL)) AS stg_roaming
  FROM   stg.subscr_travel_analysis_src
  GROUP  BY vf_number, cust_key, out_of_country_dt, UPPER(country);

-- ---------------------------------------------------------------------
-- 3. one row per key
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg.subscr_travel_analysis_key;
CREATE TABLE stg.subscr_travel_analysis_key STORED AS PARQUET AS
  SELECT vf_number, cust_key, out_of_country_dt,
         SORT_ARRAY(COLLECT_LIST(ord_country))                             AS countries,
         MAX(has_stg)                                                      AS has_stg,
         MAX(extract_dt)                                                   AS extract_dt,
         MAX(ads_return)                                                   AS ads_return,
         MAX(stg_return).v                                                 AS stg_return,
         MAX(ads_roaming)                                                  AS ads_roaming,
         MAX(stg_roaming).v                                                AS stg_roaming
  FROM   stg.subscr_travel_analysis_country
  GROUP  BY vf_number, cust_key, out_of_country_dt;

-- ---------------------------------------------------------------------
-- 4. final load
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE ads.subscr_travel_analysis
SELECT vf_number,
       NULLIF(cust_key, '#NULL#')                                          AS cust_num,
       out_of_country_dt,
       -- key in today's file: latest real landing date, else ads date, else open
       -- key not in today's file: unchanged
       IF(has_stg = 1,
          COALESCE(stg_return, ads_return, CAST('2999-12-31 00:00:00' AS timestamp)),
          ads_return)                                                      AS return_to_country_dt,
       NULLIF(REGEXP_REPLACE(CONCAT_WS(', ', countries), '[0-9]{15}#', ''), '')    AS travelling_country,
       CASE WHEN stg_roaming IS NULL                     THEN ads_roaming
            WHEN NULLIF(TRIM(ads_roaming), '') IS NULL   THEN stg_roaming
            WHEN INSTR(CONCAT('|||', ads_roaming, '|||'), CONCAT('|||', stg_roaming, '|||')) > 0
                                                         THEN ads_roaming
            ELSE CONCAT(stg_roaming, '|||', ads_roaming)
       END                                                                 AS roaming_sp_name,
       extract_dt
FROM   stg.subscr_travel_analysis_key;
