-- =====================================================================
-- Daily load : dev_ads.travel_analysis  (CDP / Hive 3)
--
-- Key : vf_number + cust_num + out_of_country_dt
--
-- One INSERT OVERWRITE, no intermediate tables, no joins:
--   target rows + landing rows -> UNION ALL -> GROUP BY key+country
--   (first occurrence) -> GROUP BY key (merge).
--
-- travelling_country : existing order kept, new countries appended by
-- extract_dt, no duplicates (trim + case-insensitive), stage NULL / ''
-- / QATAR skipped.
-- =====================================================================

SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.map.aggr=true;
SET hive.optimize.reducededuplication=true;
SET parquet.compression=SNAPPY;

CREATE EXTERNAL TABLE IF NOT EXISTS dev_stg.travel_analysis(
  extract_dt           bigint,
  vf_number            string,
  cust_num             string,
  out_of_country_dt    bigint,
  return_to_country_dt bigint,
  travelling_country   string,
  roaming_sp_name      string)
STORED AS PARQUET
LOCATION 'ofs://appr/bigdata/upstream-landing/travel_analysis/';

CREATE TABLE IF NOT EXISTS dev_ads.travel_analysis(
  vf_number            string,
  cust_num             string,
  out_of_country_dt    timestamp,
  return_to_country_dt timestamp,
  travelling_country   string,
  roaming_sp_name      string,
  extract_dt           int)
STORED AS PARQUET;

WITH
-- one row per country; ord = sort key (src + extract_dt + position)
src AS (
  -- existing target: list exploded, original position kept
  SELECT t.vf_number,
         COALESCE(t.cust_num, '#NULL#')                                    AS cust_key,
         t.out_of_country_dt,
         0                                                                 AS is_stg,
         t.extract_dt,
         t.return_to_country_dt,
         t.roaming_sp_name,
         CONCAT('000000000', LPAD(CAST(c.pos AS string), 6, '0'))          AS ord,
         NULLIF(TRIM(c.country), '')                                       AS country
  FROM   dev_ads.travel_analysis t
  LATERAL VIEW OUTER POSEXPLODE(SPLIT(t.travelling_country, ',')) c AS pos, country

  UNION ALL

  -- landing: QATAR / blank -> NULL, ordered by extract_dt
  SELECT TRIM(vf_number),
         COALESCE(NULLIF(TRIM(cust_num), ''), '#NULL#'),
         CAST(FROM_UNIXTIME(CAST(out_of_country_dt / 1000 AS bigint)) AS timestamp),
         1,
         CAST(FROM_UNIXTIME(CAST(extract_dt / 1000 AS bigint), 'yyyyMMdd') AS int),
         CASE WHEN return_to_country_dt = 6854
              THEN CAST('2999-12-31 00:00:00' AS timestamp)
              ELSE CAST(FROM_UNIXTIME(CAST(return_to_country_dt / 1000 AS bigint)) AS timestamp)
         END,
         NULLIF(TRIM(roaming_sp_name), ''),
         CONCAT('1', FROM_UNIXTIME(CAST(extract_dt / 1000 AS bigint), 'yyyyMMdd'), '000000'),
         CASE WHEN UPPER(TRIM(travelling_country)) = 'QATAR' THEN NULL
              ELSE NULLIF(TRIM(travelling_country), '')
         END
  FROM   dev_stg.travel_analysis
  WHERE  NULLIF(TRIM(vf_number), '') IS NOT NULL
    AND  out_of_country_dt IS NOT NULL
    AND  extract_dt IS NOT NULL
),
-- one row per key + country: first occurrence wins -> no duplicates
per_country AS (
  SELECT vf_number, cust_key, out_of_country_dt,
         MIN(CONCAT(ord, '#', country))                                    AS ord_country,
         MAX(extract_dt)                                                   AS extract_dt,
         MAX(IF(is_stg = 0, return_to_country_dt, NULL))                   AS tgt_return,
         MAX(IF(is_stg = 1 AND return_to_country_dt < CAST('2999-12-31 00:00:00' AS timestamp),
                NAMED_STRUCT('e', extract_dt, 'v', return_to_country_dt), NULL)) AS stg_return,
         MAX(IF(is_stg = 1 AND return_to_country_dt = CAST('2999-12-31 00:00:00' AS timestamp),
                1, 0))                                                     AS stg_open,
         MAX(IF(is_stg = 0, roaming_sp_name, NULL))                        AS tgt_roaming,
         MAX(IF(is_stg = 1 AND roaming_sp_name IS NOT NULL,
                NAMED_STRUCT('e', extract_dt, 'v', roaming_sp_name), NULL)) AS stg_roaming
  FROM   src
  GROUP  BY vf_number, cust_key, out_of_country_dt, UPPER(country)
),
-- one row per key
per_key AS (
  SELECT vf_number, cust_key, out_of_country_dt,
         SORT_ARRAY(COLLECT_LIST(ord_country))                             AS countries,
         MAX(extract_dt)                                                   AS extract_dt,
         MAX(tgt_return)                                                   AS tgt_return,
         MAX(stg_return).v                                                 AS stg_return,
         MAX(stg_open)                                                     AS stg_open,
         MAX(tgt_roaming)                                                  AS tgt_roaming,
         MAX(stg_roaming).v                                                AS stg_roaming
  FROM   per_country
  GROUP  BY vf_number, cust_key, out_of_country_dt
)
INSERT OVERWRITE TABLE dev_ads.travel_analysis
SELECT vf_number,
       NULLIF(cust_key, '#NULL#')                                          AS cust_num,
       out_of_country_dt,
       COALESCE(stg_return, tgt_return,
                IF(stg_open = 1, CAST('2999-12-31 00:00:00' AS timestamp), NULL)) AS return_to_country_dt,
       NULLIF(REGEXP_REPLACE(CONCAT_WS(', ', countries), '[0-9]{15}#', ''), '')    AS travelling_country,
       CASE WHEN stg_roaming IS NULL                     THEN tgt_roaming
            WHEN NULLIF(TRIM(tgt_roaming), '') IS NULL   THEN stg_roaming
            WHEN INSTR(CONCAT('|||', tgt_roaming, '|||'), CONCAT('|||', stg_roaming, '|||')) > 0
                                                         THEN tgt_roaming
            ELSE CONCAT(stg_roaming, '|||', tgt_roaming)
       END                                                                 AS roaming_sp_name,
       extract_dt
FROM   per_key;
