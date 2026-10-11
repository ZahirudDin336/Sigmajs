-- =====================================================================
-- Post-load checks for dev_ads.travel_analysis. Every query must return 0.
-- =====================================================================

-- 1. One row per business key
SELECT COUNT(*) AS dup_keys
FROM  (SELECT vf_number, COALESCE(cust_num, '#NULL#') k, out_of_country_dt
       FROM   dev_ads.travel_analysis
       GROUP  BY vf_number, COALESCE(cust_num, '#NULL#'), out_of_country_dt
       HAVING COUNT(*) > 1) x;

-- 2. No duplicate country inside travelling_country (case-insensitive)
SELECT COUNT(*) AS dup_countries
FROM  (SELECT a.vf_number, a.cust_num, a.out_of_country_dt, UPPER(TRIM(t.c)) c
       FROM   dev_ads.travel_analysis a
       LATERAL VIEW EXPLODE(SPLIT(a.travelling_country, ',')) t AS c
       GROUP  BY a.vf_number, a.cust_num, a.out_of_country_dt, UPPER(TRIM(t.c))
       HAVING COUNT(*) > 1) x;

-- 3. No empty token (",," / leading / trailing comma) and no untrimmed token
SELECT COUNT(*) AS bad_tokens
FROM   dev_ads.travel_analysis a
LATERAL VIEW EXPLODE(SPLIT(a.travelling_country, ', ')) t AS c
WHERE  TRIM(t.c) = '' OR t.c <> TRIM(t.c);

-- 4. Every valid landing key is present in the target
SELECT COUNT(*) AS missing_keys
FROM  (SELECT DISTINCT TRIM(vf_number) AS vf_number,
              COALESCE(NULLIF(TRIM(cust_num), ''), '#NULL#') AS cust_key,
              CAST(FROM_UNIXTIME(CAST(out_of_country_dt / 1000 AS bigint)) AS timestamp) AS out_of_country_dt
       FROM   dev_stg.travel_analysis
       WHERE  vf_number IS NOT NULL AND TRIM(vf_number) <> ''
         AND  out_of_country_dt IS NOT NULL AND extract_dt IS NOT NULL) s
LEFT   JOIN dev_ads.travel_analysis a
  ON   s.vf_number = a.vf_number
 AND   s.cust_key = COALESCE(a.cust_num, '#NULL#')
 AND   s.out_of_country_dt = a.out_of_country_dt
WHERE  a.vf_number IS NULL;
