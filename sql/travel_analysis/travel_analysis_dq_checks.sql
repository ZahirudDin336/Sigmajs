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

-- 3. No empty token (",," / leading / trailing comma)
SELECT COUNT(*) AS empty_tokens
FROM   dev_ads.travel_analysis a
LATERAL VIEW EXPLODE(SPLIT(a.travelling_country, ',')) t AS c
WHERE  TRIM(t.c) = '';

-- 4. QATAR not introduced by today's load
SELECT COUNT(*) AS qatar_added
FROM   dev_ads.travel_analysis a
JOIN   dev_stg.delta_travel_analysis_key k
  ON   a.vf_number = k.vf_number
 AND   COALESCE(a.cust_num, '#NULL#') = COALESCE(k.cust_num, '#NULL#')
 AND   a.out_of_country_dt = k.out_of_country_dt
LEFT   JOIN dev_stg.travel_analysis_dev_ads p
  ON   a.vf_number = p.vf_number
 AND   COALESCE(a.cust_num, '#NULL#') = COALESCE(p.cust_num, '#NULL#')
 AND   a.out_of_country_dt = p.out_of_country_dt
WHERE  CONCAT(',', UPPER(REGEXP_REPLACE(a.travelling_country, '\\s*,\\s*', ',')), ',') LIKE '%,QATAR,%'
  AND  NOT (CONCAT(',', UPPER(REGEXP_REPLACE(COALESCE(p.travelling_country, ''), '\\s*,\\s*', ',')), ',') LIKE '%,QATAR,%');

-- 5. Row count reconciliation: target = previous target + new keys
SELECT (SELECT COUNT(*) FROM dev_ads.travel_analysis)
     - (SELECT COUNT(*) FROM dev_stg.travel_analysis_dev_ads)
     - (SELECT COUNT(*)
        FROM   dev_stg.delta_travel_analysis_key s
        LEFT   JOIN dev_stg.travel_analysis_dev_ads a
          ON   s.vf_number = a.vf_number
         AND   COALESCE(s.cust_num, '#NULL#') = COALESCE(a.cust_num, '#NULL#')
         AND   s.out_of_country_dt = a.out_of_country_dt
        WHERE  a.vf_number IS NULL) AS count_diff;
