-- =====================================================================
-- Post-load checks for ads.subscr_travel_analysis. Every query must return 0.
-- =====================================================================

-- 1. One row per key
SELECT COUNT(*) AS dup_keys
FROM  (SELECT vf_number, COALESCE(cust_num, '#NULL#') k, out_of_country_dt
       FROM   ads.subscr_travel_analysis
       GROUP  BY vf_number, COALESCE(cust_num, '#NULL#'), out_of_country_dt
       HAVING COUNT(*) > 1) x;

-- 2. No duplicate country inside travelling_country (case-insensitive)
SELECT COUNT(*) AS dup_countries
FROM  (SELECT a.vf_number, a.cust_num, a.out_of_country_dt, UPPER(TRIM(t.c)) c
       FROM   ads.subscr_travel_analysis a
       LATERAL VIEW EXPLODE(SPLIT(a.travelling_country, ',')) t AS c
       GROUP  BY a.vf_number, a.cust_num, a.out_of_country_dt, UPPER(TRIM(t.c))
       HAVING COUNT(*) > 1) x;

-- 3. No empty or untrimmed country
SELECT COUNT(*) AS bad_tokens
FROM   ads.subscr_travel_analysis a
LATERAL VIEW EXPLODE(SPLIT(a.travelling_country, ', ')) t AS c
WHERE  TRIM(t.c) = '' OR t.c <> TRIM(t.c);

-- 4. QATAR never taken from today's file
SELECT COUNT(*) AS qatar_in_delta
FROM   stg.subscr_travel_analysis_delta
WHERE  UPPER(country) = 'QATAR';

-- 5. Every trip in today's file is in ads
SELECT COUNT(*) AS missing_keys
FROM  (SELECT DISTINCT vf_number, cust_key, out_of_country_dt
       FROM   stg.subscr_travel_analysis_delta) s
LEFT   JOIN ads.subscr_travel_analysis a
  ON   s.vf_number = a.vf_number
 AND   s.cust_key = COALESCE(a.cust_num, '#NULL#')
 AND   s.out_of_country_dt = a.out_of_country_dt
WHERE  a.vf_number IS NULL;

-- 6. No duplicate roaming provider inside roaming_sp_name
SELECT COUNT(*) AS dup_roaming
FROM  (SELECT a.vf_number, a.cust_num, a.out_of_country_dt, TRIM(t.r) r
       FROM   ads.subscr_travel_analysis a
       LATERAL VIEW EXPLODE(SPLIT(a.roaming_sp_name, '\\|\\|\\|')) t AS r
       GROUP  BY a.vf_number, a.cust_num, a.out_of_country_dt, TRIM(t.r)
       HAVING COUNT(*) > 1) x;
