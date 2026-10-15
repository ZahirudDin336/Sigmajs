-- 2-year rebuild as received, used by run_rebuild_test.py --original to show its gaps.
drop table if exists dev_stg.travel_analysis_fix_20261004_ads_rebuild;
create table dev_stg.travel_analysis_fix_20261004_ads_rebuild as
SELECT
    vf_number,
    cust_num,
    out_of_country_dt,
    MAX(cast(case when max_return_to_country_dt is null then '2999-12-31 00:00:00' else max_return_to_country_dt END as timestamp)) AS return_to_country_dt,
    REGEXP_REPLACE(CONCAT_WS(', ',
            SORT_ARRAY( COLLECT_LIST(
                    CAST(extract_dt AS STRING)
                    || '|'
                    || travelling_country
                ) )), '[^|,]+\\|', '' ) AS travelling_country,
  concat_ws(' ||| ', collect_set( case
	when roaming_sp_name is not null and trim(roaming_sp_name) <> ''
	THEN trim(roaming_sp_name) END)) AS roaming_sp_name,
	max(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(extract_Dt), 'yyyyMMdd') AS INT)) as extract_dt
FROM
(
    SELECT
        vf_number,
        cust_num,
        out_of_country_dt,
        return_to_country_dt ,
        TRIM(travelling_country) AS travelling_country, roaming_sp_name,
        extract_dt,
        ROW_NUMBER() OVER
        ( PARTITION BY
                vf_number, cust_num, out_of_country_dt,
                TRIM(travelling_country)
            ORDER BY extract_dt ASC
        ) AS country_rn,
            MAX(
        NULLIF( return_to_country_dt, CAST('2999-12-31 00:00:00' AS TIMESTAMP)))
		OVER(
            PARTITION BY vf_number,  cust_num,  CAST(out_of_country_dt AS TIMESTAMP) ) AS max_return_to_country_dt
    FROM DEV_RAW.EDW_SUBSCR_TRAVEL_ANALYSIS_SNAPSHOT_2Y_20261003
)base_table
WHERE country_rn = 1 AND UPPER(travelling_country) <> 'QATAR'
GROUP BY vf_number, cust_num,  out_of_country_dt;
