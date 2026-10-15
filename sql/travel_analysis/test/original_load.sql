-- Current production script (as received), used by run_test.py --original to show its issues.
drop table if exists stg.delta_travel_analysis;
create table stg.delta_travel_analysis AS 
SELECT
        vf_number,
        cust_num,
        cast( from_unixtime(cast(OUT_OF_COUNTRY_DT/1000 as bigint)) as timestamp) as out_of_country_dt,
        case when RETURN_TO_COUNTRY_DT is null then cast('2999-12-31 00:00:00' as timestamp)
        else cast( from_unixtime(cast(RETURN_TO_COUNTRY_DT/1000 as bigint)) as timestamp) end as RETURN_TO_COUNTRY_DT,
        CASE
            WHEN travelling_country IS NULL
              OR TRIM(travelling_country) = ''
              OR TRIM(UPPER(travelling_country)) = 'QATAR'
            THEN NULL
            ELSE TRIM(travelling_country)
        END AS travelling_country,
        roaming_sp_name,
        cast(from_unixtime(cast(extract_dt / 1000 as bigint), 'yyyyMMdd') as int) as extract_dt
    FROM stg.subscr_travel_analysis
    WHERE vf_number IS NOT NULL
      AND out_of_country_dt IS NOT NULL;

drop table if exists stg.travel_analysis_ads;  
create table stg.travel_analysis_ads AS 
SELECT vf_number, cust_num, out_of_country_dt, return_to_country_dt, travelling_country, roaming_sp_name, extract_dt
   from ads.subscr_travel_analysis;

insert overwrite table ads.subscr_travel_analysis
select 
s.vf_number as vf_number,
s.cust_num as cust_num,
s.out_of_country_dt as out_of_country_dt,
coalesce(case 
when s.return_to_country_dt <> cast('2999-12-31 00:00:00' as timestamp) 
then s.return_to_country_dt 
else a.return_to_country_dt end, cast('2999-12-31 00:00:00' as timestamp)) 
as return_to_country_dt,
case
    when s.travelling_country is null or s.travelling_country = ''
        then a.travelling_country   when a.travelling_country is null or a.travelling_country= ''
        then trim(s.travelling_country)
    when concat( ',',upper(trim(a.travelling_country)),',') like concat(
             '%,',upper(trim(s.travelling_country)),',%')
        then a.travelling_country
    else concat( trim(a.travelling_country), ', ', trim(s.travelling_country))
END as travelling_country,
case when 
s.roaming_sp_name is not null and a.roaming_sp_name is not null 
then concat(s.roaming_sp_name, '|||', a.roaming_sp_name) 
else  a.roaming_sp_name end as roaming_sp_name,
coalesce(s.extract_dt,a.extract_dt) as extract_dt
 from stg.delta_travel_analysis s 
 left join stg.travel_analysis_ads a 
 on s.vf_number=a.vf_number and s.out_of_country_dt=a.out_of_country_dt and s.cust_num=a.cust_num
union all
select 
a.vf_number, a.cust_num, a.out_of_country_dt, a.return_to_country_dt, a.travelling_country, a.roaming_sp_name, a.extract_dt
 from stg.travel_analysis_ads a
 left join stg.delta_travel_analysis s
 on s.vf_number=a.vf_number and s.out_of_country_dt=a.out_of_country_dt and s.cust_num=a.cust_num
 where s.vf_number is null and s.out_of_country_dt is null and s.cust_num is null;
