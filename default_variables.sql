
use role sysadmin;
use warehouse compute_wh;
use database snowflake_sample_data;
use schema tpch_sf1000;

select current_region(),current_date(),current_role()