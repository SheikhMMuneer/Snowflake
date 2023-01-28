-- setup the context
use role sysadmin;
use database demo_db;
use schema public;
use warehouse compute_wh;


--create a raw table where change data capture will be triggered
create or replace table cdc_tbl (
  cdc_col_1  varchar ,
  cdc_col_2  varchar ,
  cdc_col_3  varchar 
);

--insert & select and see the data (we are loading 1st time)
insert into cdc_tbl values 
  ('onetime-val-11', 'onetime-val-12', 'onetime-val-13'),
  ('onetime-val-21', 'onetime-val-22', 'onetime-val-23'),
  ('onetime-val-31', 'onetime-val-32', 'onetime-val-33');
select * from cdc_tbl;

--the final table where post cdc, data will 
create or replace table final_tbl (
  final_col_1  varchar ,
  final_col_2  varchar ,
  final_col_3  varchar 
);

-- 1st time data load from cdc_table to final table, we can assume history load or onetime load
insert into final_tbl select * from cdc_tbl;
select * from final_tbl;

create or replace stream   
cdc_stream on table cdc_tbl
append_only=true;

---
create or replace task cdc_task
    warehouse = compute_wh 
    schedule  = '5 minute'
  when
    system$stream_has_data('cdc_stream')
  as
    insert into final_tbl select * from cdc_stream;


use role accountadmin;
alter task cdc_task resume;


insert into cdc_tbl values (
  'cdc-val-41', 'cdc-val-42', 'cdc-val-43'),
  ('cdc-val-51', 'cdc-val-52', 'cdc-val-53'),
  ('cdc-val-61', 'cdc-val-62', 'cdc-val-63');


-- how to see how it works
select * from table(information_schema.task_history())  order by scheduled_time;

-- you can see only the schedule items
select * from table(information_schema.task_history())  
where state ='SCHEDULED' order by scheduled_time;




