
--- Create Database & Schemas
create database ch19;
create or replace schema landing_zone;
create or replace schema curated_zone;
create or replace schema consumption_zone;


-- Create Tables (Landing Zone)
create or replace transient table landing_item (
        item_id varchar,
        item_desc varchar,
        start_date varchar,
        end_date varchar,
        price varchar,
        item_class varchar,
        item_CATEGORY varchar
) comment ='this is item table with in landing schema';

create or replace transient table landing_customer (
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    birth_day varchar,
    birth_month varchar,
    birth_year varchar,
    birth_country varchar,
    email_address varchar
) comment ='this is customer table with in landing schema';

create or replace transient table landing_order (
    order_date varchar,
    order_time varchar,
    item_id varchar,
    item_desc varchar,
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    store_id varchar,
    store_name varchar,
    order_quantity varchar,
    sale_price varchar,
    disount_amt varchar,
    coupon_amt varchar,
    net_paid varchar,
    net_paid_tax varchar,
    net_profit varchar
) comment ='this is order table with in landing schema';
    
create or replace file format my_csv_vi_webui
type = 'csv' 
compression = 'auto' 
field_delimiter = ',' 
record_delimiter = '\n' 
skip_header = 1
field_optionally_enclosed_by = '\042' 
null_if = ('\\N');

-- Load Data in order_landing Table from managed stage
copy into landing_order from @order_stg/order-history.csv.gz 
file_format = my_csv_vi_webui  on_error = 'continue';


-- Delta Load Data Set
-- Customer Delta Data
CUSTOMER_ID,SALUTATION,FIRST_NAME,LAST_NAME,BIRTH_DAY,BIRTH_MONTH,BIRTH_YEAR,BIRTH_COUNTRY,EMAIL_ADDRESS
AAAAAAAAPOJJJDAA,Dr.,Neal,Moore,14,7,1977,TOGO,Neal.Moore@lMVxi20y.edu
CUSTOMER_ID,SALUTATION,FIRST_NAME,LAST_NAME,BIRTH_DAY,BIRTH_MONTH,BIRTH_YEAR,BIRTH_COUNTRY,EMAIL_ADDRESS
AAAAAAAALKBJCPAA,Miss,Antonio,Calvin,24,2,1930,NETHERLANDS ANTILLES,Antonio.Calvin@i4jK16aM7UIKKiZ.org
AAAAAAAAPOJJJDAA,Dr.,Neal,Moore,14,7,1978,TOGO,Neal.Moore@lMVxi20y.com


-- Item Delta Data
ITEM_ID,ITEM_DESC,START_DATE,END_DATE,PRICE,ITEM_CLASS,ITEM_CATEGORY
AAAAAAAACDLBXPPP,Increases back simply satisfactory telecommunications. Fre,1997-10-27,,0.81,loose stones,Jewelry
ITEM_ID,ITEM_DESC,START_DATE,END_DATE,PRICE,ITEM_CLASS,ITEM_CATEGORY
AAAAAAAAGCLBYPPP,Firmly far achievements could not prepare natural workers; names ought to live clearl,1997-10-27,,5.72,country,Music
AAAAAAAACDLBXPPP,Increases back simply satisfactory telecommunications. Fre,1997-10-27,,6.41,tennis,Sports


-- Order Delta Data
ORDER_DATE,ORDER_TIME,ITEM_ID,ITEM_DESC,CUSTOMER_ID,SALUTATION,FIRST_NAME,LAST_NAME,STORE_ID,STORE_NAME,ORDER_QUANTITY,SALE_PRICE,DISOUNT_AMT,COUPON_AMT,NET_PAID,NET_PAID_TAX,NET_PROFIT
2000-10-27,19:59:47 PM,AAAAAAAACDLBXPPP,Increases back simply satisfactory telecommunications. Fre,AAAAAAAAPOJJJDAA,Dr.,Neal,Moore,AAAAAAAAOGDAAAAA,eing,46,23.50,0.00,0.00,1081.00,1102.62,-3209.42
ORDER_DATE,ORDER_TIME,ITEM_ID,ITEM_DESC,CUSTOMER_ID,SALUTATION,FIRST_NAME,LAST_NAME,STORE_ID,STORE_NAME,ORDER_QUANTITY,SALE_PRICE,DISOUNT_AMT,COUPON_AMT,NET_PAID,NET_PAID_TAX,NET_PROFIT
2000-10-27,11:34:55 AM,AAAAAAAAGCLBYPPP,Firmly far achievements could not prepare natural workers; names ought to live clearl,AAAAAAAALKBJCPAA,Miss,Antonio,Calvin,AAAAAAAAAGFAAAAA,cally,73,104.37,0.00,0.00,7619.01,7923.77,1739.59
2000-10-27,19:59:47 PM,AAAAAAAACDLBXPPP,Increases back simply satisfactory telecommunications. Fre,AAAAAAAAPOJJJDAA,Dr.,Neal,Moore,AAAAAAAAOGDAAAAA,eing,46,300.50,0.00,0.00,1081.00,1102.62,-3209.42


---- Curated Zone Tables
use schema ch19.curated_zone;

create or replace transient table curated_customer (
customer_pk number autoincrement,
customer_id varchar(18),
salutation varchar(10),
first_name varchar(20),
last_name varchar(30),
birth_day number,
birth_month number,
birth_year number,
birth_country varchar(20),
email_address varchar(50)
) comment ='this is customer table with in curated schema';
    
create or replace transient table curated_item (
item_pk number autoincrement,
item_id varchar(16),
item_desc varchar,
start_date date,
end_date date,
price number(7,2),
item_class varchar(50),
item_category varchar(50)
) comment ='this is item table with in curated schema';

create or replace transient table curated_order (
order_pk number autoincrement,
order_date date,
order_time varchar,
item_id varchar(16),
item_desc varchar,
customer_id varchar(18),
salutation varchar(10),
first_name varchar(20),
last_name varchar(30),
store_id varchar(16),
store_name VARCHAR(50),
order_quantity number,
sale_price number(7,2),
disount_amt number(7,2),
coupon_amt number(7,2),
net_paid number(7,2),
net_paid_tax number(7,2),
net_profit number(7,2)
) comment ='this is order table with in curated schema';


-- Curated Customer First Time Load
insert into ch19.curated_zone.curated_customer (
    customer_id ,
    salutation ,
    first_name ,
    last_name ,
    birth_day ,
    birth_month ,
    birth_year ,
    birth_country ,
    email_address ) 
select 
    customer_id ,
    salutation ,
    first_name ,
    last_name ,
    birth_day ,
    birth_month ,
    birth_year ,
    birth_country ,
    email_address 
from ch19.landing_zone.landing_customer;

-- Curated Item Dimension First Time Load
insert into ch19.curated_zone.curated_item (
    item_id,
    item_desc,
    start_date,
    end_date,
    price,
    item_class,
    item_category) 
select 
    item_id,
    item_desc,
    start_date,
    end_date,
    price,
    item_class,
    item_category
from ch19.landing_zone.landing_item;

-- Curaterd Order First Time Load
insert into ch19.curated_zone.curated_order (
    order_date ,
    order_time ,
    item_id ,
    item_desc ,
    customer_id ,
    salutation ,
    first_name ,
    last_name ,
    store_id ,
    store_name ,
    order_quantity ,
    sale_price ,
    disount_amt ,
    coupon_amt ,
    net_paid ,
    net_paid_tax ,
    net_profit) 
select 
    order_date ,
    order_time ,
    item_id ,
    item_desc ,
    customer_id ,
    salutation ,
    first_name ,
    last_name ,
    store_id ,
    store_name ,
    order_quantity ,
    sale_price ,
    disount_amt ,
    coupon_amt ,
    net_paid ,
    net_paid_tax ,
    net_profit  
from ch19.landing_zone.landing_order;

--- Create consumption layer tables
use schema ch19.consumption_zone;
create or replace table item_dim (
	item_dim_key number autoincrement,
	item_id varchar(16),
	item_desc varchar,
	start_date date,
	end_date date,
	price number(7,2),
	item_class varchar(50),
	item_category varchar(50),
	added_timestamp timestamp default current_timestamp() ,
	updated_timestamp timestamp default current_timestamp() ,
	active_flag varchar(1) default 'Y'
) comment ='this is item table with in consumption schema';

create or replace table customer_dim (
	customer_dim_key number autoincrement,
	customer_id varchar(18),
	salutation varchar(10),
	first_name varchar(20),
	last_name varchar(30),
	birth_day number,
	birth_month number,
	birth_year number,
	birth_country varchar(20),
	email_address varchar(50),
	added_timestamp timestamp default current_timestamp() ,
	updated_timestamp timestamp default current_timestamp() ,
	active_flag varchar(1) default 'Y'
) comment ='this is customer table with in consumption schema';
    
create or replace table order_fact (
	order_fact_key number autoincrement,
	order_date date,
	customer_dim_key number,
	item_dim_key number,
	order_count number,
	order_quantity number,
	sale_price number(20,2),
	disount_amt number(20,2),
	coupon_amt number(20,2),
	net_paid number(20,2),
	net_paid_tax number(20,2),
	net_profit number(20,2)
) comment ='this is order table with in consumption schema';

-- Item Dimension First Time Load
insert into ch19.consumption_zone.item_dim (
item_id,
item_desc,
start_date,
end_date,
price,
item_class,
item_category) 
select 
item_id,
item_desc,
start_date,
end_date,
price,
item_class,
item_category
from ch19.curated_zone.curated_item;

--- Customer Dimension 1st Time Load
insert into ch19.consumption_zone.customer_dim (
customer_id ,
salutation ,
first_name ,
last_name ,
birth_day ,
birth_month ,
birth_year ,
birth_country ,
email_address ) 
select 
customer_id ,
salutation ,
first_name ,
last_name ,
birth_day ,
birth_month ,
birth_year ,
birth_country ,
email_address 
from ch19.curated_zone.curated_customer;  

--- Order Fact First Time Load
insert into ch19.consumption_zone.order_fact (
order_date,
customer_dim_key ,
item_dim_key ,
order_count,
order_quantity ,
sale_price ,
disount_amt ,
coupon_amt ,
net_paid ,
net_paid_tax ,
net_profit 
) 
select 
co.order_date,
cd.customer_dim_key ,
id.item_dim_key,
count(1) as order_count,
sum(co.order_quantity) ,
sum(co.sale_price) ,
sum(co.disount_amt) ,
sum(co.coupon_amt) ,
sum(co.net_paid) ,
sum(co.net_paid_tax) ,
sum(co.net_profit)  
from ch19.curated_zone.curated_order co 
join ch19.consumption_zone.customer_dim cd on cd.customer_id = co.customer_id
join ch19.consumption_zone.item_dim id on id.item_id = co.item_id and id.item_desc = co.item_desc and id.end_date is null
group by 
co.order_date,
cd.customer_dim_key ,
id.item_dim_key
order by co.order_date;

--- Create Object and list them
--- order stage
create stage delta_orders_s3
    url = 's3://toppertips/delta/orders' 
    comment = 'feed delta order files';
-- item stage
create stage delta_items_s3
    url = 's3://toppertips/delta/items' 
    comment = 'feed delta item files';

-- customer stage
create stage delta_customer_s3
    url = 's3://toppertips/delta/customers' 
    comment = 'feed delta customer files';
        
show stages;


--- Create Pipe Objects for each of the table
create or replace pipe order_pipe
auto_ingest = true
as 
copy into landing_order from @delta_orders_s3
file_format = (type=csv COMPRESSION=none)
pattern='.*order.*[.]csv'
ON_ERROR = 'CONTINUE';

create or replace pipe item_pipe
auto_ingest = true
as 
copy into landing_item from @delta_items_s3
file_format = (type=csv COMPRESSION=none)
pattern='.*item.*[.]csv'
ON_ERROR = 'CONTINUE';

create or replace pipe customer_pipe
auto_ingest = true
as 
copy into landing_customer from @delta_customer_s3
file_format = (type=csv COMPRESSION=none)
pattern='.*customer.*[.]csv'
ON_ERROR = 'CONTINUE';

-- Step - Review Pipe Status
show pipes;
    
select system$pipe_status('order_pipe');
select system$pipe_status('item_pipe');
select system$pipe_status('customer_pipe');



---- Stream for landing Zone Tables
use schema ch19.landing_zone;

create or replace stream landing_item_stm on table landing_item
append_only = true;

create or replace stream landing_customer_stm on table landing_customer
append_only = true;

create or replace stream landing_order_stm on table landing_order
append_only = true;


--- Task Sceduler for Landing Zone
use schema ch19.curated_zone;
create or replace task order_curated_tsk
warehouse = compute_wh 
schedule  = '1 minute'
when
system$stream_has_data('ch19.landing_zone.landing_order_stm')
as
merge into ch19.curated_zone.curated_order curated_order 
using ch19.landing_zone.landing_order_stm landing_order_stm on
curated_order.order_date = landing_order_stm.order_date and 
curated_order.order_time = landing_order_stm.order_time and 
curated_order.item_id = landing_order_stm.item_id and
curated_order.item_desc = landing_order_stm.item_desc 
when matched 
then update set 
curated_order.customer_id = landing_order_stm.customer_id,
curated_order.salutation = landing_order_stm.salutation,
curated_order.first_name = landing_order_stm.first_name,
curated_order.last_name = landing_order_stm.last_name,
curated_order.store_id = landing_order_stm.store_id,
curated_order.store_name = landing_order_stm.store_name,
curated_order.order_quantity = landing_order_stm.order_quantity,
curated_order.sale_price = landing_order_stm.sale_price,
curated_order.disount_amt = landing_order_stm.disount_amt,
curated_order.coupon_amt = landing_order_stm.coupon_amt,
curated_order.net_paid = landing_order_stm.net_paid,
curated_order.net_paid_tax = landing_order_stm.net_paid_tax,
curated_order.net_profit = landing_order_stm.net_profit
when not matched then 
insert (
order_date ,
order_time ,
item_id ,
item_desc ,
customer_id ,
salutation ,
first_name ,
last_name ,
store_id ,
store_name ,
order_quantity ,
sale_price ,
disount_amt ,
coupon_amt ,
net_paid ,
net_paid_tax ,
net_profit ) 
values (
landing_order_stm.order_date ,
landing_order_stm.order_time ,
landing_order_stm.item_id ,
landing_order_stm.item_desc ,
landing_order_stm.customer_id ,
landing_order_stm.salutation ,
landing_order_stm.first_name ,
landing_order_stm.last_name ,
landing_order_stm.store_id ,
landing_order_stm.store_name ,
landing_order_stm.order_quantity ,
landing_order_stm.sale_price ,
landing_order_stm.disount_amt ,
landing_order_stm.coupon_amt ,
landing_order_stm.net_paid ,
landing_order_stm.net_paid_tax ,
landing_order_stm.net_profit );


create or replace task customer_curated_tsk
warehouse = compute_wh 
schedule  = '2 minute'
when
system$stream_has_data('customer_stm') AND system$stream_has_data('order_stm')
as
merge into ch19.curated_zone.curated_customer curated_customer 
using ch19.landing_zone.landing_customer_stm landing_customer_stm on
curated_customer.customer_id = landing_customer_stm.customer_id
when matched 
then update set 
curated_customer.salutation = landing_customer_stm.salutation,
curated_customer.first_name = landing_customer_stm.first_name,
curated_customer.last_name = landing_customer_stm.last_name,
curated_customer.birth_day = landing_customer_stm.birth_day,
curated_customer.birth_month = landing_customer_stm.birth_month,
curated_customer.birth_year = landing_customer_stm.birth_year,
curated_customer.birth_country = landing_customer_stm.birth_country,
curated_customer.email_address = landing_customer_stm.email_address
when not matched then 
insert (
customer_id ,
salutation ,
first_name ,
last_name ,
birth_day ,
birth_month ,
birth_year ,
birth_country ,
email_address ) 
values (
landing_customer_stm.customer_id ,
landing_customer_stm.salutation ,
landing_customer_stm.first_name ,
landing_customer_stm.last_name ,
landing_customer_stm.birth_day ,
landing_customer_stm.birth_month ,
landing_customer_stm.birth_year ,
landing_customer_stm.birth_country ,
landing_customer_stm.email_address );


create or replace task item_curated_tsk
warehouse = compute_wh 
schedule  = '3 minute'
when
system$stream_has_data('ch19.landing_zone.landing_item_stm')
as
merge into ch19.curated_zone.curated_item item using ch19.landing_zone.landing_item_stm landing_item_stm on
item.item_id = landing_item_stm.item_id and 
item.item_desc = landing_item_stm.item_desc and 
item.start_date = landing_item_stm.start_date
when matched 
then update set 
item.end_date = landing_item_stm.end_date,
item.price = landing_item_stm.price,
item.item_class = landing_item_stm.item_class,
item.item_category = landing_item_stm.item_category
when not matched then 
insert (
item_id,
item_desc,
start_date,
end_date,
price,
item_class,
item_category) 
values (
landing_item_stm.item_id,
landing_item_stm.item_desc,
landing_item_stm.start_date,
landing_item_stm.end_date,
landing_item_stm.price,
landing_item_stm.item_class,
landing_item_stm.item_category);


--- Start Task Job
alter task order_curated_tsk resume;
alter task customer_curated_tsk resume;
alter task item_curated_tsk resume;

--- View Status of Task Job
select *  from table(information_schema.task_history()) 
where name in ('CUSTOMER_CURATED_TSK' ,'ITEM_CURATED_TSK','ORDER_CURATED_TSK')
order by scheduled_time;


--- Consumption Layer Stream & Task
use schema ch19.curated_zone;

create or replace stream curated_item_stm on table curated_item;
create or replace stream curated_customer_stm on table curated_customer;
create or replace stream curated_order_stm on table curated_order;
    

use schema ch19.consumption_zone;

create or replace task item_consumption_tsk
warehouse = compute_wh 
schedule  = '4 minute'
when
system$stream_has_data('ch19.curated_zone.curated_item_stm')
as
merge into ch19.consumption_zone.item_dim item using ch19.curated_zone.curated_item_stm curated_item_stm on
item.item_id = curated_item_stm.item_id and 
item.start_date = curated_item_stm.start_date and 
item.item_desc = curated_item_stm.item_desc
when matched 
and curated_item_stm.METADATA$ACTION = 'INSERT'
and curated_item_stm.METADATA$ISUPDATE = 'TRUE'
then update set 
item.end_date = curated_item_stm.end_date,
item.price = curated_item_stm.price,
item.item_class = curated_item_stm.item_class,
item.item_category = curated_item_stm.item_category
when matched 
and curated_item_stm.METADATA$ACTION = 'DELETE'
and curated_item_stm.METADATA$ISUPDATE = 'FALSE'
then update set 
item.active_flag = 'N',
updated_timestamp = current_timestamp()
when not matched 
and curated_item_stm.METADATA$ACTION = 'INSERT'
and curated_item_stm.METADATA$ISUPDATE = 'FALSE'
then 
insert (
item_id,
item_desc,
start_date,
end_date,
price,
item_class,
item_category) 
values (
curated_item_stm.item_id,
curated_item_stm.item_desc,
curated_item_stm.start_date,
curated_item_stm.end_date,
curated_item_stm.price,
curated_item_stm.item_class,
curated_item_stm.item_category);
        
-----
create or replace task customer_consumption_tsk
warehouse = compute_wh 
schedule  = '5 minute'
when
system$stream_has_data('ch19.curated_zone.curated_customer_stm')
as
merge into ch19.consumption_zone.customer_dim customer using ch19.curated_zone.curated_customer_stm curated_customer_stm on
customer.customer_id = curated_customer_stm.customer_id 
when matched 
and curated_customer_stm.METADATA$ACTION = 'INSERT'
and curated_customer_stm.METADATA$ISUPDATE = 'TRUE'
then update set 
customer.salutation = curated_customer_stm.salutation,
customer.first_name = curated_customer_stm.first_name,
customer.last_name = curated_customer_stm.last_name,
customer.birth_day = curated_customer_stm.birth_day,
customer.birth_month = curated_customer_stm.birth_month,
customer.birth_year = curated_customer_stm.birth_year,
customer.birth_country = curated_customer_stm.birth_country,
customer.email_address = curated_customer_stm.email_address
when matched 
and curated_customer_stm.METADATA$ACTION = 'DELETE'
and curated_customer_stm.METADATA$ISUPDATE = 'FALSE'
then update set 
customer.active_flag = 'N',
customer.updated_timestamp = current_timestamp()
when not matched 
and curated_customer_stm.METADATA$ACTION = 'INSERT'
and curated_customer_stm.METADATA$ISUPDATE = 'FALSE'
then 
insert (
customer_id ,
salutation ,
first_name ,
last_name ,
birth_day ,
birth_month ,
birth_year ,
birth_country ,
email_address ) 
values (
curated_customer_stm.customer_id ,
curated_customer_stm.salutation ,
curated_customer_stm.first_name ,
curated_customer_stm.last_name ,
curated_customer_stm.birth_day ,
curated_customer_stm.birth_month ,
curated_customer_stm.birth_year ,
curated_customer_stm.birth_country ,
curated_customer_stm.email_address);

---
create or replace task order_fact_tsk
warehouse = compute_wh 
schedule  = '6 minute'
when
system$stream_has_data('ch19.curated_zone.curated_order_stm')
as
insert overwrite into ch19.consumption_zone.order_fact (
order_date,
customer_dim_key ,
item_dim_key ,
order_count,
order_quantity ,
sale_price ,
disount_amt ,
coupon_amt ,
net_paid ,
net_paid_tax ,
net_profit) 
select 
co.order_date,
cd.customer_dim_key ,
id.item_dim_key,
count(1) as order_count,
sum(co.order_quantity) ,
sum(co.sale_price) ,
sum(co.disount_amt) ,
sum(co.coupon_amt) ,
sum(co.net_paid) ,
sum(co.net_paid_tax) ,
sum(co.net_profit)  
from ch19.curated_zone.curated_order co 
join ch19.consumption_zone.customer_dim cd on cd.customer_id = co.customer_id
join ch19.consumption_zone.item_dim id on id.item_id = co.item_id and id.item_desc = co.item_desc and id.end_date is null
group by 
co.order_date,
cd.customer_dim_key ,
id.item_dim_key
order by co.order_date; 

-- Start Task              
alter task item_consumption_tsk resume;
alter task customer_consumption_tsk resume;
alter task order_fact_tsk resume;
       
--- View Status of Task Job
select *  from table(information_schema.task_history()) 
where name in ('ITEM_CONSUMPTION_TSK' ,'CUSTOMER_CONSUMPTION_TSK','ORDER_FACT_TSK')
order by scheduled_time;
            

--- Data Validation Post Data Loading
select count(*) from ch19.landing_zone.landing_order; --10003 (10001) new records + one update
select count(*) from ch19.landing_zone.landing_item; --2793 (2791)new records + one update
select count(*) from ch19.landing_zone.landing_customer; -- 8889 (8887) new records + one update


select count(*) from ch19.landing_zone.landing_order_stm; 
select count(*) from ch19.landing_zone.landing_item_stm; 
select count(*) from ch19.landing_zone.landing_customer_stm; 


select count(*) from ch19.curated_zone.curated_order; --10001(10002) one updated and one inserted
select count(*) from ch19.curated_zone.curated_item; --2791 (2792) one updated and one inserted
select count(*) from ch19.curated_zone.curated_customer; -- 8887 (8888) one updated and one inserted

select count(*) from ch19.consumption_zone.order_fact; --5740 (5741)
select count(*) from ch19.consumption_zone.item_dim; --2791 (2791)
select count(*) from ch19.consumption_zone.customer_dim; -- 8887 (8888)

--- 
use schema ch19.consumption_zone;
select *  from table(information_schema.task_history()) 
where name in ('ORDER_FACT_TSK')
order by scheduled_time;

-- lets validate the change using time travel feature
select * from ch19.consumption_zone.customer_dim where customer_id = 'AAAAAAAAPOJJJDAA'
union all
select * from ch19.consumption_zone.customer_dim at(offset => -60*10) where customer_id = 'AAAAAAAAPOJJJDAA';


select * from ch19.consumption_zone.item_dim where item_id = 'AAAAAAAACDLBXPPP'
union all
select * from ch19.consumption_zone.item_dim at(offset => -60*10) where item_id = 'AAAAAAAACDLBXPPP';
