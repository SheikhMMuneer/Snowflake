-- Part-01 : Snowflake Data Loader
-- so will load customer data to a customer table
-- lets quickly create a customer
create or replace transient table customer_csv (
customer_pk number(38,0),
salutation varchar(10),
first_name varchar(20),
last_name varchar(30),
gender varchar(1),
marital_status varchar(1),
day_of_birth date,
birth_country varchar(60),
email_address varchar(50),
city_name varchar(60),
zip_code varchar(10),
country_name varchar(20),
gmt_timezone_offset number(10,2),
preferred_cust_flag boolean,
registration_time timestamp_ltz(9)
);

-- lets check the data.
select * from customer_csv;


-- Part-02: Named & Unnamed Stages
-- truncate the table before loading new files..
truncate table customer_csv;

-- use put command to load data to internal user stage
put file:///tmp/ch11/part-02/03_1_customer_5_rows.csv
    @~/ch11/part_02 
    auto_compress=false;

--list the stage
list @~/ch11/part_02;

-- create file formats
create or replace file format customer_csv_ff 
type = 'csv' 
compression = 'none' 
field_delimiter = ','
skip_header = 1 ;
    

-- Approach-1: Named stage with file format name
copy into customer_csv from @~/ch11/part_02/03_1_customer_5_rows.csv
file_format = (format_name = 'customer_csv_ff');

-- lest reveiw data (1 to 5)
select * from customer_csv;

-- Approach-2: Unnamed file format and inline file format defined
copy into customer_csv from @~/ch11/part_02/03_2_customer_5_rows.csv
file_format = (type = csv);

-- revised inline file format
copy into customer_csv from @~/ch11/part_02/03_2_customer_5_rows.csv
file_format = (type = csv skip_header=1);

-- Approach-3 Picking the files from table stage directly and unnamed file format
copy into customer_csv
file_format = (type = csv);


-- lets check the data.
select * from customer_csv;


-- Part-03: Size Limit for Copy command
-- creating a table called customer_large 
create or replace transient table customer_large (
	customer_pk number(38,0),
	salutation varchar(10),
	first_name varchar(20),
	last_name varchar(30),
	gender varchar(1),
	marital_status varchar(1),
	day_of_birth date,
	birth_country varchar(60),
	email_address varchar(50),
	city_name varchar(60),
	zip_code varchar(10),
	country_name varchar(20),
	gmt_timezone_offset number(10,2),
	preferred_cust_flag boolean,
	registration_time timestamp_ltz(9)
);

-- file format..
create or replace file format csv_gz_ff
type = 'csv' 
compression = 'gzip' 
field_delimiter = ','
field_optionally_enclosed_by = '\042'
skip_header = 1 ;

-- list the files
list @~/ch05/customer/csv/500k;

use warehouse compute_wh;

-- copy command with 50Mb limit..
copy into customer_large
    from @~/ch05/customer/csv/500k
    file_format = csv_gz_ff
    on_error = 'continue'
    force = true
    SIZE_LIMIT = 50000000 --50Mb
    pattern='.*[.]csv[.]gz'; 


-- Part-04: Large File or Smaller Files
-- a large single customer data .. 164Mb.. uncompressed 500+mb
list @~/tmp/customer/large/;

-- customer_v1.. will be used to load data for a single file
create or replace transient table customer_v1 (
	customer_pk number(38,0),
	salutation varchar(10),
	first_name varchar(20),
	last_name varchar(30),
	gender varchar(1),
	marital_status varchar(1),
	day_of_birth date,
	birth_country varchar(60),
	email_address varchar(50),
	city_name varchar(60),
	zip_code varchar(10),
	country_name varchar(20),
	gmt_timezone_offset number(10,2),
	preferred_cust_flag boolean,
	registration_time timestamp_ltz(9)
);

create or replace file format csv_gz_ff_v1
    type = 'csv' 
    compression = 'gzip' 
    field_delimiter = ','
    field_optionally_enclosed_by = '\042'
    skip_header = 1;
    
-- les run the copy command
copy into customer_v1
    from @~/tmp/customer/large/customer_very_big_530Mb.csv.gz
    file_format = csv_gz_ff_v1
    on_error = 'continue'
    force = true;

-- same file chunked into small files (not partitioned yet)
list @~/tmp/customer/large_partition;

-- they will be loaded into customer_v2 
create or replace transient table customer_v2 (
	customer_pk number(38,0),
	salutation varchar(10),
	first_name varchar(20),
	last_name varchar(30),
	gender varchar(1),
	marital_status varchar(1),
	day_of_birth date,
	birth_country varchar(60),
	email_address varchar(50),
	city_name varchar(60),
	zip_code varchar(10),
	country_name varchar(20),
	gmt_timezone_offset number(10,2),
	preferred_cust_flag boolean,
	registration_time timestamp_ltz(9)
);

-- another file format.. where head is removed..
create or replace file format csv_gz_ff_v2
    type = 'csv' 
    compression = 'gzip' 
    field_delimiter = ','
    field_optionally_enclosed_by = '\042';

use warehouse compute_wh;

-- copy command
copy into customer_v2
    from @~/tmp/customer/large_partition
    file_format = csv_gz_ff_v2
    on_error = 'continue'
    force = true
    pattern='.*[.]gz'; 
