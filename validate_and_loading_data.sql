-- customer table with 15 columns having different data types
create or replace transient table customer_validation (
	customer_pk number(38,0),
	salutation varchar(10),
	first_name varchar(50),
	last_name varchar(50),
	gender varchar(1),
	marital_status varchar(1),
	day_of_birth date,
	birth_country varchar(60),
	email_address varchar(50),
	city_name varchar(60),
	zip_code varchar(10),
	country_name varchar(20),
	gmt_timezone_offset number(10),
	preferred_cust_flag boolean,
	registration_time timestamp_ltz(9)
);


-- Create a file format called csv_ff
create or replace file format csv_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ',' 
    record_delimiter = '\n' 
    skip_header = 1 
    field_optionally_enclosed_by = '\047';


-- SnowSQL Put Commmand & List User Stage
-- lets load the data using put command
put file:///tmp/ch08/*  
@~/ch08/small-csv 
auto_compress=false;

-- list the user stage location 
list @~/ch08/small-csv;


-- Run Copy Command (Without Validation Flag):
copy into customer_validation
    from @~/ch08/small-csv/customer_01_one_error.csv
    file_format = csv_ff;

-- this is one additioal property, which will just validate but not load the data
copy into customer_validation
    from @~/ch08/small-csv/customer_01_one_error.csv
    file_format = csv_ff
    on_error = 'continue';


-- Run Copy Command (With Validation Flag):
-- Option-1
--   validation_mode = return_errors;
--   Returns all errors (parsing, conversion, etc.) across all files specified in the COPY statement.
    
-- Option-2
-- validation_mode = return_n_rows;
--     validation_mode = return_errors;
    
-- Option-3
-- validation_mode = return_all_errors;
--  Returns all errors across all files specified in the COPY statement,
    
    copy into customer_validation
    from @~/ch08/small-csv/customer_01_one_error.csv
    file_format = csv_ff
    force = true
    validation_mode = return_all_errors;
    

-- Option-2
copy into customer_validation
    from @~/ch08/small-csv/customer_01_one_error.csv
    file_format = csv_ff;
    validation_mode = return_1_rows;
    
-- Option-3
copy into customer_validation
    from @~/ch08/small-csv/customer_01_one_error.csv
    file_format = csv_ff;
    validation_mode = return_all_errors;


--- Multiple Line error	
-- list the user stage location (again if you are not see my stage rleated video, pls watch them later.)
list @~/ch08/small-csv  ;

-- run copy command to load data from stage to table 
-- we don't know where all are the issues
copy into customer_validation
    from @~/ch08/small-csv/customer_02_three_errors.csv
    file_format = csv_ff
    on_error = 'continue'
    force = true
    validation_mode = return_errors;    
    
-- run without validation mode and skip the error records
copy into customer_validation
    from @~/ch08/small-csv/customer_02_three_errors.csv
    file_format = csv_ff
    on_error = 'continue'
    force = true;
    
-- Check the table
select * from customer_validation;


--- One Line Many errors

list @~/ch08/small-csv  ;

-- run copy command to load data from stage to table 
-- we don't know where all are the issues
copy into customer_validation
    from @~/ch08/small-csv/customer_03_one_line_many_error.csv
    file_format = csv_ff
    on_error = 'continue'
    force = true
    validation_mode = return_errors;
    
    
    
    
-- run without validation mode and skip the error records
copy into customer_validation
    from @~/ch08/small-csv/customer_03_one_line_many_error.csv
    file_format = csv_ff
    on_error = 'continue'
    force = true;
    
-- check the table
select * from customer_validation;

-----
create or replace file format csv_gz_ff type = ‘csv’ 
compression = ‘gzip’ field_delimiter = ‘,’
field_optionally_enclosed_by = ‘\042’ skip_header = 1 ;


–-- copy command with validation mode = for all errors 
copy into customer_validation from @~/ch08/csv/partition file_format = csv_gz_ff on_error = ‘continue’ 
force = true 
pattern=’.*[.]csv[.]gz’; 
validation_mode = return_all_errors;

select * from customer_validation limit 10;



–-- what happens if we say first 10 rows 
copy into customer_validation from @~/ch08/csv/partition
file_format = csv_gz_ff 
on_error = ‘continue’
force = true pattern=’.*[.]csv[.]gz’ 
validation_mode = return_10_rows;


-- check the time taken, & which all files are picked


select * from table(validate(customer_validation, job_id=>'query-id'));
-- 01a81f86-3200-94b8-0002-14d20005a25e



----- Validation of Files
select * from table(validate(customer_validation, job_id=>'01a81f81-3200-93d3-0002-14d20005d3a6'));


-- validation mode does not support transformation
copy into customer_validation from 
(
select distinct * from @~/ch08/csv/partition t
)
file_format = csv_gz_ff
on_error = 'continue'
force = true
pattern='.*[.]csv[.]gz'
validation_mode = return_all_errors;

