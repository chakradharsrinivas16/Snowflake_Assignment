-- Question - 1
CREATE ROLE DEVELOPER;
-- Creating the `DEVELOPER` role
CREATE ROLE ADMIN;
-- Creating the `Admin` role
CREATE ROLE PII;
-- Creating the `PII` role
-- Granting roles so as to replicate the given hierchy
GRANT ROLE DEVELOPER TO ROLE ADMIN;
-- Making relation between Developer to admin
GRANT ROLE ADMIN TO ROLE ACCOUNTADMIN;
-- Making relation between admin to accountadmin
GRANT ROLE PII TO ROLE ACCOUNTADMIN;
-- Making relation between PII to accountadmin
-- Question - 2
-- Create an M-sized warehouse using the accountadmin role, name -> assignment_wh and use it for all the queries
CREATE
OR REPLACE WAREHOUSE assignment_wh WITH WAREHOUSE_SIZE = 'MEDIUM';
-- As we need to run all queries in admin role, so we need give all privileges on created ware house(assignment_wh) to admin role.
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE ADMIN;
-- As we need to create database in admin role, so we need give privilege to create database to admin role.
GRANT CREATE DATABASE ON ACCOUNT TO ROLE admin;
-- Question - 3
-- Switch to the admin role
USE ROLE ADMIN;
-- Question - 4
-- Create a database assignment_db
CREATE DATABASE assignment_db;
-- Question - 5
-- Create a schema my_schema
CREATE SCHEMA my_schema;
-- Question - 6
-- Create a table using any sample csv. You can get 1 by googling for sample csv’s. Preferably search for a sample employee dataset so that you have PII related columns else you can consider any column as PII.
--Creating table 'employee_data' for stroing csv data
CREATE TABLE EMPLOYEE_DATA (
    ID NUMBER,
    FIRST_NAME VARCHAR(255),
    LAST_NAME VARCHAR(255),
    EMAIL VARCHAR(255),
    DEPARTMENT VARCHAR(255),
    MOBILE_NUMBER VARCHAR(255),
    CITY VARCHAR(255),
    etl_ts timestamp default current_timestamp(),
    -- for getting the time at which the record is getting inserted
    etl_by varchar default 'snowsight',
    -- for getting application name from which the record was inserted
    file_name varchar -- for getting the name of the file used to insert data into the table.
);
-- We created table as per the taken data and created columns as per their data.
-- Question - 7
-- Also, create a variant version of this dataset, i.e we transformed data into .json format
-- We created a file format called my_json_format which holds data of format json
CREATE
OR REPLACE FILE FORMAT my_json_format TYPE = JSON;
-- Creating a table variant_table which holds the raw data, i.e all data as one in variant coloumn
CREATE TABLE variant_table (variant_data variant);
/* 
-- We need to put data in the table stage, as data is in local we need to run the command from snowsql.

put file://~/Desktop/employee.json @%variant_table;

-- To be run from snowsql

*/
-- Copying data into variant table create above, from table stage with format created above i.e my_json_format
COPY INTO variant_table
FROM
    @%variant_table file_format = my_json_format;
-- Displaying the data
select
    *
from
    variant_table;
    
-- Creating variant version using parse_json directly from the internal stage created earlier
CREATE OR REPLACE TABLE employees_variant
AS (
SELECT PARSE_JSON('{
    "ID": ' || t.$1 || ',
    "First_Name": "' || t.$2 || '",
    "Last_Name": "' || t.$3 || '",
    "Email": "' || t.$4 || '",
    "Department": " '|| t.$5 || ' ",
    "Contact_no": "'|| t.$6 || '",
    "City": "'|| t.$7 || '",
  }') AS employee_data
  FROM @internal_stage (pattern => '.*employee.*') t
);

select
    *
from
employees_variant;
-- Question - 8
    -- Load the file into an external and internal stage
    -- We created a file format called my_csv_format which holds data of format csv
    CREATE
    OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;
-- We created a internal stage called internal_stage with holding my_csv_format file format.
    CREATE STAGE internal_stage file_format = my_csv_format;
    /* 
    -- We need to put data in the table stage, as data is in local we need to run the command from snowsql.
    
    put file://~/Desktop/employee.csv @internal_stage; 
    
    -- To be run from snowsql
    
    */
    -- We created a storage integration object called s3_integration with holding s3 storage provider.
    CREATE STORAGE INTEGRATION s3_integration type = external_stage storage_provider = s3 enabled = true storage_aws_role_arn = 'arn:aws:iam::366068070173:role/s3_chakradhar' storage_allowed_locations = ('s3://assignment-snowflake/csv/employee.csv');
-- As we are working on admin role, we grant all on integration object
    GRANT ALL ON INTEGRATION s3_integration TO ROLE admin;
-- Describing Integration object to arrange a rrelatoinship between aws and snowflake
    DESC INTEGRATION s3_integration;
-- We created a external stage called external_stage with holding my_csv_format file format and s3 bucket url.
    CREATE
    OR REPLACE STAGE external_stage URL = 's3://assignment-snowflake/csv/employee.csv' STORAGE_INTEGRATION = s3_integration FILE_FORMAT = my_csv_format;
-- Listing both the files, for checking whether data was loaded correctly or not.
    LIST @internal_stage;
LIST @external_stage;
-- Question - 9
    -- Load data into the tables using copy into statements. In one table load from the internal stage and in another from the external.
    -- Creating table employee_internal_stage for loading employee data from internal stage.
    CREATE TABLE employee_internal_stage (
        ID NUMBER,
        FIRST_NAME VARCHAR(255),
        LAST_NAME VARCHAR(255),
        EMAIL VARCHAR(255),
        DEPARTMENT VARCHAR(255),
        CONTACT_NO VARCHAR(255),
        CITY VARCHAR(255),
        etl_ts timestamp default current_timestamp(),
        -- for getting the time at which the record is getting inserted
        etl_by varchar default 'snowsight',
        -- for getting application name from which the record was inserted
        file_name varchar -- for getting the name of the file used to insert data into the table.
    );
-- Creating table employee_internal_stage for loading employee data from external stage.
    CREATE TABLE employee_external_stage (
        ID NUMBER,
        FIRST_NAME VARCHAR(255),
        LAST_NAME VARCHAR(255),
        EMAIL VARCHAR(255),
        DEPARTMENT VARCHAR(255),
        CONTACT_NO VARCHAR(255),
        CITY VARCHAR(255),
        etl_ts timestamp default current_timestamp(),
        -- for getting the time at which the record is getting inserted
        etl_by varchar default 'snowsight',
        -- for getting application name from which the record was inserted
        file_name varchar -- for getting the name of the file used to insert data into the table.
    );
-- Copying data into respective table from corresponding stages and we are fetching the table data using metadata function.
    COPY INTO employee_internal_stage(
        id,
        first_name,
        last_name,
        email,
        department,
        contact_no,
        city,
        file_name
    )
FROM
    (
        SELECT
            emp.$1,
            emp.$2,
            emp.$3,
            emp.$4,
            emp.$5,
            emp.$6,
            emp.$7,
            METADATA$FILENAME
        FROM
            @internal_stage/employee.csv.gz (file_format => my_csv_format) emp
    );
COPY INTO employee_external_stage(
        id,
        first_name,
        last_name,
        email,
        department,
        contact_no,
        city,
        file_name
    )
FROM
    (
        SELECT
            emp.$1,
            emp.$2,
            emp.$3,
            emp.$4,
            emp.$5,
            emp.$6,
            emp.$7,
            METADATA$FILENAME
        FROM
            @external_stage (file_format => my_csv_format) emp
    );
-- Displaying the employee data to check whether they are loaded or not.
select
    *
from
    employee_internal_stage
limit
    10;
select
    *
from
    employee_external_stage
limit
    10;
-- Question - 10
    -- Upload any parquet file to the stage location and infer the schema of the file
    -- We created a file format called my_parquet_format which holds data of format csv
    CREATE FILE FORMAT my_parquet_format TYPE = parquet;
-- We created a stage called parquet_stage with holding my_parquet_format file format.
    CREATE STAGE parquet_stage file_format = my_parquet_format;
    /* 
    -- We need to put data in the table stage, as data is in local we need to run the command from snowsql.
    
    put file://~/Desktop/employee.parquet @parquet_stage;
    
    -- To be run from snowsql
    */
    -- Query to Infer about the schema
SELECT
    *
FROM
    TABLE(
        INFER_SCHEMA(
            LOCATION => '@parquet_stage',
            FILE_FORMAT => 'my_parquet_format'
        )
    );
-- Question - 11
    -- Run a select query on the staged parquet file without loading it to a snowflake table
SELECT
    *
from
    @parquet_stage/employee.parquet;
-- Question - 12
    -- Add masking policy to the PII columns such that fields like email, phone number, etc. show as **masked** to a user with the developer role. If the role is PII the value of these columns should be visible
    -- Creating masking policy for given constraints.
    CREATE
    OR REPLACE MASKING POLICY email_mask AS (VAL string) RETURNS string -> CASE
        WHEN CURRENT_ROLE() = 'PII' THEN VAL
        ELSE '****MASK****'
    END;
CREATE
    OR REPLACE MASKING POLICY contact_Mask AS (VAL string) RETURNS string -> CASE
        WHEN CURRENT_ROLE() = 'PII' THEN VAL
        ELSE '****MASK****'
    END;
-- Applying those policies to table by altering them
    -- Adding the email_mask policy to employee_internal_stage
ALTER TABLE
    IF EXISTS employee_internal_stage
MODIFY
    EMAIL
SET
    MASKING POLICY email_mask;
-- Adding the email_mask policy to employee_external_stage
ALTER TABLE
    IF EXISTS employee_external_stage
MODIFY
    EMAIL
SET
    MASKING POLICY email_mask;
-- Adding the contact_mask policy to employee_internal_stage
ALTER TABLE
    IF EXISTS employee_internal_stage
MODIFY
    contact_no
SET
    MASKING POLICY contact_mask;
-- Adding the conatct_mask policy to employee_external_stage
ALTER TABLE
    IF EXISTS employee_external_stage
MODIFY
    contact_no
SET
    MASKING POLICY contact_mask;
-- Displaying data from Admin view
SELECT
    *
FROM
    employee_internal_stage
LIMIT
    10;
SELECT
    *
FROM
    employee_external_stage
LIMIT
    10;
-- Displaying data from PII view
    USE ROLE ACCOUNTADMIN;
-- Granting required previlages to role PII
    GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE PII;
GRANT
SELECT
    ON TABLE assignment_db.my_schema.employee_internal_stage TO ROLE PII;
GRANT
SELECT
    ON TABLE assignment_db.my_schema.employee_external_stage TO ROLE PII;
USE ROLE PII;
-- using the role PII
    -- Displaying data from PII role
SELECT
    *
FROM
    employee_internal_stage
LIMIT
    10;
SELECT
    *
FROM
    employee_external_stage
LIMIT
    10;
-- Displaying data from Developer view
    USE ROLE ACCOUNTADMIN;
-- Granting required previlages to role developer
    GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE DEVELOPER;
GRANT
SELECT
    ON TABLE assignment_db.my_schema.employee_internal_stage TO ROLE DEVELOPER;
GRANT
SELECT
    ON TABLE assignment_db.my_schema.employee_external_stage TO ROLE DEVELOPER;
USE ROLE DEVELOPER;
-- using the role Developer
    -- Displaying data from developer role
SELECT
    *
FROM
    employee_internal_stage
LIMIT
    10;
SELECT
    *
FROM
    employee_external_stage
LIMIT
    10;
