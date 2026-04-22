// WORKSHEET SECTION 0 

// AWS INTEGRATION 
CREATE STORAGE INTEGRATION IF NOT EXISTS  s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = "S3"
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::434779526829:role/snowflake-s3-access-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://hf-approval-datalake-us-prod-434779526829-us-east-2-an/');

DESC INTEGRATION s3_integration; // Getting STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID for AWS

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;


CREATE DATABASE IF NOT EXISTS approval_db;
CREATE SCHEMA IF NOT EXISTS approval_db.bronze;
CREATE SCHEMA IF NOT EXISTS approval_db.silver;
CREATE SCHEMA IF NOT EXISTS approval_db.gold;
USE WAREHOUSE OTTER_WH;
USE DATABASE approval_db;
USE SCHEMA bronze;
CREATE TAG IF NOT EXISTS OtterProject;

// STAGE
CREATE OR REPLACE STAGE stage_raw
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://hf-approval-datalake-us-prod-434779526829-us-east-2-an/raw/'
  FILE_FORMAT = json_format;

CREATE STAGE IF NOT EXISTS approval_db.bronze.scripts_stage;

REMOVE @approval_db.bronze.stage_raw;
LIST @approval_db.bronze.stage_raw;
ALTER STAGE approval_db.bronze.stage_raw REFRESH;


SELECT $1
FROM @bronze.stage_raw/approval.json;