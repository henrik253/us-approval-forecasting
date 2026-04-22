USE WAREHOUSE OTTER_WH;
USE DATABASE approval_db;
USE SCHEMA BRONZE;
-- expect around 3min runtime 
EXECUTE IMMEDIATE FROM @scripts_stage/deploy_pipeline.sql;



