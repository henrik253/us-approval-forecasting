USE WAREHOUSE OTTER_WH;
USE DATABASE approval_db;
USE SCHEMA bronze;

EXECUTE IMMEDIATE FROM '@scripts_stage/01_bronze_silver.sql';
SELECT SYSTEM$WAIT(5);
EXECUTE IMMEDIATE FROM '@scripts_stage/02_gold.sql';
EXECUTE IMMEDIATE FROM '@scripts_stage/03_project_requirements.sql';


-- Updated to include Compute Pool and Runtime parameters
CREATE OR REPLACE NOTEBOOK gold.model_training_notebook
    FROM '@approval_db.bronze.scripts_stage'
    MAIN_FILE = 'forecast_03.ipynb'
    QUERY_WAREHOUSE = 'OTTER_WH'
    COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
    RUNTIME_NAME = 'SYSTEM$BASIC_RUNTIME';

EXECUTE NOTEBOOK approval_db.gold.model_training_notebook();


CREATE OR REPLACE STREAMLIT gold.approval_forecast_dashboard
    ROOT_LOCATION = '@approval_db.bronze.scripts_stage'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'OTTER_WH';