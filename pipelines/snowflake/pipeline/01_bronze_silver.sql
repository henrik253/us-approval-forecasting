USE WAREHOUSE OTTER_WH;
USE DATABASE approval_db;
USE SCHEMA bronze;

// RAW/Destination Tables, instead of naming convention SLV_ or  CUR_ , naming comes from schemas
CREATE OR REPLACE TABLE bronze.ECONOMIC (
    date        DATE             NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    indicator   VARCHAR(255)     NOT NULL,
    series      VARCHAR(255)     NOT NULL
) WITH TAG (OtterProject = 'True');

CREATE OR REPLACE TABLE bronze.APPROVAL (
    date          DATE             NOT NULL,
    sample_size   INTEGER          NOT NULL,
    approval      DOUBLE PRECISION NOT NULL,
    disapproval   DOUBLE PRECISION NOT NULL
) WITH TAG (OtterProject = 'True');

CREATE OR REPLACE TABLE bronze.SENTIMENT (
    date      DATE             NOT NULL,
    tone      DOUBLE PRECISION NOT NULL
) WITH TAG (OtterProject = 'True');

COPY INTO bronze.ECONOMIC (date, value, indicator, series)
FROM (
  SELECT 
    $1:date::DATE, 
    $1:value::DOUBLE PRECISION, 
    $1:indicator::VARCHAR, 
    $1:series::VARCHAR 
  FROM @bronze.stage_raw
) PATTERN = '.*economic.*\\.json';



CREATE OR REPLACE PIPE bronze.pipe_economic
AUTO_INGEST = TRUE
AS
COPY INTO bronze.ECONOMIC (date, value, indicator, series)
FROM (
  SELECT 
    $1:date::DATE, 
    $1:value::DOUBLE PRECISION, 
    $1:indicator::VARCHAR, 
    $1:series::VARCHAR 
  FROM @bronze.stage_raw
) PATTERN = '.*economic.*\\.json';

COPY INTO bronze.APPROVAL (date, sample_size, approval, disapproval)
FROM (
  SELECT 
    $1:date::DATE, 
    $1:sample_size::INTEGER, 
    $1:approval::DOUBLE PRECISION, 
    $1:disapproval::DOUBLE PRECISION 
  FROM @bronze.stage_raw
) PATTERN = '.*approval.*\\.json';

CREATE OR REPLACE PIPE bronze.pipe_approval
AUTO_INGEST = TRUE
AS
COPY INTO bronze.APPROVAL (date, sample_size, approval, disapproval)
FROM (
  SELECT 
    $1:date::DATE, 
    $1:sample_size::INTEGER, 
    $1:approval::DOUBLE PRECISION, 
    $1:disapproval::DOUBLE PRECISION 
  FROM @bronze.stage_raw
) PATTERN = '.*approval.*\\.json';

COPY INTO bronze.SENTIMENT (date, tone)
FROM (
  SELECT 
    TO_DATE($1:date::STRING, 'YYYYMMDD"T"HH24MISS"Z"'), 
    $1:tone::DOUBLE PRECISION 
  FROM @bronze.stage_raw
) PATTERN = '.*media_sentiment.*\\.json';

CREATE OR REPLACE PIPE bronze.pipe_sentiment
AUTO_INGEST = TRUE
AS
COPY INTO bronze.SENTIMENT (date, tone)
FROM (
  SELECT 
    TO_DATE($1:date::STRING, 'YYYYMMDD"T"HH24MISS"Z"'), 
    $1:tone::DOUBLE PRECISION 
  FROM @bronze.stage_raw
) PATTERN = '.*media_sentiment.*\\.json';

ALTER PIPE bronze.pipe_economic REFRESH; // trigger pipes
ALTER PIPE bronze.pipe_approval REFRESH;
ALTER PIPE bronze.pipe_sentiment REFRESH;

SHOW PIPES IN SCHEMA bronze;

SELECT SYSTEM$PIPE_STATUS('bronze.pipe_economic');
SELECT SYSTEM$PIPE_STATUS('bronze.pipe_approval');
SELECT SYSTEM$PIPE_STATUS('bronze.pipe_sentiment');

-- Check if loaded
SELECT * FROM bronze.ECONOMIC;  // COUNT(*)<- wont work since count actually doesnt count rows. Took couple hours to figure out btw
SELECT * FROM bronze.APPROVAL;    
SELECT * FROM bronze.SENTIMENT; 


// SILVER LAYER
// THIS PROJECTS DOES NOT NEED ANY KIMBALL-style structure and it doesnt make sense for TIME SERIES DATA 

CREATE OR REPLACE VIEW SILVER.ECONOMIC AS
SELECT
    TO_DATE(DATE)        AS DATE,
    LOWER(SERIES)        AS SERIES,
    TRY_TO_DOUBLE(VALUE) AS VALUE
FROM BRONZE.ECONOMIC
WHERE VALUE IS NOT NULL;

// Averaging since data contains multiple polls from different regions for a same date
CREATE OR REPLACE VIEW SILVER.APPROVAL AS 
SELECT 
    TO_DATE(DATE) AS DATE, 
    AVG(TRY_TO_DOUBLE(APPROVAL)) AS APPROVAL, 
    AVG(TRY_TO_DOUBLE(DISAPPROVAL)) AS DISAPPROVAL 
FROM BRONZE.APPROVAL GROUP BY DATE; 

// In case sentiment has multiple values for a date just avg them 
CREATE OR REPLACE VIEW SILVER.SENTIMENT AS 
SELECT
    TO_DATE(DATE) AS DATE,
    AVG(TRY_TO_DOUBLE(TONE)) AS TONE
FROM BRONZE.SENTIMENT
GROUP BY DATE;

SELECT
    e.SERIES,
    COUNT(*) AS row_count
FROM SILVER.ECONOMIC e
LEFT JOIN SILVER.APPROVAL a 
    ON e.DATE = a.DATE
LEFT JOIN SILVER.SENTIMENT s 
    ON e.DATE = s.DATE WHERE e.date >= '2025-02-20'
GROUP BY e.SERIES
ORDER BY row_count DESC;


SELECT e.DATE, e.*, a.*, s.* FROM SILVER.ECONOMIC e JOIN SILVER.APPROVAL a ON e.DATE = a.DATE JOIN SILVER.SENTIMENT s ON e.DATE = s.DATE ORDER BY e.DATE, e.SERIES;












