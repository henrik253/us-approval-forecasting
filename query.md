# Query History

---

## Q24 — 2026-04-22

Add a proper README describing the project, including a screenshot from screenshots/.

---

## Q23 — 2026-04-22

Write a dataset_description.md summarizing all data sources used in the project.

---

## Q22 — 2026-04-22

When executing the Lambda handler, will it overwrite existing files in the S3 bucket?

---

## Q21 — 2026-04-22

Update the Lambda requirements.txt.

---

## Q20 — 2026-04-22

Change the AWS Lambda handler so that end_date is always capped to today's actual date — never a future date.

---

## Q19 — 2026-04-22

Operate on the Streamlit app to reflect notebook changes: include disapproval prediction alongside approval, remove train/test split visualization, show predictions and feature importances only. In the forecasting notebook, change APPROVAL_RAW and DISAPPROVAL_RAW to APPROVAL_7D_SMA and DISAPPROVAL_7D_SMA as prediction targets.

---

## Q18 — 2026-04-22

Make another table that stores the cutoff day. Also write both models' feature importances back to a SQL table.

---

## Q17 — 2026-04-22

Operate on the notebook models/forecast_03 and create a second prediction model for the disapproval rating. Also make sure that this data is written to the sql tables as well, as you can see in the last cell.

---

## Q16 — 2026-04-17

Add a notebook cell (after data loading) that gives a schema overview of each JSON file — column names, Python type, inferred SQL type, nullable flag, and an example value — plus a suggested CREATE TABLE DDL, to assist with SQL table design.

---

## Q15 — 2026-04-17

Update analysis.ipynb to use JSON-based caching: create src/resources/, check on each run if today's JSON already exists (per-source files named {source}_{YYYY-MM-DD}.json); if so load from disk, otherwise fetch via the fetchers and save. Convert each raw JSON list to a DataFrame — FRED pivoted to wide format (date index × series columns), polls and sentiment as long DataFrames.

---

## Q14 — 2026-04-14

Rewrite fetch_sources/fred.py, gdelt.py, votehub.py and handler.py to remove pandas entirely. All fetchers now return plain lists of dicts. GDELT drops rolling-mean smoothing. FRED fetch_panel returns a flat list with a series key instead of a wide DataFrame. Remove pandas from requirements.txt (boto3 stays out too since it is pre-installed on the Lambda runtime).

---

## Q13 — 2026-04-14

Add all missed queries from today to query.md.

---

## Q12 — 2026-04-14

Commit and push handler.py and requirements.txt changes (debug mode + S3 upload).

---

## Q11 — 2026-04-14

Add debug mode to pipelines/lambda/handler.py: read a debug key from the event dict (true/false), skip real fetches when true and use hardcoded dummy data instead. Store data (debug or real) to S3 as JSON. All S3 config (bucket name, key prefix, region) must come from environment variables.

---

## Q10 — 2026-04-14

Commit the buildspec.yml bugfix that anchors cd commands with $CODEBUILD_SRC_DIR.

---

## Q9 — 2026-04-14

Commit and push the buildspec.yml changes.

---

## Q8 — 2026-04-14

Edit pipelines/lambda/buildspec.yml for the current folder structure: fix Python version (3.14 → 3.12), correct requirements.txt and lib paths relative to repo root, replace lambda_function.py with handler.py, and include the fetch_sources/ package in the deployment zip.

---

## Q7 — 2026-04-13

Restructure repo: create pipelines/lambda/ with handler.py and requirements.txt; split data_loader.py and data_utils.py into pipelines/lambda/fetch_sources/fred.py, gdelt.py, votehub.py with per-source error handling. Remove old src/data_loader.py and src/data_utils.py. Update analysis.ipynb imports.

---

## Q6 — 2026-04-13

Change section 4 FRED plots from histograms to time series line plots with dates on the X-axis (using AutoDateLocator + ConciseDateFormatter to reflect each series' native interval).

---

## Q5 — 2026-04-13

Remove z-score standardization from section 4 and plot raw values instead. Add section 5 for media sentiment (GDELT tone/volume time series) and section 6 for President approval ratings.

---

## Q4 — 2026-04-13

Edit data_loader.py to add a method that returns a dict where keys represent the category of data and values are DataFrames. The dict should contain economic data from FRED, approval data, and media sentiment. Update analysis.ipynb accordingly.

---

## Q3 — 2026-04-12

Add a file for enviroment files, make sure its mentioned in the .gitignore file. Also make sure the the analysis.ipynb exectues the env file before the API's are called.

---

## Q2 — 2026-04-12

To bring the project to a starting point I want you to write the analysis.ipynb. Make a fetch for all the functions offered in data_utils.py, the actually call for all method should happen in the dataloader that operates as a bridge between those files. In the analysis.ipynb perform a basic cleaning of the data. Check for null values, explore data by plotting the distribution of the data standardized.

---

## Q1 — 2026-04-11

Read the file in `src/data_utils.py`. Put all the functions into a single class. This class should be able to cache values from GDELT, since they have restrictions in the API use. Also clean the code such that hyperparams like the weblinks and API keys can be set in the class. Apply other best practices that make the use of this class as easy as possible. Also create a `query.md` file that appends every query I ask you to it to keep a clean history.
