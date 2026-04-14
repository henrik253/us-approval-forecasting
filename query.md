# Query History

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
