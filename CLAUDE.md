# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Forecasting US approvals (US President) using a data warehousing pipeline and machine learning models. Master's-level Data Warehousing project.

## Workflow

- After EVERY user request, append the query to query.md before finishing the response.

## Architecture

The project follows the **Medallion architecture** for data processing, combined with a separate ML layer:

1. **Bronze** (`pipelines/sql/raw_bronce.sql`) — raw ingestion from source
2. **Silver** (`pipelines/sql/clean_silver.sql`) — cleaning and normalization
3. **Gold** (`pipelines/sql/obt_gold.sql`) — one big table (OBT) for analytics/modeling

**Python pipeline** (`pipelines/scripts/`):
- `client.py` — fetches data from external source/API
- `load_handler.py` — loads data into the warehouse layers

**ML layer** (`models/`):
- `train.py` / `predict.py` — training and inference
- `prototype.ipynb` — exploratory prototyping notebook

**Shared utilities** (`src/data_loader.py`) — reusable data loading logic consumed by both pipeline and models.

**Infrastructure** (`infrastructure/main.tf`) — Terraform for cloud resource provisioning.

## Development Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt   # once created
```

## SQL Layer Convention

SQL files map to warehouse layers — keep DDL/DML separated by layer. The naming convention is `<layer>_<tier>.sql` (bronze/silver/gold).
