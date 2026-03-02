# Amazon Sales Analytics Platform

Portfolio-grade analytics engineering project that takes a raw Kaggle Amazon dataset and turns it into validated warehouse models, business marts, KPI queries, and a read-only AI analyst agent.

## Project Outcome
- Built an end-to-end ETL pipeline (`extract -> transform -> load -> model -> validate`).
- Normalized denormalized source data into analytics-friendly dimensions/facts.
- Added data quality gates for PK/FK integrity and mart health.
- Created category/product/review marts for reporting.
- Added a read-only SQL AI analyst agent for natural-language analytics questions.

## Tech Stack
- `Python` (pipeline orchestration, tests, agent)
- `PostgreSQL` (warehouse in Docker)
- `SQLAlchemy` + `psycopg2` (database access)
- `PyTest` (data tests)
- `Docker Compose` (local infra)
- `SQL` (modeling, marts, KPI queries)
- `OpenAI SDK` (agent SQL generation with guardrails)

## Architecture
See full diagram and layer details in [Architecture](./docs/architecture.md).

## Data Model
### Staging
- `analytics.stg_amazon_products`

### Normalized Core
- `analytics.dim_product`
- `analytics.dim_category`
- `analytics.bridge_product_category`
- `analytics.fact_review`

### Business Marts
- `analytics.mart_product_performance`
- `analytics.mart_category_performance`
- `analytics.mart_review_quality`

## Repository Structure
```text
amazon_sales/
  data/
  docs/
    architecture.md
    interview_demo_script.md
    metrics.md
  sql/
    02_dim_product.sql
    03_dim_category.sql
    04_bridge_product_category.sql
    05_fact_review.sql
    06_validation_checks.sql
    07_mart_product_performance.sql
    08_mart_category_performance.sql
    09_mart_review_quality.sql
    10_validation_marts.sql
    kpi_01_category_product_coverage.sql
    kpi_02_discount_vs_rating.sql
    kpi_03_top_products_by_review_volume.sql
  src/
    pipeline.py
    etl/
    agents/
  tests/
```

## Quick Start
1. Start PostgreSQL container:
```powershell
docker compose --env-file .env up -d
```

2. Run pipeline:
```powershell
.\.venv\Scripts\python.exe -m src.pipeline
```

3. Run tests:
```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_model_integrity.py tests\test_marts.py -q
```

4. Run KPI queries (example):
```powershell
docker exec -i amazon_postgres psql -U postgres -d amazon_sales -f /docker-entrypoint-initdb.d/kpi_01_category_product_coverage.sql
```

## Data Quality Framework
- Transform-level checks in `src/etl/transform.py`
- Model integrity gate in `src/etl/load.py` (`validate_normalized_tables`)
- Mart quality gate in `src/etl/load.py` (`validate_marts`)
- SQL validation scripts:
  - `sql/06_validation_checks.sql`
  - `sql/10_validation_marts.sql`
- Automated tests:
  - `tests/test_model_integrity.py`
  - `tests/test_marts.py`

## AI Analyst Agent
- File: `src/agents/analyst_agent.py`
- Behavior:
  - Generates SQL from business questions
  - Allows only read-only `SELECT/WITH`
  - Restricts queries to approved mart tables
  - Returns rows + generated SQL for auditability

## KPI Pack
- Product coverage by top category
- Discount vs rating by category
- Top products by review volume
- KPI SQL files in `sql/kpi_*.sql`

## Author Notes
This project demonstrates core analytics engineering capabilities:
- building reliable pipelines
- modeling data warehouses
- enforcing data quality gates
- delivering analyst-ready marts
- enabling safe AI-assisted analytics
