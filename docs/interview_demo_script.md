# Interview Demo Script

## Goal
Show end-to-end ownership: data engineering, modeling, validation, analytics, and AI-assisted querying.

## Demo Length
- Target: 12 to 15 minutes.

## 1) 60-Second Opening
Use this talk track:
"I built a full analytics engineering pipeline on Amazon product/review data. It ingests messy raw CSV, standardizes and validates it, models it into dimensions/facts, builds business marts, and exposes those marts to a guarded read-only AI SQL analyst agent."

## 2) Show Project Structure (1 minute)
Highlight:
- `src/pipeline.py`
- `src/etl/*`
- `sql/02-10` + `sql/kpi_*`
- `tests/*`
- `src/agents/analyst_agent.py`

## 3) Run Pipeline Live (2 minutes)
```powershell
docker compose --env-file .env up -d
.\.venv\Scripts\python.exe -m src.pipeline
```
What to say:
- "This executes extract, transform, load, normalized modeling, and validations in one run."

## 4) Show Data Quality Gates (2 minutes)
```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_model_integrity.py tests\test_marts.py -q
docker exec -i amazon_postgres psql -U postgres -d amazon_sales -f /docker-entrypoint-initdb.d/06_validation_checks.sql
docker exec -i amazon_postgres psql -U postgres -d amazon_sales -f /docker-entrypoint-initdb.d/10_validation_marts.sql
```
What to say:
- "I fail fast on PK/FK integrity issues and unusable marts."

## 5) Show Business Insights (3 minutes)
Run:
```powershell
docker exec -i amazon_postgres psql -U postgres -d amazon_sales -f /docker-entrypoint-initdb.d/kpi_01_category_product_coverage.sql
docker exec -i amazon_postgres psql -U postgres -d amazon_sales -f /docker-entrypoint-initdb.d/kpi_02_discount_vs_rating.sql
docker exec -i amazon_postgres psql -U postgres -d amazon_sales -f /docker-entrypoint-initdb.d/kpi_03_top_products_by_review_volume.sql
```
What to say:
- "These marts make common business questions one query away."

## 6) Show AI Agent Layer (3 minutes)
Explain:
- Agent generates SQL from a business question.
- SQL is validated as read-only.
- Agent can only query approved mart tables.
- SQL and results are returned for traceability.

Optional run:
```powershell
.\.venv\Scripts\python.exe src\agents\analyst_agent.py
```

## 7) Close With Engineering Tradeoffs (1 to 2 minutes)
Say:
- "I prioritized deterministic rebuilds and validation gates over premature optimization."
- "The source review text is comma-heavy, so I kept both tokenized and raw review content for traceability."
- "Next step is orchestration and incremental loads, then a dashboard front-end."

## Common Interview Questions and Short Answers
- Why normalize first?
  - "To avoid duplicated logic, enforce referential integrity, and produce reliable metrics."
- Why marts after normalized tables?
  - "They provide business-friendly grain and performance for BI and agent queries."
- How is AI made safe?
  - "Read-only SQL policy, forbidden keyword blocklist, and table allowlist."
- How would you productionize?
  - "Add orchestration (Airflow), CI test gate, observability, and partitioned incremental loads."
