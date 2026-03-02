# Architecture Diagram

```mermaid
flowchart TD
    A[Raw CSV data/amazon.csv] --> B[Extract\nsrc/etl/extract.py]
    B --> C[Transform\nsrc/etl/transform.py]
    C --> D[Staging Table\nanalytics.stg_amazon_products]
    D --> E[Normalized Models\nsql/02-05]
    E --> F[Integrity Validation\nsql/06 + tests/test_model_integrity.py]
    F --> G[Business Marts\nsql/07-09]
    G --> H[Mart Validation\nsql/10 + tests/test_marts.py]
    H --> I[KPI SQL Pack\nsql/kpi_*.sql]
    H --> J[AI Analyst Agent\nsrc/agents/analyst_agent.py]
    J --> K[Read-only SQL Answers\nApproved marts only]
```

## Layer Summary
- `Raw`: immutable source file.
- `Staging`: typed, cleaned canonical row set.
- `Normalized`: star-like model for stable joins and integrity.
- `Marts`: business-facing aggregates for dashboards and reporting.
- `AI Agent`: controlled natural-language interface to marts.

## Reliability Controls
- Pipeline quality gate at transform stage.
- PK/FK integrity checks after normalized model build.
- Mart health checks after mart build.
- Automated tests for CI-ready validation.
