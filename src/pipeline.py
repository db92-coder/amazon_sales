from __future__ import annotations

from pathlib import Path

from src.etl.config import DEFAULT_SOURCE_CSV, DEFAULT_STAGING_CSV, get_database_url
from src.etl.extract import extract_csv
from src.etl.load import build_normalized_tables, load_to_postgres, persist_staging_csv
from src.etl.transform import run_quality_checks, transform_amazon_data


def main() -> int:
    # Start from the default location we agreed for the source dataset.
    source_csv = DEFAULT_SOURCE_CSV
    # Backward-compatible fallback if you later reorganize data into data/raw/.
    if not source_csv.exists():
        fallback = Path("data") / "raw" / "amazon.csv"
        if fallback.exists():
            source_csv = fallback

    # Extract step: read the raw CSV exactly as text first to avoid early type errors.
    print(f"[extract] reading {source_csv}")
    raw_df = extract_csv(source_csv)
    print(f"[extract] rows={len(raw_df)}, cols={len(raw_df.columns)}")

    # Transform step: clean encoding/currency artifacts and cast key numeric fields.
    print("[transform] cleaning and standardizing")
    clean_df = transform_amazon_data(raw_df)

    # Data quality gate: fail the pipeline if critical business rules are broken.
    issues = run_quality_checks(clean_df)
    if issues:
        print("[quality] checks failed:")
        for issue in issues:
            print(f"  - {issue}")
        return 1
    print("[quality] checks passed")

    # Persist a local staging artifact so you can inspect transformed rows quickly.
    print(f"[load] writing staging csv -> {DEFAULT_STAGING_CSV}")
    persist_staging_csv(clean_df, DEFAULT_STAGING_CSV)

    # Build database connection from environment variables in .env.
    database_url = get_database_url()
    # Load step: push the transformed dataframe into Postgres staging table.
    print("[load] loading to postgres table analytics.stg_amazon_products")
    load_to_postgres(clean_df, database_url, table_name="stg_amazon_products")

    # Modeling step: build normalized warehouse tables from the staging table.
    print("[model] building normalized tables in analytics schema")
    build_normalized_tables(database_url, Path("sql"))

    print("[done] pipeline completed successfully")
    return 0


if __name__ == "__main__":
    # Standard Python entrypoint so `python -m src.pipeline` runs main().
    raise SystemExit(main())
