from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, text
import pandas as pd


def persist_staging_csv(df: pd.DataFrame, output_path: Path) -> None:
    # Ensure staging directory exists before writing output.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Persist transformed data for quick local inspection/debugging.
    df.to_csv(output_path, index=False, encoding="utf-8")


def load_to_postgres(df: pd.DataFrame, database_url: str, table_name: str = "stg_amazon_products") -> None:
    # Create SQLAlchemy engine with connection health checks enabled.
    engine = create_engine(database_url, pool_pre_ping=True)
    # Guarantee target schema exists before loading table.
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE SCHEMA IF NOT EXISTS analytics;
                """
            )
        )

    # Replace staging table on each run to keep the pipeline deterministic.
    df.to_sql(
        name=table_name,
        con=engine,
        schema="analytics",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=500,
    )


def run_sql_file(database_url: str, sql_file: Path) -> None:
    # Execute raw SQL scripts used for warehouse modeling.
    sql_text = sql_file.read_text(encoding="utf-8")
    engine = create_engine(database_url, pool_pre_ping=True)
    # Use a DBAPI cursor to allow scripts with multiple SQL statements.
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            cursor.execute(sql_text)
        raw_conn.commit()
    finally:
        raw_conn.close()


def build_normalized_tables(database_url: str, sql_dir: Path) -> None:
    # Execute in dependency order: dimensions first, then bridge/facts.
    ordered_files = [
        sql_dir / "02_dim_product.sql",
        sql_dir / "03_dim_category.sql",
        sql_dir / "04_bridge_product_category.sql",
        sql_dir / "05_fact_review.sql",
    ]
    for sql_file in ordered_files:
        if not sql_file.exists():
            raise FileNotFoundError(f"Missing SQL script: {sql_file}")
        run_sql_file(database_url, sql_file)


def build_marts(database_url: str, sql_dir: Path) -> None:
    # Execute marts after normalized dimensions/facts are built.
    ordered_files = [
        sql_dir / "07_mart_product_performance.sql",
        sql_dir / "08_mart_category_performance.sql",
        sql_dir / "09_mart_review_quality.sql",
    ]
    for sql_file in ordered_files:
        if not sql_file.exists():
            raise FileNotFoundError(f"Missing SQL script: {sql_file}")
        run_sql_file(database_url, sql_file)


def validate_normalized_tables(database_url: str) -> list[str]:
    # Rule name + query returning a single integer violation count.
    checks: list[tuple[str, str]] = [
        (
            "dim_product_duplicate_product_id",
            """
            SELECT COUNT(*) FROM (
                SELECT product_id
                FROM analytics.dim_product
                GROUP BY product_id
                HAVING COUNT(*) > 1
            ) d;
            """,
        ),
        (
            "dim_category_duplicate_category_id",
            """
            SELECT COUNT(*) FROM (
                SELECT category_id
                FROM analytics.dim_category
                GROUP BY category_id
                HAVING COUNT(*) > 1
            ) d;
            """,
        ),
        (
            "bridge_orphan_product_id",
            """
            SELECT COUNT(*)
            FROM analytics.bridge_product_category b
            LEFT JOIN analytics.dim_product p
              ON p.product_id = b.product_id
            WHERE p.product_id IS NULL;
            """,
        ),
        (
            "bridge_orphan_category_id",
            """
            SELECT COUNT(*)
            FROM analytics.bridge_product_category b
            LEFT JOIN analytics.dim_category c
              ON c.category_id = b.category_id
            WHERE c.category_id IS NULL;
            """,
        ),
        (
            "fact_review_orphan_product_id",
            """
            SELECT COUNT(*)
            FROM analytics.fact_review f
            LEFT JOIN analytics.dim_product p
              ON p.product_id = f.product_id
            WHERE p.product_id IS NULL;
            """,
        ),
    ]

    issues: list[str] = []
    engine = create_engine(database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        for rule_name, sql_query in checks:
            violation_count = conn.execute(text(sql_query)).scalar_one()
            if int(violation_count) > 0:
                issues.append(f"{rule_name}={violation_count}")

    return issues


def validate_marts(database_url: str) -> list[str]:
    # Checks focused on mart usability and expected non-null KPI fields.
    checks: list[tuple[str, str]] = [
        (
            "mart_product_performance_empty",
            "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM analytics.mart_product_performance;",
        ),
        (
            "mart_category_performance_empty",
            "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM analytics.mart_category_performance;",
        ),
        (
            "mart_review_quality_empty",
            "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM analytics.mart_review_quality;",
        ),
        (
            "mart_product_performance_null_product_id",
            """
            SELECT COUNT(*)
            FROM analytics.mart_product_performance
            WHERE product_id IS NULL OR TRIM(product_id) = '';
            """,
        ),
        (
            "mart_category_performance_null_top_category",
            """
            SELECT COUNT(*)
            FROM analytics.mart_category_performance
            WHERE top_category IS NULL OR TRIM(top_category) = '';
            """,
        ),
    ]

    issues: list[str] = []
    engine = create_engine(database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        for rule_name, sql_query in checks:
            violation_count = conn.execute(text(sql_query)).scalar_one()
            if int(violation_count) > 0:
                issues.append(f"{rule_name}={violation_count}")

    return issues
