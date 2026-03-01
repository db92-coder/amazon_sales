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
