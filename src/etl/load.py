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
