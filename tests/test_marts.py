from __future__ import annotations

from sqlalchemy import create_engine, text

from src.etl.config import get_database_url


def _scalar(query: str) -> int:
    # Helper to fetch one integer metric from Postgres.
    engine = create_engine(get_database_url(), pool_pre_ping=True)
    with engine.begin() as conn:
        return int(conn.execute(text(query)).scalar_one())


def test_mart_product_performance_not_empty() -> None:
    count = _scalar("SELECT COUNT(*) FROM analytics.mart_product_performance;")
    assert count > 0


def test_mart_category_performance_not_empty() -> None:
    count = _scalar("SELECT COUNT(*) FROM analytics.mart_category_performance;")
    assert count > 0


def test_mart_review_quality_not_empty() -> None:
    count = _scalar("SELECT COUNT(*) FROM analytics.mart_review_quality;")
    assert count > 0


def test_mart_product_performance_has_no_null_product_id() -> None:
    null_count = _scalar(
        """
        SELECT COUNT(*)
        FROM analytics.mart_product_performance
        WHERE product_id IS NULL OR TRIM(product_id) = '';
        """
    )
    assert null_count == 0

