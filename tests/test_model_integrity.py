from __future__ import annotations

from sqlalchemy import create_engine, text

from src.etl.config import get_database_url


def _scalar(query: str) -> int:
    # Small helper to run one SQL and return a single integer result.
    engine = create_engine(get_database_url(), pool_pre_ping=True)
    with engine.begin() as conn:
        return int(conn.execute(text(query)).scalar_one())


def test_dim_product_product_id_is_unique() -> None:
    duplicates = _scalar(
        """
        SELECT COUNT(*)
        FROM (
            SELECT product_id
            FROM analytics.dim_product
            GROUP BY product_id
            HAVING COUNT(*) > 1
        ) d;
        """
    )
    assert duplicates == 0


def test_bridge_has_no_orphan_product_ids() -> None:
    orphan_count = _scalar(
        """
        SELECT COUNT(*)
        FROM analytics.bridge_product_category b
        LEFT JOIN analytics.dim_product p
          ON p.product_id = b.product_id
        WHERE p.product_id IS NULL;
        """
    )
    assert orphan_count == 0


def test_bridge_has_no_orphan_category_ids() -> None:
    orphan_count = _scalar(
        """
        SELECT COUNT(*)
        FROM analytics.bridge_product_category b
        LEFT JOIN analytics.dim_category c
          ON c.category_id = b.category_id
        WHERE c.category_id IS NULL;
        """
    )
    assert orphan_count == 0


def test_fact_review_has_no_orphan_products() -> None:
    orphan_count = _scalar(
        """
        SELECT COUNT(*)
        FROM analytics.fact_review f
        LEFT JOIN analytics.dim_product p
          ON p.product_id = f.product_id
        WHERE p.product_id IS NULL;
        """
    )
    assert orphan_count == 0

