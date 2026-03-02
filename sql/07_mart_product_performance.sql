-- Product-level mart used by dashboards and agent answers.
DROP TABLE IF EXISTS analytics.mart_product_performance;

CREATE TABLE analytics.mart_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.discounted_price,
    p.actual_price,
    p.discount_percentage,
    p.discount_amount,
    p.rating,
    p.rating_count,
    COUNT(f.review_sk) AS review_count,
    ROUND(
        AVG(
            CASE
                WHEN NULLIF(TRIM(f.review_content_item), '') IS NOT NULL THEN 1
                ELSE 0
            END
        )::numeric,
        4
    ) AS review_text_coverage_ratio
FROM analytics.dim_product p
LEFT JOIN analytics.fact_review f
  ON f.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.discounted_price,
    p.actual_price,
    p.discount_percentage,
    p.discount_amount,
    p.rating,
    p.rating_count;

ALTER TABLE analytics.mart_product_performance
    ADD CONSTRAINT mart_product_performance_pk PRIMARY KEY (product_id);

