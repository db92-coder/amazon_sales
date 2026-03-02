-- Category-level performance mart at top-category grain.
DROP TABLE IF EXISTS analytics.mart_category_performance;

CREATE TABLE analytics.mart_category_performance AS
WITH product_top_category AS (
    SELECT DISTINCT
        b.product_id,
        c.category_name AS top_category
    FROM analytics.bridge_product_category b
    JOIN analytics.dim_category c
      ON c.category_id = b.category_id
    WHERE c.category_level = 1
)
SELECT
    ptc.top_category,
    COUNT(DISTINCT ptc.product_id) AS product_count,
    ROUND(AVG(p.discount_percentage)::numeric, 2) AS avg_discount_pct,
    ROUND(AVG(p.rating)::numeric, 2) AS avg_rating,
    SUM(COALESCE(mp.review_count, 0)) AS total_review_count,
    ROUND(AVG(mp.review_text_coverage_ratio)::numeric, 4) AS avg_review_text_coverage_ratio
FROM product_top_category ptc
JOIN analytics.dim_product p
  ON p.product_id = ptc.product_id
LEFT JOIN analytics.mart_product_performance mp
  ON mp.product_id = ptc.product_id
GROUP BY ptc.top_category
ORDER BY product_count DESC, ptc.top_category;

CREATE UNIQUE INDEX mart_category_performance_top_category_ux
    ON analytics.mart_category_performance (top_category);

