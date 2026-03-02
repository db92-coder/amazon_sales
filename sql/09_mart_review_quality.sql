-- Review-quality mart summarized by top category.
DROP TABLE IF EXISTS analytics.mart_review_quality;

CREATE TABLE analytics.mart_review_quality AS
WITH review_base AS (
    SELECT
        f.review_sk,
        f.product_id,
        f.review_id,
        f.user_id,
        f.review_title,
        f.review_content_item,
        CASE WHEN NULLIF(TRIM(f.review_content_item), '') IS NOT NULL THEN 1 ELSE 0 END AS has_review_text
    FROM analytics.fact_review f
),
product_top_category AS (
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
    COUNT(rb.review_sk) AS review_count,
    COUNT(DISTINCT rb.review_id) AS distinct_review_id_count,
    COUNT(DISTINCT rb.user_id) AS distinct_user_count,
    ROUND(AVG(rb.has_review_text)::numeric, 4) AS review_text_coverage_ratio
FROM product_top_category ptc
LEFT JOIN review_base rb
  ON rb.product_id = ptc.product_id
GROUP BY ptc.top_category
ORDER BY review_count DESC, ptc.top_category;

CREATE UNIQUE INDEX mart_review_quality_top_category_ux
    ON analytics.mart_review_quality (top_category);

