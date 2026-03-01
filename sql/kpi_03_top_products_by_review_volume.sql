-- KPI: top products by number of reviews in fact table.
SELECT
    p.product_id,
    p.product_name,
    COUNT(*) AS review_count,
    ROUND(
        AVG(
            CASE
                WHEN NULLIF(TRIM(f.review_content_item), '') IS NOT NULL THEN 1
                ELSE 0
            END
        )::numeric,
        2
    ) AS has_review_text_ratio
FROM analytics.fact_review f
JOIN analytics.dim_product p
  ON p.product_id = f.product_id
GROUP BY p.product_id, p.product_name
ORDER BY review_count DESC, p.product_id
LIMIT 20;
