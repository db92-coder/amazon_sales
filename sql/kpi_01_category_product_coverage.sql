-- KPI: product coverage by top-level category (level 1).
SELECT
    c.category_name AS top_category,
    COUNT(DISTINCT b.product_id) AS product_count
FROM analytics.bridge_product_category b
JOIN analytics.dim_category c
  ON c.category_id = b.category_id
WHERE c.category_level = 1
GROUP BY c.category_name
ORDER BY product_count DESC, c.category_name;

