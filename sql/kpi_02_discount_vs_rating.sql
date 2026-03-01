-- KPI: average discount and rating by top-level category.
SELECT
    c.category_name AS top_category,
    ROUND(AVG(p.discount_percentage)::numeric, 2) AS avg_discount_pct,
    ROUND(AVG(p.rating)::numeric, 2) AS avg_rating,
    COUNT(DISTINCT p.product_id) AS product_count
FROM analytics.dim_product p
JOIN analytics.bridge_product_category b
  ON b.product_id = p.product_id
JOIN analytics.dim_category c
  ON c.category_id = b.category_id
WHERE c.category_level = 1
GROUP BY c.category_name
HAVING COUNT(DISTINCT p.product_id) >= 5
ORDER BY avg_discount_pct DESC, avg_rating DESC;

