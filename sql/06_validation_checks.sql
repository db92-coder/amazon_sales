-- PK uniqueness checks.
SELECT 'dim_product_duplicate_product_id' AS check_name, COUNT(*) AS violation_count
FROM (
    SELECT product_id
    FROM analytics.dim_product
    GROUP BY product_id
    HAVING COUNT(*) > 1
) d
UNION ALL
SELECT 'dim_category_duplicate_category_id' AS check_name, COUNT(*) AS violation_count
FROM (
    SELECT category_id
    FROM analytics.dim_category
    GROUP BY category_id
    HAVING COUNT(*) > 1
) d
UNION ALL
SELECT 'bridge_duplicate_pairs' AS check_name, COUNT(*) AS violation_count
FROM (
    SELECT product_id, category_id
    FROM analytics.bridge_product_category
    GROUP BY product_id, category_id
    HAVING COUNT(*) > 1
) d
UNION ALL
SELECT 'fact_review_duplicate_review_sk' AS check_name, COUNT(*) AS violation_count
FROM (
    SELECT review_sk
    FROM analytics.fact_review
    GROUP BY review_sk
    HAVING COUNT(*) > 1
) d;

-- FK integrity checks.
SELECT 'bridge_orphan_product_id' AS check_name, COUNT(*) AS violation_count
FROM analytics.bridge_product_category b
LEFT JOIN analytics.dim_product p
  ON p.product_id = b.product_id
WHERE p.product_id IS NULL
UNION ALL
SELECT 'bridge_orphan_category_id' AS check_name, COUNT(*) AS violation_count
FROM analytics.bridge_product_category b
LEFT JOIN analytics.dim_category c
  ON c.category_id = b.category_id
WHERE c.category_id IS NULL
UNION ALL
SELECT 'fact_review_orphan_product_id' AS check_name, COUNT(*) AS violation_count
FROM analytics.fact_review f
LEFT JOIN analytics.dim_product p
  ON p.product_id = f.product_id
WHERE p.product_id IS NULL;

