-- Consolidated mart checks for SQLTools.
SELECT 'mart_product_performance_empty' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violation_count
FROM analytics.mart_product_performance
UNION ALL
SELECT 'mart_category_performance_empty' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violation_count
FROM analytics.mart_category_performance
UNION ALL
SELECT 'mart_review_quality_empty' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violation_count
FROM analytics.mart_review_quality
UNION ALL
SELECT 'mart_product_performance_null_product_id' AS check_name,
       COUNT(*) AS violation_count
FROM analytics.mart_product_performance
WHERE product_id IS NULL OR TRIM(product_id) = ''
UNION ALL
SELECT 'mart_category_performance_null_top_category' AS check_name,
       COUNT(*) AS violation_count
FROM analytics.mart_category_performance
WHERE top_category IS NULL OR TRIM(top_category) = ''
UNION ALL
SELECT 'mart_review_quality_null_top_category' AS check_name,
       COUNT(*) AS violation_count
FROM analytics.mart_review_quality
WHERE top_category IS NULL OR TRIM(top_category) = '';

