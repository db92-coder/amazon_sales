SELECT COUNT(*) AS row_count
FROM analytics.stg_amazon_products;

SELECT
    COUNT(*) AS null_product_id
FROM analytics.stg_amazon_products
WHERE product_id IS NULL OR TRIM(product_id) = '';

SELECT
    MIN(rating) AS min_rating,
    MAX(rating) AS max_rating
FROM analytics.stg_amazon_products;

SELECT
    COUNT(*) AS invalid_discount_rows
FROM analytics.stg_amazon_products
WHERE discounted_price IS NOT NULL
  AND actual_price IS NOT NULL
  AND discounted_price > actual_price;

