-- Rebuild product dimension from staging.
DROP TABLE IF EXISTS analytics.dim_product;

CREATE TABLE analytics.dim_product AS
SELECT DISTINCT ON (product_id)
    product_id,
    product_name,
    discounted_price,
    actual_price,
    discount_percentage,
    discount_amount,
    rating,
    rating_count,
    about_product,
    img_link,
    product_link
FROM analytics.stg_amazon_products
WHERE product_id IS NOT NULL
  AND TRIM(product_id) <> ''
ORDER BY product_id, rating_count DESC NULLS LAST;

ALTER TABLE analytics.dim_product
    ADD CONSTRAINT dim_product_pk PRIMARY KEY (product_id);

