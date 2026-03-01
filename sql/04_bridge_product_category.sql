-- Rebuild many-to-many bridge between products and category levels.
DROP TABLE IF EXISTS analytics.bridge_product_category;

CREATE TABLE analytics.bridge_product_category AS
WITH exploded AS (
    SELECT DISTINCT
        s.product_id,
        TRIM(category_item) AS category_name,
        category_level::INT AS category_level
    FROM analytics.stg_amazon_products s
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(s.category, '|')) WITH ORDINALITY AS c(category_item, category_level)
    WHERE s.product_id IS NOT NULL
      AND TRIM(s.product_id) <> ''
      AND s.category IS NOT NULL
      AND TRIM(s.category) <> ''
      AND TRIM(category_item) <> ''
)
SELECT
    e.product_id,
    dc.category_id
FROM exploded e
JOIN analytics.dim_category dc
  ON dc.category_level = e.category_level
 AND dc.category_name = e.category_name;

ALTER TABLE analytics.bridge_product_category
    ADD CONSTRAINT bridge_product_category_pk PRIMARY KEY (product_id, category_id);

ALTER TABLE analytics.bridge_product_category
    ADD CONSTRAINT bridge_product_category_product_fk
    FOREIGN KEY (product_id) REFERENCES analytics.dim_product (product_id);

ALTER TABLE analytics.bridge_product_category
    ADD CONSTRAINT bridge_product_category_category_fk
    FOREIGN KEY (category_id) REFERENCES analytics.dim_category (category_id);

