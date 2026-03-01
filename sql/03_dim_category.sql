-- Rebuild category dimension by splitting the category path into levels.
DROP TABLE IF EXISTS analytics.dim_category CASCADE;

CREATE TABLE analytics.dim_category AS
WITH exploded AS (
    SELECT DISTINCT
        TRIM(category_item) AS category_name,
        category_level::INT AS category_level
    FROM analytics.stg_amazon_products s
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(s.category, '|')) WITH ORDINALITY AS c(category_item, category_level)
    WHERE s.category IS NOT NULL
      AND TRIM(s.category) <> ''
      AND TRIM(category_item) <> ''
)
SELECT
    ROW_NUMBER() OVER (ORDER BY category_level, category_name) AS category_id,
    category_name,
    category_level
FROM exploded;

ALTER TABLE analytics.dim_category
    ADD CONSTRAINT dim_category_pk PRIMARY KEY (category_id);

CREATE UNIQUE INDEX dim_category_level_name_ux
    ON analytics.dim_category (category_level, category_name);
