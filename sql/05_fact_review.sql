-- Rebuild review fact table by exploding comma-separated list columns.
-- Note: review_content contains commas in natural language text, so review_content_item
-- may be noisier than other review fields. We keep review_content_raw for traceability.
DROP TABLE IF EXISTS analytics.fact_review;

CREATE TABLE analytics.fact_review AS
WITH base AS (
    SELECT
        ROW_NUMBER() OVER () AS source_row_id,
        product_id,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content
    FROM analytics.stg_amazon_products
    WHERE product_id IS NOT NULL
      AND TRIM(product_id) <> ''
),
review_ids AS (
    SELECT
        b.source_row_id,
        b.product_id,
        r.ordinality::INT AS review_ordinal,
        NULLIF(TRIM(r.review_id_item), '') AS review_id
    FROM base b
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(b.review_id, ''), ',')) WITH ORDINALITY AS r(review_id_item, ordinality)
),
user_ids AS (
    SELECT
        b.source_row_id,
        u.ordinality::INT AS review_ordinal,
        NULLIF(TRIM(u.user_id_item), '') AS user_id
    FROM base b
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(b.user_id, ''), ',')) WITH ORDINALITY AS u(user_id_item, ordinality)
),
user_names AS (
    SELECT
        b.source_row_id,
        un.ordinality::INT AS review_ordinal,
        NULLIF(TRIM(un.user_name_item), '') AS user_name
    FROM base b
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(b.user_name, ''), ',')) WITH ORDINALITY AS un(user_name_item, ordinality)
),
review_titles AS (
    SELECT
        b.source_row_id,
        rt.ordinality::INT AS review_ordinal,
        NULLIF(TRIM(rt.review_title_item), '') AS review_title
    FROM base b
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(b.review_title, ''), ',')) WITH ORDINALITY AS rt(review_title_item, ordinality)
),
review_content_items AS (
    SELECT
        b.source_row_id,
        rc.ordinality::INT AS review_ordinal,
        NULLIF(TRIM(rc.review_content_item), '') AS review_content_item
    FROM base b
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(b.review_content, ''), ',')) WITH ORDINALITY AS rc(review_content_item, ordinality)
)
SELECT
    ROW_NUMBER() OVER (ORDER BY r.source_row_id, r.review_ordinal) AS review_sk,
    r.product_id,
    r.review_ordinal,
    r.review_id,
    ui.user_id,
    un.user_name,
    rt.review_title,
    rci.review_content_item,
    b.review_content AS review_content_raw
FROM review_ids r
JOIN base b
  ON b.source_row_id = r.source_row_id
LEFT JOIN user_ids ui
  ON ui.source_row_id = r.source_row_id
 AND ui.review_ordinal = r.review_ordinal
LEFT JOIN user_names un
  ON un.source_row_id = r.source_row_id
 AND un.review_ordinal = r.review_ordinal
LEFT JOIN review_titles rt
  ON rt.source_row_id = r.source_row_id
 AND rt.review_ordinal = r.review_ordinal
LEFT JOIN review_content_items rci
  ON rci.source_row_id = r.source_row_id
 AND rci.review_ordinal = r.review_ordinal
WHERE r.review_id IS NOT NULL;

ALTER TABLE analytics.fact_review
    ADD CONSTRAINT fact_review_pk PRIMARY KEY (review_sk);

ALTER TABLE analytics.fact_review
    ADD CONSTRAINT fact_review_product_fk
    FOREIGN KEY (product_id) REFERENCES analytics.dim_product (product_id);

CREATE INDEX fact_review_product_id_ix
    ON analytics.fact_review (product_id);

CREATE INDEX fact_review_review_id_ix
    ON analytics.fact_review (review_id);

