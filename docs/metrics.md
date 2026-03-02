# Metrics Dictionary

## Scope
- Data source: `analytics.stg_amazon_products`
- Normalized model: `dim_product`, `dim_category`, `bridge_product_category`, `fact_review`
- Business marts: `mart_product_performance`, `mart_category_performance`, `mart_review_quality`

## KPI Definitions

### Product Count
- Definition: Number of distinct products.
- Formula: `COUNT(DISTINCT product_id)`
- Grain:
- Product mart: one row per product (`1`)
- Category mart: products grouped by `top_category`

### Average Discount %
- Definition: Mean percentage discount across products in scope.
- Formula: `AVG(discount_percentage)`
- Grain:
- Product mart: product-level attribute
- Category mart: grouped by `top_category`

### Average Rating
- Definition: Mean product rating across products in scope.
- Formula: `AVG(rating)`
- Grain:
- Product mart: product-level attribute
- Category mart: grouped by `top_category`

### Review Count
- Definition: Total number of review fact rows.
- Formula: `COUNT(review_sk)` or `SUM(review_count)` depending on table
- Grain:
- Product mart: reviews per product
- Category mart: summed reviews for products in category

### Review Text Coverage Ratio
- Definition: Share of reviews with non-empty review text tokens.
- Formula: `AVG(CASE WHEN review_content_item IS NOT NULL AND TRIM(review_content_item) <> '' THEN 1 ELSE 0 END)`
- Grain:
- Product mart: per product ratio
- Category marts: average/summarized by `top_category`

### Distinct Review ID Count
- Definition: Number of unique `review_id` values.
- Formula: `COUNT(DISTINCT review_id)`
- Grain:
- Review quality mart: grouped by `top_category`

### Distinct User Count
- Definition: Number of unique `user_id` values.
- Formula: `COUNT(DISTINCT user_id)`
- Grain:
- Review quality mart: grouped by `top_category`

## Data Quality Notes
- `review_content` in source CSV is comma-heavy free text, so tokenized `review_content_item` is a proxy metric.
- For strict NLP quality metrics, use `review_content_raw` from `fact_review` and process with a text pipeline.

