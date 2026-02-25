from __future__ import annotations

import pandas as pd


def _normalize_text(value: str | None) -> str | None:
    # Preserve nulls; trim whitespace around real text values.
    if value is None:
        return None
    return value.strip()


def _to_numeric_from_money(series: pd.Series) -> pd.Series:
    # Strip currency symbols (e.g., ₹ or broken encoding chars), then cast to numeric.
    cleaned = (
        series.fillna("")
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(r"[^0-9.]", "", regex=True)
    )
    cleaned = cleaned.replace("", pd.NA)
    return pd.to_numeric(cleaned, errors="coerce")


def _to_numeric_from_percent(series: pd.Series) -> pd.Series:
    # Remove "%" and any non-numeric noise, then cast to float.
    cleaned = (
        series.fillna("")
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(r"[^0-9.]", "", regex=True)
    )
    cleaned = cleaned.replace("", pd.NA)
    return pd.to_numeric(cleaned, errors="coerce")


def _to_numeric_from_count(series: pd.Series) -> pd.Series:
    # Remove separators and non-digit chars, then cast to integer-compatible numeric.
    cleaned = (
        series.fillna("")
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(r"[^0-9]", "", regex=True)
    )
    cleaned = cleaned.replace("", pd.NA)
    return pd.to_numeric(cleaned, errors="coerce")


def transform_amazon_data(df: pd.DataFrame) -> pd.DataFrame:
    # Work on a copy to avoid side effects on the raw dataframe.
    out = df.copy()

    # Text fields we want normalized for consistent downstream joins/filtering.
    text_columns = [
        "product_id",
        "product_name",
        "category",
        "about_product",
        "user_id",
        "user_name",
        "review_id",
        "review_title",
        "review_content",
        "img_link",
        "product_link",
    ]
    # Trim whitespace in each selected text column.
    for col in text_columns:
        if col in out.columns:
            out[col] = out[col].map(_normalize_text)

    # Convert raw string metrics into numeric types used for analytics.
    out["discounted_price"] = _to_numeric_from_money(out["discounted_price"])
    out["actual_price"] = _to_numeric_from_money(out["actual_price"])
    out["discount_percentage"] = _to_numeric_from_percent(out["discount_percentage"])
    out["rating"] = pd.to_numeric(out["rating"], errors="coerce")
    # Pandas nullable integer keeps NA support while enforcing integer semantics.
    out["rating_count"] = _to_numeric_from_count(out["rating_count"]).astype("Int64")

    # Derived measure used in dashboards and validation checks.
    out["discount_amount"] = out["actual_price"] - out["discounted_price"]

    return out


def run_quality_checks(df: pd.DataFrame) -> list[str]:
    # Collect all issues and return them together to simplify debugging.
    issues: list[str] = []

    # Primary key candidate should never be blank.
    if df["product_id"].isna().any() or (df["product_id"].str.len() == 0).any():
        issues.append("Found null/blank product_id.")

    # Ratings outside the expected Amazon range are suspicious.
    if (df["rating"].dropna().between(0, 5) == False).any():  # noqa: E712
        issues.append("Found ratings outside [0, 5].")

    # Price values should be non-negative.
    if (df["discounted_price"].dropna() < 0).any() or (df["actual_price"].dropna() < 0).any():
        issues.append("Found negative prices.")

    # Discounted price should not exceed actual/original price.
    invalid_discount = df["discounted_price"].notna() & df["actual_price"].notna() & (
        df["discounted_price"] > df["actual_price"]
    )
    if invalid_discount.any():
        issues.append("Found rows where discounted_price > actual_price.")

    return issues
