from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

from openai import OpenAI
from sqlalchemy import create_engine, text

from src.etl.config import get_database_url


ALLOWED_TABLES = {
    "analytics.mart_product_performance",
    "analytics.mart_category_performance",
    "analytics.mart_review_quality",
}

FORBIDDEN_SQL_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "grant",
    "revoke",
    "create",
}


@dataclass
class AgentResult:
    question: str
    sql: str
    rows: list[dict[str, Any]]


class PortfolioAnalystAgent:
    """Read-only SQL analyst agent over curated mart tables."""

    def __init__(self, database_url: str | None = None, model: str = "gpt-4.1-mini") -> None:
        # Use project database config by default.
        self.database_url = database_url or get_database_url()
        self.model = model

    def _prompt(self, question: str) -> str:
        # Force agent output to be SQL only, scoped to approved marts.
        return f"""
You are a SQL analyst. Output only one PostgreSQL SELECT query and nothing else.
Allowed tables:
- analytics.mart_product_performance
- analytics.mart_category_performance
- analytics.mart_review_quality
Rules:
- Read-only SQL only (SELECT/WITH).
- No joins to unknown tables.
- Include LIMIT 200 unless the question asks for an aggregate only.
Question: {question}
"""

    def _generate_sql(self, question: str) -> str:
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is missing. Add it to your .env before running the agent.")

        client = OpenAI(api_key=api_key)
        response = client.responses.create(
            model=self.model,
            input=self._prompt(question),
            temperature=0,
        )
        sql = (response.output_text or "").strip()
        if not sql:
            raise RuntimeError("Agent returned empty SQL.")
        return sql

    def _validate_sql(self, sql: str) -> None:
        sql_lower = sql.lower().strip()
        # Require a read-only query style.
        if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
            raise ValueError("Only SELECT/WITH queries are allowed.")

        # Block mutating/DDL statements.
        for keyword in FORBIDDEN_SQL_KEYWORDS:
            if re.search(rf"\b{keyword}\b", sql_lower):
                raise ValueError(f"Forbidden SQL keyword detected: {keyword}")

        # If table references exist, require all to be from the approved mart list.
        referenced = set(
            m.group(1).lower()
            for m in re.finditer(
                r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)",
                sql,
                flags=re.IGNORECASE,
            )
        )
        allowed_lower = {name.lower() for name in ALLOWED_TABLES}
        if referenced and not referenced.issubset(allowed_lower):
            invalid = sorted(referenced - allowed_lower)
            raise ValueError(f"SQL references non-approved tables: {invalid}")

    def _run_sql(self, sql: str) -> list[dict[str, Any]]:
        # Execute SQL and return JSON-like row objects for easy UI rendering.
        engine = create_engine(self.database_url, pool_pre_ping=True)
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            rows = result.mappings().all()
        return [dict(row) for row in rows]

    def ask(self, question: str) -> AgentResult:
        sql = self._generate_sql(question)
        self._validate_sql(sql)
        rows = self._run_sql(sql)
        return AgentResult(question=question, sql=sql, rows=rows)


if __name__ == "__main__":
    # Simple CLI demo for quick manual testing.
    sample_question = "Top categories by product count with average rating."
    agent = PortfolioAnalystAgent()
    result = agent.ask(sample_question)
    print("Question:", result.question)
    print("SQL:", result.sql)
    print("Rows:", len(result.rows))
    for row in result.rows[:10]:
        print(row)

