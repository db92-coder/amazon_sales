from __future__ import annotations

from pathlib import Path

import pandas as pd


def extract_csv(path: Path) -> pd.DataFrame:
    """Read raw CSV into a dataframe with text columns preserved."""
    # Fail fast with a clear error if the file path is wrong.
    if not path.exists():
        raise FileNotFoundError(f"Source CSV not found: {path}")
    # Read as string first; we handle type casting explicitly in transform step.
    return pd.read_csv(path, dtype=str)
