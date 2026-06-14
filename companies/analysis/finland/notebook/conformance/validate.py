"""Assert a canonical DataFrame matches a schema from schemas.py."""

from __future__ import annotations

import polars as pl


def validate_table(df: pl.DataFrame, schema: dict, *, unique_key: str | None = None) -> None:
    required = set(schema)
    present = set(df.columns)
    missing = required - present
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")
    for name, dtype in schema.items():
        if df.schema[name] != dtype:
            raise ValueError(f"column {name!r} has dtype {df.schema[name]}, expected {dtype}")
    if unique_key is not None:
        non_null = df.filter(pl.col(unique_key).is_not_null())
        if non_null.height != non_null.select(unique_key).n_unique():
            raise ValueError(f"duplicate values in key column {unique_key!r}")
