from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from translator.registry import FieldConfig, SourceConfig


def clickhouse_client_from_env() -> Any:
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DATABASE"],
        secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() in {"1", "true", "yes"},
    )


@dataclass(frozen=True)
class ScannedTerm:
    """A single untranslated term discovered during a scan.

    ``static_key`` is None for dynamic fields; for static fields it holds the
    companion key-column value (e.g. ``legal_form_code``).
    """

    source_column: str
    source_text: str
    static_key: str | None


def build_scan_sql(source_config: SourceConfig, field: FieldConfig) -> str:
    original = field.original_col
    if field.static_key_col:
        select_cols = f"c.{original} AS source_text, c.{field.static_key_col} AS static_key"
    else:
        select_cols = f"c.{original} AS source_text"
    # LEFT ANTI JOIN returns rows of `c` that have NO match in the cache subquery —
    # the correct ClickHouse anti-join. Do NOT use `LEFT JOIN ... WHERE t.hash IS NULL`:
    # with join_use_nulls=0 (the default) unmatched rows get hash=0, not NULL, so the
    # IS NULL filter matches nothing and the scan returns 0 untranslated terms.
    return (
        f"SELECT DISTINCT {select_cols}\n"
        f"FROM {source_config.ch_table} AS c\n"
        f"LEFT ANTI JOIN (\n"
        f"    SELECT source_text_hash\n"
        f"    FROM corpscout.text_translations\n"
        f"    WHERE source_table = {{table:String}} AND source_column = {{column:String}}\n"
        f"    GROUP BY source_text_hash\n"
        f") AS t ON t.source_text_hash = cityHash64(c.{original})\n"
        f"WHERE c.{original} <> ''"
    )


def scan_untranslated_terms(client: Any, source_config: SourceConfig) -> list[ScannedTerm]:
    terms: list[ScannedTerm] = []
    for field in source_config.fields:
        result = client.query(
            build_scan_sql(source_config, field),
            parameters={"table": source_config.ch_table, "column": field.original_col},
        )
        for row in result.result_rows:
            source_text = row[0]
            static_key = row[1] if field.static_key_col else None
            terms.append(
                ScannedTerm(source_column=field.original_col, source_text=source_text, static_key=static_key)
            )
    return terms


def query_arrow(client: Any, sql: str, parameters: dict[str, Any] | None = None) -> Any:
    """Execute a ClickHouse query and return the result as a PyArrow Table.

    Wraps ``client.query_arrow()`` (clickhouse-connect) with a default empty
    parameters dict so callers never need to special-case None.
    """
    return client.query_arrow(sql, parameters=parameters or {})
