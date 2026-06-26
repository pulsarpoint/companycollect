from __future__ import annotations

import os
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


def build_scan_sql(source_config: SourceConfig, field: FieldConfig) -> str:
    original = field.original_col
    return (
        f"SELECT DISTINCT c.{original} AS source_text\n"
        f"FROM {source_config.ch_table} AS c\n"
        f"LEFT JOIN (\n"
        f"    SELECT source_text_hash\n"
        f"    FROM corpscout.text_translations\n"
        f"    WHERE source_slug = {{slug:String}} AND field = {{field:String}}\n"
        f"    GROUP BY source_text_hash\n"
        f") AS t ON t.source_text_hash = cityHash64(c.{original})\n"
        f"WHERE c.{original} <> '' AND t.source_text_hash IS NULL"
    )


def scan_untranslated_terms(client: Any, source_config: SourceConfig) -> list[tuple[str, str]]:
    terms: list[tuple[str, str]] = []
    for field in source_config.fields:
        result = client.query(
            build_scan_sql(source_config, field),
            parameters={"slug": source_config.source_slug, "field": field.field},
        )
        for row in result.result_rows:
            terms.append((field.field, row[0]))
    return terms
