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

    For dynamic fields ``static_key`` is None.  For static fields it holds the
    value of the companion key column (e.g. ``legal_form_code``) used to look
    up the translation in the field's static map.
    """

    field: str
    source_text: str
    static_key: str | None


def build_scan_sql(source_config: SourceConfig, field: FieldConfig) -> str:
    original = field.original_col
    if field.static_key_col:
        select_cols = (
            f"c.{original} AS source_text, c.{field.static_key_col} AS static_key"
        )
    else:
        select_cols = f"c.{original} AS source_text"
    return (
        f"SELECT DISTINCT {select_cols}\n"
        f"FROM {source_config.ch_table} AS c\n"
        f"LEFT JOIN (\n"
        f"    SELECT source_text_hash\n"
        f"    FROM corpscout.text_translations\n"
        f"    WHERE source_slug = {{slug:String}} AND field = {{field:String}}\n"
        f"    GROUP BY source_text_hash\n"
        f") AS t ON t.source_text_hash = cityHash64(c.{original})\n"
        f"WHERE c.{original} <> '' AND t.source_text_hash IS NULL"
    )


def scan_untranslated_terms(client: Any, source_config: SourceConfig) -> list[ScannedTerm]:
    terms: list[ScannedTerm] = []
    for field in source_config.fields:
        result = client.query(
            build_scan_sql(source_config, field),
            parameters={"slug": source_config.source_slug, "field": field.field},
        )
        for row in result.result_rows:
            source_text = row[0]
            static_key = row[1] if field.static_key_col else None
            terms.append(ScannedTerm(field=field.field, source_text=source_text, static_key=static_key))
    return terms
