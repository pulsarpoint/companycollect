from __future__ import annotations

from typing import Any

from translator.queue import FlushTranslationRow
from translator.registry import SourceConfig


def _staging_table_name(run_id: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in run_id)
    return f"corpscout.text_translations_stage_{safe}"


def build_flush_select_sql(staging_table: str) -> str:
    return (
        "INSERT INTO corpscout.text_translations\n"
        "    (source_slug, field, source_text_hash, source_lang, target_lang,\n"
        "     translated_text, provider, model, version)\n"
        "SELECT\n"
        "    {slug:String}, field, cityHash64(source_text), {lang:String}, 'en',\n"
        "    translated_text, {provider:String}, {model:String}, {version:UInt64}\n"
        f"FROM {staging_table}"
    )


def flush_translations(
    client: Any,
    source_config: SourceConfig,
    rows: list[FlushTranslationRow],
    *,
    provider: str,
    model: str,
    version: int,
    run_id: str,
) -> int:
    data = [
        [row.field, row.source_text, row.translated_text]
        for row in rows
        if row.translated_text != ""
    ]
    if not data:
        return 0

    staging = _staging_table_name(run_id)
    client.command(
        f"CREATE TABLE IF NOT EXISTS {staging} "
        "(field String, source_text String, translated_text String) ENGINE = Memory"
    )
    try:
        client.insert(staging, data, column_names=["field", "source_text", "translated_text"])
        client.command(
            build_flush_select_sql(staging),
            parameters={
                "slug": source_config.source_slug,
                "lang": source_config.source_lang,
                "provider": provider,
                "model": model,
                "version": version,
            },
        )
    finally:
        client.command(f"DROP TABLE IF EXISTS {staging}")
    return len(data)
