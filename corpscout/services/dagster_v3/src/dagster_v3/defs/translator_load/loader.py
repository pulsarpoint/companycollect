"""ClickHouse scan and static-insert helpers for translation loader assets.

Loader contract (the system's dedup economics live here):
- Scan ONLY distinct texts not yet in corpscout.text_translations (anti-join).
- Never enqueue whitespace-only texts: the model correctly returns an empty
  translation for them, which the translator records as a PERMANENT failed
  queue item (2026-07-20: 12 whitespace-only Latvian texts poisoned the
  global failed count and failed every later source's loader).
- Never enqueue texts over 8,000 chars: the translator batches 50 texts
  into one LLM prompt, and a single malformed multi-megabyte blob makes
  the head-of-queue batch exceed the model context forever -- the batch
  retries identically and the whole queue stalls (2026-07-21: 102 packed
  se_companies blobs, max 1.8M chars, froze 1.9M pending texts; genuine
  descriptions are ~1k chars at p99.9).
- Compute cityHash64 in ClickHouse SQL — never in Python — so hashes always
  agree with past runs.
- Static-map columns never touch the LLM: insert straight into
  text_translations with provider='static'.
Table/column values are interpolated into SQL — loader configs are trusted,
developer-authored code.
"""

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

type StaticTranslationRows = Sequence[tuple[str, int, str]]


@dataclass(frozen=True)
class TranslationField:
    table: str
    column: str


def build_scan_sql(table: str, column: str, extra_where: str | None = None) -> str:
    """Untranslated texts for one column.

    `extra_where` scopes the scan, for a table whose rows are not all in one
    language: company_entity_types holds Swedish, Norwegian, Finnish and
    Portuguese labels side by side, and each must be enqueued with its own
    source language or the translator is told Swedish is Portuguese.
    """
    scope = "" if extra_where is None else f"\n  AND ({extra_where})"
    return f"""
SELECT DISTINCT
    c.{column} AS source_text,
    cityHash64(c.{column}) AS source_text_hash
FROM {table} AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{table}' AND source_column = '{column}'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.{column})
WHERE trim(BOTH ' \\t\\r\\n' FROM c.{column}) != ''
  AND length(c.{column}) <= 8000{scope}"""


def build_static_scan_sql(table: str, column: str, key_column: str) -> str:
    return f"""
SELECT DISTINCT
    c.{column} AS source_text,
    cityHash64(c.{column}) AS source_text_hash,
    c.{key_column} AS {key_column}
FROM {table} AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{table}' AND source_column = '{column}'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.{column})
WHERE trim(BOTH ' \\t\\r\\n' FROM c.{column}) != ''
  AND length(c.{column}) <= 8000{scope}"""


def insert_static_translations(
    client: Any,
    table: str,
    column: str,
    source_lang: str,
    target_lang: str,
    rows: StaticTranslationRows,
    mapping: Mapping[str, str],
) -> int:
    """Insert map-translated rows straight into text_translations; unknown keys skipped."""
    version = int(time.time())
    values = []
    for text, hash_, key in rows:
        translated = mapping.get(key, "")
        if not text or not translated:
            continue
        values.append(
            (
                table,
                column,
                hash_,
                source_lang,
                target_lang,
                translated,
                "static",
                "static",
                version,
            )
        )
    if not values:
        return 0
    client.execute(
        """
        INSERT INTO corpscout.text_translations (
            source_table, source_column, source_text_hash,
            source_lang, target_lang, translated_text, provider, model, version
        ) VALUES
        """,
        values,
    )
    return len(values)
