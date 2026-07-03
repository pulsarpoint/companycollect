"""Shared loader helpers: anti-join scans + bulk enqueue to the Go translator.

Loader contract (the system's dedup economics live here):
- Scan ONLY distinct texts not yet in corpscout.text_translations (anti-join).
- Compute cityHash64 in ClickHouse SQL — never in Python — so hashes always
  agree with past runs.
- POST chunks of at most 10k items; hashes are decimal STRINGS (uint64 does
  not fit JSON numbers).
- Static-map columns never touch the LLM: insert straight into
  text_translations with provider='static'.
Table/column values are interpolated into SQL — loader configs are trusted,
developer-authored code.
"""

import time
from dataclasses import dataclass

MAX_ITEMS_PER_REQUEST = 10_000


@dataclass(frozen=True)
class LoaderField:
    table: str
    column: str


@dataclass(frozen=True)
class LoaderSource:
    source_lang: str
    target_lang: str
    source_language_name: str
    target_language_name: str
    fields: tuple[LoaderField, ...]


def build_scan_sql(table: str, column: str) -> str:
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
WHERE c.{column} <> ''"""


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
WHERE c.{column} <> ''"""


def enqueue_items(session, api_url, source, field, rows, chunk_size=MAX_ITEMS_PER_REQUEST):
    """POST (text, hash) rows in chunks; returns summed {'received','inserted'}."""
    totals = {"received": 0, "inserted": 0}
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        payload = {
            "source_lang": source.source_lang,
            "target_lang": source.target_lang,
            "source_language_name": source.source_language_name,
            "target_language_name": source.target_language_name,
            "items": [
                {
                    "source_table": field.table,
                    "source_column": field.column,
                    "source_text": text,
                    "source_text_hash": str(hash_),
                }
                for text, hash_ in chunk
            ],
        }
        response = session.post(f"{api_url}/v1/queue/items", json=payload, timeout=60)
        response.raise_for_status()
        body = response.json()
        totals["received"] += body["received"]
        totals["inserted"] += body["inserted"]
    return totals


def insert_static_translations(client, table, column, source_lang, target_lang, rows, mapping):
    """Insert map-translated rows straight into text_translations; unknown keys skipped."""
    version = int(time.time())
    values = []
    for text, hash_, key in rows:
        translated = mapping.get(key, "")
        if not text or not translated:
            continue
        values.append(
            (table, column, hash_, source_lang, target_lang, translated, "static", "static", version)
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
