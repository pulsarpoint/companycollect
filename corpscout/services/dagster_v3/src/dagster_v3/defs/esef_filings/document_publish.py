"""Atomic ClickHouse publish for source-preserving ESEF document products.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.translator_load.resource import TranslatorResource

GROUP_NAME = "esef"

_ESEF_CONCEPT_LABEL_SOURCE_TABLE = tables.QUALIFIED_ESEF_DOCUMENT_CONCEPT_LABELS_TABLE
_ESEF_LANGUAGE_NAMES = {
    "bg": "Bulgarian",
    "cs": "Czech",
    "da": "Danish",
    "de": "German",
    "el": "Greek",
    "es": "Spanish",
    "et": "Estonian",
    "fi": "Finnish",
    "fr": "French",
    "ga": "Irish",
    "hr": "Croatian",
    "hu": "Hungarian",
    "is": "Icelandic",
    "it": "Italian",
    "lt": "Lithuanian",
    "lv": "Latvian",
    "mt": "Maltese",
    "nl": "Dutch",
    "no": "Norwegian",
    "pl": "Polish",
    "pt": "Portuguese",
    "ro": "Romanian",
    "sk": "Slovak",
    "sl": "Slovenian",
    "sv": "Swedish",
}


def _esef_official_translation_insert_sql() -> str:
    """Publish report-language→English taxonomy pairs into text_translations."""
    return f"""
INSERT INTO corpscout.text_translations (
    source_table,
    source_column,
    source_text_hash,
    source_lang,
    target_lang,
    translated_text,
    provider,
    model,
    version
)
WITH candidates AS (
    SELECT
        cityHash64(source.label) AS source_text_hash,
        source.language AS source_lang,
        'en' AS target_lang,
        argMax(
            target.label,
            tuple(
                target.language = 'en',
                target.resolved_at,
                target.source_document_id,
                target.concept_qname,
                target.label_role,
                target.label_id
            )
        ) AS translated_text,
        toUInt64(toUnixTimestamp64Milli(max(target.resolved_at))) AS version
    FROM {_ESEF_CONCEPT_LABEL_SOURCE_TABLE} AS source
    INNER JOIN {_ESEF_CONCEPT_LABEL_SOURCE_TABLE} AS target
        ON target.source_document_id = source.source_document_id
       AND target.concept_qname = source.concept_qname
       AND target.label_role = source.label_role
       AND (
           target.language = 'en'
           OR startsWith(target.language, 'en-')
       )
    WHERE source.is_report_language
      AND source.language != 'en'
      AND NOT startsWith(source.language, 'en-')
      AND source.label != ''
      AND target.label != ''
    GROUP BY source_text_hash, source_lang
),
current AS (
    SELECT
        source_text_hash,
        source_lang,
        argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = '{_ESEF_CONCEPT_LABEL_SOURCE_TABLE}'
      AND source_column = 'label'
      AND target_lang = 'en'
    GROUP BY source_text_hash, source_lang
)
SELECT
    '{_ESEF_CONCEPT_LABEL_SOURCE_TABLE}',
    'label',
    candidates.source_text_hash,
    candidates.source_lang,
    candidates.target_lang,
    candidates.translated_text,
    'taxonomy',
    'esef-official-taxonomy',
    candidates.version
FROM candidates
LEFT JOIN current
    ON current.source_text_hash = candidates.source_text_hash
   AND current.source_lang = candidates.source_lang
WHERE current.source_text_hash = 0
   OR current.translated_text != candidates.translated_text
"""


def _esef_missing_concept_translation_scan_sql() -> str:
    """Return report-language labels without an English translation."""
    return f"""
SELECT DISTINCT
    source.language AS source_lang,
    source.label AS source_text,
    cityHash64(source.label) AS source_text_hash
FROM {_ESEF_CONCEPT_LABEL_SOURCE_TABLE} AS source
LEFT ANTI JOIN corpscout.text_translations AS translation
    ON translation.source_table = '{_ESEF_CONCEPT_LABEL_SOURCE_TABLE}'
   AND translation.source_column = 'label'
   AND source.language = translation.source_lang
   AND translation.target_lang = 'en'
   AND translation.source_text_hash = cityHash64(source.label)
WHERE source.is_report_language
  AND source.language != ''
  AND source.language != 'en'
  AND NOT startsWith(source.language, 'en-')
  AND trim(BOTH ' \\t\\r\\n' FROM source.label) != ''
  AND length(source.label) <= 8000
ORDER BY source_lang, source_text_hash
"""


def _esef_concept_translation_coverage_sql() -> str:
    return f"""
WITH source AS (
    SELECT DISTINCT
        language AS source_lang,
        cityHash64(label) AS source_text_hash
    FROM {_ESEF_CONCEPT_LABEL_SOURCE_TABLE}
    WHERE is_report_language
      AND language != ''
      AND language != 'en'
      AND NOT startsWith(language, 'en-')
      AND trim(BOTH ' \\t\\r\\n' FROM label) != ''
      AND length(label) <= 8000
), translated AS (
    SELECT DISTINCT source_lang, source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{_ESEF_CONCEPT_LABEL_SOURCE_TABLE}'
      AND source_column = 'label'
      AND target_lang = 'en'
)
SELECT
    count() AS source_texts,
    countIf(
        (source_lang, source_text_hash) IN (
            SELECT source_lang, source_text_hash FROM translated
        )
    ) AS translated_texts
FROM source
"""


def _esef_language_name(language: str) -> str | None:
    return _ESEF_LANGUAGE_NAMES.get(language.lower().split("-", maxsplit=1)[0])


@dg.asset(
    name="esef_document_concept_official_translations_clickhouse",
    deps=[
        dg.AssetDep(
            dg.AssetKey("esef_document_concept_labels_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        )
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "xbrl", "taxonomy"},
    description=(
        "Copies authoritative report-language-to-English ESEF taxonomy label "
        "pairs into text_translations. Other official languages remain in the "
        "document-scoped concept-label evidence table."
    ),
)
def esef_document_concept_official_translations_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        client.execute(_esef_official_translation_insert_sql())
        [(official_pair_count,)] = client.execute(
            f"""
SELECT count()
FROM corpscout.text_translations
WHERE source_table = '{_ESEF_CONCEPT_LABEL_SOURCE_TABLE}'
  AND source_column = 'label'
  AND target_lang = 'en'
  AND provider = 'taxonomy'
"""
        )
    return dg.MaterializeResult(
        metadata={"official_translation_pair_count": int(official_pair_count)}
    )


@dg.asset(
    deps=[esef_document_concept_official_translations_clickhouse],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "llm", "xbrl", "taxonomy"},
    description=(
        "Translates report-language ESEF concept labels only when the source "
        "taxonomy has supplied no English equivalent. Translations remain in "
        "the shared cache and never modify source taxonomy evidence."
    ),
)
def esef_document_concept_translation_load(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed
    with clickhouse.get_connection() as client:
        untranslated_rows = client.execute(_esef_missing_concept_translation_scan_sql())

    rows_by_language: dict[str, list[tuple[str, int]]] = {}
    unsupported_languages: set[str] = set()
    skipped_unsupported_label_count = 0
    for source_lang, source_text, source_text_hash in untranslated_rows:
        language = str(source_lang)
        if _esef_language_name(language) is None:
            unsupported_languages.add(language)
            skipped_unsupported_label_count += 1
            continue
        rows_by_language.setdefault(language, []).append(
            (str(source_text), int(source_text_hash))
        )
    if unsupported_languages:
        context.log.warning(
            "skipping %d ESEF concept labels across %d unsupported source "
            "languages: %s",
            skipped_unsupported_label_count,
            len(unsupported_languages),
            ", ".join(sorted(unsupported_languages)),
        )

    received = 0
    inserted = 0
    warnings: list[str] = []
    for source_lang in sorted(rows_by_language):
        rows = rows_by_language[source_lang]
        language_name = _esef_language_name(source_lang)
        context.log.info(
            "%s: %d untranslated ESEF taxonomy labels",
            source_lang,
            len(rows),
        )
        result = translator.enqueue_translation_rows(
            source_table=_ESEF_CONCEPT_LABEL_SOURCE_TABLE,
            source_column="label",
            source_lang=source_lang,
            target_lang="en",
            source_language_name=language_name,
            target_language_name="English",
            rows=rows,
        )
        received += result.received
        inserted += result.inserted
        warnings.extend(result.workflow_start_warnings)

    if warnings:
        raise dg.Failure(
            description="translator accepted rows but failed to start its workflow",
            metadata={
                "warning_count": len(warnings),
                "warnings": dg.MetadataValue.json(warnings),
            },
        )
    if received > 0:
        stats = translator.wait_for_queue_completion(baseline_failed=baseline_failed)
        context.log.info(
            "translator queue completed: input=%d pending=%d output=%d failed=%d",
            stats.input,
            stats.pending,
            stats.output,
            stats.failed,
        )

    return dg.MaterializeResult(
        metadata={
            "scanned": len(untranslated_rows),
            "enqueued_received": received,
            "enqueued_inserted": inserted,
            "scanned_by_language": dg.MetadataValue.json(
                {
                    language: len(rows)
                    for language, rows in sorted(rows_by_language.items())
                }
            ),
            "skipped_unsupported_languages": dg.MetadataValue.json(
                sorted(unsupported_languages)
            ),
            "skipped_unsupported_label_count": skipped_unsupported_label_count,
        }
    )


@dg.asset_check(
    asset=esef_document_concept_translation_load,
    name="translations_present",
)
def esef_document_concept_translation_coverage(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        [(source_texts, translated_texts)] = client.execute(
            _esef_concept_translation_coverage_sql()
        )
    return dg.AssetCheckResult(
        passed=int(source_texts) == int(translated_texts),
        # A missing translation degrades a page, it does not break a
        # pipeline (see translator_load/coverage.py) -- must not block a
        # materialisation.
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "source_texts": int(source_texts),
            "translated_texts": int(translated_texts),
            "missing": int(source_texts) - int(translated_texts),
        },
    )


defs = dg.Definitions(
    assets=[
        esef_document_concept_official_translations_clickhouse,
        esef_document_concept_translation_load,
    ]
)
