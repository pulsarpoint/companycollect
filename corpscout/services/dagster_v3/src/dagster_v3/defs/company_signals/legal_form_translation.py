"""Legal-form labels in English.

`company_entity_types.source_label` is the register's own term for a legal
form — Bankaktiebolag, Ideell förening, Enskild firma, Sociedade Empresária
Limitada — so a company list showed every country in its own language.

`entity_type_label` is already English and deliberately coarse: 21 of Sweden's
57 codes collapse to "Company", which would print Bankaktiebolag and
Försäkringsaktiebolag identically. It answers "what kind of thing is this",
which is the entity badge's question, not "what legal form is it".

So the label is translated. Unlike every other loader here, ONE table holds
several languages — Swedish, Norwegian, Finnish and Portuguese side by side —
so each country is scanned and enqueued separately with its own source
language. Telling the translator that Swedish is Portuguese would produce
confident nonsense rather than an error.

Small and bounded: 211 distinct labels across four countries, each paid for
once by the loader's anti-join. For comparison, PNCP's contract objects are
57,229.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.translator_load.loader import build_scan_sql
from dagster_v3.defs.translator_load.resource import (
    TranslatorResource,
    translator_queue_health_check,
)

SOURCE_TABLE = "corpscout.company_entity_types"
SOURCE_COLUMN = "source_label"
TARGET_LANG = "en"
TARGET_LANGUAGE_NAME = "English"

# The language each register writes its legal forms in. A country absent here
# is not translated rather than being guessed at — a wrong source language is
# worse than an untranslated label, because it still produces English.
COUNTRY_LANGUAGES: tuple[tuple[str, str, str], ...] = (
    ("SE", "sv", "Swedish"),
    ("NO", "no", "Norwegian"),
    ("FI", "fi", "Finnish"),
    ("BR", "pt", "Portuguese"),
)


@dg.asset(
    deps=[dg.AssetKey("company_entity_types_clickhouse")],
    group_name="company_signals",
    kinds={"python", "clickhouse"},
    description=(
        "Scan company_entity_types for untranslated legal-form labels, one "
        "language per country, and enqueue them to the translator service."
    ),
)
def company_entity_types_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed
    scanned = 0
    enqueued_received = 0
    enqueued_inserted = 0
    warnings: list[str] = []

    with clickhouse.get_connection() as client:
        for country_code, source_lang, source_language_name in COUNTRY_LANGUAGES:
            rows = client.execute(
                build_scan_sql(
                    SOURCE_TABLE,
                    SOURCE_COLUMN,
                    extra_where=f"country_code = '{country_code}'",
                )
            )
            scanned += len(rows)
            context.log.info(
                "%s: %d untranslated legal-form labels (%s)",
                country_code,
                len(rows),
                source_language_name,
            )
            if not rows:
                continue
            result = translator.enqueue_translation_rows(
                source_table=SOURCE_TABLE,
                source_column=SOURCE_COLUMN,
                source_lang=source_lang,
                target_lang=TARGET_LANG,
                source_language_name=source_language_name,
                target_language_name=TARGET_LANGUAGE_NAME,
                rows=rows,
            )
            enqueued_received += result.received
            enqueued_inserted += result.inserted
            warnings.extend(result.workflow_start_warnings)

    if warnings:
        raise dg.Failure(
            description="translator accepted rows but failed to start its workflow",
            metadata={
                "warning_count": len(warnings),
                "warnings": dg.MetadataValue.json(warnings),
            },
        )

    if enqueued_received > 0:
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
            "scanned": scanned,
            "enqueued_received": enqueued_received,
            "enqueued_inserted": enqueued_inserted,
        }
    )


@dg.asset_check(
    asset=company_entity_types_translation_load, name="translator_queue_healthy"
)
def company_entity_types_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)
