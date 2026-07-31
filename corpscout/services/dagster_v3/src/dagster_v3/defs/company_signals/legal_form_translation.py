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

Curated where a hand-written map exists (see legal_form_english), machine
translated otherwise. The curated rows go in first, so the machine scan's
anti-join then treats them as already done and never overwrites a reviewed
term with a plausible-sounding one.

Small and bounded: 211 distinct labels across four countries, each paid for
once. For comparison, PNCP's contract objects are 57,229.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.company_signals.legal_form_english import english_by_code
from dagster_v3.defs.translator_load.loader import (
    build_scan_sql,
    build_static_scan_sql,
    insert_static_translations,
)
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


@dg.asset(
    deps=[dg.AssetKey("company_entity_types_clickhouse")],
    group_name="company_signals",
    kinds={"clickhouse"},
    description=(
        "Insert the hand-curated English for legal forms, straight into "
        "text_translations. No translator involved."
    ),
)
def company_entity_types_curated_english(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Curated terms, written without asking the translator anything.

    Separate from the machine loader on purpose. A legal form is a term of art
    and there are only a couple of hundred, so they are mapped by hand and
    reviewed in a diff — and that work must not be blocked by whether a
    translation service happens to be reachable. It also runs FIRST in
    practice: the machine scan's anti-join then sees these as already
    translated and never overwrites a reviewed term with a plausible one.
    """
    inserted_by_country: dict[str, object] = {}
    total = 0

    with clickhouse.get_connection() as client:
        for country_code, source_lang, _ in COUNTRY_LANGUAGES:
            labels = {
                str(code): str(label)
                for code, label in client.execute(
                    f"SELECT legal_form_code, any(source_label) FROM {SOURCE_TABLE} "
                    f"WHERE country_code = '{country_code}' GROUP BY legal_form_code"
                )
            }
            curated = english_by_code(country_code, labels)
            if not curated:
                continue
            static_rows = client.execute(
                build_static_scan_sql(
                    SOURCE_TABLE,
                    SOURCE_COLUMN,
                    "legal_form_code",
                    extra_where=f"country_code = '{country_code}'",
                )
            )
            inserted = insert_static_translations(
                client,
                SOURCE_TABLE,
                SOURCE_COLUMN,
                source_lang,
                TARGET_LANG,
                static_rows,
                curated,
            )
            inserted_by_country[f"{country_code}_inserted"] = inserted
            total += inserted
            context.log.info("%s: %d curated legal forms inserted", country_code, inserted)

    return dg.MaterializeResult(metadata={"inserted": total, **inserted_by_country})
