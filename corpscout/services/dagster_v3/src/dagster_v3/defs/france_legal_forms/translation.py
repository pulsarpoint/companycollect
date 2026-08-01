"""French legal forms in English: curated first, then the machine for the tail.

Two assets, deliberately split.

`france_legal_forms_curated_english` writes the 50 hand-reviewed terms. SARL,
SAS, SCI and SCOP are terms of art whose English wording should not drift, and
that work must not be blocked by whether the translator service happens to be
reachable. It runs first, so the machine scan's anti-join sees those labels as
already translated and never proposes an alternative for them.

`france_legal_forms_translation_load` sends what is left -- 259 of 309 codes --
to the translator. That is a change of kind, not just of scale: before the
INSEE nomenclature existed there was nothing to translate but a four-digit
number, and guessing at what `6540` means is exactly the failure this codebase
avoids. Translating "Societe civile immobiliere" from INSEE's own French is an
ordinary translation job.

Curation still outranks the machine permanently, whichever ran last: the
fr_legal_forms_translated view picks argMax over (provider = 'static',
version), so a later machine run cannot overwrite a reviewed term.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.france_sirene.resources import FR_LEGAL_FORM_EN_BY_CODE
from dagster_v3.defs.translator_load.coverage import translation_coverage_result
from dagster_v3.defs.translator_load.loader import (
    TranslationField,
    build_scan_sql,
    build_static_scan_sql,
    insert_static_translations,
)
from dagster_v3.defs.translator_load.resource import (
    TranslatorResource,
    translator_queue_health_check,
)

SOURCE_TABLE = "corpscout.fr_legal_forms"
SOURCE_COLUMN = "label_fr"
KEY_COLUMN = "code"
SOURCE_LANG = "fr"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "French"
TARGET_LANGUAGE_NAME = "English"


@dg.asset(
    deps=[dg.AssetKey("france_legal_forms_clickhouse")],
    group_name="france_legal_forms",
    kinds={"clickhouse"},
    description=(
        "Insert the hand-curated English for French legal forms straight into "
        "text_translations. No translator involved."
    ),
)
def france_legal_forms_curated_english(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        static_rows = client.execute(
            build_static_scan_sql(SOURCE_TABLE, SOURCE_COLUMN, KEY_COLUMN)
        )
        inserted = insert_static_translations(
            client,
            SOURCE_TABLE,
            SOURCE_COLUMN,
            SOURCE_LANG,
            TARGET_LANG,
            static_rows,
            FR_LEGAL_FORM_EN_BY_CODE,
        )
    context.log.info("FR: %d curated legal forms inserted", inserted)
    return dg.MaterializeResult(
        metadata={"inserted": inserted, "curated_terms": len(FR_LEGAL_FORM_EN_BY_CODE)}
    )


@dg.asset(
    deps=[dg.AssetKey("france_legal_forms_curated_english")],
    group_name="france_legal_forms",
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.fr_legal_forms for legal-form labels the curated map "
        "does not cover, enqueue them to the translator, and wait."
    ),
)
def france_legal_forms_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed

    with clickhouse.get_connection() as client:
        # The anti-join inside build_scan_sql is what keeps the curated terms
        # untouched: they already carry a row, so they are not rescanned.
        untranslated_rows = client.execute(build_scan_sql(SOURCE_TABLE, SOURCE_COLUMN))
        context.log.info(
            "scanned %d untranslated labels for %s.%s",
            len(untranslated_rows),
            SOURCE_TABLE,
            SOURCE_COLUMN,
        )
        enqueue_result = translator.enqueue_translation_rows(
            source_table=SOURCE_TABLE,
            source_column=SOURCE_COLUMN,
            source_lang=SOURCE_LANG,
            target_lang=TARGET_LANG,
            source_language_name=SOURCE_LANGUAGE_NAME,
            target_language_name=TARGET_LANGUAGE_NAME,
            rows=untranslated_rows,
        )
        for warning in enqueue_result.workflow_start_warnings:
            context.log.warning("translator workflow start warning: %s", warning)

    if enqueue_result.workflow_start_warnings:
        raise dg.Failure(
            description="translator accepted rows but failed to start its workflow",
            metadata={
                "warning_count": len(enqueue_result.workflow_start_warnings),
                "warnings": dg.MetadataValue.json(
                    enqueue_result.workflow_start_warnings
                ),
            },
        )

    if enqueue_result.received > 0:
        completion_stats = translator.wait_for_queue_completion(
            baseline_failed=baseline_failed
        )
        context.log.info(
            "translator queue completed: input=%d pending=%d output=%d failed=%d",
            completion_stats.input,
            completion_stats.pending,
            completion_stats.output,
            completion_stats.failed,
        )

    return dg.MaterializeResult(
        metadata={
            "enqueued_received": enqueue_result.received,
            "enqueued_inserted": enqueue_result.inserted,
        }
    )


@dg.asset_check(
    asset=france_legal_forms_translation_load, name="translator_queue_healthy"
)
def france_legal_forms_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)


@dg.asset_check(
    asset=france_legal_forms_translation_load, name="translations_present"
)
def france_legal_forms_translation_coverage(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """How many INSEE labels exist, and how many are translated."""
    return translation_coverage_result(
        clickhouse, (TranslationField(SOURCE_TABLE, SOURCE_COLUMN),)
    )
