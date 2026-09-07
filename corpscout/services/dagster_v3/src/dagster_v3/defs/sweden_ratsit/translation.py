"""Ratsit business descriptions -> the translator service (sv -> en).

``business_description`` is the Swedish verksamhetsbeskrivning Ratsit republishes. Most of
it is the same text Bolagsverket holds (65,080 of 66,910 distinct texts on 2026-09-07)
and migration 000390 seeded those under this table's key, so the scan enqueues only what
Ratsit alone has. Keyed on ``corpscout.se_ratsit_company`` and read back through
``se_ratsit_company_translated`` by the basic-info Ratsit extractor -- the same shape as
``sweden_company/translation.py`` and the other country loaders.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from dagster_v3.defs.translator_load.coverage import translation_coverage_result
from dagster_v3.defs.translator_load.loader import TranslationField, build_scan_sql
from dagster_v3.defs.translator_load.resource import (
    TranslatorResource,
    translator_queue_health_check,
)

GROUP_NAME = "sweden_ratsit"
SOURCE_LANG = "sv"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "Swedish"
TARGET_LANGUAGE_NAME = "English"
# Scoped to the current normalizer: rows of a retired normalizer version are read by
# nothing and must not be enqueued (94 v1 rows remained on 2026-09-07).
BUSINESS_DESCRIPTION_FIELD = TranslationField(
    "corpscout.se_ratsit_company",
    "business_description",
    SOURCE_LANG,
    TARGET_LANG,
    extra_where=f"normalizer_version = '{RATSIT_NORMALIZER_VERSION}'",
)


@dg.asset(
    deps=[dg.AssetKey("se_ratsit_normalized")],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.se_ratsit_company.business_description (current normalizer) for "
        "untranslated texts (anti-join vs text_translations under this table's key), "
        "enqueue them to the translator service, and wait for queue completion."
    ),
)
def sweden_ratsit_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed
    with clickhouse.get_connection() as client:
        untranslated_rows = client.execute(
            build_scan_sql(
                BUSINESS_DESCRIPTION_FIELD.table,
                BUSINESS_DESCRIPTION_FIELD.column,
                source_lang=BUSINESS_DESCRIPTION_FIELD.source_lang,
                target_lang=BUSINESS_DESCRIPTION_FIELD.target_lang,
                extra_where=BUSINESS_DESCRIPTION_FIELD.extra_where,
            )
        )
    context.log.info("scanned %d untranslated business descriptions", len(untranslated_rows))
    enqueue_result = translator.enqueue_translation_rows(
        source_table=BUSINESS_DESCRIPTION_FIELD.table,
        source_column=BUSINESS_DESCRIPTION_FIELD.column,
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
                "warnings": dg.MetadataValue.json(enqueue_result.workflow_start_warnings),
            },
        )
    if enqueue_result.received > 0:
        completion_stats = translator.wait_for_queue_completion(baseline_failed=baseline_failed)
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


@dg.asset_check(asset=sweden_ratsit_translation_load, name="translator_queue_healthy")
def sweden_ratsit_translator_queue_health_check(translator: TranslatorResource) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)


@dg.asset_check(asset=sweden_ratsit_translation_load, name="translations_present")
def sweden_ratsit_translation_coverage(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    """How many current-normalizer Ratsit descriptions exist, and how many are translated."""
    return translation_coverage_result(clickhouse, (BUSINESS_DESCRIPTION_FIELD,))
