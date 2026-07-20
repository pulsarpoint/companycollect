"""Latvia UR translation loader: what to translate and how, for this source.

Source-specific translation knowledge lives here, next to the ingest assets:
one LLM-translated free-text column and no static maps. The asset keeps the
ClickHouse scan and translator enqueue steps visible.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.translator_load.loader import (
    TranslationField,
    build_scan_sql,
)
from dagster_v3.defs.translator_load.resource import (
    TranslatorResource,
    translator_queue_health_check,
)

SOURCE_LANG = "lv"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "Latvian"
TARGET_LANGUAGE_NAME = "English"
TRANSLATION_FIELDS = (
    TranslationField("corpscout.lv_companies", "activity_text_original"),
)


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.lv_companies for untranslated texts, enqueue them to "
        "the translator service, and wait for queue completion."
    ),
)
def latvia_ur_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed
    enqueued_received = 0
    enqueued_inserted = 0
    workflow_start_warnings: list[str] = []

    with clickhouse.get_connection() as client:
        for field in TRANSLATION_FIELDS:
            untranslated_rows = client.execute(
                build_scan_sql(field.table, field.column)
            )
            context.log.info(
                "scanned %d untranslated texts for %s.%s",
                len(untranslated_rows),
                field.table,
                field.column,
            )
            enqueue_result = translator.enqueue_translation_rows(
                source_table=field.table,
                source_column=field.column,
                source_lang=SOURCE_LANG,
                target_lang=TARGET_LANG,
                source_language_name=SOURCE_LANGUAGE_NAME,
                target_language_name=TARGET_LANGUAGE_NAME,
                rows=untranslated_rows,
            )
            enqueued_received += enqueue_result.received
            enqueued_inserted += enqueue_result.inserted
            workflow_start_warnings.extend(enqueue_result.workflow_start_warnings)
            for warning in enqueue_result.workflow_start_warnings:
                context.log.warning("translator workflow start warning: %s", warning)

    if workflow_start_warnings:
        raise dg.Failure(
            description="translator accepted rows but failed to start its workflow",
            metadata={
                "warning_count": len(workflow_start_warnings),
                "warnings": dg.MetadataValue.json(workflow_start_warnings),
            },
        )

    if enqueued_received > 0:
        context.log.info(
            "waiting for translator queue completion: received=%d inserted=%d",
            enqueued_received,
            enqueued_inserted,
        )
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
            "enqueued_received": enqueued_received,
            "enqueued_inserted": enqueued_inserted,
            "translation_completed": True,
        }
    )


@dg.asset_check(asset=latvia_ur_translation_load, name="translator_queue_healthy")
def latvia_ur_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)
