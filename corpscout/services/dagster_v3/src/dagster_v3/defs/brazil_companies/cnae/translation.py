"""CNAE translation loader: English names for Brazil's industry classification.

Mirrors `brazil_pncp/translation.py`. One field: `description_pt` on
`br_cnae_categories`, the only text IBGE publishes and the only thing standing
between a reader and a seven-digit number. 71.9 million establishments carry one
of these codes.

Why translation rather than a correspondence table: CONCLA publishes CNAE in
Portuguese only. The English names in NACE and ISIC cover the levels those
classifications share with CNAE, not CNAE's own 1,332 subclasses — and the
subclasses are exactly where Brazil's detail lives (`5611-2/03` snack bars and
tea houses is its own thing, NACE stops at 56 food and beverage service).

Cost is small and bounded, which is why the whole vocabulary is enqueued rather
than only the codes in use: 2,394 rows holding 1,866 distinct texts — a class
and its single subclass usually share a description — and the loader's
anti-join pays for each distinct text exactly once. For comparison, PNCP's
contract objects are 57,229 distinct texts.

The two loader guards are checked here as they were for PNCP, measured over the
loaded vocabulary: 0 of the 2,394 descriptions are blank and the longest is 187
characters, far below the 8,000 limit that once stalled the queue.

One thing to know about the output: IBGE publishes these in CAPITALS
("COMÉRCIO VAREJISTA DE ARTIGOS DO VESTUÁRIO E ACESSÓRIOS"), stored verbatim as
published. Whatever casing the translator returns is what the view will show.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.translator_load.loader import TranslationField, build_scan_sql
from dagster_v3.defs.translator_load.resource import (
    TranslatorResource,
    translator_queue_health_check,
)

SOURCE_LANG = "pt"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "Portuguese"
TARGET_LANGUAGE_NAME = "English"

# The base table, not the translated view -- scanning the view would anti-join
# against its own output.
TRANSLATION_FIELDS = (
    TranslationField("corpscout.br_cnae_categories", "description_pt"),
)


@dg.asset(
    deps=[dg.AssetKey("brazil_comp_cnae_categories_clickhouse")],
    group_name="brazil_comp_cnae",
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.br_cnae_categories for untranslated CNAE descriptions "
        "(anti-join vs text_translations), enqueue them to the translator "
        "service, and wait for queue completion."
    ),
)
def brazil_comp_cnae_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed
    enqueued_received = 0
    enqueued_inserted = 0
    scanned = 0
    workflow_start_warnings: list[str] = []

    with clickhouse.get_connection() as client:
        for field in TRANSLATION_FIELDS:
            untranslated_rows = client.execute(
                build_scan_sql(field.table, field.column)
            )
            scanned += len(untranslated_rows)
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
            "scanned": scanned,
            "enqueued_received": enqueued_received,
            "enqueued_inserted": enqueued_inserted,
            "translation_completed": True,
        }
    )


@dg.asset_check(asset=brazil_comp_cnae_translation_load, name="translator_queue_healthy")
def brazil_comp_cnae_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)
