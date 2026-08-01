"""Sweden XBRL concept vocabulary: feed table + translation loader.

The facts table holds ~290M rows but only ~1.8k distinct Swedish concept
names (``Nettoomsattning``, ``RakenskapsarForstaDag``, ...). The feed asset
maintains ``corpscout.se_financial_facts_concepts`` (INSERT new concepts
only -- merge semantics, never replace) so the translator loader scans a
tiny table instead of the facts table. The loader then enqueues untranslated
concept names to the Go translator service -- the sole writer of
``text_translations`` -- and the migration-owned
``se_financial_concept_labels`` view joins the results back.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.translator_load.coverage import translation_coverage_result
from dagster_v3.defs.translator_load.loader import TranslationField, build_scan_sql
from dagster_v3.defs.translator_load.resource import (
    TranslatorResource,
    translator_queue_health_check,
)

GROUP_NAME = "sweden_financial"
QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE = "corpscout.se_financial_facts_concepts"

SOURCE_LANG = "sv"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "Swedish"
TARGET_LANGUAGE_NAME = "English"

_INSERT_NEW_CONCEPTS_SQL = f"""
INSERT INTO {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE}
    (concept_local_name, concept_namespace)
SELECT DISTINCT
    f.concept_local_name,
    f.concept_namespace
FROM corpscout.se_financial_facts AS f
LEFT ANTI JOIN {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE} AS e
    ON e.concept_local_name = f.concept_local_name
   AND e.concept_namespace = f.concept_namespace
WHERE f.concept_local_name <> ''
"""


@dg.asset(
    deps=[
        dg.AssetDep(
            dg.AssetKey("sweden_financial_backfill_facts_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        "sweden_financial_current_facts_clickhouse",
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "xbrl"},
    metadata={"table": QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE},
    description=(
        "Maintains the distinct Swedish XBRL concept vocabulary table by "
        "inserting concepts newly seen in se_financial_facts -- never a "
        "replace, so translated concepts are never re-queued."
    ),
)
def se_financial_facts_concepts(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        before = int(
            client.execute(
                f"SELECT count() FROM {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE}"
            )[0][0]
        )
        client.execute(_INSERT_NEW_CONCEPTS_SQL)
        after = int(
            client.execute(
                f"SELECT count() FROM {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE}"
            )[0][0]
        )
    context.log.info(
        "Sweden concept vocabulary: %d known, %d newly inserted", before, after - before
    )
    return dg.MaterializeResult(
        metadata={
            "concepts_total": after,
            "concepts_inserted": after - before,
        }
    )


@dg.asset(
    deps=[se_financial_facts_concepts],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Scan the concept vocabulary for untranslated names (anti-join vs "
        "text_translations), enqueue them to the translator service, and "
        "wait for queue completion."
    ),
)
def sweden_financial_concepts_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed
    with clickhouse.get_connection() as client:
        untranslated_rows = client.execute(
            build_scan_sql(
                QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE, "concept_local_name"
            )
        )
    context.log.info("scanned %d untranslated concept names", len(untranslated_rows))
    enqueue_result = translator.enqueue_translation_rows(
        source_table=QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE,
        source_column="concept_local_name",
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
    asset=sweden_financial_concepts_translation_load,
    name="translator_queue_healthy",
)
def sweden_financial_concepts_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)


@dg.asset_check(asset=sweden_financial_concepts_translation_load, name="translations_present")
def sweden_financial_concepts_translation_coverage(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    """How many XBRL concept names exist, and how many are translated."""
    return translation_coverage_result(clickhouse, (TranslationField(QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE, "concept_local_name"),))
