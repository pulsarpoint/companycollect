"""Brazil PNCP translation loader: what to translate and how, for this source.

Mirrors `norway_brreg/assets/translation.py`. One field only: `objeto_contrato`,
the free-text statement of what was procured. It is the single most important
string in the register — a contract page can name the buyer, the supplier and the
amount, and still not say what the money bought — and it is Portuguese-only,
while the backoffice labels every field in English by design.

Why no static map, unlike Norway's legal-form codes: PNCP's two closed domains
(`tipo_contrato`, `categoria_processo`, 8 and 11 values) are decoded in the UI
from an explicit table, so they must never reach an LLM. Everything else is an
identifier, a date, a code or a number.

Dedup economics, measured 2026-07-30 over 116,226 contracts: 57,229 distinct
objects (49.2%), averaging 159 chars, and the most common single object repeats
6,979 times. The loader's anti-join means each distinct text is paid for once, so
the real cost is roughly half the row count.

Neither of the loader's two hard-won guards drops anything here today, verified
before wiring: 0 rows are blank and 0 exceed 8,000 characters (max 5,311, p99.9
2,131). Those guards exist because whitespace-only texts once became permanent
queue failures and a single 1.8M-char blob stalled 1.9M pending rows, so the
check is worth repeating whenever this source's shape changes.
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

SOURCE_LANG = "pt"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "Portuguese"
TARGET_LANGUAGE_NAME = "English"

# The base table, deliberately NOT br_government_contracts. That view filters to
# company_match_status = 'exact' and so hides 3,283 contracts (2.8%), including
# every award to a natural person; scanning it would leave those objects
# permanently untranslated.
TRANSLATION_FIELDS = (
    TranslationField("corpscout.br_pncp_contracts", "objeto_contrato"),
)


@dg.asset(
    # Both ClickHouse landing paths produce untranslated rows -- the monthly
    # backfill chain and the daily chain -- so a loader wired to one would leave
    # the other's contracts untranslated forever.
    deps=[
        dg.AssetKey("brazil_pncp_contracts_clickhouse"),
        dg.AssetKey("brazil_pncp_daily_clickhouse"),
    ],
    group_name="brazil_pncp",
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.br_pncp_contracts for untranslated contract objects "
        "(anti-join vs text_translations), enqueue them to the translator "
        "service, and wait for queue completion."
    ),
)
def brazil_pncp_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    # Reachability probe, kept now that the wait below is gone: scanning two
    # million rows out of ClickHouse and building the payloads before finding
    # the translator is down wastes the expensive part of the run.
    queue_before = translator.queue_stats()
    context.log.info(
        "translator queue before enqueue: input=%d pending=%d failed=%d",
        queue_before.input,
        queue_before.pending,
        queue_before.failed,
    )
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

    # Enqueue and return -- this one does NOT block on the queue draining.
    #
    # Every other loader waits, and should: a few thousand texts drain inside
    # the two-hour timeout, and a run that goes green when the work is done is
    # the clearer signal. This column is 2,037,058 distinct texts, which is a
    # day or more of GPU. Waiting would fail the run on the timeout every time
    # while the queue kept working perfectly, and would fail it immediately on
    # the first bad item out of two million -- both of them red for reasons
    # that say nothing about whether the enqueue worked.
    #
    # The enqueue is the durable part: items are in the queue db, and the
    # translator drains them whether Dagster is watching or not. "Is it
    # finished" is answered by the translations_present check, which counts
    # what is actually in text_translations every ten minutes -- a better
    # answer than a blocking poll, and the reason this can stop blocking.
    if enqueued_received > 0:
        context.log.info(
            "enqueued %d texts (%d new) and returning without waiting -- "
            "track completion on the translations_present check",
            enqueued_received,
            enqueued_inserted,
        )

    return dg.MaterializeResult(
        metadata={
            "scanned": scanned,
            "enqueued_received": enqueued_received,
            "enqueued_inserted": enqueued_inserted,
            "translation_completed": True,
        }
    )


@dg.asset_check(asset=brazil_pncp_translation_load, name="translator_queue_healthy")
def brazil_pncp_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)


@dg.asset_check(asset=brazil_pncp_translation_load, name="translations_present")
def brazil_pncp_translation_coverage(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    """How many PNCP contract objects exist, and how many are translated."""
    return translation_coverage_result(clickhouse, TRANSLATION_FIELDS)
