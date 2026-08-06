"""Norway Brreg translation loader: what to translate and how, for this source.

Source-specific translation knowledge lives here, next to the ingest assets:
the LLM-translated free-text columns, the statically-mapped legal-form column
with its code→English dictionary, and the loader asset that runs after either
ClickHouse landing path. The asset keeps its scan, translator enqueue, and
static-insert steps visible; shared modules only own the SQL builders and the
translator HTTP resource.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

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

# Moved verbatim from the retired translator configs (Go config/sources JSON
# and Python translator.static_maps).
LEGAL_FORM_DESCRIPTION_EN_BY_CODE: dict[str, str] = {
    "ADOS": "Administrative unit - public sector",
    "ANNA": "Other legal entity",
    "ANS": "General partnership",
    "AS": "Private limited company",
    "ASA": "Public limited company",
    "BA": "Company with limited liability",
    "BBL": "Housing cooperative building association",
    "BO": "Other estate",
    "BRL": "Housing cooperative",
    "DA": "General partnership with shared liability",
    "ENK": "Sole proprietorship",
    "ESEK": "Condominium (owner-section co-ownership)",
    "FKF": "County municipal enterprise",
    "FLI": "Association/club/institution",
    "FYLK": "County authority",
    "GFS": "Mutual insurance company",
    "IKS": "Inter-municipal company",
    "KF": "Municipal enterprise",
    "KBO": "Bankruptcy estate",
    "KIRK": "Church of Norway",
    "KOMM": "Municipality",
    "KS": "Limited partnership",
    "KTRF": "Office-sharing arrangement",
    "NUF": "Norwegian-registered foreign company",
    "OPMV": "Separately divided unit (VAT Act section 2-2)",
    "ORGL": "Organisational subdivision",
    "PERS": "Other registered individuals",
    "PK": "Pension fund",
    "PRE": "Shipping partnership",
    "SA": "Cooperative",
    "SAM": "Co-ownership under property law",
    "SE": "European company (SE)",
    "SF": "State enterprise",
    "SPA": "Savings bank",
    "STAT": "The State",
    "STI": "Foundation",
    "SÆR": "Other enterprise under special legislation",
    "TVAM": "Compulsorily registered for VAT",
    "UTLA": "Foreign entity",
    "VPFO": "Securities fund",
}

SOURCE_LANG = "no"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "Norwegian"
TARGET_LANGUAGE_NAME = "English"
TRANSLATION_FIELDS = (
    TranslationField(
        "corpscout.no_companies",
        "articles_purpose_original",
        SOURCE_LANG,
        TARGET_LANG,
    ),
    TranslationField(
        "corpscout.no_companies",
        "activity_text_original",
        SOURCE_LANG,
        TARGET_LANG,
    ),
)
LEGAL_FORM_FIELD = TranslationField(
    "corpscout.no_companies",
    "legal_form_description_original",
    SOURCE_LANG,
    TARGET_LANG,
)


@dg.asset(
    # Both ClickHouse landing paths (manual full snapshot and daily updates)
    # produce untranslated rows, so both are upstream of the loader.
    deps=[
        dg.AssetKey("norway_brreg_entities_snapshot_clickhouse"),
        dg.AssetKey("norway_brreg_entity_updates_clickhouse"),
    ],
    group_name="norway_brreg",
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.no_companies for untranslated texts (anti-join vs "
        "text_translations), enqueue them to the translator service, wait for "
        "queue completion, and insert static legal-form translations directly."
    ),
)
def norway_brreg_translation_load(
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
                build_scan_sql(
                    field.table,
                    field.column,
                    source_lang=field.source_lang,
                    target_lang=field.target_lang,
                )
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

        static_rows = client.execute(
            build_static_scan_sql(
                LEGAL_FORM_FIELD.table,
                LEGAL_FORM_FIELD.column,
                "legal_form_code",
                source_lang=LEGAL_FORM_FIELD.source_lang,
                target_lang=LEGAL_FORM_FIELD.target_lang,
            )
        )
        static_inserted = insert_static_translations(
            client,
            LEGAL_FORM_FIELD.table,
            LEGAL_FORM_FIELD.column,
            SOURCE_LANG,
            TARGET_LANG,
            static_rows,
            LEGAL_FORM_DESCRIPTION_EN_BY_CODE,
        )

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

    context.log.info("static legal forms inserted: %d", static_inserted)
    return dg.MaterializeResult(
        metadata={
            "enqueued_received": enqueued_received,
            "enqueued_inserted": enqueued_inserted,
            "static_inserted": static_inserted,
            "translation_completed": True,
        }
    )


@dg.asset_check(asset=norway_brreg_translation_load, name="translator_queue_healthy")
def norway_brreg_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)


@dg.asset_check(asset=norway_brreg_translation_load, name="translations_present")
def norway_brreg_translation_coverage(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    """How many Norwegian free-text fields exist, and how many are translated."""
    return translation_coverage_result(clickhouse, TRANSLATION_FIELDS)
