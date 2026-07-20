"""Sweden company translation loaders: free text via the translator service,
codes via a curated dictionary table.

``activity_description`` is Swedish business-purpose free text (~1.95M
distinct) -> scanned and enqueued to the Go translator service (the sole
writer of ``text_translations``), exactly the Norway pattern. Legal-form and
status-reason values are Bolagsverket/SCB CODES, not prose -- an LLM would
guess -- so they get English labels from the curated in-repo dictionaries
below, seeded into ``corpscout.se_code_labels`` (migration 000150 owns the
schema; the ``se_companies_translated`` view joins both sources back).

Label curation notes: the ``*-ORGFO`` codes are Bolagsverket organisation
forms and the numeric codes are SCB legal forms (juridisk form); the
``*-AVORG`` codes are deregistration reasons. Each label keeps the Swedish
term in parentheses so a questionable curation is self-checking against the
source terminology. Unknown/garbage values (a handful of malformed source
rows carry free text in the code column) simply resolve to '' in the view.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.translator_load.loader import build_scan_sql
from dagster_v3.defs.translator_load.resource import (
    TranslatorResource,
    translator_queue_health_check,
)

GROUP_NAME = "sweden_company"
QUALIFIED_SE_CODE_LABELS_TABLE = "corpscout.se_code_labels"

SOURCE_LANG = "sv"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "Swedish"
TARGET_LANGUAGE_NAME = "English"

LEGAL_FORM_LABEL_EN_BY_CODE: dict[str, str] = {
    # Bolagsverket organisation forms (…-ORGFO)
    "AB-ORGFO": "Limited company (aktiebolag)",
    "E-ORGFO": "Sole trader (enskild näringsidkare)",
    "HB-ORGFO": "General partnership (handelsbolag)",
    "KB-ORGFO": "Limited partnership (kommanditbolag)",
    "BRF-ORGFO": "Housing cooperative (bostadsrättsförening)",
    "EK-ORGFO": "Economic association (ekonomisk förening)",
    "FL-ORGFO": "Branch of foreign company (filial)",
    "I-ORGFO": "Non-profit association (ideell förening)",
    "S-ORGFO": "Foundation (stiftelse)",
    "BF-ORGFO": "Housing association (bostadsförening)",
    "KHF-ORGFO": "Cooperative tenancy association (kooperativ hyresrättsförening)",
    "FAB-ORGFO": "Insurance limited company (försäkringsaktiebolag)",
    "SB-ORGFO": "Savings bank (sparbank)",
    "BFL-ORGFO": "Branch of foreign bank (bankfilial)",
    "OFB-ORGFO": "Mutual insurance company (ömsesidigt försäkringsbolag)",
    "BAB-ORGFO": "Banking limited company (bankaktiebolag)",
    "TSF-ORGFO": "Registered religious community (trossamfund)",
    "SF-ORGFO": "Joint farming association (sambruksförening)",
    "FOF-ORGFO": "Insurance association (försäkringsförening)",
    "TPF-ORGFO": "Occupational pension association (tjänstepensionsförening)",
    "SE-ORGFO": "European company (SE)",
    "TPAB-ORGFO": "Occupational pension limited company (tjänstepensionsaktiebolag)",
    "SCE-ORGFO": "European cooperative society (SCE)",
    "MB-ORGFO": "Member bank (medlemsbank)",
    "OTPB-ORGFO": "Mutual occupational pension company (ömsesidigt tjänstepensionsbolag)",
    # SCB legal forms (juridisk form)
    "10": "Natural person (sole proprietor)",
    "21": "Simple partnership (enkelt bolag)",
    "22": "Shipping partnership (partrederi)",
    "23": "Securities fund (värdepappersfond)",
    "31": "General or limited partnership (handels-/kommanditbolag)",
    "41": "Banking limited company (bankaktiebolag)",
    "42": "Insurance limited company (försäkringsaktiebolag)",
    "43": "European company (europabolag)",
    "49": "Other limited company (övriga aktiebolag)",
    "51": "Economic association (ekonomisk förening)",
    "53": "Housing cooperative (bostadsrättsförening)",
    "54": "Cooperative tenancy association (kooperativ hyresrättsförening)",
    "55": "European cooperative society (europakooperativ)",
    "61": "Non-profit association (ideell förening)",
    "62": "Joint property management association (samfällighet)",
    "63": "Registered religious community (registrerat trossamfund)",
    "71": "Family foundation (familjestiftelse)",
    "72": "Other foundation or fund (övrig stiftelse/fond)",
    "81": "Central government authority (statlig enhet)",
    "82": "Municipality (kommun)",
    "83": "Municipal association (kommunalförbund)",
    "84": "Region (regional authority)",
    "87": "Public corporation or institution (offentlig korporation/anstalt)",
    "88": "Mortgage association (hypoteksförening)",
    "91": "Undistributed estate of deceased (oskiftat dödsbo)",
    "92": "Mutual insurance company (ömsesidigt försäkringsbolag)",
    "93": "Savings bank (sparbank)",
    "94": "Benevolent society (understödsförening)",
    "95": "Unemployment insurance fund (arbetslöshetskassa)",
    "96": "Foreign legal person (utländsk juridisk person)",
    "98": "Other Swedish legal person under special legislation",
    "99": "Legal form not determined",
}

STATUS_REASON_LABEL_EN_BY_CODE: dict[str, str] = {
    "VERKUPP-AVORG": "Deregistered — business ceased (verksamheten upphörd)",
    "OVERK-AVORG": "Deregistered — business transferred (verksamheten överlåten)",
    "KKAV-AVORG": "Deregistered — bankruptcy concluded (konkurs avslutad)",
    "FUAV-AVORG": "Deregistered — merger completed (fusion avslutad)",
    "LIAV-AVORG": "Deregistered — liquidation concluded (likvidation avslutad)",
    "AKEJH-AVORG": (
        "Deregistered — share capital not raised to required minimum "
        "(aktiekapital ej höjt)"
    ),
    "NYINN-AVORG": "Deregistered — new owner registered (ny innehavare)",
    "DELAV-AVORG": "Deregistered — demerger completed (delning avslutad)",
    "VDSAK-AVORG": "Deregistered — managing director missing (VD saknas)",
    "UTLKKLI-AVORG": (
        "Deregistered — foreign bankruptcy or liquidation "
        "(utländsk konkurs/likvidation)"
    ),
    "AVREG-AVORG": "Deregistered (avregistrerad)",
    "ARSEED-AVORG": "Deregistered — annual report not filed (årsredovisning saknas)",
    "BABAKEJH-AVORG": (
        "Deregistered — bank share capital not raised "
        "(bankaktiebolag, aktiekapital ej höjt)"
    ),
    "OMAV-AVORG": "Deregistered — reorganisation completed (ombildning avslutad)",
    "GROMAV-AVORG": (
        "Deregistered — cross-border conversion completed "
        "(gränsöverskridande ombildning avslutad)"
    ),
    "OMBAB-AVORG": (
        "Deregistered — converted to banking limited company "
        "(ombildat till bankaktiebolag)"
    ),
    "DOM-AVORG": "Deregistered — by court ruling (domstolsbeslut)",
}


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    metadata={"table": QUALIFIED_SE_CODE_LABELS_TABLE},
    description=(
        "Seeds corpscout.se_code_labels from the curated in-repo legal-form "
        "and status-reason dictionaries. ReplacingMergeTree(version) + "
        "argMax in the consuming view make re-seeding after a label "
        "correction effective without deletes."
    ),
)
def se_code_labels_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database="corpscout", tables=("se_code_labels",)
    )
    rows = [
        ("legal_form", code, label)
        for code, label in LEGAL_FORM_LABEL_EN_BY_CODE.items()
    ] + [
        ("status_reason", code, label)
        for code, label in STATUS_REASON_LABEL_EN_BY_CODE.items()
    ]
    with clickhouse.get_connection() as client:
        client.execute(
            f"INSERT INTO {QUALIFIED_SE_CODE_LABELS_TABLE} "
            "(code_type, code, label_en) VALUES",
            rows,
        )
    context.log.info("seeded %d code labels", len(rows))
    return dg.MaterializeResult(
        metadata={
            "rows": len(rows),
            "legal_form_labels": len(LEGAL_FORM_LABEL_EN_BY_CODE),
            "status_reason_labels": len(STATUS_REASON_LABEL_EN_BY_CODE),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_company_companies_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.se_companies.activity_description for untranslated "
        "texts (anti-join vs text_translations), enqueue them to the "
        "translator service, and wait for queue completion."
    ),
)
def sweden_company_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        untranslated_rows = client.execute(
            build_scan_sql("corpscout.se_companies", "activity_description")
        )
    context.log.info(
        "scanned %d untranslated activity descriptions", len(untranslated_rows)
    )
    enqueue_result = translator.enqueue_translation_rows(
        source_table="corpscout.se_companies",
        source_column="activity_description",
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
        completion_stats = translator.wait_for_queue_completion()
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
    asset=sweden_company_translation_load,
    name="translator_queue_healthy",
)
def sweden_company_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)
