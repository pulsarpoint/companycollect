"""Sweden company translation loaders: free text via the translator service,
codes via a curated dictionary table.

``activity_description`` is Swedish business-purpose free text (~1.95M
distinct) -> scanned and enqueued to the Go translator service (the sole
writer of ``text_translations``), exactly the Norway pattern. Legal-form and
status-reason values are Bolagsverket/SCB CODES, not prose -- an LLM would
guess -- so they get labels from the curated in-repo dictionaries below,
seeded into ``corpscout.se_code_labels`` (migration 000150 owns the schema,
000305 adds ``label_sv``; the ``se_companies_translated`` view joins both
sources back, and the SCB info artifact copies the legal-form pair into
``se_company_info_scb``).

Legal forms carry BOTH languages: the Swedish name is the official term a
Swedish-facing surface shows and the English one is the gloss beside it.
Status reasons carry English only -- there is no published Swedish name list
for the Bolagsverket deregistration-reason codes to curate from.

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
from dagster_v3.defs.translator_load.coverage import translation_coverage_result
from dagster_v3.defs.translator_load.loader import TranslationField, build_scan_sql
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
ACTIVITY_DESCRIPTION_FIELD = TranslationField(
    "corpscout.se_companies",
    "activity_description",
    SOURCE_LANG,
    TARGET_LANG,
)

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

# The OFFICIAL Swedish name of every legal form, beside the English gloss above --
# Bolagsverket's organisationsform names for the ``*-ORGFO`` codes and SCB's juridisk-form
# names for the numeric ones. This is the name a Swedish-facing surface shows; the English
# label is what stands beside it. Same key set as LEGAL_FORM_LABEL_EN_BY_CODE (pinned by a
# test), and every entry is cross-checked against the Swedish term the English label keeps
# in parentheses -- the nine codes whose parenthetical is not a Swedish term (the two EU
# forms named by their abbreviation, the four glossed in English or not glossed at all, and
# the three SCB names the English label abbreviates with a slash) are pinned by hand in that
# same test, one stated reason each.
#
# There is deliberately no STATUS_REASON_LABEL_SV_BY_CODE: the Bolagsverket
# deregistration-reason codes have no published Swedish name list to curate from, so the
# seed writes '' for their label_sv rather than inventing one.
LEGAL_FORM_LABEL_SV_BY_CODE: dict[str, str] = {
    # Bolagsverket organisation forms (…-ORGFO)
    "AB-ORGFO": "Aktiebolag",
    "E-ORGFO": "Enskild näringsidkare",
    "HB-ORGFO": "Handelsbolag",
    "KB-ORGFO": "Kommanditbolag",
    "BRF-ORGFO": "Bostadsrättsförening",
    "EK-ORGFO": "Ekonomisk förening",
    "FL-ORGFO": "Filial",
    "I-ORGFO": "Ideell förening",
    "S-ORGFO": "Stiftelse",
    "BF-ORGFO": "Bostadsförening",
    "KHF-ORGFO": "Kooperativ hyresrättsförening",
    "FAB-ORGFO": "Försäkringsaktiebolag",
    "SB-ORGFO": "Sparbank",
    "BFL-ORGFO": "Bankfilial",
    "OFB-ORGFO": "Ömsesidigt försäkringsbolag",
    "BAB-ORGFO": "Bankaktiebolag",
    "TSF-ORGFO": "Trossamfund",
    "SF-ORGFO": "Sambruksförening",
    "FOF-ORGFO": "Försäkringsförening",
    "TPF-ORGFO": "Tjänstepensionsförening",
    "SE-ORGFO": "Europabolag",
    "TPAB-ORGFO": "Tjänstepensionsaktiebolag",
    "SCE-ORGFO": "Europakooperativ",
    "MB-ORGFO": "Medlemsbank",
    "OTPB-ORGFO": "Ömsesidigt tjänstepensionsbolag",
    # SCB legal forms (juridisk form)
    "10": "Fysisk person",
    "21": "Enkelt bolag",
    "22": "Partrederi",
    "23": "Värdepappersfond",
    "31": "Handelsbolag, kommanditbolag",
    "41": "Bankaktiebolag",
    "42": "Försäkringsaktiebolag",
    "43": "Europabolag",
    "49": "Övriga aktiebolag",
    "51": "Ekonomisk förening",
    "53": "Bostadsrättsförening",
    "54": "Kooperativ hyresrättsförening",
    "55": "Europakooperativ",
    "61": "Ideell förening",
    "62": "Samfällighet",
    "63": "Registrerat trossamfund",
    "71": "Familjestiftelse",
    "72": "Övrig stiftelse och fond",
    "81": "Statlig enhet",
    "82": "Kommun",
    "83": "Kommunalförbund",
    "84": "Region",
    "87": "Offentlig korporation och anstalt",
    "88": "Hypoteksförening",
    "91": "Oskiftat dödsbo",
    "92": "Ömsesidigt försäkringsbolag",
    "93": "Sparbank",
    "94": "Understödsförening",
    "95": "Arbetslöshetskassa",
    "96": "Utländsk juridisk person",
    "98": "Övrig svensk juridisk person, bildad enligt särskild lagstiftning",
    "99": "Juridisk form ej utredd",
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
        "and status-reason dictionaries -- legal forms in English AND Swedish, "
        "status reasons in English only. ReplacingMergeTree(version) + "
        "argMax in the consumers make re-seeding after a label "
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
        ("legal_form", code, label, LEGAL_FORM_LABEL_SV_BY_CODE[code])
        for code, label in LEGAL_FORM_LABEL_EN_BY_CODE.items()
    ] + [
        # No curated Swedish source for the Bolagsverket deregistration-reason codes, so
        # their label_sv is written as '' rather than invented -- the same value the
        # column defaults to, and what every consumer already reads through ifNull.
        ("status_reason", code, label, "")
        for code, label in STATUS_REASON_LABEL_EN_BY_CODE.items()
    ]
    with clickhouse.get_connection() as client:
        # version is left to its DEFAULT now(), so every seed is a NEW version of each
        # code: ReplacingMergeTree(version) plus argMax(version) in the consumers make a
        # corrected label effective without a delete.
        client.execute(
            f"INSERT INTO {QUALIFIED_SE_CODE_LABELS_TABLE} "
            "(code_type, code, label_en, label_sv) VALUES",
            rows,
        )
    context.log.info("seeded %d code labels", len(rows))
    return dg.MaterializeResult(
        metadata={
            "rows": len(rows),
            "legal_form_labels": len(LEGAL_FORM_LABEL_EN_BY_CODE),
            "legal_form_labels_sv": len(LEGAL_FORM_LABEL_SV_BY_CODE),
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
    baseline_failed = translator.queue_stats().failed
    with clickhouse.get_connection() as client:
        untranslated_rows = client.execute(
            build_scan_sql(
                ACTIVITY_DESCRIPTION_FIELD.table,
                ACTIVITY_DESCRIPTION_FIELD.column,
                source_lang=ACTIVITY_DESCRIPTION_FIELD.source_lang,
                target_lang=ACTIVITY_DESCRIPTION_FIELD.target_lang,
            )
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
    asset=sweden_company_translation_load,
    name="translator_queue_healthy",
)
def sweden_company_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)


@dg.asset_check(asset=sweden_company_translation_load, name="translations_present")
def sweden_company_translation_coverage(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    """How many Swedish activity descriptions exist, and how many are translated."""
    return translation_coverage_result(clickhouse, (ACTIVITY_DESCRIPTION_FIELD,))
