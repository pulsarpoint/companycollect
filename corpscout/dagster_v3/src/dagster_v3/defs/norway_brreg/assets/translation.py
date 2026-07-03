"""Norway Brreg translation loader: what to translate and how, for this source.

Source-specific translation knowledge lives here, next to the ingest assets:
the LLM-translated free-text columns, the statically-mapped legal-form column
with its code→English dictionary, and the loader asset that runs after either
ClickHouse landing path. The generic scan/enqueue/static-insert machinery is
imported from ``dagster_v3.defs.translator_load.loader``.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.translator_load.loader import (
    LoaderField,
    LoaderSource,
    build_static_scan_sql,
    insert_static_translations,
    load_source,
    stats_check,
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

_NORWAY = LoaderSource(
    source_lang="no",
    target_lang="en",
    source_language_name="Norwegian",
    target_language_name="English",
    fields=(
        LoaderField("corpscout.no_companies", "articles_purpose_original"),
        LoaderField("corpscout.no_companies", "activity_text_original"),
    ),
)
_NORWAY_STATIC = LoaderField("corpscout.no_companies", "legal_form_description_original")


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
        "text_translations), enqueue them to the translator service, and insert "
        "static legal-form translations directly."
    ),
)
def norway_brreg_translation_load(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    totals = load_source(context, clickhouse, _NORWAY)
    with clickhouse.get_connection() as client:
        static_rows = client.execute(
            build_static_scan_sql(_NORWAY_STATIC.table, _NORWAY_STATIC.column, "legal_form_code")
        )
        static_inserted = insert_static_translations(
            client,
            _NORWAY_STATIC.table,
            _NORWAY_STATIC.column,
            _NORWAY.source_lang,
            _NORWAY.target_lang,
            static_rows,
            LEGAL_FORM_DESCRIPTION_EN_BY_CODE,
        )
    context.log.info("static legal forms inserted: %d", static_inserted)
    return dg.MaterializeResult(
        metadata={
            "enqueued_received": totals["received"],
            "enqueued_inserted": totals["inserted"],
            "static_inserted": static_inserted,
        }
    )


@dg.asset_check(asset=norway_brreg_translation_load, name="translator_stats_reachable")
def norway_brreg_translator_stats_check() -> dg.AssetCheckResult:
    return stats_check()
