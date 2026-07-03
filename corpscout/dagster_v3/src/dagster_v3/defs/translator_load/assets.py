"""Loader assets: scan ClickHouse for untranslated texts and enqueue them."""

import os

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.translator_load.loader import (
    LoaderField,
    LoaderSource,
    build_scan_sql,
    build_static_scan_sql,
    enqueue_items,
    insert_static_translations,
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

_LATVIA = LoaderSource(
    source_lang="lv",
    target_lang="en",
    source_language_name="Latvian",
    target_language_name="English",
    fields=(LoaderField("corpscout.lv_companies", "activity_text_original"),),
)


def _api_url() -> str:
    return os.environ.get("TRANSLATOR_API_URL", "http://localhost:8080").rstrip("/")


def _load_source(context: AssetExecutionContext, clickhouse: ClickhouseResource, source: LoaderSource) -> dict:
    session = dlt_requests.Session()
    totals = {"received": 0, "inserted": 0}
    with clickhouse.get_connection() as client:
        for field in source.fields:
            rows = client.execute(build_scan_sql(field.table, field.column))
            context.log.info("scanned %d untranslated texts for %s.%s", len(rows), field.table, field.column)
            field_totals = enqueue_items(session, _api_url(), source, field, rows)
            totals["received"] += field_totals["received"]
            totals["inserted"] += field_totals["inserted"]
    return totals


@dg.asset(
    deps=[dg.AssetKey("norway_brreg_entities_snapshot_clickhouse")],
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
    totals = _load_source(context, clickhouse, _NORWAY)
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


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.lv_companies for untranslated texts and enqueue them "
        "to the translator service."
    ),
)
def latvia_ur_translation_load(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    totals = _load_source(context, clickhouse, _LATVIA)
    return dg.MaterializeResult(
        metadata={
            "enqueued_received": totals["received"],
            "enqueued_inserted": totals["inserted"],
        }
    )


def _stats_check(session=None) -> dg.AssetCheckResult:
    """Assert /v1/queue/stats is reachable and reports queue counts."""
    session = session or dlt_requests.Session()
    try:
        response = session.get(f"{_api_url()}/v1/queue/stats", timeout=10)
        response.raise_for_status()
        stats = response.json()
    except Exception as error:  # noqa: BLE001 - reachability check reports any failure
        return dg.AssetCheckResult(passed=False, metadata={"error": str(error)})
    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "input": stats.get("input", 0),
            "pending": stats.get("pending", 0),
            "output": stats.get("output", 0),
            "failed": stats.get("failed", 0),
        },
    )


@dg.asset_check(asset=norway_brreg_translation_load, name="translator_stats_reachable")
def norway_brreg_translator_stats_check() -> dg.AssetCheckResult:
    return _stats_check()


@dg.asset_check(asset=latvia_ur_translation_load, name="translator_stats_reachable")
def latvia_ur_translator_stats_check() -> dg.AssetCheckResult:
    return _stats_check()


defs = dg.Definitions(
    assets=[norway_brreg_translation_load, latvia_ur_translation_load],
    asset_checks=[norway_brreg_translator_stats_check, latvia_ur_translator_stats_check],
)
