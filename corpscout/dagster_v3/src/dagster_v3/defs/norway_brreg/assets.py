import gzip
import hashlib
import json
from collections.abc import Callable, Iterator
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol

import dagster as dg
import dlt
import duckdb
import ijson
import requests
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from dagster_v3.defs.norway_brreg import tables
from dagster_v3.defs.norway_brreg.clickhouse import (
    prepare_norway_brreg_clickhouse_tables,
)
from dagster_v3.exchange_rates import ExchangeRateClient

COUNTRY = "NO"
GROUP_NAME = "norway_brreg"
DLT_DATASET_NAME = "norway_brreg"
ENTITIES_TABLE = "entities"
FINANCIAL_STATEMENTS_TABLE = "financial_statements"
ENTITY_SOURCE_SLUG = "norway_brregenhet"
FINANCIAL_SOURCE_SLUG = "norway_brregregnskap"
NORWAY_BRREG_DUCKDB_PATH = Path("data/norway_brreg.duckdb")
BRREG_BASE_URL = "https://data.brreg.no/enhetsregisteret/api"
BRREG_REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"


BRREG_ENTITIES_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
    "org_number": {"data_type": "text", "nullable": False},
    "vat_id": {"data_type": "text"},
    "legal_name": {"data_type": "text"},
    "legal_form_code": {"data_type": "text"},
    "legal_form_description_original": {"data_type": "text"},
    "legal_form_description_en": {"data_type": "text"},
    "registration_date": {"data_type": "text"},
    "incorporation_date": {"data_type": "text"},
    "website": {"data_type": "text"},
    "phone": {"data_type": "text"},
    "nace1_code": {"data_type": "text"},
    "nace1_description_original": {"data_type": "text"},
    "nace1_description_en": {"data_type": "text"},
    "nace2_code": {"data_type": "text"},
    "nace2_description_original": {"data_type": "text"},
    "nace2_description_en": {"data_type": "text"},
    "nace3_code": {"data_type": "text"},
    "nace3_description_original": {"data_type": "text"},
    "nace3_description_en": {"data_type": "text"},
    "articles_purpose_original": {"data_type": "text"},
    "articles_purpose_en": {"data_type": "text"},
    "activity_text_original": {"data_type": "text"},
    "activity_text_en": {"data_type": "text"},
    "company_description_original": {"data_type": "text"},
    "company_description_en": {"data_type": "text"},
    "employee_count": {"data_type": "bigint"},
    "has_registered_employee_count": {"data_type": "bool"},
    "business_address_lines": {"data_type": "text"},
    "business_postal_code": {"data_type": "text"},
    "business_city": {"data_type": "text"},
    "business_municipality": {"data_type": "text"},
    "business_municipality_code": {"data_type": "text"},
    "business_country_code": {"data_type": "text"},
    "is_vat_registered": {"data_type": "bool"},
    "is_enterprise_register_registered": {"data_type": "bool"},
    "is_group_member": {"data_type": "bool"},
    "parent_org_number": {"data_type": "text"},
    "last_submitted_accounts_year": {"data_type": "text"},
    "status": {"data_type": "text"},
    "is_active": {"data_type": "bool"},
    "source_url": {"data_type": "text"},
    "raw_entity": {"data_type": "text"},
}


BRREG_FINANCIAL_STATEMENTS_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
    "org_number": {"data_type": "text", "nullable": False},
    "legal_name": {"data_type": "text"},
    "website": {"data_type": "text"},
    "last_submitted_accounts_year": {"data_type": "text"},
    "filing_id": {"data_type": "bigint"},
    "journal_number": {"data_type": "text"},
    "accounts_type": {"data_type": "text"},
    "legal_form_code": {"data_type": "text"},
    "is_parent_company": {"data_type": "bool"},
    "period_start_date": {"data_type": "date"},
    "period_end_date": {"data_type": "date"},
    "fiscal_year": {"data_type": "bigint"},
    "currency": {"data_type": "text"},
    "liquidation_accounts": {"data_type": "bool"},
    "statement_layout": {"data_type": "text"},
    "is_not_audited": {"data_type": "bool"},
    "opted_out_audit": {"data_type": "bool"},
    "is_small_enterprise": {"data_type": "bool"},
    "accounting_rules": {"data_type": "text"},
    "operating_revenue_amount_original": {"data_type": "decimal"},
    "operating_revenue_amount_usd": {"data_type": "decimal"},
    "operating_costs_amount_original": {"data_type": "decimal"},
    "operating_costs_amount_usd": {"data_type": "decimal"},
    "operating_result_amount_original": {"data_type": "decimal"},
    "operating_result_amount_usd": {"data_type": "decimal"},
    "net_financial_items_amount_original": {"data_type": "decimal"},
    "net_financial_items_amount_usd": {"data_type": "decimal"},
    "pretax_result_amount_original": {"data_type": "decimal"},
    "pretax_result_amount_usd": {"data_type": "decimal"},
    "net_result_amount_original": {"data_type": "decimal"},
    "net_result_amount_usd": {"data_type": "decimal"},
    "total_assets_amount_original": {"data_type": "decimal"},
    "total_assets_amount_usd": {"data_type": "decimal"},
    "current_assets_amount_original": {"data_type": "decimal"},
    "current_assets_amount_usd": {"data_type": "decimal"},
    "fixed_assets_amount_original": {"data_type": "decimal"},
    "fixed_assets_amount_usd": {"data_type": "decimal"},
    "equity_amount_original": {"data_type": "decimal"},
    "equity_amount_usd": {"data_type": "decimal"},
    "total_debt_amount_original": {"data_type": "decimal"},
    "total_debt_amount_usd": {"data_type": "decimal"},
    "current_liabilities_amount_original": {"data_type": "decimal"},
    "current_liabilities_amount_usd": {"data_type": "decimal"},
    "long_term_liabilities_amount_original": {"data_type": "decimal"},
    "long_term_liabilities_amount_usd": {"data_type": "decimal"},
    "fx_rate_to_usd": {"data_type": "decimal"},
    "fx_rate_date": {"data_type": "date"},
    "fx_source": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "raw_financial_record": {"data_type": "text"},
}


class HttpSession(Protocol):
    headers: dict[str, str]

    def get(self, url: str, *, timeout: int) -> Any: ...


class ExchangeRates(Protocol):
    def usd_rate(self, *, currency: str, rate_date: str) -> Any: ...


class NorwayBrregDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name == ENTITIES_TABLE:
            return spec.replace_attributes(
                key="norway_brreg_entities_duckdb",
                deps=[],
                group_name=GROUP_NAME,
                description="Norway Brreg entity bulk data loaded to local DuckDB with dlt.",
                kinds={"python", "dlt", "duckdb"},
            )
        if data.resource.name == FINANCIAL_STATEMENTS_TABLE:
            return spec.replace_attributes(
                key="norway_brreg_financial_statements_duckdb",
                deps=["norway_brreg_entities_duckdb"],
                group_name=GROUP_NAME,
                description="Norway Brreg annual accounts loaded to local DuckDB with dlt.",
                kinds={"python", "dlt", "duckdb"},
            )
        return spec


@dlt.source(name="norway_brreg_entities")
def norway_brreg_entities_source(
    *,
    base_url: str = BRREG_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    run_id: str = "",
    session: HttpSession | None = None,
) -> DltResource:
    return _entities_resource(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        run_id=run_id,
        session=session,
    )


@dlt.source(name="norway_brreg_financial_statements")
def norway_brreg_financial_statements_source(
    *,
    database_path: str | Path = NORWAY_BRREG_DUCKDB_PATH,
    base_url: str = BRREG_REGNSKAP_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    run_id: str = "",
    session: HttpSession | None = None,
    exchange_rates: ExchangeRates | None = None,
) -> DltResource:
    orgs = norway_brreg_financial_orgs_resource(database_path=database_path)
    return orgs | _financial_statements_resource(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        run_id=run_id,
        session=session,
        exchange_rates=exchange_rates,
    )


@dlt.resource(
    name=ENTITIES_TABLE,
    write_disposition="replace",
    primary_key="org_number",
    columns=BRREG_ENTITIES_COLUMNS,
)
def _entities_resource(
    *,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    session: HttpSession | None,
) -> Iterator[dict[str, Any]]:
    response_body = _download_bytes(
        url=f"{base_url}/enheter/lastned",
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        session=session,
    )
    for line_number, entity in enumerate(_stream_gzip_json_array(response_body), start=1):
        yield _entity_row(entity, line_number=line_number, run_id=run_id)


@dlt.resource(name="financial_orgs", selected=False)
def norway_brreg_financial_orgs_resource(
    *,
    database_path: str | Path,
) -> Iterator[dict[str, str]]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            """
            select org_number, legal_name, website, last_submitted_accounts_year
            from norway_brreg.entities
            where is_active = true
              and nullif(trim(website), '') is not null
              and nullif(trim(last_submitted_accounts_year), '') is not null
            order by org_number
            """
        ).fetchall()

    for org_number, legal_name, website, last_submitted_accounts_year in rows:
        yield {
            "org_number": _string(org_number),
            "legal_name": _string(legal_name),
            "website": _string(website),
            "last_submitted_accounts_year": _string(last_submitted_accounts_year),
        }


@dlt.transformer(
    name=FINANCIAL_STATEMENTS_TABLE,
    write_disposition="replace",
    primary_key=["org_number", "period_end_date", "accounts_type"],
    columns=BRREG_FINANCIAL_STATEMENTS_COLUMNS,
)
def _financial_statements_resource(
    org: dict[str, Any],
    *,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    session: HttpSession | None,
    exchange_rates: ExchangeRates | None,
) -> Iterator[dict[str, Any]]:
    source_url = f"{base_url}/{org['org_number']}"
    http_session = session or requests.Session()
    http_session.headers["User-Agent"] = user_agent
    response = http_session.get(source_url, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return
    yield from build_financial_statement_rows(
        payload,
        org=org,
        exchange_rates=exchange_rates or ExchangeRateClient.from_env(),
        run_id=run_id,
        source_url=source_url,
    )


def run_norway_brreg_entities_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: HttpSession | None = None,
) -> Any:
    return norway_brreg_pipeline(database_path, pipeline_name="norway_brreg_entities").run(
        norway_brreg_entities_source(run_id=run_id, session=session)
    )


def run_norway_brreg_financial_statements_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: HttpSession | None = None,
    exchange_rates: ExchangeRates | None = None,
) -> Any:
    return norway_brreg_pipeline(
        database_path,
        pipeline_name="norway_brreg_financial_statements",
    ).run(
        norway_brreg_financial_statements_source(
            database_path=database_path,
            run_id=run_id,
            session=session,
            exchange_rates=exchange_rates,
        )
    )


def norway_brreg_pipeline(database_path: str | Path, *, pipeline_name: str) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=DLT_DATASET_NAME,
        dev_mode=False,
    )


@dlt_assets(
    dlt_source=norway_brreg_entities_source(),
    dlt_pipeline=norway_brreg_pipeline(
        NORWAY_BRREG_DUCKDB_PATH,
        pipeline_name="norway_brreg_entities",
    ),
    name="norway_brreg_entities_duckdb",
    dagster_dlt_translator=NorwayBrregDltTranslator(),
)
def norway_brreg_entities_duckdb_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    """Load Brreg entity bulk data to local DuckDB with dlt."""
    context.log.info(
        "Starting Norway Brreg entity dlt load: source_url=%s, duckdb_path=%s, "
        "dataset=%s, table=%s",
        f"{BRREG_BASE_URL}/enheter/lastned",
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
    )
    yield from dlt.run(
        context=context,
        dlt_source=norway_brreg_entities_source(run_id=context.run_id),
        dlt_pipeline=norway_brreg_pipeline(
            NORWAY_BRREG_DUCKDB_PATH,
            pipeline_name="norway_brreg_entities",
        ),
    )
    context.log.info(
        "Completed Norway Brreg entity dlt load: duckdb_path=%s, dataset=%s, table=%s",
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
    )


@dlt_assets(
    dlt_source=norway_brreg_financial_statements_source(),
    dlt_pipeline=norway_brreg_pipeline(
        NORWAY_BRREG_DUCKDB_PATH,
        pipeline_name="norway_brreg_financial_statements",
    ),
    name="norway_brreg_financial_statements_duckdb",
    dagster_dlt_translator=NorwayBrregDltTranslator(),
)
def norway_brreg_financial_statements_duckdb_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    """Load Brreg annual accounts for selected entities to local DuckDB with dlt."""
    context.log.info(
        "Starting Norway Brreg financial statements dlt load: source_url=%s, "
        "duckdb_path=%s, input_table=%s.%s, output_table=%s.%s",
        BRREG_REGNSKAP_BASE_URL,
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
        DLT_DATASET_NAME,
        FINANCIAL_STATEMENTS_TABLE,
    )
    yield from dlt.run(
        context=context,
        dlt_source=norway_brreg_financial_statements_source(
            database_path=NORWAY_BRREG_DUCKDB_PATH,
            run_id=context.run_id,
        ),
        dlt_pipeline=norway_brreg_pipeline(
            NORWAY_BRREG_DUCKDB_PATH,
            pipeline_name="norway_brreg_financial_statements",
        ),
    )
    context.log.info(
        "Completed Norway Brreg financial statements dlt load: duckdb_path=%s, "
        "table=%s.%s",
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        FINANCIAL_STATEMENTS_TABLE,
    )


@dg.asset(
    deps=[
        dg.AssetKey("norway_brreg_entities_duckdb"),
        dg.AssetKey("norway_brreg_financial_statements_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    description="Norway Brreg company and annual-account final tables exported to ClickHouse.",
)
def norway_brreg_clickhouse_tables(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Norway Brreg ClickHouse export: duckdb_path=%s, companies_table=%s, "
        "financial_statements_table=%s",
        NORWAY_BRREG_DUCKDB_PATH,
        tables.QUALIFIED_COMPANIES_TABLE,
        tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
    )
    counts = export_norway_brreg_clickhouse_tables(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    context.log.info(
        "Completed Norway Brreg ClickHouse export: companies=%s, financial_statements=%s",
        counts["companies"],
        counts["financial_statements"],
    )
    return dg.MaterializeResult(
        metadata={
            "companies": counts["companies"],
            "financial_statements": counts["financial_statements"],
            "companies_table": tables.QUALIFIED_COMPANIES_TABLE,
            "financial_statements_table": tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
        }
    )


defs = dg.Definitions(
    assets=[
        norway_brreg_entities_duckdb_asset,
        norway_brreg_financial_statements_duckdb_asset,
        norway_brreg_clickhouse_tables,
    ]
)


def build_entity_rows(entities: list[dict[str, Any]], *, run_id: str) -> list[dict[str, Any]]:
    return [
        _entity_row(entity, line_number=index, run_id=run_id)
        for index, entity in enumerate(entities, start=1)
    ]


def build_financial_statement_rows(
    records: list[dict[str, Any]],
    *,
    org: dict[str, Any],
    exchange_rates: ExchangeRates,
    run_id: str,
    source_url: str,
) -> list[dict[str, Any]]:
    return [
        _financial_statement_row(
            record,
            org=org,
            line_number=index,
            exchange_rates=exchange_rates,
            run_id=run_id,
            source_url=source_url,
        )
        for index, record in enumerate(records, start=1)
        if isinstance(record, dict)
    ]


def export_norway_brreg_clickhouse_tables(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., None] | None = None,
) -> dict[str, int]:
    _log(
        log,
        "Preparing Norway Brreg ClickHouse tables: database=%s, companies_table=%s, "
        "financial_statements_table=%s",
        tables.NORWAY_BRREG_DATABASE,
        tables.QUALIFIED_COMPANIES_TABLE,
        tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
    )
    prepare_norway_brreg_clickhouse_tables(clickhouse)
    _log(log, "Opening Norway Brreg DuckDB staging database: path=%s", database_path)
    with duckdb.connect(str(database_path), read_only=True) as connection:
        _log(
            log,
            "Reading Norway Brreg company rows from DuckDB: table=%s.%s",
            DLT_DATASET_NAME,
            ENTITIES_TABLE,
        )
        company_rows = _fetch_duckdb_rows(
            connection,
            dataset=DLT_DATASET_NAME,
            table=ENTITIES_TABLE,
            columns=tables.COMPANIES_COLUMNS,
        )
        _log(log, "Read Norway Brreg company rows from DuckDB: rows=%s", len(company_rows))
        _log(
            log,
            "Reading Norway Brreg financial statement rows from DuckDB: table=%s.%s",
            DLT_DATASET_NAME,
            FINANCIAL_STATEMENTS_TABLE,
        )
        financial_rows = _fetch_duckdb_rows(
            connection,
            dataset=DLT_DATASET_NAME,
            table=FINANCIAL_STATEMENTS_TABLE,
            columns=tables.FINANCIAL_STATEMENTS_COLUMNS,
        )
        _log(
            log,
            "Read Norway Brreg financial statement rows from DuckDB: rows=%s",
            len(financial_rows),
        )

    with clickhouse.get_connection() as client:
        if company_rows:
            _log(
                log,
                "Inserting Norway Brreg company rows into ClickHouse: table=%s, rows=%s",
                tables.QUALIFIED_COMPANIES_TABLE,
                len(company_rows),
            )
            client.insert(
                tables.QUALIFIED_COMPANIES_TABLE,
                company_rows,
                column_names=tables.COMPANIES_COLUMNS,
            )
        else:
            _log(log, "Skipping Norway Brreg company ClickHouse insert: rows=0")
        if financial_rows:
            _log(
                log,
                "Inserting Norway Brreg financial statement rows into ClickHouse: "
                "table=%s, rows=%s",
                tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
                len(financial_rows),
            )
            client.insert(
                tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
                financial_rows,
                column_names=tables.FINANCIAL_STATEMENTS_COLUMNS,
            )
        else:
            _log(log, "Skipping Norway Brreg financial statement ClickHouse insert: rows=0")

    _log(
        log,
        "Finished Norway Brreg ClickHouse export: companies=%s, financial_statements=%s",
        len(company_rows),
        len(financial_rows),
    )

    return {
        "companies": len(company_rows),
        "financial_statements": len(financial_rows),
    }


def source_payload_hash(payload: dict[str, Any]) -> str:
    body = _json_dumps(payload, sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _financial_statement_row(
    record: dict[str, Any],
    *,
    org: dict[str, Any],
    line_number: int,
    exchange_rates: ExchangeRates,
    run_id: str,
    source_url: str,
) -> dict[str, Any]:
    virksomhet = _dict(record.get("virksomhet"))
    period = _dict(record.get("regnskapsperiode"))
    revisjon = _dict(record.get("revisjon"))
    principles = _dict(record.get("regnkapsprinsipper"))
    result = _dict(record.get("resultatregnskapResultat"))
    operating = _dict(result.get("driftsresultat"))
    revenue = _dict(operating.get("driftsinntekter"))
    costs = _dict(operating.get("driftskostnad"))
    financial = _dict(result.get("finansresultat"))
    assets = _dict(record.get("eiendeler"))
    current_assets = _dict(assets.get("omloepsmidler"))
    fixed_assets = _dict(assets.get("anleggsmidler"))
    equity_debt = _dict(record.get("egenkapitalGjeld"))
    equity = _dict(equity_debt.get("egenkapital"))
    debt = _dict(equity_debt.get("gjeldOversikt"))
    current_debt = _dict(debt.get("kortsiktigGjeld"))
    long_debt = _dict(debt.get("langsiktigGjeld"))

    currency = _string(record.get("valuta")).upper()
    period_end_date = _string(period.get("tilDato"))
    fx_rate = exchange_rates.usd_rate(currency=currency, rate_date=period_end_date)

    amounts = {
        "operating_revenue": _decimal_or_none(revenue.get("sumDriftsinntekter")),
        "operating_costs": _decimal_or_none(costs.get("sumDriftskostnad")),
        "operating_result": _decimal_or_none(operating.get("driftsresultat")),
        "net_financial_items": _decimal_or_none(financial.get("nettoFinans")),
        "pretax_result": _decimal_or_none(result.get("ordinaertResultatFoerSkattekostnad")),
        "net_result": _decimal_or_none(result.get("aarsresultat")),
        "total_assets": _decimal_or_none(assets.get("sumEiendeler")),
        "current_assets": _decimal_or_none(current_assets.get("sumOmloepsmidler")),
        "fixed_assets": _decimal_or_none(fixed_assets.get("sumAnleggsmidler")),
        "equity": _decimal_or_none(equity.get("sumEgenkapital")),
        "total_debt": _decimal_or_none(debt.get("sumGjeld")),
        "current_liabilities": _decimal_or_none(current_debt.get("sumKortsiktigGjeld")),
        "long_term_liabilities": _decimal_or_none(long_debt.get("sumLangsiktigGjeld")),
    }

    row: dict[str, Any] = {
        "country_iso2": COUNTRY,
        "source_slug": FINANCIAL_SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": _string(record.get("id")),
        "source_payload_hash": source_payload_hash(record),
        "org_number": _string(virksomhet.get("organisasjonsnummer")) or _string(org.get("org_number")),
        "legal_name": _string(org.get("legal_name")),
        "website": _string(org.get("website")),
        "last_submitted_accounts_year": _string(org.get("last_submitted_accounts_year")),
        "filing_id": _int_or_none(record.get("id")),
        "journal_number": _string(record.get("journalnr")),
        "accounts_type": _string(record.get("regnskapstype")),
        "legal_form_code": _string(virksomhet.get("organisasjonsform")),
        "is_parent_company": _bool(virksomhet.get("morselskap")),
        "period_start_date": _string(period.get("fraDato")),
        "period_end_date": period_end_date,
        "fiscal_year": _fiscal_year(period_end_date),
        "currency": currency,
        "liquidation_accounts": _bool(record.get("avviklingsregnskap")),
        "statement_layout": _string(record.get("oppstillingsplan")),
        "is_not_audited": _bool(revisjon.get("ikkeRevidertAarsregnskap")),
        "opted_out_audit": _bool(revisjon.get("fravalgRevisjon")),
        "is_small_enterprise": _bool(principles.get("smaaForetak")),
        "accounting_rules": _string(principles.get("regnskapsregler")),
        "fx_rate_to_usd": fx_rate.rate,
        "fx_rate_date": fx_rate.rate_date,
        "fx_source": fx_rate.source,
        "source_url": source_url,
        "raw_financial_record": _json_dumps(record),
    }
    for field_name, amount in amounts.items():
        row[f"{field_name}_amount_original"] = amount
        row[f"{field_name}_amount_usd"] = None if amount is None else fx_rate.convert(amount)
    return row


def _entity_row(entity: dict[str, Any], *, line_number: int, run_id: str) -> dict[str, Any]:
    org_number = _string(entity.get("organisasjonsnummer"))
    vat_registered = _bool(entity.get("registrertIMvaregisteret"))
    business_address = _dict(entity.get("forretningsadresse"))
    legal_form = _dict(entity.get("organisasjonsform"))
    nace1 = _dict(entity.get("naeringskode1"))
    nace2 = _dict(entity.get("naeringskode2"))
    nace3 = _dict(entity.get("naeringskode3"))
    status = _entity_status(entity)
    legal_form_description_original = _string(legal_form.get("beskrivelse"))
    nace1_description_original = _string(nace1.get("beskrivelse"))
    nace2_description_original = _string(nace2.get("beskrivelse"))
    nace3_description_original = _string(nace3.get("beskrivelse"))
    articles_purpose_original = _joined_text_lines(entity.get("vedtektsfestetFormaal"))
    activity_text_original = _joined_text_lines(entity.get("aktivitet"))
    return {
        "country_iso2": COUNTRY,
        "source_slug": ENTITY_SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": org_number,
        "source_payload_hash": source_payload_hash(entity),
        "org_number": org_number,
        "vat_id": f"NO{org_number}MVA" if vat_registered and org_number else "",
        "legal_name": _string(entity.get("navn")),
        "legal_form_code": _string(legal_form.get("kode")),
        "legal_form_description_original": legal_form_description_original,
        "legal_form_description_en": "",
        "registration_date": _string(entity.get("registreringsdatoEnhetsregisteret")),
        "incorporation_date": _string(entity.get("stiftelsesdato")),
        "website": _string(entity.get("hjemmeside")),
        "phone": _string(entity.get("telefon")),
        "nace1_code": _string(nace1.get("kode")),
        "nace1_description_original": nace1_description_original,
        "nace1_description_en": "",
        "nace2_code": _string(nace2.get("kode")),
        "nace2_description_original": nace2_description_original,
        "nace2_description_en": "",
        "nace3_code": _string(nace3.get("kode")),
        "nace3_description_original": nace3_description_original,
        "nace3_description_en": "",
        "articles_purpose_original": articles_purpose_original,
        "articles_purpose_en": "",
        "activity_text_original": activity_text_original,
        "activity_text_en": "",
        "company_description_original": activity_text_original,
        "company_description_en": "",
        "employee_count": _int_or_none(entity.get("antallAnsatte")),
        "has_registered_employee_count": _bool(entity.get("harRegistrertAntallAnsatte")),
        "business_address_lines": _address_lines(business_address),
        "business_postal_code": _string(business_address.get("postnummer")),
        "business_city": _string(business_address.get("poststed")),
        "business_municipality": _string(business_address.get("kommune")),
        "business_municipality_code": _string(business_address.get("kommunenummer")),
        "business_country_code": _string(business_address.get("landkode")),
        "is_vat_registered": vat_registered,
        "is_enterprise_register_registered": _bool(entity.get("registrertIForetaksregisteret")),
        "is_group_member": _bool(entity.get("erIKonsern")),
        "parent_org_number": _string(entity.get("overordnetEnhet")),
        "last_submitted_accounts_year": _string(entity.get("sisteInnsendteAarsregnskap")),
        "status": status,
        "is_active": status == "active",
        "source_url": _source_url(entity),
        "raw_entity": _json_dumps(entity),
    }


def _download_bytes(
    *,
    url: str,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
) -> bytes:
    http_session = session or requests.Session()
    http_session.headers["User-Agent"] = user_agent
    response = http_session.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def _stream_gzip_json_array(body: bytes) -> Iterator[dict[str, Any]]:
    with gzip.GzipFile(fileobj=BytesIO(body)) as gzip_file:
        for record in ijson.items(gzip_file, "item"):
            if isinstance(record, dict):
                yield record


def _entity_status(entity: dict[str, Any]) -> str:
    if _bool(entity.get("konkurs")):
        return "bankrupt"
    if _bool(entity.get("underTvangsavviklingEllerTvangsopplosning")):
        return "compulsory_liquidation"
    if _bool(entity.get("underAvvikling")):
        return "liquidation"
    return "active"


def _source_url(entity: dict[str, Any]) -> str:
    links = _dict(entity.get("_links"))
    self_link = _dict(links.get("self"))
    return _string(self_link.get("href"))


def _json_dumps(payload: dict[str, Any], *, sort_keys: bool = False) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            return str(value)
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _log(log: Callable[..., None] | None, message: str, *args: Any) -> None:
    if log is not None:
        log(message, *args)


def _fetch_duckdb_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    dataset: str,
    table: str,
    columns: tuple[str, ...],
) -> list[tuple[Any, ...]]:
    select_list = ", ".join(columns)
    return connection.execute(
        f"select {select_list} from {dataset}.{table} order by org_number"
    ).fetchall()


def _address_lines(address: dict[str, Any]) -> str:
    return "\n".join(_string(line) for line in _list(address.get("adresse")) if _string(line))


def _joined_text_lines(value: Any) -> str:
    return "\n".join(_string(line) for line in _list(value) if _string(line))


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _bool(value: Any) -> bool:
    return bool(value)


def _int_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) else None


def _decimal_or_none(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _fiscal_year(period_end_date: str) -> int | None:
    return int(period_end_date[:4]) if len(period_end_date) >= 4 else None


def _string(value: Any) -> str:
    return "" if value is None else str(value)
