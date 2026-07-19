from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
import re
from typing import Any

import duckdb
from openai import OpenAI

from exchange_rates import ExchangeRateRequest
from dagster_v3.defs.norway_brreg_financial.models import (
    AnnualAccountConceptMappingResponse,
    AnnualAccountDocument,
    AnnualAccountPage,
    AnnualAccountWord,
    ExtractedAnnualAccountFact,
)

ANNUAL_ACCOUNT_DATASET = "annual_accounts"
PARSER_VERSION = "norway-annual-account-geometry-v2"
MAPPING_VERSION = "norway-annual-account-concepts-v1"
LLM_PROMPT_VERSION = "norway-annual-account-label-map-v1"
LLM_MAX_TOKENS = 4_096
LLM_MAX_BATCH_SIZE = 2
SOURCE_SLUG = "norway_brreg_annual_accounts_pdf"
SOURCE_BUCKET = "source-norway-brreg"

METRIC_NAMES = (
    "operating_revenue",
    "operating_costs",
    "operating_result",
    "net_financial_items",
    "pretax_result",
    "tax_expense",
    "net_result",
    "fixed_assets",
    "current_assets",
    "inventory",
    "current_receivables",
    "cash_and_bank",
    "total_assets",
    "equity",
    "current_liabilities",
    "long_term_liabilities",
    "total_liabilities",
    "personnel_expenses",
    "wages_and_salaries",
    "depreciation",
    "employees",
)

BUILTIN_CONCEPTS: dict[str, str] = {
    "sum inntekter": "operating_revenue",
    "sum driftsinntekter": "operating_revenue",
    "driftsinntekter": "operating_revenue",
    "salgsinntekt": "operating_revenue",
    "sum kostnader": "operating_costs",
    "sum driftskostnader": "operating_costs",
    "driftsresultat": "operating_result",
    "netto finans": "net_financial_items",
    "resultat før skattekostnad": "pretax_result",
    "resultat før skatt": "pretax_result",
    "skattekostnad": "tax_expense",
    "skattekostnad på resultat": "tax_expense",
    "årsresultat": "net_result",
    "sum anleggsmidler": "fixed_assets",
    "sum omløpsmidler": "current_assets",
    "sum varer": "inventory",
    "sum fordringer": "current_receivables",
    "sum bankinnskudd kontanter og lignende": "cash_and_bank",
    "bankinnskudd kontanter og lignende": "cash_and_bank",
    "bankinnskudd kontanter o l": "cash_and_bank",
    "sum eiendeler": "total_assets",
    "sum egenkapital": "equity",
    "sum kortsiktig gjeld": "current_liabilities",
    "sum langsiktig gjeld": "long_term_liabilities",
    "sum gjeld": "total_liabilities",
    "lønnskostnad": "personnel_expenses",
    "lønn og feriepenger": "wages_and_salaries",
    "avskrivning": "depreciation",
    "avskrivning på varige driftsmidler og immaterielle eiendeler": "depreciation",
    "antall ansatte": "employees",
}

_YEAR_PATTERN = re.compile(r"^(?:19|20)\d{2}$")
_DAY_FIRST_DATE_PATTERN = re.compile(
    r"^(?P<day>\d{1,2})[./-](?P<month>\d{1,2})[./-]"
    r"(?P<year>(?:19|20)\d{2})\.?$"
)
_YEAR_FIRST_DATE_PATTERN = re.compile(
    r"^(?P<year>(?:19|20)\d{2})[./-](?P<month>\d{1,2})[./-]"
    r"(?P<day>\d{1,2})$"
)
_ACCOUNT_SCOPE_PATTERN = re.compile(
    r"\b(?:konsern|morselskap|morforetak|group|parent)\b",
    re.IGNORECASE,
)
_NUMBER_TOKEN_PATTERN = re.compile(
    r"^(?:[-+]?\d+(?:[.,]\d+)?|[-+]?\(\d+(?:[.,]\d+)?\)|[-+]?\[\d+(?:[.,]\d+)?\])$"
)
_NOTE_PATTERN = re.compile(r"^\d+(?:\s*,\s*\d+)*$")
_LABEL_CLEAN_PATTERN = re.compile(r"[^0-9a-zæøå]+", re.IGNORECASE)


def ensure_annual_account_duckdb_schema(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    metric_columns = ", ".join(
        f"{metric}_amount_original decimal(38, 6), {metric}_amount_usd decimal(38, 6)"
        for metric in METRIC_NAMES
    )
    connection.execute(f"create schema if not exists {ANNUAL_ACCOUNT_DATASET}")
    connection.execute(
        f"""
        create table if not exists {ANNUAL_ACCOUNT_DATASET}.documents (
            document_id varchar primary key,
            country_iso2 varchar not null,
            source_slug varchar not null,
            source_run_id varchar not null,
            org_number varchar not null,
            legal_name varchar not null,
            source_filing_year integer not null,
            source_chunk varchar not null,
            source_json_object_key varchar not null,
            source_json_uri varchar not null,
            source_json_sha256 varchar not null,
            source_pdf_url varchar not null,
            source_pdf_sha256 varchar not null,
            source_pdf_size_bytes bigint not null,
            retrieved_at timestamp with time zone,
            pdf_page_count integer not null,
            native_text_page_count integer not null,
            ocr_page_count integer not null,
            parse_status varchar not null,
            parse_warnings varchar not null,
            fact_count bigint not null,
            parser_version varchar not null,
            resolved_at timestamp with time zone not null
        )
        """
    )
    connection.execute(
        f"""
        create table if not exists {ANNUAL_ACCOUNT_DATASET}.facts (
            fact_id varchar primary key,
            document_id varchar not null,
            country_iso2 varchar not null,
            source_slug varchar not null,
            source_run_id varchar not null,
            org_number varchar not null,
            source_filing_year integer not null,
            source_chunk varchar not null,
            fact_ordinal bigint not null,
            page_number integer not null,
            line_number integer not null,
            statement_type varchar not null,
            table_title varchar not null,
            raw_label varchar not null,
            normalized_label varchar not null,
            canonical_concept varchar,
            column_label varchar not null,
            fiscal_year integer,
            period_end_date date,
            is_comparative boolean not null,
            value_kind varchar not null,
            raw_value varchar not null,
            numeric_value decimal(38, 10) not null,
            currency varchar not null,
            unit_scale decimal(38, 6) not null,
            amount_original decimal(38, 10),
            amount_usd decimal(38, 10),
            fx_rate_to_usd decimal(38, 12),
            fx_rate_date date,
            fx_source varchar,
            bbox varchar not null,
            evidence varchar not null,
            ocr_confidence double not null,
            extraction_method varchar not null,
            mapping_method varchar not null,
            mapping_confidence double,
            quality_flags varchar not null,
            source_json_sha256 varchar not null,
            parser_version varchar not null,
            resolved_at timestamp with time zone not null
        )
        """
    )
    connection.execute(
        f"""
        create table if not exists {ANNUAL_ACCOUNT_DATASET}.concept_mappings (
            normalized_label varchar not null,
            statement_type varchar not null,
            canonical_concept varchar,
            mapping_method varchar not null,
            mapping_confidence double not null,
            model varchar,
            prompt_version varchar not null,
            raw_response varchar,
            mapped_at timestamp with time zone not null,
            primary key (normalized_label, statement_type)
        )
        """
    )
    connection.execute(
        f"""
        create table if not exists {ANNUAL_ACCOUNT_DATASET}.metrics (
            metric_id varchar primary key,
            document_id varchar not null,
            country_iso2 varchar not null,
            source_slug varchar not null,
            source_run_id varchar not null,
            org_number varchar not null,
            legal_name varchar not null,
            source_filing_year integer not null,
            source_chunk varchar not null,
            fiscal_year integer not null,
            period_end_date date,
            is_comparative boolean not null,
            currency varchar not null,
            {metric_columns},
            source_fact_count bigint not null,
            mapped_fact_count bigint not null,
            unmapped_numeric_fact_count bigint not null,
            validation_status varchar not null,
            metric_warnings varchar not null,
            source_fact_ids varchar not null,
            mapping_version varchar not null,
            fx_rate_to_usd decimal(38, 12),
            fx_rate_date date,
            fx_source varchar,
            source_pdf_url varchar not null,
            source_json_uri varchar not null,
            source_json_sha256 varchar not null,
            resolved_at timestamp with time zone not null
        )
        """
    )


def extract_annual_account_facts(
    document: AnnualAccountDocument,
    *,
    source_json_sha256: str,
) -> list[ExtractedAnnualAccountFact]:
    document_currency, document_unit_scale = _currency_and_scale(
        "\n".join(page.text for page in document.pages)
    )
    facts: list[ExtractedAnnualAccountFact] = []
    for page in document.pages:
        facts.extend(
            _page_facts(
                document,
                page=page,
                source_json_sha256=source_json_sha256,
                first_ordinal=len(facts) + 1,
                document_currency=document_currency,
                document_unit_scale=document_unit_scale,
            )
        )
    return facts


def load_annual_account_documents(
    *,
    connection: duckdb.DuckDBPyConnection,
    storage: Any,
    filing_year: int,
    chunk_key: str,
    source_run_id: str,
) -> dict[str, int]:
    ensure_annual_account_duckdb_schema(connection)
    keys = storage.list_annual_account_document_keys(
        filing_year=filing_year,
        chunk_key=chunk_key,
    )
    rows: list[tuple[Any, ...]] = []
    json_bytes = 0
    resolved_at = datetime.now(UTC)
    for key in keys:
        body = storage.read_response(key)
        document = AnnualAccountDocument.model_validate_json(body)
        if document.filing_year != filing_year:
            raise RuntimeError(
                f"Annual-account JSON filing year mismatch: key={key} "
                f"expected={filing_year} actual={document.filing_year}"
            )
        source_json_sha256 = hashlib.sha256(body).hexdigest()
        rows.append(
            (
                document.document_id,
                document.country_iso2,
                SOURCE_SLUG,
                source_run_id,
                document.org_number,
                document.legal_name,
                filing_year,
                chunk_key,
                key,
                f"s3://{SOURCE_BUCKET}/{key}",
                source_json_sha256,
                document.source_pdf_url,
                document.source_pdf_sha256,
                document.source_pdf_size_bytes,
                datetime.fromisoformat(document.retrieved_at.replace("Z", "+00:00")),
                document.pdf_page_count,
                document.native_text_page_count,
                document.ocr_page_count,
                "loaded",
                "[]",
                0,
                PARSER_VERSION,
                resolved_at,
            )
        )
        json_bytes += len(body)

    connection.execute("begin transaction")
    try:
        connection.execute(
            f"delete from {ANNUAL_ACCOUNT_DATASET}.documents "
            "where source_filing_year = ? and source_chunk = ?",
            [filing_year, chunk_key],
        )
        if rows:
            connection.executemany(
                f"insert into {ANNUAL_ACCOUNT_DATASET}.documents values "
                f"({', '.join(['?'] * 23)})",
                rows,
            )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
    return {"document_count": len(rows), "json_bytes": json_bytes}


def replace_annual_account_facts(
    *,
    connection: duckdb.DuckDBPyConnection,
    storage: Any,
    filing_year: int,
    chunk_key: str,
    source_run_id: str,
) -> dict[str, int]:
    ensure_annual_account_duckdb_schema(connection)
    documents = connection.execute(
        f"""
        select document_id, source_json_object_key, source_json_sha256
        from {ANNUAL_ACCOUNT_DATASET}.documents
        where source_filing_year = ? and source_chunk = ?
        order by document_id
        """,
        [filing_year, chunk_key],
    ).fetchall()
    fact_rows: list[tuple[Any, ...]] = []
    fact_counts: dict[str, int] = {}
    resolved_at = datetime.now(UTC)
    for document_id, key, expected_json_sha256 in documents:
        body = storage.read_response(key)
        actual_json_sha256 = hashlib.sha256(body).hexdigest()
        if actual_json_sha256 != expected_json_sha256:
            raise RuntimeError(
                f"Annual-account JSON hash mismatch: key={key} "
                f"expected={expected_json_sha256} actual={actual_json_sha256}"
            )
        document = AnnualAccountDocument.model_validate_json(body)
        facts = extract_annual_account_facts(
            document,
            source_json_sha256=actual_json_sha256,
        )
        fact_counts[str(document_id)] = len(facts)
        fact_rows.extend(
            _fact_row(
                fact,
                source_run_id=source_run_id,
                source_chunk=chunk_key,
                resolved_at=resolved_at,
            )
            for fact in facts
        )

    connection.execute("begin transaction")
    try:
        connection.execute(
            f"delete from {ANNUAL_ACCOUNT_DATASET}.facts "
            "where source_filing_year = ? and source_chunk = ?",
            [filing_year, chunk_key],
        )
        if fact_rows:
            connection.executemany(
                f"insert into {ANNUAL_ACCOUNT_DATASET}.facts values "
                f"({', '.join(['?'] * 40)})",
                fact_rows,
            )
        for document_id, count in fact_counts.items():
            connection.execute(
                f"update {ANNUAL_ACCOUNT_DATASET}.documents "
                "set parse_status = 'parsed', fact_count = ?, parser_version = ?, "
                "resolved_at = ? where document_id = ?",
                [count, PARSER_VERSION, resolved_at, document_id],
            )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
    return {"document_count": len(documents), "fact_count": len(fact_rows)}


def apply_builtin_concept_mappings(
    *,
    connection: duckdb.DuckDBPyConnection,
    filing_year: int,
    chunk_key: str,
) -> dict[str, int]:
    ensure_annual_account_duckdb_schema(connection)
    labels = connection.execute(
        f"""
        select distinct normalized_label, statement_type
        from {ANNUAL_ACCOUNT_DATASET}.facts
        where source_filing_year = ? and source_chunk = ?
        """,
        [filing_year, chunk_key],
    ).fetchall()
    rows = [
        (
            normalized_label,
            statement_type,
            BUILTIN_CONCEPTS[normalized_label],
            "dictionary",
            1.0,
            None,
            LLM_PROMPT_VERSION,
            None,
            datetime.now(UTC),
        )
        for normalized_label, statement_type in labels
        if normalized_label in BUILTIN_CONCEPTS
    ]
    if rows:
        connection.executemany(
            f"insert or replace into {ANNUAL_ACCOUNT_DATASET}.concept_mappings "
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    _apply_mapping_table_to_facts(
        connection,
        filing_year=filing_year,
        chunk_key=chunk_key,
    )
    return {"dictionary_mapping_count": len(rows)}


def apply_llm_concept_mappings(
    *,
    connection: duckdb.DuckDBPyConnection,
    filing_year: int,
    chunk_key: str,
    base_url: str,
    api_key: str,
    model: str,
    batch_size: int,
    workers: int,
    timeout_seconds: int,
) -> dict[str, int]:
    apply_builtin_concept_mappings(
        connection=connection,
        filing_year=filing_year,
        chunk_key=chunk_key,
    )
    pending = connection.execute(
        f"""
        select distinct facts.normalized_label, facts.statement_type
        from {ANNUAL_ACCOUNT_DATASET}.facts as facts
        left join {ANNUAL_ACCOUNT_DATASET}.concept_mappings as mappings
          on mappings.normalized_label = facts.normalized_label
         and mappings.statement_type = facts.statement_type
        where facts.source_filing_year = ? and facts.source_chunk = ?
          and mappings.normalized_label is null
        order by facts.statement_type, facts.normalized_label
        """,
        [filing_year, chunk_key],
    ).fetchall()
    if not pending:
        return {"requested_mapping_count": 0, "llm_mapping_count": 0}
    if not 1 <= batch_size <= LLM_MAX_BATCH_SIZE:
        raise ValueError(
            f"LLM mapping batch size must be between 1 and {LLM_MAX_BATCH_SIZE}"
        )
    if workers < 1 or timeout_seconds < 1:
        raise ValueError("LLM mapping workers and timeout must be positive")

    client = OpenAI(
        base_url=base_url.rstrip("/"),
        api_key=api_key,
        timeout=float(timeout_seconds),
        max_retries=2,
    )
    batches = [
        pending[index : index + batch_size]
        for index in range(0, len(pending), batch_size)
    ]
    completed: list[
        tuple[list[tuple[str, str]], AnnualAccountConceptMappingResponse, str]
    ] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _request_llm_mappings, client, batch=batch, model=model
            ): batch
            for batch in batches
        }
        for future in as_completed(futures):
            response, raw_response = future.result()
            completed.append((futures[future], response, raw_response))

    mapped_at = datetime.now(UTC)
    rows: list[tuple[Any, ...]] = []
    for batch, response, raw_response in completed:
        by_id = {mapping.input_id: mapping for mapping in response.mappings}
        if set(by_id) != set(range(len(batch))):
            raise RuntimeError(
                "LLM concept mapping response does not cover every input label"
            )
        for input_id, (normalized_label, statement_type) in enumerate(batch):
            mapping = by_id[input_id]
            canonical_concept = mapping.canonical_concept
            if canonical_concept is not None and canonical_concept not in METRIC_NAMES:
                raise RuntimeError(
                    f"LLM returned unsupported canonical concept: {canonical_concept}"
                )
            rows.append(
                (
                    normalized_label,
                    statement_type,
                    canonical_concept,
                    "llm" if canonical_concept is not None else "unmapped",
                    mapping.confidence,
                    model,
                    LLM_PROMPT_VERSION,
                    raw_response,
                    mapped_at,
                )
            )
    connection.executemany(
        f"insert or replace into {ANNUAL_ACCOUNT_DATASET}.concept_mappings "
        "values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    _apply_mapping_table_to_facts(
        connection,
        filing_year=filing_year,
        chunk_key=chunk_key,
    )
    return {"requested_mapping_count": len(pending), "llm_mapping_count": len(rows)}


def apply_annual_account_usd_conversion(
    *,
    connection: duckdb.DuckDBPyConnection,
    exchange_rates: Any,
    filing_year: int,
    chunk_key: str,
) -> dict[str, int]:
    ensure_annual_account_duckdb_schema(connection)
    pairs = connection.execute(
        f"""
        select distinct upper(currency), cast(period_end_date as varchar)
        from {ANNUAL_ACCOUNT_DATASET}.facts
        where source_filing_year = ? and source_chunk = ?
          and amount_original is not null and amount_usd is null
          and currency <> '' and period_end_date is not null
        """,
        [filing_year, chunk_key],
    ).fetchall()
    requests = [
        ExchangeRateRequest(currency=currency, rate_date=rate_date)
        for currency, rate_date in pairs
    ]
    rates: dict[tuple[str, str], Any] = {}
    for start in range(0, len(requests), 50):
        batch = requests[start : start + 50]
        try:
            rates.update(exchange_rates.usd_rates(batch))
        except LookupError:
            for request in batch:
                try:
                    rates.update(exchange_rates.usd_rates([request]))
                except LookupError:
                    continue
    converted = 0
    for currency, rate_date in pairs:
        rate = rates.get((currency, rate_date))
        if rate is None:
            continue
        matching_before = int(
            connection.execute(
                f"""
                select count(*)
                from {ANNUAL_ACCOUNT_DATASET}.facts
                where source_filing_year = ? and source_chunk = ?
                  and upper(currency) = ? and period_end_date = cast(? as date)
                  and amount_original is not null and amount_usd is null
                """,
                [filing_year, chunk_key, currency, rate_date],
            ).fetchone()[0]
        )
        connection.execute(
            f"""
            update {ANNUAL_ACCOUNT_DATASET}.facts
            set amount_usd = cast(amount_original * ? as decimal(38, 10)),
                fx_rate_to_usd = ?, fx_rate_date = ?, fx_source = ?
            where source_filing_year = ? and source_chunk = ?
              and upper(currency) = ? and period_end_date = cast(? as date)
              and amount_original is not null and amount_usd is null
            """,
            [
                rate.rate,
                rate.rate,
                rate.rate_date,
                rate.source,
                filing_year,
                chunk_key,
                currency,
                rate_date,
            ],
        )
        matching_after = int(
            connection.execute(
                f"""
                select count(*)
                from {ANNUAL_ACCOUNT_DATASET}.facts
                where source_filing_year = ? and source_chunk = ?
                  and upper(currency) = ? and period_end_date = cast(? as date)
                  and amount_original is not null and amount_usd is null
                """,
                [filing_year, chunk_key, currency, rate_date],
            ).fetchone()[0]
        )
        converted += matching_before - matching_after
    remaining = int(
        connection.execute(
            f"select count(*) from {ANNUAL_ACCOUNT_DATASET}.facts "
            "where source_filing_year = ? and source_chunk = ? "
            "and amount_original is not null and amount_usd is null",
            [filing_year, chunk_key],
        ).fetchone()[0]
    )
    return {"converted_fact_count": converted, "unconverted_fact_count": remaining}


def build_annual_account_metrics(
    *,
    connection: duckdb.DuckDBPyConnection,
    filing_year: int,
    chunk_key: str,
    source_run_id: str,
) -> dict[str, int]:
    ensure_annual_account_duckdb_schema(connection)
    original_columns = ",\n".join(
        f"max(facts.amount_original) filter (where canonical_concept = '{metric}' "
        f"and concept_rank = 1 and mapping_confidence >= 0.95 "
        f"and ocr_confidence >= 80) as {metric}_amount_original"
        for metric in METRIC_NAMES
    )
    usd_columns = ",\n".join(
        f"max(facts.amount_usd) filter (where canonical_concept = '{metric}' "
        f"and concept_rank = 1 and mapping_confidence >= 0.95 "
        f"and ocr_confidence >= 80) as {metric}_amount_usd"
        for metric in METRIC_NAMES
    )
    connection.execute(
        f"delete from {ANNUAL_ACCOUNT_DATASET}.metrics "
        "where source_filing_year = ? and source_chunk = ?",
        [filing_year, chunk_key],
    )
    connection.execute(
        f"""
        insert into {ANNUAL_ACCOUNT_DATASET}.metrics by name
        with ranked_facts as (
            select
                facts.*,
                row_number() over (
                    partition by document_id, fiscal_year, canonical_concept
                    order by
                        case mapping_method when 'dictionary' then 0 else 1 end,
                        case
                            when statement_type in ('income_statement', 'balance_sheet')
                                then 0
                            else 1
                        end,
                        case when normalized_label like 'sum %' then 0 else 1 end,
                        mapping_confidence desc nulls last,
                        ocr_confidence desc,
                        fact_ordinal
                ) as concept_rank
            from {ANNUAL_ACCOUNT_DATASET}.facts as facts
            where facts.source_filing_year = ? and facts.source_chunk = ?
        )
        select
            sha256(facts.document_id || ':' || cast(facts.fiscal_year as varchar)) as metric_id,
            facts.document_id,
            'NO' as country_iso2,
            '{SOURCE_SLUG}' as source_slug,
            ? as source_run_id,
            facts.org_number,
            documents.legal_name,
            facts.source_filing_year,
            facts.source_chunk,
            facts.fiscal_year,
            max(facts.period_end_date) as period_end_date,
            bool_or(facts.is_comparative) as is_comparative,
            max(facts.currency) as currency,
            {original_columns},
            {usd_columns},
            count(*) as source_fact_count,
            count(*) filter (where canonical_concept is not null) as mapped_fact_count,
            count(*) filter (where canonical_concept is null) as unmapped_numeric_fact_count,
            'validated' as validation_status,
            '[]' as metric_warnings,
            to_json(
                list(
                    struct_pack(concept := canonical_concept, fact_id := fact_id)
                    order by fact_ordinal
                ) filter (where canonical_concept is not null and concept_rank = 1)
            ) as source_fact_ids,
            '{MAPPING_VERSION}' as mapping_version,
            max(facts.fx_rate_to_usd) as fx_rate_to_usd,
            max(facts.fx_rate_date) as fx_rate_date,
            max(facts.fx_source) as fx_source,
            documents.source_pdf_url,
            documents.source_json_uri,
            documents.source_json_sha256,
            now() as resolved_at
        from ranked_facts as facts
        inner join {ANNUAL_ACCOUNT_DATASET}.documents as documents using (document_id)
        where facts.fiscal_year is not null
        group by
            facts.document_id, facts.org_number, documents.legal_name,
            facts.source_filing_year, facts.source_chunk, facts.fiscal_year,
            documents.source_pdf_url, documents.source_json_uri, documents.source_json_sha256
        having count(*) filter (
            where canonical_concept is not null
              and mapping_confidence >= 0.95 and ocr_confidence >= 80
        ) > 0
        """,
        [filing_year, chunk_key, source_run_id],
    )
    _mark_metrics_requiring_review(
        connection,
        filing_year=filing_year,
        chunk_key=chunk_key,
    )
    metric_count = int(
        connection.execute(
            f"select count(*) from {ANNUAL_ACCOUNT_DATASET}.metrics "
            "where source_filing_year = ? and source_chunk = ?",
            [filing_year, chunk_key],
        ).fetchone()[0]
    )
    return {"metric_row_count": metric_count}


def _mark_metrics_requiring_review(
    connection: duckdb.DuckDBPyConnection,
    *,
    filing_year: int,
    chunk_key: str,
) -> None:
    duplicate_rows = connection.execute(
        f"""
        select distinct metrics.metric_id
        from {ANNUAL_ACCOUNT_DATASET}.metrics as metrics
        inner join (
            select document_id, fiscal_year, canonical_concept
            from {ANNUAL_ACCOUNT_DATASET}.facts
            where source_filing_year = ? and source_chunk = ?
              and canonical_concept is not null
              and mapping_confidence >= 0.95 and ocr_confidence >= 80
            group by document_id, fiscal_year, canonical_concept
            having count(distinct numeric_value) > 1
        ) as duplicates
          on duplicates.document_id = metrics.document_id
         and duplicates.fiscal_year = metrics.fiscal_year
        """,
        [filing_year, chunk_key],
    ).fetchall()
    warnings_by_metric: dict[str, list[str]] = {
        str(metric_id): ["duplicate_canonical_concept_values"]
        for (metric_id,) in duplicate_rows
    }
    balance_rows = connection.execute(
        f"""
        select metric_id
        from {ANNUAL_ACCOUNT_DATASET}.metrics
        where source_filing_year = ? and source_chunk = ?
          and total_assets_amount_original is not null
          and equity_amount_original is not null
          and total_liabilities_amount_original is not null
          and abs(
                total_assets_amount_original
                - (equity_amount_original + total_liabilities_amount_original)
              ) > greatest(1, abs(total_assets_amount_original) * 0.001)
        """,
        [filing_year, chunk_key],
    ).fetchall()
    for (metric_id,) in balance_rows:
        warnings_by_metric.setdefault(str(metric_id), []).append(
            "balance_equation_failed"
        )
    if not warnings_by_metric:
        return
    connection.executemany(
        f"""
        update {ANNUAL_ACCOUNT_DATASET}.metrics
        set validation_status = 'review', metric_warnings = ?
        where metric_id = ?
        """,
        [
            (json.dumps(warnings, sort_keys=True), metric_id)
            for metric_id, warnings in warnings_by_metric.items()
        ],
    )


def llm_settings() -> tuple[str, str, str]:
    return (
        os.getenv(
            "NORWAY_ANNUAL_ACCOUNT_LLM_BASE_URL",
            "http://100.77.62.33:8888/v1",
        ),
        os.getenv(
            "NORWAY_ANNUAL_ACCOUNT_LLM_MODEL",
            "RedHatAI/Qwen3.6-35B-A3B-NVFP4",
        ),
        os.getenv("NORWAY_ANNUAL_ACCOUNT_LLM_API_KEY", "x"),
    )


def _page_facts(
    document: AnnualAccountDocument,
    *,
    page: AnnualAccountPage,
    source_json_sha256: str,
    first_ordinal: int,
    document_currency: str,
    document_unit_scale: Decimal,
) -> list[ExtractedAnnualAccountFact]:
    lines = _page_lines(page)
    year_header = _select_year_header(lines, filing_year=document.filing_year)
    if year_header is None:
        return []
    header_line_number, year_columns = year_header
    duplicate_period_columns = len({year for year, _center in year_columns}) < len(
        year_columns
    )
    statement_type, table_title = _statement_type(page.text)
    page_currency, page_unit_scale = _currency_and_scale(page.text)
    currency = page_currency or document_currency
    unit_scale = (
        page_unit_scale if _has_explicit_unit_scale(page.text) else document_unit_scale
    )
    fact_ordinal = first_ordinal
    facts: list[ExtractedAnnualAccountFact] = []
    for line_number, words in lines:
        values = _line_values(
            words,
            year_columns=year_columns,
            is_header_line=line_number == header_line_number,
        )
        if not values:
            continue
        label_words = _label_words(words, year_columns=year_columns)
        raw_label = " ".join(word.text for word in label_words).strip(" :-")
        normalized_label = _normalize_label(raw_label)
        if len(normalized_label) < 2 or _ignore_label(normalized_label):
            continue
        evidence = " ".join(word.text for word in words)
        for column_index, year, value_words, numeric_value in values:
            quality_flags = ["period_end_inferred_from_year"]
            if duplicate_period_columns:
                quality_flags.append("ambiguous_duplicate_period_columns")
            confidence = sum(word.confidence for word in value_words) / len(value_words)
            if confidence < 80:
                quality_flags.append("low_ocr_confidence")
            raw_value = " ".join(word.text for word in value_words)
            fact_key = (
                f"{document.document_id}:{page.page_number}:{line_number}:"
                f"{column_index}:{year}:{raw_label}:{raw_value}"
            )
            bbox = _union_bbox(value_words)
            facts.append(
                ExtractedAnnualAccountFact(
                    fact_id=hashlib.sha256(fact_key.encode()).hexdigest(),
                    document_id=document.document_id,
                    org_number=document.org_number,
                    source_filing_year=document.filing_year,
                    fact_ordinal=fact_ordinal,
                    page_number=page.page_number,
                    line_number=line_number,
                    statement_type=statement_type,
                    table_title=table_title,
                    raw_label=raw_label,
                    normalized_label=normalized_label,
                    column_label=(
                        f"{year}:column_{column_index + 1}"
                        if duplicate_period_columns
                        else str(year)
                    ),
                    fiscal_year=year,
                    period_end_date=f"{year}-12-31",
                    is_comparative=year != year_columns[0][0],
                    value_kind="monetary" if currency else "numeric",
                    raw_value=raw_value,
                    numeric_value=numeric_value,
                    currency=currency,
                    unit_scale=unit_scale,
                    amount_original=numeric_value * unit_scale if currency else None,
                    bbox=bbox,
                    evidence=evidence,
                    ocr_confidence=confidence,
                    extraction_method="word_geometry",
                    quality_flags=tuple(quality_flags),
                    source_json_sha256=source_json_sha256,
                    parser_version=PARSER_VERSION,
                )
            )
            fact_ordinal += 1
    return facts


def _page_lines(page: AnnualAccountPage) -> list[tuple[int, list[AnnualAccountWord]]]:
    grouped: dict[tuple[int, int, int], list[AnnualAccountWord]] = defaultdict(list)
    for word in page.words:
        grouped[(word.block_number, word.paragraph_number, word.line_number)].append(
            word
        )
    ordered = sorted(
        grouped.items(),
        key=lambda item: (
            min(word.bbox[1] for word in item[1]),
            min(word.bbox[0] for word in item[1]),
        ),
    )
    return [
        (
            index,
            sorted(words, key=lambda word: (word.bbox[0], word.word_number)),
        )
        for index, (_source_line, words) in enumerate(ordered, start=1)
    ]


def _select_year_header(
    lines: Sequence[tuple[int, Sequence[AnnualAccountWord]]],
    *,
    filing_year: int,
) -> tuple[int, list[tuple[int, float]]] | None:
    candidates: list[
        tuple[tuple[int, int, int, int], int, list[tuple[int, float]]]
    ] = []
    for line_index, (line_number, words) in enumerate(lines):
        columns: list[tuple[int, float]] = []
        full_date_count = 0
        for word in words:
            period = _accounting_period_year(word.text)
            if period is None:
                continue
            year, is_full_date = period
            if not is_full_date and not filing_year - 5 <= year <= filing_year + 1:
                continue
            columns.append((year, (word.bbox[0] + word.bbox[2]) / 2))
            full_date_count += int(is_full_date)
        if columns:
            ordered_columns = sorted(columns, key=lambda value: value[1])
            distinct_years = {year for year, _center in ordered_columns}
            if len(ordered_columns) > 1 and len(distinct_years) == 1:
                previous_words = lines[line_index - 1][1] if line_index > 0 else ()
                scope_text = " ".join(word.text for word in (*previous_words, *words))
                if _ACCOUNT_SCOPE_PATTERN.search(scope_text) is None:
                    continue
            score = (
                len(distinct_years),
                len(ordered_columns),
                full_date_count,
                -line_number,
            )
            candidates.append((score, line_number, ordered_columns))
    if not candidates:
        return None
    _score, line_number, columns = max(candidates, key=lambda candidate: candidate[0])
    return line_number, columns


def _accounting_period_year(text: str) -> tuple[int, bool] | None:
    token = text.strip()
    if _YEAR_PATTERN.fullmatch(token):
        return int(token), False
    for pattern in (_DAY_FIRST_DATE_PATTERN, _YEAR_FIRST_DATE_PATTERN):
        match = pattern.fullmatch(token)
        if match is None:
            continue
        year = int(match.group("year"))
        month = int(match.group("month"))
        day = int(match.group("day"))
        try:
            date(year, month, day)
        except ValueError:
            return None
        return year, True
    return None


def _line_values(
    words: Sequence[AnnualAccountWord],
    *,
    year_columns: Sequence[tuple[int, float]],
    is_header_line: bool,
) -> list[tuple[int, int, list[AnnualAccountWord], Decimal]]:
    if is_header_line:
        return []
    centers = [center for _year, center in year_columns]
    if len(centers) == 1:
        gap = 0.18
        boundaries = [centers[0] - gap * 0.5, centers[0] + gap * 0.5]
    else:
        boundaries = [centers[0] - (centers[1] - centers[0]) * 0.5]
        boundaries.extend(
            (left_center + right_center) / 2
            for left_center, right_center in zip(centers, centers[1:])
        )
        boundaries.append(centers[-1] + (centers[-1] - centers[-2]) * 0.5)
    values: list[tuple[int, int, list[AnnualAccountWord], Decimal]] = []
    for index, (year, _center) in enumerate(year_columns):
        selected = [
            word
            for word in words
            if boundaries[index]
            <= (word.bbox[0] + word.bbox[2]) / 2
            < boundaries[index + 1]
            and _NUMBER_TOKEN_PATTERN.fullmatch(word.text.strip())
        ]
        if not selected:
            continue
        numeric_value = _parse_numeric_words(selected)
        if numeric_value is not None:
            values.append((index, year, selected, numeric_value))
    return values


def _label_words(
    words: Sequence[AnnualAccountWord],
    *,
    year_columns: Sequence[tuple[int, float]],
) -> list[AnnualAccountWord]:
    centers = [center for _year, center in year_columns]
    gap = centers[1] - centers[0] if len(centers) > 1 else 0.18
    label = [
        word
        for word in words
        if (word.bbox[0] + word.bbox[2]) / 2 < centers[0] - gap * 0.5
    ]
    while label and _NOTE_PATTERN.fullmatch(label[-1].text.strip()):
        label.pop()
    return label


def _parse_numeric_words(words: Sequence[AnnualAccountWord]) -> Decimal | None:
    text = "".join(word.text.strip() for word in words)
    negative_parentheses = text.startswith("(") and text.endswith(")")
    text = text.strip("()[]").replace(" ", "").replace(",", ".")
    try:
        value = Decimal(text)
    except InvalidOperation:
        return None
    return -value if negative_parentheses else value


def _statement_type(page_text: str) -> tuple[str, str]:
    normalized = page_text.upper()
    if "RESULTATREGNSKAP" in normalized:
        return "income_statement", "RESULTATREGNSKAP"
    if "BALANSE" in normalized:
        return "balance_sheet", "BALANSE"
    if "KONTANTSTR" in normalized:
        return "cash_flow", "KONTANTSTRØM"
    if "EGENKAPITAL" in normalized:
        return "equity", "EGENKAPITAL"
    if "NOTE" in normalized:
        return "note", "NOTE"
    return "other", ""


def _currency_and_scale(page_text: str) -> tuple[str, Decimal]:
    normalized = page_text.upper()
    currency_match = re.search(r"BELØP\s+I\s*:\s*([A-Z]{3})", normalized)
    currency = currency_match.group(1) if currency_match is not None else ""
    scale = (
        Decimal("1000000")
        if "MILLION" in normalized
        else Decimal("1000")
        if "TUSEN" in normalized
        else Decimal("1")
    )
    return currency, scale


def _has_explicit_unit_scale(page_text: str) -> bool:
    normalized = page_text.upper()
    return "MILLION" in normalized or "TUSEN" in normalized


def _normalize_label(label: str) -> str:
    return _LABEL_CLEAN_PATTERN.sub(" ", label.casefold()).strip()


def _ignore_label(normalized_label: str) -> bool:
    return normalized_label.startswith(
        (
            "organisasjonsnr",
            "organisasjonsnummer",
            "utskriftsdato",
            "side ",
        )
    )


def _union_bbox(
    words: Sequence[AnnualAccountWord],
) -> tuple[float, float, float, float]:
    return (
        min(word.bbox[0] for word in words),
        min(word.bbox[1] for word in words),
        max(word.bbox[2] for word in words),
        max(word.bbox[3] for word in words),
    )


def _fact_row(
    fact: ExtractedAnnualAccountFact,
    *,
    source_run_id: str,
    source_chunk: str,
    resolved_at: datetime,
) -> tuple[Any, ...]:
    return (
        fact.fact_id,
        fact.document_id,
        "NO",
        SOURCE_SLUG,
        source_run_id,
        fact.org_number,
        fact.source_filing_year,
        source_chunk,
        fact.fact_ordinal,
        fact.page_number,
        fact.line_number,
        fact.statement_type,
        fact.table_title,
        fact.raw_label,
        fact.normalized_label,
        fact.canonical_concept,
        fact.column_label,
        fact.fiscal_year,
        fact.period_end_date,
        fact.is_comparative,
        fact.value_kind,
        fact.raw_value,
        fact.numeric_value,
        fact.currency,
        fact.unit_scale,
        fact.amount_original,
        fact.amount_usd,
        fact.fx_rate_to_usd,
        fact.fx_rate_date,
        fact.fx_source,
        json.dumps(fact.bbox),
        fact.evidence,
        fact.ocr_confidence,
        fact.extraction_method,
        fact.mapping_method,
        fact.mapping_confidence,
        json.dumps(fact.quality_flags),
        fact.source_json_sha256,
        fact.parser_version,
        resolved_at,
    )


def _apply_mapping_table_to_facts(
    connection: duckdb.DuckDBPyConnection,
    *,
    filing_year: int,
    chunk_key: str,
) -> None:
    connection.execute(
        f"""
        update {ANNUAL_ACCOUNT_DATASET}.facts as facts
        set canonical_concept = mappings.canonical_concept,
            mapping_method = mappings.mapping_method,
            mapping_confidence = mappings.mapping_confidence
        from {ANNUAL_ACCOUNT_DATASET}.concept_mappings as mappings
        where facts.normalized_label = mappings.normalized_label
          and facts.statement_type = mappings.statement_type
          and facts.source_filing_year = ? and facts.source_chunk = ?
        """,
        [filing_year, chunk_key],
    )


def _request_llm_mappings(
    client: OpenAI,
    *,
    batch: Sequence[tuple[str, str]],
    model: str,
) -> tuple[AnnualAccountConceptMappingResponse, str]:
    inputs = [
        {"input_id": index, "label": label, "statement_type": statement_type}
        for index, (label, statement_type) in enumerate(batch)
    ]
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "Map Norwegian accounting labels to the supplied canonical allowlist. "
                    "Map only when the label itself is an exact equivalent of the canonical "
                    "measure. Never roll a component into a broader total: share capital is "
                    "not total equity, an individual receivable is not total receivables, "
                    "tax payable is not tax expense, and an individual liability is not "
                    "total liabilities. Use null whenever the label is a component, note, "
                    "heading, company name, or has no exact semantic mapping."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {"allowlist": METRIC_NAMES, "inputs": inputs},
                    ensure_ascii=False,
                ),
            },
        ],
        temperature=0,
        max_tokens=LLM_MAX_TOKENS,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "annual_account_concept_mappings",
                "schema": AnnualAccountConceptMappingResponse.model_json_schema(),
                "strict": True,
            },
        },
        extra_body={"chat_template_kwargs": {"enable_thinking": False}},
    )
    content = response.choices[0].message.content
    if content is None:
        raise RuntimeError("LLM concept mapping returned no content")
    json_start = content.find("{")
    json_end = content.rfind("}")
    if json_start < 0 or json_end < json_start:
        raise RuntimeError("LLM concept mapping did not return a JSON object")
    raw_response = content[json_start : json_end + 1]
    return AnnualAccountConceptMappingResponse.model_validate_json(
        raw_response
    ), raw_response
