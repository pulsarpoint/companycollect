from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
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
import pyarrow as pa

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
MAPPING_VERSION = "norway-annual-account-concepts-v2"
LLM_PROMPT_VERSION = "norway-annual-account-label-map-v3"
LLM_MAX_TOKENS = 4_096
LLM_MAX_BATCH_SIZE = 2
SOURCE_SLUG = "norway_brreg_annual_accounts_pdf"
SOURCE_BUCKET = "source-norway-brreg"
DEFAULT_INSERT_BATCH_ROWS = 50_000
_DOCUMENT_STAGE_TABLE = "_norway_annual_account_document_stage"
_DOCUMENT_BATCH_RELATION = "_norway_annual_account_document_batch"
_FACT_STAGE_TABLE = "_norway_annual_account_fact_stage"
_FACT_BATCH_RELATION = "_norway_annual_account_fact_batch"
_FACT_COUNT_STAGE_TABLE = "_norway_annual_account_fact_count_stage"
_FACT_COUNT_BATCH_RELATION = "_norway_annual_account_fact_count_batch"
_CONCEPT_MAPPING_BATCH_RELATION = "_norway_annual_account_concept_mapping_batch"
_FX_STAGE_TABLE = "_norway_annual_account_fx_stage"
_FX_BATCH_RELATION = "_norway_annual_account_fx_batch"
_VALIDATION_WARNING_BATCH_RELATION = (
    "_norway_annual_account_validation_warning_batch"
)

_DOCUMENT_COLUMNS = (
    "document_id",
    "country_iso2",
    "source_slug",
    "source_run_id",
    "org_number",
    "legal_name",
    "source_filing_year",
    "source_chunk",
    "source_json_object_key",
    "source_json_uri",
    "source_json_sha256",
    "source_pdf_url",
    "source_pdf_sha256",
    "source_pdf_size_bytes",
    "retrieved_at",
    "pdf_page_count",
    "native_text_page_count",
    "ocr_page_count",
    "parse_status",
    "parse_warnings",
    "fact_count",
    "parser_version",
    "resolved_at",
)
_DOCUMENT_ARROW_SCHEMA = pa.schema(
    [
        pa.field("document_id", pa.string(), nullable=False),
        pa.field("country_iso2", pa.string(), nullable=False),
        pa.field("source_slug", pa.string(), nullable=False),
        pa.field("source_run_id", pa.string(), nullable=False),
        pa.field("org_number", pa.string(), nullable=False),
        pa.field("legal_name", pa.string(), nullable=False),
        pa.field("source_filing_year", pa.int32(), nullable=False),
        pa.field("source_chunk", pa.string(), nullable=False),
        pa.field("source_json_object_key", pa.string(), nullable=False),
        pa.field("source_json_uri", pa.string(), nullable=False),
        pa.field("source_json_sha256", pa.string(), nullable=False),
        pa.field("source_pdf_url", pa.string(), nullable=False),
        pa.field("source_pdf_sha256", pa.string(), nullable=False),
        pa.field("source_pdf_size_bytes", pa.int64(), nullable=False),
        pa.field("retrieved_at", pa.timestamp("us", tz="UTC")),
        pa.field("pdf_page_count", pa.int32(), nullable=False),
        pa.field("native_text_page_count", pa.int32(), nullable=False),
        pa.field("ocr_page_count", pa.int32(), nullable=False),
        pa.field("parse_status", pa.string(), nullable=False),
        pa.field("parse_warnings", pa.string(), nullable=False),
        pa.field("fact_count", pa.int64(), nullable=False),
        pa.field("parser_version", pa.string(), nullable=False),
        pa.field("resolved_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)
_FACT_COLUMNS = (
    "fact_id",
    "document_id",
    "country_iso2",
    "source_slug",
    "source_run_id",
    "org_number",
    "source_filing_year",
    "source_chunk",
    "fact_ordinal",
    "page_number",
    "line_number",
    "statement_type",
    "table_title",
    "raw_label",
    "normalized_label",
    "canonical_concept",
    "column_label",
    "fiscal_year",
    "period_end_date",
    "is_comparative",
    "value_kind",
    "raw_value",
    "numeric_value",
    "currency",
    "unit_scale",
    "amount_original",
    "amount_usd",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "bbox",
    "evidence",
    "ocr_confidence",
    "extraction_method",
    "mapping_method",
    "mapping_confidence",
    "quality_flags",
    "source_json_sha256",
    "parser_version",
    "resolved_at",
)
_FACT_ARROW_SCHEMA = pa.schema(
    [
        pa.field("fact_id", pa.string(), nullable=False),
        pa.field("document_id", pa.string(), nullable=False),
        pa.field("country_iso2", pa.string(), nullable=False),
        pa.field("source_slug", pa.string(), nullable=False),
        pa.field("source_run_id", pa.string(), nullable=False),
        pa.field("org_number", pa.string(), nullable=False),
        pa.field("source_filing_year", pa.int32(), nullable=False),
        pa.field("source_chunk", pa.string(), nullable=False),
        pa.field("fact_ordinal", pa.int64(), nullable=False),
        pa.field("page_number", pa.int32(), nullable=False),
        pa.field("line_number", pa.int32(), nullable=False),
        pa.field("statement_type", pa.string(), nullable=False),
        pa.field("table_title", pa.string(), nullable=False),
        pa.field("raw_label", pa.string(), nullable=False),
        pa.field("normalized_label", pa.string(), nullable=False),
        pa.field("canonical_concept", pa.string()),
        pa.field("column_label", pa.string(), nullable=False),
        pa.field("fiscal_year", pa.int32()),
        pa.field("period_end_date", pa.string()),
        pa.field("is_comparative", pa.bool_(), nullable=False),
        pa.field("value_kind", pa.string(), nullable=False),
        pa.field("raw_value", pa.string(), nullable=False),
        pa.field("numeric_value", pa.string(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("unit_scale", pa.string(), nullable=False),
        pa.field("amount_original", pa.string()),
        pa.field("amount_usd", pa.string()),
        pa.field("fx_rate_to_usd", pa.string()),
        pa.field("fx_rate_date", pa.string()),
        pa.field("fx_source", pa.string()),
        pa.field("bbox", pa.string(), nullable=False),
        pa.field("evidence", pa.string(), nullable=False),
        pa.field("ocr_confidence", pa.float64(), nullable=False),
        pa.field("extraction_method", pa.string(), nullable=False),
        pa.field("mapping_method", pa.string(), nullable=False),
        pa.field("mapping_confidence", pa.float64()),
        pa.field("quality_flags", pa.string(), nullable=False),
        pa.field("source_json_sha256", pa.string(), nullable=False),
        pa.field("parser_version", pa.string(), nullable=False),
        pa.field("resolved_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)
_FACT_COUNT_ARROW_SCHEMA = pa.schema(
    [
        pa.field("document_id", pa.string(), nullable=False),
        pa.field("fact_count", pa.int64(), nullable=False),
    ]
)
_CONCEPT_MAPPING_COLUMNS = (
    "normalized_label",
    "statement_type",
    "canonical_concept",
    "mapping_method",
    "mapping_confidence",
    "model",
    "prompt_version",
    "raw_response",
    "mapped_at",
)
_CONCEPT_MAPPING_ARROW_SCHEMA = pa.schema(
    [
        pa.field("normalized_label", pa.string(), nullable=False),
        pa.field("statement_type", pa.string(), nullable=False),
        pa.field("canonical_concept", pa.string()),
        pa.field("mapping_method", pa.string(), nullable=False),
        pa.field("mapping_confidence", pa.float64(), nullable=False),
        pa.field("model", pa.string()),
        pa.field("prompt_version", pa.string(), nullable=False),
        pa.field("raw_response", pa.string()),
        pa.field("mapped_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)
_FX_COLUMNS = (
    "currency",
    "period_end_date",
    "fx_rate",
    "fx_rate_date",
    "fx_source",
)
_FX_ARROW_SCHEMA = pa.schema(
    [
        pa.field("currency", pa.string(), nullable=False),
        pa.field("period_end_date", pa.date32(), nullable=False),
        pa.field("fx_rate", pa.string(), nullable=False),
        pa.field("fx_rate_date", pa.date32(), nullable=False),
        pa.field("fx_source", pa.string()),
    ]
)
_VALIDATION_WARNING_COLUMNS = ("metric_id", "metric_warnings")
_VALIDATION_WARNING_ARROW_SCHEMA = pa.schema(
    [
        pa.field("metric_id", pa.string(), nullable=False),
        pa.field("metric_warnings", pa.string(), nullable=False),
    ]
)

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
_METRIC_NAMES_SQL = ", ".join(f"'{name}'" for name in METRIC_NAMES)

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
_CANONICAL_CONCEPT_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,127}$")
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
    _create_document_stage_table(connection)
    keys = storage.list_annual_account_document_keys(
        filing_year=filing_year,
        chunk_key=chunk_key,
    )
    json_bytes = 0
    resolved_at = datetime.now(UTC)

    def document_rows() -> Iterable[tuple[Any, ...]]:
        nonlocal json_bytes
        for key in keys:
            body = storage.read_response(key)
            document = AnnualAccountDocument.model_validate_json(body)
            if document.filing_year != filing_year:
                raise RuntimeError(
                    f"Annual-account JSON filing year mismatch: key={key} "
                    f"expected={filing_year} actual={document.filing_year}"
                )
            source_json_sha256 = hashlib.sha256(body).hexdigest()
            json_bytes += len(body)
            yield (
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

    document_count = _append_document_stage_rows(
        connection=connection,
        rows=document_rows(),
    )

    connection.execute("begin transaction")
    try:
        connection.execute(
            f"delete from {ANNUAL_ACCOUNT_DATASET}.documents "
            "where source_filing_year = ? and source_chunk = ?",
            [filing_year, chunk_key],
        )
        if document_count:
            connection.execute(
                f"insert into {ANNUAL_ACCOUNT_DATASET}.documents "
                f"select * from {_DOCUMENT_STAGE_TABLE}"
            )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
    return {"document_count": document_count, "json_bytes": json_bytes}


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
    _create_fact_stage_tables(connection)
    fact_counts: list[tuple[str, int]] = []
    resolved_at = datetime.now(UTC)

    def fact_rows() -> Iterable[tuple[Any, ...]]:
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
            fact_counts.append((str(document_id), len(facts)))
            for fact in facts:
                yield _fact_row(
                    fact,
                    source_run_id=source_run_id,
                    source_chunk=chunk_key,
                    resolved_at=resolved_at,
                )

    fact_count = _append_fact_stage_rows(
        connection=connection,
        rows=fact_rows(),
    )
    if fact_counts:
        _insert_fact_count_stage_rows(
            connection,
            fact_counts,
        )

    connection.execute("begin transaction")
    try:
        connection.execute(
            f"delete from {ANNUAL_ACCOUNT_DATASET}.facts "
            "where source_filing_year = ? and source_chunk = ?",
            [filing_year, chunk_key],
        )
        if fact_count:
            connection.execute(
                f"insert into {ANNUAL_ACCOUNT_DATASET}.facts "
                f"select * from {_FACT_STAGE_TABLE}"
            )
        connection.execute(
            f"""
            update {ANNUAL_ACCOUNT_DATASET}.documents as documents
            set parse_status = 'parsed',
                fact_count = counts.fact_count,
                parser_version = ?,
                resolved_at = ?
            from {_FACT_COUNT_STAGE_TABLE} as counts
            where documents.document_id = counts.document_id
              and documents.source_filing_year = ?
              and documents.source_chunk = ?
            """,
            [PARSER_VERSION, resolved_at, filing_year, chunk_key],
        )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
    return {"document_count": len(documents), "fact_count": fact_count}


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
        _upsert_concept_mapping_rows(connection, rows)
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
          and (
                mappings.normalized_label is null
                or (
                    mappings.prompt_version <> ?
                    and mappings.mapping_method in (
                        'unmapped', 'llm_unsupported', 'llm_invalid'
                    )
                )
              )
        order by facts.statement_type, facts.normalized_label
        """,
        [filing_year, chunk_key, LLM_PROMPT_VERSION],
    ).fetchall()
    if not pending:
        return {
            "requested_mapping_count": 0,
            "llm_mapping_count": 0,
            "extended_mapping_count": 0,
            "unmapped_mapping_count": 0,
            "invalid_mapping_count": 0,
        }
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
    llm_mapping_count = 0
    extended_mapping_count = 0
    unmapped_mapping_count = 0
    invalid_mapping_count = 0
    for batch, response, raw_response in completed:
        by_id = {mapping.input_id: mapping for mapping in response.mappings}
        if set(by_id) != set(range(len(batch))):
            raise RuntimeError(
                "LLM concept mapping response does not cover every input label"
            )
        for input_id, (normalized_label, statement_type) in enumerate(batch):
            mapping = by_id[input_id]
            canonical_concept = mapping.canonical_concept
            if canonical_concept is None:
                mapping_method = "unmapped"
                unmapped_mapping_count += 1
            elif canonical_concept in METRIC_NAMES:
                mapping_method = "llm"
                llm_mapping_count += 1
            elif _CANONICAL_CONCEPT_PATTERN.fullmatch(canonical_concept):
                mapping_method = "llm_extended"
                extended_mapping_count += 1
            else:
                canonical_concept = None
                mapping_method = "llm_invalid"
                invalid_mapping_count += 1
            rows.append(
                (
                    normalized_label,
                    statement_type,
                    canonical_concept,
                    mapping_method,
                    mapping.confidence,
                    model,
                    LLM_PROMPT_VERSION,
                    raw_response,
                    mapped_at,
                )
            )
    _upsert_concept_mapping_rows(connection, rows)
    _apply_mapping_table_to_facts(
        connection,
        filing_year=filing_year,
        chunk_key=chunk_key,
    )
    return {
        "requested_mapping_count": len(pending),
        "llm_mapping_count": llm_mapping_count,
        "extended_mapping_count": extended_mapping_count,
        "unmapped_mapping_count": unmapped_mapping_count,
        "invalid_mapping_count": invalid_mapping_count,
    }


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
    fx_rows: list[tuple[Any, ...]] = []
    for currency, rate_date in pairs:
        rate = rates.get((currency, rate_date))
        if rate is None:
            continue
        fx_rows.append(
            (
                currency,
                date.fromisoformat(rate_date),
                str(rate.rate),
                date.fromisoformat(str(rate.rate_date)),
                rate.source,
            )
        )
    _replace_fx_stage_rows(connection, fx_rows)
    matching_before = _unconverted_annual_account_fact_count(
        connection,
        filing_year=filing_year,
        chunk_key=chunk_key,
    )
    if fx_rows:
        connection.execute(
            f"""
            update {ANNUAL_ACCOUNT_DATASET}.facts as facts
            set amount_usd = cast(
                    cast(facts.amount_original as decimal(38, 6)) * fx.fx_rate
                    as decimal(38, 10)
                ),
                fx_rate_to_usd = fx.fx_rate,
                fx_rate_date = fx.fx_rate_date,
                fx_source = fx.fx_source
            from {_FX_STAGE_TABLE} as fx
            where facts.source_filing_year = ?
              and facts.source_chunk = ?
              and upper(facts.currency) = fx.currency
              and facts.period_end_date = fx.period_end_date
              and facts.amount_original is not null
              and facts.amount_usd is null
            """,
            [filing_year, chunk_key],
        )
    remaining = _unconverted_annual_account_fact_count(
        connection,
        filing_year=filing_year,
        chunk_key=chunk_key,
    )
    return {
        "converted_fact_count": matching_before - remaining,
        "unconverted_fact_count": remaining,
    }


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
            count(*) filter (
                where canonical_concept in ({_METRIC_NAMES_SQL})
            ) as mapped_fact_count,
            count(*) filter (
                where canonical_concept is null
                   or canonical_concept not in ({_METRIC_NAMES_SQL})
            ) as unmapped_numeric_fact_count,
            'validated' as validation_status,
            '[]' as metric_warnings,
            to_json(
                list(
                    struct_pack(concept := canonical_concept, fact_id := fact_id)
                    order by fact_ordinal
                ) filter (
                    where canonical_concept in ({_METRIC_NAMES_SQL})
                      and concept_rank = 1
                )
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
            where canonical_concept in ({_METRIC_NAMES_SQL})
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
              and canonical_concept in ({_METRIC_NAMES_SQL})
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
    _update_metric_validation_warnings(
        connection,
        [
            (metric_id, json.dumps(warnings, sort_keys=True))
            for metric_id, warnings in warnings_by_metric.items()
        ],
    )


def _update_metric_validation_warnings(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[str, str]],
) -> None:
    arrow_table = _rows_to_arrow_table(
        rows=rows,
        columns=_VALIDATION_WARNING_COLUMNS,
        schema=_VALIDATION_WARNING_ARROW_SCHEMA,
    )
    connection.register(_VALIDATION_WARNING_BATCH_RELATION, arrow_table)
    try:
        connection.execute(
            f"""
            update {ANNUAL_ACCOUNT_DATASET}.metrics as metrics
            set validation_status = 'review',
                metric_warnings = warnings.metric_warnings
            from {_VALIDATION_WARNING_BATCH_RELATION} as warnings
            where metrics.metric_id = warnings.metric_id
            """
        )
    finally:
        connection.unregister(_VALIDATION_WARNING_BATCH_RELATION)


def _unconverted_annual_account_fact_count(
    connection: duckdb.DuckDBPyConnection,
    *,
    filing_year: int,
    chunk_key: str,
) -> int:
    return int(
        connection.execute(
            f"""
            select count(*)
            from {ANNUAL_ACCOUNT_DATASET}.facts
            where source_filing_year = ? and source_chunk = ?
              and amount_original is not null and amount_usd is null
            """,
            [filing_year, chunk_key],
        ).fetchone()[0]
    )


def _replace_fx_stage_rows(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[Any, ...]],
) -> None:
    connection.execute(
        f"""
        create or replace temp table {_FX_STAGE_TABLE} (
            currency varchar not null,
            period_end_date date not null,
            fx_rate decimal(38, 12) not null,
            fx_rate_date date not null,
            fx_source varchar
        )
        """
    )
    if not rows:
        return

    arrow_table = _rows_to_arrow_table(
        rows=rows,
        columns=_FX_COLUMNS,
        schema=_FX_ARROW_SCHEMA,
    )
    connection.register(_FX_BATCH_RELATION, arrow_table)
    try:
        connection.execute(
            f"""
            insert into {_FX_STAGE_TABLE}
            select
                currency,
                period_end_date,
                cast(fx_rate as decimal(38, 12)),
                fx_rate_date,
                fx_source
            from {_FX_BATCH_RELATION}
            """
        )
    finally:
        connection.unregister(_FX_BATCH_RELATION)


def llm_settings() -> tuple[str, str, str]:
    base_url = os.getenv("DEEPSEEK_URL", "").strip()
    model = os.getenv("DEEPSEEK_MODEL", "").strip()
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    missing = [
        name
        for name, value in (
            ("DEEPSEEK_URL", base_url),
            ("DEEPSEEK_MODEL", model),
            ("DEEPSEEK_API_KEY", api_key),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing required DeepSeek configuration: {', '.join(missing)}"
        )
    return base_url, model, api_key


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
        str(fact.numeric_value),
        fact.currency,
        str(fact.unit_scale),
        None if fact.amount_original is None else str(fact.amount_original),
        None if fact.amount_usd is None else str(fact.amount_usd),
        None if fact.fx_rate_to_usd is None else str(fact.fx_rate_to_usd),
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


def _create_document_stage_table(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        f"create or replace temp table {_DOCUMENT_STAGE_TABLE} as "
        f"select * from {ANNUAL_ACCOUNT_DATASET}.documents where false"
    )


def _append_document_stage_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    rows: Iterable[tuple[Any, ...]],
    batch_rows: int = DEFAULT_INSERT_BATCH_ROWS,
) -> int:
    if batch_rows < 1:
        raise ValueError("Norway annual-account document batch size must be positive")

    batch: list[tuple[Any, ...]] = []
    inserted = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_rows:
            _insert_document_stage_batch(connection, batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        _insert_document_stage_batch(connection, batch)
        inserted += len(batch)
    return inserted


def _insert_document_stage_batch(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[Any, ...]],
) -> None:
    arrow_table = _rows_to_arrow_table(
        rows=rows,
        columns=_DOCUMENT_COLUMNS,
        schema=_DOCUMENT_ARROW_SCHEMA,
    )
    connection.register(_DOCUMENT_BATCH_RELATION, arrow_table)
    try:
        connection.execute(
            f"insert into {_DOCUMENT_STAGE_TABLE} "
            f"select * from {_DOCUMENT_BATCH_RELATION}"
        )
    finally:
        connection.unregister(_DOCUMENT_BATCH_RELATION)


def _create_fact_stage_tables(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        f"create or replace temp table {_FACT_STAGE_TABLE} as "
        f"select * from {ANNUAL_ACCOUNT_DATASET}.facts where false"
    )
    connection.execute(
        f"create or replace temp table {_FACT_COUNT_STAGE_TABLE} ("
        "document_id varchar not null, fact_count bigint not null)"
    )


def _append_fact_stage_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    rows: Iterable[tuple[Any, ...]],
    batch_rows: int = DEFAULT_INSERT_BATCH_ROWS,
) -> int:
    if batch_rows < 1:
        raise ValueError("Norway annual-account fact batch size must be positive")

    batch: list[tuple[Any, ...]] = []
    inserted = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_rows:
            _insert_fact_stage_batch(connection, batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        _insert_fact_stage_batch(connection, batch)
        inserted += len(batch)
    return inserted


def _insert_fact_stage_batch(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[Any, ...]],
) -> None:
    arrow_table = _rows_to_arrow_table(
        rows=rows,
        columns=_FACT_COLUMNS,
        schema=_FACT_ARROW_SCHEMA,
    )
    connection.register(_FACT_BATCH_RELATION, arrow_table)
    try:
        connection.execute(
            f"""
            insert into {_FACT_STAGE_TABLE}
            select
                fact_id,
                document_id,
                country_iso2,
                source_slug,
                source_run_id,
                org_number,
                source_filing_year,
                source_chunk,
                fact_ordinal,
                page_number,
                line_number,
                statement_type,
                table_title,
                raw_label,
                normalized_label,
                canonical_concept,
                column_label,
                fiscal_year,
                cast(period_end_date as date),
                is_comparative,
                value_kind,
                raw_value,
                cast(numeric_value as decimal(38, 10)),
                currency,
                cast(unit_scale as decimal(38, 6)),
                cast(amount_original as decimal(38, 10)),
                cast(amount_usd as decimal(38, 10)),
                cast(fx_rate_to_usd as decimal(38, 12)),
                cast(fx_rate_date as date),
                fx_source,
                bbox,
                evidence,
                ocr_confidence,
                extraction_method,
                mapping_method,
                mapping_confidence,
                quality_flags,
                source_json_sha256,
                parser_version,
                resolved_at
            from {_FACT_BATCH_RELATION}
            """
        )
    finally:
        connection.unregister(_FACT_BATCH_RELATION)


def _insert_fact_count_stage_rows(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[str, int]],
) -> None:
    arrow_table = _rows_to_arrow_table(
        rows=rows,
        columns=("document_id", "fact_count"),
        schema=_FACT_COUNT_ARROW_SCHEMA,
    )
    connection.register(_FACT_COUNT_BATCH_RELATION, arrow_table)
    try:
        connection.execute(
            f"insert into {_FACT_COUNT_STAGE_TABLE} "
            f"select * from {_FACT_COUNT_BATCH_RELATION}"
        )
    finally:
        connection.unregister(_FACT_COUNT_BATCH_RELATION)


def _rows_to_arrow_table(
    *,
    rows: list[tuple[Any, ...]],
    columns: tuple[str, ...],
    schema: pa.Schema,
) -> pa.Table:
    if any(len(row) != len(columns) for row in rows):
        raise ValueError(
            f"Norway annual-account rows must contain {len(columns)} columns"
        )
    return pa.Table.from_arrays(
        [
            pa.array((row[index] for row in rows), type=field.type)
            for index, field in enumerate(schema)
        ],
        schema=schema,
    )


def _upsert_concept_mapping_rows(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[Any, ...]],
) -> None:
    arrow_table = _rows_to_arrow_table(
        rows=rows,
        columns=_CONCEPT_MAPPING_COLUMNS,
        schema=_CONCEPT_MAPPING_ARROW_SCHEMA,
    )
    connection.register(_CONCEPT_MAPPING_BATCH_RELATION, arrow_table)
    try:
        connection.execute(
            f"insert or replace into {ANNUAL_ACCOUNT_DATASET}.concept_mappings "
            f"select * from {_CONCEPT_MAPPING_BATCH_RELATION}"
        )
    finally:
        connection.unregister(_CONCEPT_MAPPING_BATCH_RELATION)


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
                    "Map every meaningful Norwegian accounting label to a precise, stable "
                    "English snake_case concept. Prefer a supplied core metric concept only "
                    "when it is an exact semantic equivalent. Preserve components outside "
                    "the core list with a precise extended concept, for example share capital "
                    "as share_capital. Never roll a component into a broader total: share "
                    "capital is not total equity, an individual receivable is not total "
                    "receivables, tax payable is not tax expense, and an individual liability "
                    "is not total liabilities. Use null only for headings, notes, company "
                    "names, or text that is not a financial concept. Return only a JSON object "
                    "with exactly one mapping for every input_id, using this shape: "
                    '{"mappings":[{"input_id":0,"canonical_concept":"share_capital",'
                    '"confidence":0.99}]}.'
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {"core_metric_concepts": METRIC_NAMES, "inputs": inputs},
                    ensure_ascii=False,
                ),
            },
        ],
        temperature=0,
        max_tokens=LLM_MAX_TOKENS,
        response_format={"type": "json_object"},
        extra_body={"thinking": {"type": "disabled"}},
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
