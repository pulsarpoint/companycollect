from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import dagster as dg
import duckdb
import pytest
from pytest import MonkeyPatch

from dagster_v3.defs.norway_brreg_financial import annual_account_financials
from dagster_v3.defs.norway_brreg_financial.annual_account_financials import (
    ANNUAL_ACCOUNT_DATASET,
    MAPPING_VERSION,
    PARSER_VERSION,
    apply_annual_account_usd_conversion,
    apply_builtin_concept_mappings,
    apply_llm_concept_mappings,
    build_annual_account_metrics,
    ensure_annual_account_duckdb_schema,
    extract_annual_account_facts,
    load_annual_account_documents,
    replace_annual_account_facts,
)
from dagster_v3.defs.norway_brreg_financial.annual_account_clickhouse import (
    REPORT_COLUMNS,
    publish_annual_account_partition,
)
from dagster_v3.defs.norway_brreg_financial import annual_account_clickhouse
from dagster_v3.defs.norway_brreg_financial.assets.annual_account_financials import (
    norway_brreg_annual_account_documents_duckdb,
    norway_brreg_annual_account_fact_mappings_duckdb,
    norway_brreg_annual_account_facts_clickhouse,
    norway_brreg_annual_account_facts_duckdb,
    norway_brreg_annual_account_facts_usd_duckdb,
    norway_brreg_annual_account_metrics_clickhouse,
    norway_brreg_annual_account_metrics_duckdb,
    norway_brreg_annual_account_reports_clickhouse,
)
from dagster_v3.defs.norway_brreg_financial.models import (
    AnnualAccountConceptMappingResponse,
    AnnualAccountDocument,
)


class FakeAnnualAccountStorage:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def list_annual_account_document_keys(
        self,
        *,
        filing_year: int,
        chunk_key: str,
    ) -> list[str]:
        prefix = (
            "norway_brreg/annual_accounts/documents/"
            f"year={filing_year}/chunk={chunk_key}/"
        )
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_response(self, key: str) -> bytes:
        return self.objects[key]


class TrackingDuckDbConnection:
    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self._connection = connection
        self.arrow_batches: list[tuple[str, int]] = []
        self.statements: list[str] = []

    def execute(self, *args: Any, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        if args and isinstance(args[0], str):
            self.statements.append(" ".join(args[0].split()).lower())
        return self._connection.execute(*args, **kwargs)

    def register(
        self,
        view_name: str,
        python_object: Any,
    ) -> duckdb.DuckDBPyConnection:
        self.arrow_batches.append((view_name, python_object.num_rows))
        return self._connection.register(view_name, python_object)

    def unregister(self, view_name: str) -> duckdb.DuckDBPyConnection:
        return self._connection.unregister(view_name)

    def executemany(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("Norway annual accounts must use Arrow inserts")


class FailingFactPublishConnection(TrackingDuckDbConnection):
    def execute(self, *args: Any, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        if args and isinstance(args[0], str):
            statement = " ".join(args[0].split()).lower()
            target_insert = (
                f"insert into {ANNUAL_ACCOUNT_DATASET}.facts "
                f"select * from {annual_account_financials._FACT_STAGE_TABLE}"
            )
            if statement == target_insert:
                raise RuntimeError("synthetic fact publish failure")
        return super().execute(*args, **kwargs)


class FakeExchangeRates:
    def __init__(self, rate: Decimal = Decimal("0.1")) -> None:
        self.rate = rate

    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]:
        return {
            (request.currency, request.rate_date): SimpleNamespace(
                rate=self.rate,
                rate_date=request.rate_date,
                source="test-rates",
            )
            for request in requests
        }


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def __enter__(self) -> FakeClickHouseClient:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str, _parameters: object = None) -> list[tuple[int]]:
        self.statements.append(statement)
        return []


class FakeClickHouseResource:
    def __init__(self) -> None:
        self.client = FakeClickHouseClient()

    def get_connection(self) -> FakeClickHouseClient:
        return self.client


def test_duckdb_schema_does_not_collide_with_file_catalog(tmp_path: Path) -> None:
    database_path = tmp_path / "norway_brreg_annual_accounts.duckdb"

    with duckdb.connect(str(database_path)) as connection:
        ensure_annual_account_duckdb_schema(connection)
        tables = {
            row[0]
            for row in connection.execute(
                "select table_name from information_schema.tables "
                "where table_schema = ?",
                [ANNUAL_ACCOUNT_DATASET],
            ).fetchall()
        }

    assert tables == {"concept_mappings", "documents", "facts", "metrics"}


def test_financial_fact_parser_uses_geometry_and_never_invents_comparatives() -> None:
    document = AnnualAccountDocument.model_validate(_sample_document())

    facts = extract_annual_account_facts(document, source_json_sha256="a" * 64)
    amounts = {(fact.raw_label, fact.fiscal_year): fact.numeric_value for fact in facts}

    assert amounts[("Annen driftskostnad", 2025)] == Decimal("44539")
    assert amounts[("Annen driftskostnad", 2024)] == Decimal("62699")
    assert amounts[("Driftsresultat", 2025)] == Decimal("-45313")
    assert amounts[("Driftsresultat", 2024)] == Decimal("-63402")
    assert amounts[("Annen renteinntekt", 2025)] == Decimal("3")
    assert ("Annen renteinntekt", 2024) not in amounts
    assert all(fact.raw_label != "2" for fact in facts)
    assert all(fact.source_json_sha256 == "a" * 64 for fact in facts)


def test_financial_fact_parser_uses_full_date_headers_not_amounts_as_years() -> None:
    document_data = _sample_document()
    document_data["legal_name"] = "SS-BYGG AS"
    document_data["org_number"] = "814269132"
    document_data["document_id"] = "no-brreg-annual-account:814269132:2025"
    document_data["pages"] = [
        {
            "page_number": 10,
            "extraction_method": "tesseract_ocr",
            "width": 1000.0,
            "height": 1400.0,
            "text": (
                "BALANSE Note 31.12.2025 31.12.2024 "
                "Aksjekapital 30 000 30 000 "
                "Annen kortsiktig gjeld 2000 2000"
            ),
            "mean_word_confidence": 95.0,
            "words": [
                _word("Note", 0.52, 0.58, line=1, word=1),
                _word("31.12.2025", 0.67, 0.77, line=1, word=2),
                _word("31.12.2024", 0.81, 0.93, line=1, word=3),
                _word("Aksjekapital", 0.08, 0.21, line=2, word=1),
                _word("30", 0.72, 0.74, line=2, word=2),
                _word("000", 0.75, 0.78, line=2, word=3),
                _word("30", 0.86, 0.88, line=2, word=4),
                _word("000", 0.89, 0.92, line=2, word=5),
                _word("Annen", 0.08, 0.13, line=3, word=1),
                _word("kortsiktig", 0.14, 0.21, line=3, word=2),
                _word("gjeld", 0.22, 0.27, line=3, word=3),
                _word("2000", 0.72, 0.78, line=3, word=4),
                _word("2000", 0.86, 0.92, line=3, word=5),
            ],
        }
    ]

    facts = extract_annual_account_facts(
        AnnualAccountDocument.model_validate(document_data),
        source_json_sha256="a" * 64,
    )
    amounts = {(fact.raw_label, fact.fiscal_year): fact.numeric_value for fact in facts}

    assert {fact.fiscal_year for fact in facts} == {2024, 2025}
    assert amounts[("Aksjekapital", 2025)] == Decimal("30000")
    assert amounts[("Aksjekapital", 2024)] == Decimal("30000")
    assert amounts[("Annen kortsiktig gjeld", 2025)] == Decimal("2000")
    assert amounts[("Annen kortsiktig gjeld", 2024)] == Decimal("2000")
    assert len({fact.fact_id for fact in facts}) == len(facts)


def test_financial_fact_parser_preserves_repeated_year_columns() -> None:
    document_data = _sample_document()
    document_data["pages"] = [
        {
            "page_number": 1,
            "extraction_method": "native_text",
            "width": 1000.0,
            "height": 1400.0,
            "text": (
                "BALANSE Konsern Morselskap 31.12.2025 31.12.2025 "
                "Kortsiktig gjeld 2000 2000"
            ),
            "mean_word_confidence": 100.0,
            "words": [
                _word("Konsern", 0.67, 0.75, line=1, word=1),
                _word("Morselskap", 0.81, 0.92, line=1, word=2),
                _word("31.12.2025", 0.67, 0.77, line=2, word=1),
                _word("31.12.2025", 0.81, 0.93, line=2, word=2),
                _word("Kortsiktig", 0.08, 0.17, line=3, word=1),
                _word("gjeld", 0.18, 0.24, line=3, word=2),
                _word("2000", 0.72, 0.78, line=3, word=3),
                _word("2000", 0.86, 0.92, line=3, word=4),
            ],
        }
    ]

    facts = extract_annual_account_facts(
        AnnualAccountDocument.model_validate(document_data),
        source_json_sha256="a" * 64,
    )
    debt_facts = [fact for fact in facts if fact.raw_label == "Kortsiktig gjeld"]

    assert len(debt_facts) == 2
    assert {fact.fiscal_year for fact in debt_facts} == {2025}
    assert {fact.numeric_value for fact in debt_facts} == {Decimal("2000")}
    assert len({fact.fact_id for fact in debt_facts}) == 2
    assert {fact.column_label for fact in debt_facts} == {
        "2025:column_1",
        "2025:column_2",
    }
    assert not any(fact.is_comparative for fact in debt_facts)
    assert all(
        "ambiguous_duplicate_period_columns" in fact.quality_flags
        for fact in debt_facts
    )


def test_financial_fact_parser_rejects_accounting_period_range_as_header() -> None:
    document_data = _sample_document()
    document_data["pages"] = [
        {
            "page_number": 1,
            "extraction_method": "native_text",
            "width": 1000.0,
            "height": 1400.0,
            "text": (
                "Årsregnskapets periode 01.01.2025 - 31.12.2025 Aksjekapital 2000 2000"
            ),
            "mean_word_confidence": 100.0,
            "words": [
                _word("Årsregnskapets", 0.08, 0.20, line=1, word=1),
                _word("periode", 0.21, 0.27, line=1, word=2),
                _word("01.01.2025", 0.55, 0.65, line=1, word=3),
                _word("-", 0.66, 0.67, line=1, word=4),
                _word("31.12.2025", 0.70, 0.80, line=1, word=5),
                _word("Aksjekapital", 0.08, 0.21, line=2, word=1),
                _word("2000", 0.58, 0.64, line=2, word=2),
                _word("2000", 0.72, 0.78, line=2, word=3),
            ],
        }
    ]

    facts = extract_annual_account_facts(
        AnnualAccountDocument.model_validate(document_data),
        source_json_sha256="a" * 64,
    )

    assert facts == []


def test_financial_fact_parser_inherits_document_currency_and_ignores_identity_rows() -> (
    None
):
    document_data = _sample_document()
    pages = document_data["pages"]
    assert isinstance(pages, list)
    pages.append(
        {
            "page_number": 2,
            "extraction_method": "tesseract_ocr",
            "width": 1000.0,
            "height": 1400.0,
            "text": "BALANSE Note 2025 2024",
            "mean_word_confidence": 95.0,
            "words": [
                _word("Note", 0.52, 0.58, line=1, word=1),
                _word("2025", 0.70, 0.76, line=1, word=2),
                _word("2024", 0.87, 0.93, line=1, word=3),
                _word("Organisasjonsnummer", 0.05, 0.30, line=2, word=1),
                _word("811725102", 0.68, 0.76, line=2, word=2),
                _word("Sum", 0.05, 0.10, line=3, word=1),
                _word("eiendeler", 0.11, 0.24, line=3, word=2),
                _word("1000", 0.70, 0.76, line=3, word=3),
                _word("900", 0.88, 0.93, line=3, word=4),
            ],
        }
    )
    document_data["pdf_page_count"] = 2
    document_data["ocr_page_count"] = 2

    facts = extract_annual_account_facts(
        AnnualAccountDocument.model_validate(document_data),
        source_json_sha256="a" * 64,
    )
    balance_facts = [fact for fact in facts if fact.page_number == 2]

    assert {fact.raw_label for fact in balance_facts} == {"Sum eiendeler"}
    assert {fact.currency for fact in balance_facts} == {"NOK"}
    assert {fact.amount_original for fact in balance_facts} == {
        Decimal("1000"),
        Decimal("900"),
    }


def test_duckdb_stages_preserve_documents_facts_mappings_and_validated_metrics() -> (
    None
):
    raw_document = json.dumps(_sample_document(), ensure_ascii=False).encode()
    key = (
        "norway_brreg/annual_accounts/documents/"
        "year=2025/chunk=bucket_00/org=811725102/document.json"
    )
    connection = duckdb.connect(":memory:")
    ensure_annual_account_duckdb_schema(connection)

    storage = FakeAnnualAccountStorage({key: raw_document})
    document_counts = load_annual_account_documents(
        connection=connection,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-1",
    )
    fact_counts = replace_annual_account_facts(
        connection=connection,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-2",
    )
    mapping_counts = apply_builtin_concept_mappings(
        connection=connection,
        filing_year=2025,
        chunk_key="bucket_00",
    )
    metric_counts = build_annual_account_metrics(
        connection=connection,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-3",
    )

    assert document_counts == {"document_count": 1, "json_bytes": len(raw_document)}
    assert fact_counts["document_count"] == 1
    assert fact_counts["fact_count"] >= 5
    assert mapping_counts["dictionary_mapping_count"] >= 1
    assert metric_counts["metric_row_count"] == 2
    assert connection.execute(
        f"select source_json_sha256, parser_version from {ANNUAL_ACCOUNT_DATASET}.documents"
    ).fetchone() == (hashlib.sha256(raw_document).hexdigest(), PARSER_VERSION)
    assert connection.execute(
        f"select canonical_concept from {ANNUAL_ACCOUNT_DATASET}.facts "
        "where raw_label = 'Driftsresultat' limit 1"
    ).fetchone() == ("operating_result",)
    assert connection.execute(
        f"select mapping_version from {ANNUAL_ACCOUNT_DATASET}.metrics limit 1"
    ).fetchone() == (MAPPING_VERSION,)


def test_document_and_fact_replacement_uses_arrow_and_one_set_update() -> None:
    raw_document = json.dumps(_sample_document(), ensure_ascii=False).encode()
    key = (
        "norway_brreg/annual_accounts/documents/"
        "year=2025/chunk=bucket_00/org=811725102/document.json"
    )
    storage = FakeAnnualAccountStorage({key: raw_document})
    connection = duckdb.connect(":memory:")
    tracking = TrackingDuckDbConnection(connection)

    document_counts = load_annual_account_documents(
        connection=tracking,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-documents",
    )
    first_fact_counts = replace_annual_account_facts(
        connection=tracking,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-facts",
    )
    second_fact_counts = replace_annual_account_facts(
        connection=tracking,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-facts",
    )

    document_updates = [
        statement
        for statement in tracking.statements
        if statement.startswith(f"update {ANNUAL_ACCOUNT_DATASET}.documents")
    ]
    stored_document = connection.execute(
        f"select parse_status, fact_count from {ANNUAL_ACCOUNT_DATASET}.documents"
    ).fetchone()
    stored_fact_count = connection.execute(
        f"select count(*) from {ANNUAL_ACCOUNT_DATASET}.facts"
    ).fetchone()[0]

    assert document_counts == {"document_count": 1, "json_bytes": len(raw_document)}
    assert first_fact_counts == second_fact_counts
    assert stored_document == ("parsed", first_fact_counts["fact_count"])
    assert stored_fact_count == first_fact_counts["fact_count"]
    assert len(document_updates) == 2
    assert all(
        f"from {annual_account_financials._FACT_COUNT_STAGE_TABLE}" in statement
        for statement in document_updates
    )
    assert [batch[0] for batch in tracking.arrow_batches] == [
        annual_account_financials._DOCUMENT_BATCH_RELATION,
        annual_account_financials._FACT_BATCH_RELATION,
        annual_account_financials._FACT_COUNT_BATCH_RELATION,
        annual_account_financials._FACT_BATCH_RELATION,
        annual_account_financials._FACT_COUNT_BATCH_RELATION,
    ]


def test_empty_partition_replacement_preserves_empty_metadata() -> None:
    connection = duckdb.connect(":memory:")
    storage = FakeAnnualAccountStorage({})

    document_counts = load_annual_account_documents(
        connection=connection,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-documents",
    )
    fact_counts = replace_annual_account_facts(
        connection=connection,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-facts",
    )

    assert document_counts == {"document_count": 0, "json_bytes": 0}
    assert fact_counts == {"document_count": 0, "fact_count": 0}


def test_fact_hash_failure_preserves_previous_partition() -> None:
    connection, storage = _loaded_sample_partition()
    before_document = connection.execute(
        f"select parse_status, fact_count from {ANNUAL_ACCOUNT_DATASET}.documents"
    ).fetchone()
    before_facts = connection.execute(
        f"select fact_id, raw_value from {ANNUAL_ACCOUNT_DATASET}.facts "
        "order by fact_id"
    ).fetchall()
    key = next(iter(storage.objects))
    storage.objects[key] = b"changed after document load"

    with pytest.raises(RuntimeError, match="Annual-account JSON hash mismatch"):
        replace_annual_account_facts(
            connection=connection,
            storage=storage,
            filing_year=2025,
            chunk_key="bucket_00",
            source_run_id="failed-run",
        )

    assert connection.execute(
        f"select parse_status, fact_count from {ANNUAL_ACCOUNT_DATASET}.documents"
    ).fetchone() == before_document
    assert connection.execute(
        f"select fact_id, raw_value from {ANNUAL_ACCOUNT_DATASET}.facts "
        "order by fact_id"
    ).fetchall() == before_facts


def test_fact_publish_failure_rolls_back_and_allows_retry() -> None:
    connection, storage = _loaded_sample_partition()
    before_document = connection.execute(
        f"select parse_status, fact_count from {ANNUAL_ACCOUNT_DATASET}.documents"
    ).fetchone()
    before_facts = connection.execute(
        f"select fact_id, raw_value from {ANNUAL_ACCOUNT_DATASET}.facts "
        "order by fact_id"
    ).fetchall()

    with pytest.raises(RuntimeError, match="synthetic fact publish failure"):
        replace_annual_account_facts(
            connection=FailingFactPublishConnection(connection),
            storage=storage,
            filing_year=2025,
            chunk_key="bucket_00",
            source_run_id="failed-run",
        )

    assert connection.execute(
        f"select parse_status, fact_count from {ANNUAL_ACCOUNT_DATASET}.documents"
    ).fetchone() == before_document
    assert connection.execute(
        f"select fact_id, raw_value from {ANNUAL_ACCOUNT_DATASET}.facts "
        "order by fact_id"
    ).fetchall() == before_facts

    retry_counts = replace_annual_account_facts(
        connection=connection,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="retry-run",
    )

    assert retry_counts == {
        "document_count": 1,
        "fact_count": len(before_facts),
    }


def test_fact_stage_streams_500k_wide_rows_in_bounded_arrow_batches() -> None:
    connection = duckdb.connect(":memory:")
    ensure_annual_account_duckdb_schema(connection)
    annual_account_financials._create_fact_stage_tables(connection)
    tracking = TrackingDuckDbConnection(connection)
    resolved_at = datetime(2026, 7, 25, tzinfo=UTC)

    def rows() -> Iterator[tuple[Any, ...]]:
        for index in range(500_000):
            if index == 50_000:
                assert tracking.arrow_batches == [
                    (annual_account_financials._FACT_BATCH_RELATION, 50_000)
                ]
            yield _synthetic_fact_row(index, resolved_at=resolved_at)

    inserted = annual_account_financials._append_fact_stage_rows(
        connection=tracking,
        rows=rows(),
    )
    stored = connection.execute(
        f"select count(*), min(fact_id), max(fact_id) "
        f"from {annual_account_financials._FACT_STAGE_TABLE}"
    ).fetchone()

    assert inserted == 500_000
    assert stored == (500_000, "fact-000000", "fact-499999")
    assert tracking.arrow_batches == [
        (annual_account_financials._FACT_BATCH_RELATION, 50_000)
    ] * 10


def test_usd_conversion_only_updates_missing_amounts() -> None:
    connection, _storage = _loaded_sample_partition()

    first = apply_annual_account_usd_conversion(
        connection=connection,
        exchange_rates=FakeExchangeRates(),
        filing_year=2025,
        chunk_key="bucket_00",
    )
    second = apply_annual_account_usd_conversion(
        connection=connection,
        exchange_rates=FakeExchangeRates(),
        filing_year=2025,
        chunk_key="bucket_00",
    )

    assert first["converted_fact_count"] > 0
    assert first["unconverted_fact_count"] == 0
    assert second == {"converted_fact_count": 0, "unconverted_fact_count": 0}
    amount_original, amount_usd, fx_source = connection.execute(
        f"select amount_original, amount_usd, fx_source "
        f"from {ANNUAL_ACCOUNT_DATASET}.facts where amount_original is not null limit 1"
    ).fetchone()
    assert amount_usd == amount_original * Decimal("0.1")
    assert fx_source == "test-rates"


def test_usd_conversion_limits_rate_scale_before_multiplication() -> None:
    connection, _storage = _loaded_sample_partition()
    high_scale_rate = Decimal("0.09512345678901234567890123456")
    connection.execute(
        f"update {ANNUAL_ACCOUNT_DATASET}.facts "
        "set amount_original = 2000, amount_usd = null "
        "where raw_label = 'Driftsresultat'"
    )

    counts = apply_annual_account_usd_conversion(
        connection=connection,
        exchange_rates=FakeExchangeRates(high_scale_rate),
        filing_year=2025,
        chunk_key="bucket_00",
    )

    amount_usd, stored_rate = connection.execute(
        f"select amount_usd, fx_rate_to_usd "
        f"from {ANNUAL_ACCOUNT_DATASET}.facts "
        "where raw_label = 'Driftsresultat' limit 1"
    ).fetchone()
    assert counts["unconverted_fact_count"] == 0
    assert amount_usd == Decimal("190.2469135780")
    assert stored_rate == Decimal("0.095123456789")


def test_llm_mapping_preserves_extended_concepts(
    monkeypatch: MonkeyPatch,
) -> None:
    connection, _storage = _loaded_sample_partition()

    def extended_concept_response(
        _client: object,
        *,
        batch: list[tuple[str, str]],
        model: str,
    ) -> tuple[AnnualAccountConceptMappingResponse, str]:
        response_data = {
            "mappings": [
                {
                    "input_id": input_id,
                    "canonical_concept": "share_capital",
                    "confidence": 0.99,
                }
                for input_id, _item in enumerate(batch)
            ]
        }
        raw_response = json.dumps(response_data)
        return (
            AnnualAccountConceptMappingResponse.model_validate(response_data),
            raw_response,
        )

    monkeypatch.setattr(
        annual_account_financials,
        "OpenAI",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        annual_account_financials,
        "_request_llm_mappings",
        extended_concept_response,
    )

    counts = apply_llm_concept_mappings(
        connection=connection,
        filing_year=2025,
        chunk_key="bucket_00",
        base_url="http://llm.invalid/v1",
        api_key="test",
        model="test-model",
        batch_size=2,
        workers=2,
        timeout_seconds=30,
    )

    extended_count, raw_response_count = connection.execute(
        f"select count(*), count(raw_response) "
        f"from {ANNUAL_ACCOUNT_DATASET}.concept_mappings "
        "where mapping_method = 'llm_extended' "
        "and canonical_concept = 'share_capital'"
    ).fetchone()
    mapped_fact_count = connection.execute(
        f"select count(*) from {ANNUAL_ACCOUNT_DATASET}.facts "
        "where mapping_method = 'llm_extended' "
        "and canonical_concept = 'share_capital'"
    ).fetchone()[0]
    metric_counts = build_annual_account_metrics(
        connection=connection,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="mapping-test",
    )
    projected_concepts = {
        source_fact["concept"]
        for (source_fact_ids,) in connection.execute(
            f"select source_fact_ids from {ANNUAL_ACCOUNT_DATASET}.metrics"
        ).fetchall()
        for source_fact in json.loads(source_fact_ids)
    }
    assert counts["requested_mapping_count"] > 0
    assert counts["llm_mapping_count"] == 0
    assert counts["extended_mapping_count"] == counts["requested_mapping_count"]
    assert counts["unmapped_mapping_count"] == 0
    assert counts["invalid_mapping_count"] == 0
    assert extended_count == counts["extended_mapping_count"]
    assert mapped_fact_count > 0
    assert raw_response_count == extended_count
    assert metric_counts["metric_row_count"] > 0
    assert "share_capital" not in projected_concepts


def test_metric_validation_keeps_ambiguous_facts_and_marks_review() -> None:
    connection, _storage = _loaded_sample_partition()
    apply_builtin_concept_mappings(
        connection=connection,
        filing_year=2025,
        chunk_key="bucket_00",
    )
    connection.execute(
        f"update {ANNUAL_ACCOUNT_DATASET}.facts "
        "set canonical_concept = 'operating_result', mapping_confidence = 1 "
        "where raw_label = 'Annen driftskostnad'"
    )

    build_annual_account_metrics(
        connection=connection,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-metrics",
    )

    validation_status, warnings, source_fact_ids = connection.execute(
        f"select validation_status, metric_warnings, source_fact_ids "
        f"from {ANNUAL_ACCOUNT_DATASET}.metrics where fiscal_year = 2025"
    ).fetchone()
    assert validation_status == "review"
    assert json.loads(warnings) == ["duplicate_canonical_concept_values"]
    assert len(json.loads(source_fact_ids)) == 1


def test_clickhouse_publish_atomically_replaces_one_partition(monkeypatch: Any) -> None:
    connection, _storage = _loaded_sample_partition()
    clickhouse = FakeClickHouseResource()
    monkeypatch.setattr(
        annual_account_clickhouse,
        "assert_clickhouse_tables_exist",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        annual_account_clickhouse,
        "export_duckdb_connection_table_to_clickhouse",
        lambda **_kwargs: 1,
    )

    published = publish_annual_account_partition(
        duckdb_connection=connection,
        clickhouse=clickhouse,
        duckdb_table="documents",
        clickhouse_table="no_financial_reports",
        columns=REPORT_COLUMNS,
        filing_year=2025,
        chunk_key="bucket_00",
        log=None,
    )

    assert published == 1
    statements = "\n".join(clickhouse.client.statements).lower()
    assert "replace partition tuple(2025, 'bucket_00')" in statements
    assert "drop table if exists" in statements


def test_annual_account_financial_asset_graph_keeps_each_stage_separate() -> None:
    documents_json = dg.AssetKey("norway_brreg_annual_account_documents_json")
    documents_duckdb = dg.AssetKey("norway_brreg_annual_account_documents_duckdb")
    facts_duckdb = dg.AssetKey("norway_brreg_annual_account_facts_duckdb")
    mappings_duckdb = dg.AssetKey("norway_brreg_annual_account_fact_mappings_duckdb")
    facts_usd = dg.AssetKey("norway_brreg_annual_account_facts_usd_duckdb")
    metrics_duckdb = dg.AssetKey("norway_brreg_annual_account_metrics_duckdb")

    assert norway_brreg_annual_account_documents_duckdb.asset_deps[
        documents_duckdb
    ] == {documents_json}
    assert norway_brreg_annual_account_facts_duckdb.asset_deps[facts_duckdb] == {
        documents_duckdb
    }
    assert norway_brreg_annual_account_fact_mappings_duckdb.asset_deps[
        mappings_duckdb
    ] == {facts_duckdb}
    assert norway_brreg_annual_account_facts_usd_duckdb.asset_deps[facts_usd] == {
        facts_duckdb
    }
    assert norway_brreg_annual_account_metrics_duckdb.asset_deps[metrics_duckdb] == {
        mappings_duckdb,
        facts_usd,
    }
    assert norway_brreg_annual_account_reports_clickhouse.asset_deps[
        dg.AssetKey("norway_brreg_annual_account_reports_clickhouse")
    ] == {documents_duckdb}
    assert norway_brreg_annual_account_facts_clickhouse.asset_deps[
        dg.AssetKey("norway_brreg_annual_account_facts_clickhouse")
    ] == {mappings_duckdb, facts_usd}
    assert norway_brreg_annual_account_metrics_clickhouse.asset_deps[
        dg.AssetKey("norway_brreg_annual_account_metrics_clickhouse")
    ] == {metrics_duckdb}


def _loaded_sample_partition() -> tuple[
    duckdb.DuckDBPyConnection, FakeAnnualAccountStorage
]:
    raw_document = json.dumps(_sample_document(), ensure_ascii=False).encode()
    key = (
        "norway_brreg/annual_accounts/documents/"
        "year=2025/chunk=bucket_00/org=811725102/document.json"
    )
    connection = duckdb.connect(":memory:")
    storage = FakeAnnualAccountStorage({key: raw_document})
    load_annual_account_documents(
        connection=connection,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-documents",
    )
    replace_annual_account_facts(
        connection=connection,
        storage=storage,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="run-facts",
    )
    return connection, storage


def _sample_document() -> dict[str, object]:
    words: list[dict[str, object]] = []

    def add_line(line_number: int, values: list[tuple[str, float, float]]) -> None:
        for word_number, (text, left, right) in enumerate(values, start=1):
            words.append(
                {
                    "text": text,
                    "bbox": [left, line_number / 20, right, line_number / 20 + 0.02],
                    "confidence": 95.0,
                    "block_number": 1,
                    "paragraph_number": 1,
                    "line_number": line_number,
                    "word_number": word_number,
                }
            )

    add_line(
        1,
        [
            ("Beløp", 0.05, 0.11),
            ("i:", 0.12, 0.14),
            ("NOK", 0.15, 0.20),
            ("Note", 0.52, 0.58),
            ("2025", 0.70, 0.76),
            ("2024", 0.87, 0.93),
        ],
    )
    add_line(
        2,
        [
            ("Annen", 0.05, 0.12),
            ("driftskostnad", 0.13, 0.30),
            ("2", 0.55, 0.56),
            ("44", 0.67, 0.70),
            ("539", 0.71, 0.76),
            ("62", 0.84, 0.87),
            ("699", 0.88, 0.93),
        ],
    )
    add_line(
        3,
        [
            ("Driftsresultat", 0.05, 0.24),
            ("-45", 0.66, 0.70),
            ("313", 0.71, 0.76),
            ("-63", 0.83, 0.87),
            ("402", 0.88, 0.93),
        ],
    )
    add_line(
        4,
        [("Annen", 0.05, 0.12), ("renteinntekt", 0.13, 0.28), ("3", 0.74, 0.76)],
    )
    return {
        "schema_version": 1,
        "document_id": "no-brreg-annual-account:811725102:2025",
        "country_iso2": "NO",
        "source_system": "brreg_annual_accounts_copy",
        "source_run_id": "ocr-run",
        "org_number": "811725102",
        "legal_name": "FORLAND CONSULTING AS",
        "filing_year": 2025,
        "source_pdf_url": "https://example.test/annual-account.pdf",
        "source_pdf_sha256": "b" * 64,
        "source_pdf_size_bytes": 1234,
        "retrieved_at": "2026-05-30T00:00:00+00:00",
        "pdf_page_count": 1,
        "native_text_page_count": 0,
        "ocr_page_count": 1,
        "extraction": {
            "pdf_engine": "pymupdf",
            "pdf_engine_version": "1.0",
            "ocr_engine": "tesseract",
            "ocr_languages": "nor+eng",
            "ocr_page_segmentation_mode": 4,
            "bbox_coordinate_space": "normalized_page",
        },
        "pages": [
            {
                "page_number": 1,
                "extraction_method": "tesseract_ocr",
                "width": 1000.0,
                "height": 1400.0,
                "text": "RESULTATREGNSKAP Beløp i: NOK Note 2025 2024",
                "mean_word_confidence": 95.0,
                "words": words,
            }
        ],
    }


def _synthetic_fact_row(
    index: int,
    *,
    resolved_at: datetime,
) -> tuple[Any, ...]:
    return (
        f"fact-{index:06d}",
        f"document-{index // 10:06d}",
        "NO",
        annual_account_financials.SOURCE_SLUG,
        "bulk-run",
        "811725102",
        2025,
        "bucket_00",
        index,
        1,
        1,
        "income_statement",
        "RESULTATREGNSKAP",
        "Driftsresultat",
        "driftsresultat",
        "operating_result",
        "2025",
        2025,
        "2025-12-31",
        False,
        "amount",
        "1",
        "1",
        "NOK",
        "1",
        "1",
        None,
        None,
        None,
        None,
        "[0.0, 0.0, 1.0, 1.0]",
        "Driftsresultat 1",
        95.0,
        "tesseract_ocr",
        "dictionary",
        1.0,
        "[]",
        "a" * 64,
        PARSER_VERSION,
        resolved_at,
    )


def _word(
    text: str,
    left: float,
    right: float,
    *,
    line: int,
    word: int,
) -> dict[str, object]:
    return {
        "text": text,
        "bbox": [left, line / 20, right, line / 20 + 0.02],
        "confidence": 95.0,
        "block_number": 1,
        "paragraph_number": 1,
        "line_number": line,
        "word_number": word,
    }
