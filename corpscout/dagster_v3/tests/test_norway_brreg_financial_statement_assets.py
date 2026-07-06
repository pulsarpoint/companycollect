from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC
from datetime import date
from datetime import datetime
from decimal import Decimal
from typing import Any

import dagster as dg
import pyarrow as pa
import polars as pl
import pytest

from dagster_v3.defs.norway_brreg_financial import financial_normalize
from dagster_v3.defs.norway_brreg_financial.assets import financial_statements
from dagster_v3.defs.norway_brreg_financial.constants import NORWAY_BRREG_FINANCIAL_BUCKET
from dagster_v3.defs.norway_brreg_financial.assets.financial_statements import (
    norway_brreg_financial_statements_snapshot_clickhouse,
    norway_brreg_financial_statements_snapshot_parquet,
    norway_brreg_financial_statements_snapshot_usd_parquet,
    norway_brreg_financial_statements_updates_clickhouse,
    norway_brreg_financial_statements_updates_parquet,
    norway_brreg_financial_statements_updates_usd_parquet,
)
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
)
from dagster_v3.defs.norway_brreg import resolved_tables as no_tables


class FakeUsdRate:
    rate = Decimal("0.10")
    rate_date = "2024-12-31"
    source = "test-fx"

    def convert(self, amount: Decimal) -> Decimal:
        return amount * self.rate


class FakeExchangeRates:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []

    def usd_rates(self, requests):
        self.requests.extend((request.currency, request.rate_date) for request in requests)
        return {
            (request.currency, request.rate_date): FakeUsdRate()
            for request in requests
        }


class FakeExchangeRateClientFactory:
    def __init__(self, exchange_rates: FakeExchangeRates) -> None:
        self.exchange_rates = exchange_rates
        self.calls = 0

    def from_env(self) -> FakeExchangeRates:
        self.calls += 1
        return self.exchange_rates


class FakeFinancialStorage:
    def __init__(
        self,
        *,
        historical_raw_fetches_frame: pl.DataFrame | None = None,
        snapshot_fetches: pl.DataFrame | None = None,
        update_fetches: dict[str, pl.DataFrame] | None = None,
        snapshot_statements: pl.DataFrame | None = None,
        update_statements: dict[str, pl.DataFrame] | None = None,
        snapshot_usd_statements: pl.DataFrame | None = None,
        update_usd_statements: dict[str, pl.DataFrame] | None = None,
    ) -> None:
        self.historical_raw_fetches_frame = historical_raw_fetches_frame
        self.snapshot_fetches = snapshot_fetches
        self.update_fetches = update_fetches or {}
        self.snapshot_statements = snapshot_statements
        self.update_statements = update_statements or {}
        self.snapshot_usd_statements = snapshot_usd_statements
        self.update_usd_statements = update_usd_statements or {}
        self.historical_raw_fetch_reads = 0
        self.snapshot_fetch_reads = 0
        self.update_fetch_reads: list[str] = []
        self.snapshot_statement_reads = 0
        self.update_statement_reads: list[str] = []
        self.snapshot_usd_reads = 0
        self.update_usd_reads: list[str] = []

    def read_consolidated_historical_fetches(self) -> pl.DataFrame:
        self.historical_raw_fetch_reads += 1
        assert self.historical_raw_fetches_frame is not None
        return self.historical_raw_fetches_frame

    def read_snapshot_fetches(self) -> pl.DataFrame:
        self.snapshot_fetch_reads += 1
        assert self.snapshot_fetches is not None
        return self.snapshot_fetches

    def read_update_fetches(self, partition_date: str) -> pl.DataFrame:
        self.update_fetch_reads.append(partition_date)
        return self.update_fetches[partition_date]

    def write_snapshot_statements(self, frame: pl.DataFrame) -> str:
        self.snapshot_statements = frame
        return "snapshot/statements.parquet"

    def write_update_statements(self, partition_date: str, frame: pl.DataFrame) -> str:
        self.update_statements[partition_date] = frame
        return f"updates/{partition_date}/statements.parquet"

    def read_snapshot_statements(self) -> pl.DataFrame:
        self.snapshot_statement_reads += 1
        assert self.snapshot_statements is not None
        return self.snapshot_statements

    def read_update_statements(self, partition_date: str) -> pl.DataFrame:
        self.update_statement_reads.append(partition_date)
        return self.update_statements[partition_date]

    def write_snapshot_usd_statements(self, frame: pl.DataFrame) -> str:
        self.snapshot_usd_statements = frame
        return "snapshot/statements_usd.parquet"

    def write_update_usd_statements(self, partition_date: str, frame: pl.DataFrame) -> str:
        self.update_usd_statements[partition_date] = frame
        return f"updates/{partition_date}/statements_usd.parquet"

    def read_snapshot_usd_statements(self) -> pl.DataFrame:
        self.snapshot_usd_reads += 1
        assert self.snapshot_usd_statements is not None
        return self.snapshot_usd_statements

    def read_update_usd_statements(self, partition_date: str) -> pl.DataFrame:
        self.update_usd_reads.append(partition_date)
        return self.update_usd_statements[partition_date]


class FakeEntityStorage:
    def __init__(self, update_tables: dict[str, pl.DataFrame]) -> None:
        self.update_tables = update_tables
        self.update_read_calls: list[tuple[str, str]] = []

    def read_normalized_update_table(
        self,
        partition_date: str,
        table_name: str,
    ) -> pl.DataFrame:
        self.update_read_calls.append((partition_date, table_name))
        return self.update_tables[table_name]


class FakeClickHouseResource:
    def __init__(self, client: "FakeClickHouseClient") -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.events: list[tuple[str, object | None]] = []
        self.row_insert_calls: list[
            tuple[str | None, str, list[tuple[object, ...]], tuple[str, ...]]
        ] = []

    def execute(self, sql: str, params: object | None = None) -> list[tuple[str]]:
        self.events.append((sql, params))
        if "FROM system.tables" in sql:
            return [(no_tables.NO_FINANCIAL_STATEMENTS_TABLE,)]
        return []

    def insert_rows(
        self,
        table: str,
        rows: list[tuple[object, ...]],
        columns: tuple[str, ...] | list[str],
        database: str | None = None,
    ) -> None:
        self.row_insert_calls.append((database, table, rows, tuple(columns)))


class FakeArrowClickHouseClient(FakeClickHouseClient):
    def __init__(self) -> None:
        super().__init__()
        self.arrow_inserts: list[tuple[str | None, str, pa.Table]] = []

    def insert_arrow(
        self,
        table: str,
        arrow_table: pa.Table,
        database: str | None = None,
    ) -> None:
        self.arrow_inserts.append((database, table, arrow_table))


def test_snapshot_statement_asset_reads_historical_raw_fetches_without_fetching_brreg() -> None:
    storage = FakeFinancialStorage(
        historical_raw_fetches_frame=pl.DataFrame(
            [
                _success_fetch_row("923609016", [_financial_record()]),
                _failure_fetch_row("811685852"),
            ]
        )
    )

    result = norway_brreg_financial_statements_snapshot_parquet(
        context=dg.build_asset_context(),
        norway_brreg_financial_storage=storage,
    )

    assert storage.historical_raw_fetch_reads == 1
    assert storage.snapshot_fetch_reads == 0
    assert storage.snapshot_statements is not None
    assert storage.snapshot_statements.columns == list(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_FINANCIAL_STATEMENTS_TABLE]
    )
    rows = storage.snapshot_statements.to_dicts()
    assert len(rows) == 1
    assert rows[0]["org_number"] == "923609016"
    assert rows[0]["operating_revenue_amount_original"] == Decimal("72543000000")
    assert rows[0]["operating_revenue_amount_usd"] is None
    assert result.metadata["fetch_row_count"] == 2
    assert result.metadata["successful_fetch_count"] == 1
    assert result.metadata["statement_row_count"] == 1
    assert result.metadata["s3_bucket"] == NORWAY_BRREG_FINANCIAL_BUCKET
    assert result.metadata["s3_key"] == "snapshot/statements.parquet"


def test_update_statements_asset_writes_empty_partition_with_resolved_columns() -> None:
    storage = FakeFinancialStorage(update_fetches={"2026-06-30": _empty_fetch_frame()})

    result = norway_brreg_financial_statements_updates_parquet(
        context=dg.build_asset_context(partition_key="2026-06-30"),
        norway_brreg_financial_storage=storage,
    )

    assert storage.update_fetch_reads == ["2026-06-30"]
    assert storage.update_statements["2026-06-30"].to_dicts() == []
    assert storage.update_statements["2026-06-30"].columns == list(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_FINANCIAL_STATEMENTS_TABLE]
    )
    assert result.metadata["partition_date"] == "2026-06-30"
    assert result.metadata["fetch_row_count"] == 0
    assert result.metadata["successful_fetch_count"] == 0
    assert result.metadata["statement_row_count"] == 0
    assert result.metadata["s3_key"] == "updates/2026-06-30/statements.parquet"


def test_snapshot_and_update_usd_assets_call_exchange_rates_and_write_enriched_parquet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_frame = _financial_frame(
        [_resolved_financial_row(org_number="923609016", usd=False)]
    )
    storage = FakeFinancialStorage(
        snapshot_statements=original_frame,
        update_statements={"2026-06-30": original_frame},
    )
    exchange_rates = FakeExchangeRates()
    factory = FakeExchangeRateClientFactory(exchange_rates)
    monkeypatch.setattr(financial_statements, "ExchangeRateClient", factory)

    snapshot_result = norway_brreg_financial_statements_snapshot_usd_parquet(
        context=dg.build_asset_context(),
        norway_brreg_financial_storage=storage,
    )
    update_result = norway_brreg_financial_statements_updates_usd_parquet(
        context=dg.build_asset_context(partition_key="2026-06-30"),
        norway_brreg_financial_storage=storage,
    )

    assert factory.calls == 2
    assert exchange_rates.requests == [
        ("NOK", "2024-12-31"),
        ("NOK", "2024-12-31"),
    ]
    assert storage.snapshot_usd_statements is not None
    assert storage.snapshot_usd_statements.to_dicts()[0][
        "operating_revenue_amount_usd"
    ] == Decimal("100.00")
    assert storage.update_usd_statements["2026-06-30"].to_dicts()[0][
        "operating_revenue_amount_usd"
    ] == Decimal("100.00")
    assert snapshot_result.metadata["original_row_count"] == 1
    assert snapshot_result.metadata["usd_row_count"] == 1
    assert snapshot_result.metadata["rate_date_count"] == 1
    assert update_result.metadata["partition_date"] == "2026-06-30"
    assert update_result.metadata["original_row_count"] == 1
    assert update_result.metadata["usd_row_count"] == 1


def test_update_usd_asset_allows_empty_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeFinancialStorage(
        update_statements={"2026-06-30": _empty_financial_frame()}
    )
    exchange_rates = FakeExchangeRates()
    factory = FakeExchangeRateClientFactory(exchange_rates)
    monkeypatch.setattr(financial_statements, "ExchangeRateClient", factory)

    result = norway_brreg_financial_statements_updates_usd_parquet(
        context=dg.build_asset_context(partition_key="2026-06-30"),
        norway_brreg_financial_storage=storage,
    )

    assert factory.calls == 1
    assert exchange_rates.requests == []
    assert storage.update_usd_statements["2026-06-30"].to_dicts() == []
    assert storage.update_usd_statements["2026-06-30"].columns == list(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_FINANCIAL_STATEMENTS_TABLE]
    )
    assert result.metadata["original_row_count"] == 0
    assert result.metadata["usd_row_count"] == 0
    assert result.metadata["rate_date_count"] == 0


def test_snapshot_clickhouse_publish_replaces_target_table_from_snapshot_usd_parquet() -> None:
    storage = FakeFinancialStorage(
        snapshot_usd_statements=_financial_frame(
            [_resolved_financial_row(org_number="923609016", usd=True)]
        )
    )
    client = FakeClickHouseClient()

    result = norway_brreg_financial_statements_snapshot_clickhouse(
        context=dg.build_asset_context(),
        clickhouse=FakeClickHouseResource(client),
        norway_brreg_financial_storage=storage,
    )

    assert storage.snapshot_usd_reads == 1
    assert any(
        sql.startswith("CREATE TABLE `corpscout`.`_tmp_no_financial_statements_")
        for sql, _params in client.events
    )
    assert any(
        sql.startswith("EXCHANGE TABLES `corpscout`.`_tmp_no_financial_statements_")
        for sql, _params in client.events
    )
    assert any(
        sql.startswith("INSERT INTO `corpscout`.`_tmp_no_financial_statements_")
        for sql, _params in client.events
    )
    assert result.metadata["row_count"] == 1


def test_snapshot_clickhouse_publish_exports_date_columns_as_arrow_dates() -> None:
    storage = FakeFinancialStorage(
        snapshot_usd_statements=_financial_frame(
            [_resolved_financial_row(org_number="923609016", usd=True)]
        )
    )
    client = FakeArrowClickHouseClient()

    result = norway_brreg_financial_statements_snapshot_clickhouse(
        context=dg.build_asset_context(),
        clickhouse=FakeClickHouseResource(client),
        norway_brreg_financial_storage=storage,
    )

    assert result.metadata["row_count"] == 1
    assert len(client.arrow_inserts) == 1
    arrow_table = client.arrow_inserts[0][2]
    assert pa.types.is_date(arrow_table.schema.field("period_start_date").type)
    assert pa.types.is_date(arrow_table.schema.field("period_end_date").type)
    assert pa.types.is_date(arrow_table.schema.field("fx_rate_date").type)


def test_update_clickhouse_publish_deletes_affected_orgs_then_inserts_replacements() -> None:
    storage = FakeFinancialStorage(
        update_usd_statements={
            "2026-06-30": _financial_frame(
                [_resolved_financial_row(org_number="923609016", usd=True)]
            )
        }
    )
    entity_storage = FakeEntityStorage(
        update_tables={
            ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: _affected_orgs_frame("923609016"),
        }
    )
    client = FakeClickHouseClient()

    result = norway_brreg_financial_statements_updates_clickhouse(
        context=dg.build_asset_context(partition_key="2026-06-30"),
        clickhouse=FakeClickHouseResource(client),
        norway_brreg_entity_storage=entity_storage,
        norway_brreg_financial_storage=storage,
    )

    assert entity_storage.update_read_calls == [
        ("2026-06-30", ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS)
    ]
    assert storage.update_usd_reads == ["2026-06-30"]
    delete_positions = [
        index
        for index, (sql, _params) in enumerate(client.events)
        if sql.startswith("ALTER TABLE `corpscout`.`no_financial_statements`")
    ]
    insert_positions = [
        index
        for index, (sql, _params) in enumerate(client.events)
        if sql.startswith("INSERT INTO `corpscout`.`no_financial_statements`")
    ]
    assert len(delete_positions) == 1
    assert len(insert_positions) == 1
    assert max(delete_positions) < min(insert_positions)
    assert result.metadata["affected_org_count"] == 1
    assert result.metadata["row_count"] == 1


def test_update_clickhouse_publish_deletes_affected_orgs_even_without_replacements() -> None:
    storage = FakeFinancialStorage(
        update_usd_statements={"2026-06-30": _empty_financial_frame()}
    )
    entity_storage = FakeEntityStorage(
        update_tables={
            ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: _affected_orgs_frame("923609016"),
        }
    )
    client = FakeClickHouseClient()

    result = norway_brreg_financial_statements_updates_clickhouse(
        context=dg.build_asset_context(partition_key="2026-06-30"),
        clickhouse=FakeClickHouseResource(client),
        norway_brreg_entity_storage=entity_storage,
        norway_brreg_financial_storage=storage,
    )

    assert any(
        sql.startswith("ALTER TABLE `corpscout`.`no_financial_statements`")
        for sql, _params in client.events
    )
    assert not any(
        sql.startswith("INSERT INTO `corpscout`.`no_financial_statements`")
        for sql, _params in client.events
    )
    assert result.metadata["affected_org_count"] == 1
    assert result.metadata["row_count"] == 0


def test_update_clickhouse_publish_skips_empty_affected_org_partitions() -> None:
    storage = FakeFinancialStorage(
        update_usd_statements={"2026-06-30": _empty_financial_frame()}
    )
    entity_storage = FakeEntityStorage(
        update_tables={
            ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: _affected_orgs_frame(),
        }
    )
    client = FakeClickHouseClient()

    result = norway_brreg_financial_statements_updates_clickhouse(
        context=dg.build_asset_context(partition_key="2026-06-30"),
        clickhouse=FakeClickHouseResource(client),
        norway_brreg_entity_storage=entity_storage,
        norway_brreg_financial_storage=storage,
    )

    assert not any(
        sql.startswith("ALTER TABLE `corpscout`.`no_financial_statements`")
        for sql, _params in client.events
    )
    assert result.metadata["affected_org_count"] == 0
    assert result.metadata["row_count"] == 0


def test_update_clickhouse_publish_rejects_replacements_without_affected_orgs() -> None:
    storage = FakeFinancialStorage(
        update_usd_statements={
            "2026-06-30": _financial_frame(
                [_resolved_financial_row(org_number="923609016", usd=True)]
            )
        }
    )
    entity_storage = FakeEntityStorage(
        update_tables={
            ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: _affected_orgs_frame(),
        }
    )

    with pytest.raises(ValueError, match="replacement financial statement rows but no affected orgs"):
        norway_brreg_financial_statements_updates_clickhouse(
            context=dg.build_asset_context(partition_key="2026-06-30"),
            clickhouse=FakeClickHouseResource(FakeClickHouseClient()),
            norway_brreg_entity_storage=entity_storage,
            norway_brreg_financial_storage=storage,
        )


def _success_fetch_row(org_number: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "org_number": org_number,
        "legal_name": f"{org_number} AS",
        "website": "",
        "last_submitted_accounts_year": "2024",
        "source_run_id": "run-1",
        "source_url": f"https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}",
        "fetch_status": "success",
        "raw_response": json.dumps(records),
    }


def _failure_fetch_row(org_number: str) -> dict[str, Any]:
    return {
        "org_number": org_number,
        "legal_name": f"{org_number} AS",
        "website": "",
        "last_submitted_accounts_year": "2024",
        "source_run_id": "run-1",
        "source_url": f"https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}",
        "fetch_status": "not_found",
        "raw_response": "",
    }


def _empty_fetch_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "org_number": pl.Utf8,
            "legal_name": pl.Utf8,
            "website": pl.Utf8,
            "last_submitted_accounts_year": pl.Utf8,
            "source_run_id": pl.Utf8,
            "source_url": pl.Utf8,
            "fetch_status": pl.Utf8,
            "raw_response": pl.Utf8,
        }
    )


def _financial_record() -> dict[str, Any]:
    return {
        "id": 5667197,
        "journalnr": "2025428073",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "923609016",
            "organisasjonsform": "ASA",
            "morselskap": True,
        },
        "regnskapsperiode": {"fraDato": "2024-01-01", "tilDato": "2024-12-31"},
        "valuta": "NOK",
        "avviklingsregnskap": False,
        "oppstillingsplan": "store",
        "revisjon": {"ikkeRevidertAarsregnskap": False, "fravalgRevisjon": False},
        "regnkapsprinsipper": {"smaaForetak": False, "regnskapsregler": "forenkletAnvendelseIFRS"},
        "egenkapitalGjeld": {
            "egenkapital": {"sumEgenkapital": 41090000000},
            "gjeldOversikt": {
                "sumGjeld": 68060000000,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 42024000000},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 26036000000},
            },
        },
        "eiendeler": {
            "sumEiendeler": 109150000000,
            "omloepsmidler": {"sumOmloepsmidler": 45079000000},
            "anleggsmidler": {"sumAnleggsmidler": 64071000000},
        },
        "resultatregnskapResultat": {
            "driftsresultat": {
                "driftsinntekter": {"sumDriftsinntekter": 72543000000},
                "driftskostnad": {"sumDriftskostnad": 62196000000},
                "driftsresultat": 10347000000,
            },
            "finansresultat": {"nettoFinans": -2179000000},
            "ordinaertResultatFoerSkattekostnad": 8168000000,
            "aarsresultat": 8141000000,
        },
    }


def _resolved_financial_row(*, org_number: str, usd: bool) -> dict[str, Any]:
    row = {
        column: None
        for column in no_tables.RESOLVED_EXPORT_COLUMNS[
            no_tables.NO_FINANCIAL_STATEMENTS_TABLE
        ]
    }
    row.update(
        {
            "country_iso2": "NO",
            "source_system": financial_normalize.FINANCIAL_SOURCE_SLUG,
            "source_run_id": "run-1",
            "source_record_id": "5667197",
            "org_number": org_number,
            "legal_name": f"{org_number} AS",
            "last_submitted_accounts_year": "2024",
            "filing_id": 5667197,
            "journal_number": "2025428073",
            "accounts_type": "SELSKAP",
            "legal_form_code": "ASA",
            "is_parent_company": True,
            "period_start_date": "2024-01-01",
            "period_end_date": "2024-12-31",
            "fiscal_year": 2024,
            "currency": "NOK",
            "liquidation_accounts": False,
            "statement_layout": "store",
            "is_not_audited": False,
            "opted_out_audit": False,
            "is_small_enterprise": False,
            "accounting_rules": "forenkletAnvendelseIFRS",
            "operating_revenue_amount_original": Decimal("1000"),
            "operating_revenue_amount_usd": Decimal("100") if usd else None,
            "fx_rate_to_usd": Decimal("0.10") if usd else None,
            "fx_rate_date": date(2024, 12, 31) if usd else None,
            "fx_source": "test-fx" if usd else "",
            "source_url": f"https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}",
            "resolved_at": datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC),
        }
    )
    return row


def _financial_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    columns = no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_FINANCIAL_STATEMENTS_TABLE]
    return pl.DataFrame([{column: row.get(column) for column in columns} for row in rows])


def _empty_financial_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            column: pl.Utf8
            for column in no_tables.RESOLVED_EXPORT_COLUMNS[
                no_tables.NO_FINANCIAL_STATEMENTS_TABLE
            ]
        }
    )


def _affected_orgs_frame(*org_numbers: str) -> pl.DataFrame:
    return pl.DataFrame(
        [{"org_number": org_number} for org_number in org_numbers],
        schema={"org_number": pl.Utf8},
    )
