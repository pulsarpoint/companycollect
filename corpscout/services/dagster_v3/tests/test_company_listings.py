from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.company_listings import tables
from dagster_v3.defs.company_listings.assets import (
    SE_LISTING_UPSTREAM_ASSET_KEYS,
    build_se_company_listings_insert_sql,
    replace_se_company_listings_clickhouse,
    se_company_listings_clickhouse,
)

_STAGE_TABLE = "`corpscout`.`_tmp_se_company_listings_test`"


def test_se_company_listing_table_contract() -> None:
    assert tables.SE_COMPANY_LISTINGS_TABLE == "se_company_listings"
    assert tables.SE_COMPANY_LISTINGS_COLUMNS == (
        "country_code",
        "company_id",
        "issuer_lei",
        "isin",
        "mic",
        "ticker",
        "instrument_name",
        "instrument_type",
        "cfi_code",
        "notional_currency",
        "admission_date",
        "first_trade_date",
        "termination_date",
        "trading_status",
        "is_current",
        "identity_match_method",
        "identity_match_confidence",
        "listing_status_source",
        "status_conflict",
        "eodhd_symbol_key",
        "eodhd_is_delisted",
        "source_slug",
        "source_record_id",
        "source_publication_date",
        "source_retrieved_at",
        "source_run_id",
        "resolved_at",
    )


def test_se_company_listing_sql_uses_exact_firds_gleif_registry_path() -> None:
    sql = build_se_company_listings_insert_sql(_STAGE_TABLE)

    assert sql.startswith(f"INSERT INTO {_STAGE_TABLE} (")
    assert "FROM corpscout.firds_instruments_current AS f" in sql
    assert "FROM corpscout.gleif_lei_records" in sql
    assert "INNER JOIN gleif_sweden AS g ON g.lei = f.issuer_lei" in sql
    assert (
        "INNER JOIN corpscout.se_companies AS c ON c.company_id = g.company_id" in sql
    )
    assert "replaceRegexpAll(legal_entity.2, '[^0-9]', '')" in sql
    assert "length(replaceRegexpAll(legal_entity.2, '[^0-9]', '')) = 10" in sql
    assert "upperUTF8(legal_entity.1) = 'SE'" in sql
    assert "'firds_issuer_lei_gleif_registered_as'" in sql
    assert "'exact' AS identity_match_confidence" in sql
    assert "legal_name" not in sql


def test_se_company_listing_sql_applies_sweden_equity_scope() -> None:
    sql = build_se_company_listings_insert_sql(_STAGE_TABLE)

    for mic in ("XSTO", "FNSE", "XNGM", "NSME", "XSAT"):
        assert f"'{mic}'" in sql
    assert "startsWith(upperUTF8(trimBoth(f.cfi_code)), 'E')" in sql
    assert "'current' AS trading_status" in sql
    assert "toUInt8(1) AS is_current" in sql


def test_eodhd_is_optional_exact_isin_mic_enrichment() -> None:
    sql = build_se_company_listings_insert_sql(_STAGE_TABLE)

    assert "FROM corpscout.eodhd_symbols AS s" in sql
    assert "INNER JOIN corpscout.eodhd_symbol_mics AS m" in sql
    assert "GROUP BY isin, mic" in sql
    assert "argMax(" in sql
    assert "LEFT JOIN eodhd_by_listing AS e" in sql
    assert "e.isin = f.isin AND e.mic = f.mic" in sql
    assert "e.eodhd_is_delisted = 1" in sql


def test_se_company_listing_asset_dependencies_are_country_scoped() -> None:
    spec = se_company_listings_clickhouse.specs_by_key[
        se_company_listings_clickhouse.key
    ]

    assert {dep.asset_key for dep in spec.deps} == {
        dg.AssetKey(asset_key) for asset_key in SE_LISTING_UPSTREAM_ASSET_KEYS
    }
    assert SE_LISTING_UPSTREAM_ASSET_KEYS == (
        "esma_firds_clickhouse",
        "gleif_reference_clickhouse",
        "eodhd_reference_complete",
        "sweden_company_companies_clickhouse",
    )


class _FakeClickHouseClient:
    def __init__(self, quality_row: tuple[object, ...]) -> None:
        self.quality_row = quality_row
        self.statements: list[str] = []
        self.insert_parameters: dict[str, object] = {}
        self.table_checks: list[tuple[str, ...]] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params is not None else ()
            self.table_checks.append(requested)
            return [(table,) for table in requested]
        if sql.startswith("CREATE TABLE"):
            return []
        if sql.startswith("INSERT INTO"):
            self.insert_parameters = params or {}
            return []
        if "candidate_rows" in sql:
            return [self.quality_row]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def _clickhouse_resource(
    monkeypatch: pytest.MonkeyPatch,
    client: _FakeClickHouseClient,
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def test_replace_se_company_listings_is_atomic_and_reports_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quality_row = (4, 4, 3, 2, 2, 2, 3, 0, date(2026, 7, 24))
    client = _FakeClickHouseClient(quality_row)
    resource = _clickhouse_resource(monkeypatch, client)
    resolved_at = datetime(2026, 7, 24, 18, 0, tzinfo=UTC)

    metadata = replace_se_company_listings_clickhouse(
        clickhouse=resource,
        source_run_id="listing-run",
        resolved_at=resolved_at,
    )

    assert client.table_checks == [
        (
            "se_company_listings",
            "firds_instruments_current",
            "gleif_lei_records",
            "se_companies",
            "eodhd_symbols",
            "eodhd_symbol_mics",
        )
    ]
    assert any(sql.startswith("CREATE TABLE") for sql in client.statements)
    assert any(sql.startswith("INSERT INTO") for sql in client.statements)
    assert any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")
    assert client.insert_parameters == {
        "resolved_at": resolved_at,
        "source_run_id": "listing-run",
    }
    assert metadata["row_count"] == 3
    assert metadata["company_count"] == 2
    assert metadata["unresolved_candidate_rows"] == 1
    assert metadata["latest_source_publication_date"] == "2026-07-24"


def test_replace_se_company_listings_refuses_empty_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quality_row = (4, 4, 0, 0, 0, 0, 0, 0, None)
    client = _FakeClickHouseClient(quality_row)
    resource = _clickhouse_resource(monkeypatch, client)

    with pytest.raises(ValueError, match="produced no resolved listings"):
        replace_se_company_listings_clickhouse(
            clickhouse=resource,
            source_run_id="listing-run",
            resolved_at=datetime(2026, 7, 24, 18, 0, tzinfo=UTC),
        )

    assert not any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")


def test_replace_se_company_listings_refuses_duplicate_grain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quality_row = (4, 4, 4, 2, 2, 2, 3, 0, date(2026, 7, 24))
    client = _FakeClickHouseClient(quality_row)
    resource = _clickhouse_resource(monkeypatch, client)

    with pytest.raises(ValueError, match="grain mismatch"):
        replace_se_company_listings_clickhouse(
            clickhouse=resource,
            source_run_id="listing-run",
            resolved_at=datetime(2026, 7, 24, 18, 0, tzinfo=UTC),
        )

    assert not any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)
