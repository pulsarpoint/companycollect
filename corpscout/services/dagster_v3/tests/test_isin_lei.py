from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.isin_lei import tables
from dagster_v3.defs.isin_lei.assets import (
    ISIN_LEI_UPSTREAM_ASSET_KEYS,
    build_isin_lei_insert_sql,
    isin_lei_clickhouse,
    replace_isin_lei_clickhouse,
)

_STAGE_TABLE = "`corpscout`.`_tmp_isin_lei_test`"


def test_isin_lei_table_contract() -> None:
    assert tables.ISIN_LEI_TABLE == "isin_lei"
    assert tables.ISIN_LEI_COLUMNS == (
        "isin",
        "lei",
        "mapping_source",
        "venue_confirmed",
        "cfi_category",
        "first_seen_date",
        "last_seen_date",
        "source_run_id",
        "resolved_at",
    )


def test_projection_reads_firds_event_history_not_current_state() -> None:
    """Identity is durable: a delisting must not erase who issued the ISIN."""
    sql = build_isin_lei_insert_sql(_STAGE_TABLE)

    assert sql.startswith(f"INSERT INTO {_STAGE_TABLE} (")
    assert "FROM corpscout.firds_instrument_events" in sql
    assert "firds_instruments_current" not in sql


def test_projection_is_neither_country_nor_cfi_filtered() -> None:
    """FIRDS is instrument-scoped, so non-EU issuers resolve here too."""
    sql = build_isin_lei_insert_sql(_STAGE_TABLE)

    assert "competent_authority_country" not in sql
    assert "XSTO" not in sql
    assert "startsWith" not in sql


def test_projection_collapses_to_isin_lei_source_grain() -> None:
    sql = build_isin_lei_insert_sql(_STAGE_TABLE)

    assert "GROUP BY\n    isin,\n    lei" in sql
    assert "'esma_firds' AS mapping_source" in sql
    assert "toUInt8(1) AS venue_confirmed" in sql
    assert "min(source_publication_date) AS first_seen_date" in sql
    assert "max(source_publication_date) AS last_seen_date" in sql


def test_projection_keeps_latest_cfi_category_without_filtering_it() -> None:
    """cfi_category is carried so consumers can exclude SPV-issued debt."""
    sql = build_isin_lei_insert_sql(_STAGE_TABLE)

    assert "argMax(" in sql
    assert "substring(upperUTF8(trimBoth(e.cfi_code)), 1, 1)" in sql


def test_projection_drops_rows_without_both_identifiers() -> None:
    sql = build_isin_lei_insert_sql(_STAGE_TABLE)

    assert "WHERE trimBoth(e.isin) != ''" in sql
    assert "AND trimBoth(e.issuer_lei) != ''" in sql


def test_asset_depends_on_the_firds_clickhouse_export() -> None:
    spec = isin_lei_clickhouse.specs_by_key[isin_lei_clickhouse.key]

    assert {dep.asset_key for dep in spec.deps} == {
        dg.AssetKey(asset_key) for asset_key in ISIN_LEI_UPSTREAM_ASSET_KEYS
    }
    assert ISIN_LEI_UPSTREAM_ASSET_KEYS == ("esma_firds_clickhouse",)


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
        if "row_count" in sql:
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


def test_replace_isin_lei_is_atomic_and_reports_ambiguity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quality_row = (5, 4, 4, 5, 1, 0, 0, 0, date(2024, 1, 5), date(2026, 7, 24))
    client = _FakeClickHouseClient(quality_row)
    resource = _clickhouse_resource(monkeypatch, client)
    resolved_at = datetime(2026, 7, 24, 18, 0, tzinfo=UTC)

    metadata = replace_isin_lei_clickhouse(
        clickhouse=resource,
        source_run_id="isin-lei-run",
        resolved_at=resolved_at,
    )

    assert client.table_checks == [("isin_lei", "firds_instrument_events")]
    assert any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")
    assert client.insert_parameters == {
        "resolved_at": resolved_at,
        "source_run_id": "isin-lei-run",
    }
    assert metadata["row_count"] == 5
    assert metadata["isin_count"] == 4
    assert metadata["ambiguous_isin_count"] == 1
    assert metadata["earliest_first_seen_date"] == "2024-01-05"


def test_replace_isin_lei_refuses_empty_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quality_row = (0, 0, 0, 0, 0, 0, 0, 0, None, None)
    client = _FakeClickHouseClient(quality_row)
    resource = _clickhouse_resource(monkeypatch, client)

    with pytest.raises(ValueError, match="produced no ISIN to LEI mappings"):
        replace_isin_lei_clickhouse(
            clickhouse=resource,
            source_run_id="isin-lei-run",
            resolved_at=datetime(2026, 7, 24, 18, 0, tzinfo=UTC),
        )

    assert not any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")


def test_replace_isin_lei_refuses_duplicate_grain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quality_row = (5, 4, 4, 4, 1, 0, 0, 0, date(2024, 1, 5), date(2026, 7, 24))
    client = _FakeClickHouseClient(quality_row)
    resource = _clickhouse_resource(monkeypatch, client)

    with pytest.raises(ValueError, match="grain mismatch"):
        replace_isin_lei_clickhouse(
            clickhouse=resource,
            source_run_id="isin-lei-run",
            resolved_at=datetime(2026, 7, 24, 18, 0, tzinfo=UTC),
        )

    assert not any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)


def test_malformed_identifiers_are_reported_without_failing_the_rebuild(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Upstream syntax noise stays visible; it must not blank a good table."""
    quality_row = (5, 4, 4, 5, 1, 0, 2, 3, date(2024, 1, 5), date(2026, 7, 24))
    client = _FakeClickHouseClient(quality_row)
    resource = _clickhouse_resource(monkeypatch, client)

    metadata = replace_isin_lei_clickhouse(
        clickhouse=resource,
        source_run_id="isin-lei-run",
        resolved_at=datetime(2026, 7, 24, 18, 0, tzinfo=UTC),
    )

    assert metadata["malformed_isin_rows"] == 2
    assert metadata["malformed_lei_rows"] == 3
    assert any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)
