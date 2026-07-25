from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.instrument_venues.assets import (
    INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS,
    instrument_venues_clickhouse,
    replace_instrument_venues_clickhouse,
)
from dagster_v3.defs.instrument_venues.eodhd import (
    build_eodhd_instrument_venues_sql,
)

_STAGE = "`corpscout`.`_tmp_instrument_venues_test`"


def test_eodhd_projection_marks_vendor_evidence_and_needs_an_isin() -> None:
    sql = build_eodhd_instrument_venues_sql(_STAGE)

    assert "'eodhd' AS venue_source" in sql
    assert "'vendor' AS evidence_tier" in sql
    assert "FROM corpscout.eodhd_symbols AS s" in sql
    assert "INNER JOIN corpscout.eodhd_symbol_mics AS m" in sql
    assert "trimBoth(ifNull(s.isin, '')) != ''" in sql


def test_eodhd_projection_carries_delisting_into_trading_status() -> None:
    sql = build_eodhd_instrument_venues_sql(_STAGE)

    assert "s.is_delisted" in sql
    assert "AS is_current" in sql


def test_asset_depends_on_both_venue_sources() -> None:
    spec = instrument_venues_clickhouse.specs_by_key[
        instrument_venues_clickhouse.key
    ]

    assert {dep.asset_key for dep in spec.deps} == {
        dg.AssetKey(key) for key in INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS
    }
    assert INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS == (
        "esma_firds_clickhouse",
        "eodhd_reference_complete",
    )


class _FakeClickHouseClient:
    def __init__(self, quality_row: tuple[object, ...]) -> None:
        self.quality_row = quality_row
        self.statements: list[str] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params is not None else ()
            return [(table,) for table in requested]
        if "row_count" in sql:
            return [self.quality_row]
        return []


def _resource(
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


def test_replace_inserts_both_sources_then_exchanges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # row_count, isin_count, mic_count, venue_key_count, firds_rows,
    # eodhd_rows, invalid_rows, latest_source_publication_date
    client = _FakeClickHouseClient((10, 6, 3, 10, 7, 3, 0, date(2026, 7, 25)))
    resource = _resource(monkeypatch, client)

    metadata = replace_instrument_venues_clickhouse(
        clickhouse=resource,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
    )

    inserts = [s for s in client.statements if s.startswith("INSERT INTO")]
    assert len(inserts) == 2
    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")
    assert metadata["row_count"] == 10
    assert metadata["firds_rows"] == 7
    assert metadata["eodhd_rows"] == 3


def test_replace_refuses_empty_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClickHouseClient((0, 0, 0, 0, 0, 0, 0, None))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="no instrument venue rows"):
        replace_instrument_venues_clickhouse(
            clickhouse=resource,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_replace_refuses_when_a_source_collapses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A populated table must not be replaced by one missing a whole source."""
    client = _FakeClickHouseClient((7, 6, 3, 7, 7, 0, 0, date(2026, 7, 25)))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="contributed no rows"):
        replace_instrument_venues_clickhouse(
            clickhouse=resource,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)
