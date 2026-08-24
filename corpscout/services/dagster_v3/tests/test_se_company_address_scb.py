from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import dagster as dg
import pytest

from dagster_v3.defs.se_company.scb import (
    SE_COMPANY_ADDRESS_SCB_COLUMNS,
    SE_COMPANY_ADDRESS_SCB_SOURCE_COUNT_SQL,
    SE_COMPANY_ADDRESS_SCB_SQL,
    se_company_address_scb_clickhouse,
)
from tests.se_company_ddl import declared_columns, projection_aliases


def test_columns_and_projection_match_the_migration() -> None:
    assert list(SE_COMPANY_ADDRESS_SCB_COLUMNS) == [
        column for column in declared_columns("se_company_address_scb") if column != "evidence_hash"
    ]
    assert projection_aliases(SE_COMPANY_ADDRESS_SCB_SQL) == list(SE_COMPANY_ADDRESS_SCB_COLUMNS)


def test_the_candidates_cte_pins_care_of_and_street_address_to_their_own_expressions() -> None:
    """projection_aliases() reads only the trailing SELECT's `AS <name>` list, so it
    would not notice the candidates CTE assigning addresses.street_address to the
    care_of alias (and vice versa) while the trailing projection still reads
    `care_of AS care_of, street_address AS street_address` unchanged. This pins the
    CTE-level expression each alias actually holds."""
    sql = SE_COMPANY_ADDRESS_SCB_SQL
    assert "addresses.care_of AS care_of" in sql
    assert "addresses.street_address AS street_address" in sql


def test_the_where_pins_the_source_pipelines_single_address_type() -> None:
    """I2: normalized_duckdb.py hard-codes exactly one address_type per source
    ('visiting_or_postal' for scb, address_rank = 1) and this artifact's
    ORDER BY (company_id, source_record_uid) is only unique because of that. Pinning
    the type here makes a change to that upstream assumption fail this SELECT's own
    filter instead of silently losing rows to ReplacingMergeTree collapsing two
    same-keyed versions at stage-write time."""
    assert "addresses.address_type = 'visiting_or_postal'" in SE_COMPANY_ADDRESS_SCB_SQL


def test_the_select_reads_only_the_scb_rows() -> None:
    sql = SE_COMPANY_ADDRESS_SCB_SQL
    assert "FROM corpscout.se_company_addresses_current AS addresses" in sql
    assert "addresses.source = 'scb'" in sql
    assert "addresses.has_address = 1" in sql
    assert "addresses.post_town AS city" in sql
    assert "match(addresses.company_id, '^([0-9]{10}|[0-9]{12})$')" in sql
    assert "now64(3, 'UTC') AS observed_at" in sql
    assert "updated_from_raw_at" not in sql
    # SCB does not distinguish visiting from postal; the type travels as the register
    # recorded it rather than being renamed here.
    assert "toString(addresses.address_type) AS address_type" in sql
    # '' and NULL hash identically under the DDL's ifNull, so losing this nullIf would
    # be permanently invisible to evidence_hash.
    assert "nullIf(addresses.normalized_address, '')" in sql


def test_the_two_scb_assets_are_separate_and_write_separate_tables() -> None:
    from dagster_v3.definitions import defs as load_defs

    graph = load_defs().get_repository_def().asset_graph
    address = graph.get(dg.AssetKey("se_company_address_scb_clickhouse"))
    info = graph.get(dg.AssetKey("se_company_info_scb_clickhouse"))
    assert address.parent_keys == {dg.AssetKey("sweden_company_addresses_clickhouse")}
    assert address.group_name == info.group_name == "se_company_scb"
    assert address.metadata["table"] == "corpscout.se_company_address_scb"
    assert info.metadata["table"] == "corpscout.se_company_info_scb"


class _FakeClient:
    """Records executed SQL; answers system.tables and a scripted count queue."""

    def __init__(self, *, existing_tables: set[str], answers: list[list[tuple]]) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.existing_tables = existing_tables
        self.answers = answers

    def execute(self, sql: str, params: Any = None) -> list[tuple]:
        self.executed.append((sql, params))
        if "system.tables" in sql:
            requested = tuple(params["tables"])
            return [(table,) for table in requested if table in self.existing_tables]
        if sql.strip().upper().startswith("SELECT"):
            return self.answers.pop(0)
        return []


class _FakeClickhouse:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[_FakeClient]:
        yield self._client


def test_the_tripwire_passes_when_the_source_count_matches_staged() -> None:
    """The tripwire count must apply every filter the main SELECT applies EXCEPT the
    address_type pin -- that pin is exactly the invariant being measured, so repeating
    it in the tripwire would make the two counts agree by construction and never
    catch a second address_type (or a renamed one) slipping in upstream."""
    assert "addresses.address_type = 'visiting_or_postal'" in SE_COMPANY_ADDRESS_SCB_SQL
    assert "address_type" not in SE_COMPANY_ADDRESS_SCB_SOURCE_COUNT_SQL

    client = _FakeClient(
        existing_tables={"se_company_addresses_current", "se_company_address_scb"},
        # staged/invalid, existing, anti-join(inserted), total, tripwire source count
        answers=[[(5, 0)], [(10,)], [(3,)], [(13,)], [(5,)]],
    )
    result = se_company_address_scb_clickhouse.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), _FakeClickhouse(client)
    )
    assert result.metadata["appended_count"] == 3
    assert result.metadata["total_count"] == 13


def test_the_tripwire_raises_naming_both_counts_on_mismatch() -> None:
    """I2: reproduces the reviewer's scenario -- the type-agnostic recount of source
    rows does not match what got staged under the pinned type (e.g. a second
    address_type slipped in under the same source_record_uid and ReplacingMergeTree
    silently collapsed one of them at stage-write time, or the upstream literal was
    renamed and the pinned SELECT now matches nothing). No ClickHouse error surfaces
    for that on its own; only this recount catches it."""
    client = _FakeClient(
        existing_tables={"se_company_addresses_current", "se_company_address_scb"},
        answers=[[(5, 0)], [(10,)], [(3,)], [(13,)], [(7,)]],
    )
    with pytest.raises(ValueError) as exc_info:
        se_company_address_scb_clickhouse.node_def.compute_fn.decorated_fn(
            dg.build_asset_context(), _FakeClickhouse(client)
        )
    message = str(exc_info.value)
    assert "5" in message
    assert "7" in message
