from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import dagster as dg
import pytest

from dagster_v3.defs.se_company.bolagsverket import (
    SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS,
    SE_COMPANY_ADDRESS_BOLAGSVERKET_SOURCE_COUNT_SQL,
    SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL,
    se_company_address_bolagsverket_clickhouse,
)
from tests.se_company_ddl import declared_columns, projection_aliases


def test_columns_are_the_migration_order_minus_the_materialized_hash() -> None:
    assert list(SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS) == [
        column for column in declared_columns("se_company_address_bolagsverket")
        if column != "evidence_hash"
    ]


def test_the_trailing_projection_binds_positionally_to_those_columns() -> None:
    """publish_with_stage inserts positionally, so a renamed or reordered alias in
    the trailing projection would transpose values with an otherwise-green suite.
    projection_aliases() only reads that trailing SELECT's `AS <name>` list, though:
    it would NOT catch the candidates CTE swapping which EXPRESSION a stable alias
    like care_of or street_address actually holds -- see the CTE-level assertions
    below for that."""
    assert projection_aliases(SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL) == list(
        SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS
    )


def test_the_candidates_cte_pins_care_of_and_street_address_to_their_own_expressions() -> None:
    """projection_aliases() reads only the trailing SELECT's `AS <name>` list, so it
    would not notice the candidates CTE assigning addresses.street_address to the
    care_of alias (and vice versa) while the trailing projection still reads
    `care_of AS care_of, street_address AS street_address` unchanged. This pins the
    CTE-level expression each alias actually holds."""
    sql = SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL
    assert "addresses.care_of AS care_of" in sql
    assert "addresses.street_address AS street_address" in sql
    # Same reasoning for country_code: the trailing `country_code AS country_code`
    # would not notice the CTE's CAST being dropped or the source column swapped.
    assert "CAST(addresses.country_code AS Nullable(String)) AS country_code" in sql


def test_the_where_pins_the_source_pipelines_single_address_type() -> None:
    """I2: normalized_duckdb.py hard-codes exactly one address_type per source
    ('postal' for bolagsverket, address_rank = 1) and this artifact's
    ORDER BY (company_id, source_record_uid) is only unique because of that. Pinning
    the type here makes a change to that upstream assumption fail this SELECT's own
    filter instead of silently losing rows to ReplacingMergeTree collapsing two
    same-keyed versions at stage-write time."""
    assert "addresses.address_type = 'postal'" in SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL


def test_the_select_reads_only_this_source_and_only_real_addresses() -> None:
    sql = SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL
    assert "FROM corpscout.se_company_addresses_current AS addresses" in sql
    assert "addresses.source = 'bolagsverket'" in sql
    assert "addresses.has_address = 1" in sql
    assert "addresses.post_town AS city" in sql
    assert "toString(addresses.address_fingerprint) AS address_fingerprint" in sql
    assert "match(addresses.company_id, '^([0-9]{10}|[0-9]{12})$')" in sql
    # '' and NULL hash identically under the DDL's ifNull, so losing this nullIf would
    # be permanently invisible to evidence_hash.
    assert "nullIf(addresses.normalized_address, '')" in sql


def test_observed_at_is_append_time_not_the_bulk_load_stamp() -> None:
    """updated_from_raw_at is one constant per weekly load; a version stamped with it
    would never look newer than the final row it replaces."""
    sql = SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL
    assert "now64(3, 'UTC') AS observed_at" in sql
    assert "updated_from_raw_at" not in sql


def test_the_asset_reads_the_source_layer_and_writes_its_own_table() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(
        dg.AssetKey("se_company_address_bolagsverket_clickhouse"))
    assert asset.parent_keys == {dg.AssetKey("sweden_company_addresses_clickhouse")}
    assert asset.group_name == "se_company_bolagsverket"
    assert asset.metadata["table"] == "corpscout.se_company_address_bolagsverket"


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
    assert "addresses.address_type = 'postal'" in SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL
    assert "address_type" not in SE_COMPANY_ADDRESS_BOLAGSVERKET_SOURCE_COUNT_SQL

    client = _FakeClient(
        existing_tables={"se_company_addresses_current", "se_company_address_bolagsverket"},
        # staged/invalid, existing, anti-join(inserted), total, tripwire source count
        answers=[[(5, 0)], [(10,)], [(3,)], [(13,)], [(5,)]],
    )
    result = se_company_address_bolagsverket_clickhouse.node_def.compute_fn.decorated_fn(
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
        existing_tables={"se_company_addresses_current", "se_company_address_bolagsverket"},
        answers=[[(5, 0)], [(10,)], [(3,)], [(13,)], [(7,)]],
    )
    with pytest.raises(ValueError) as exc_info:
        se_company_address_bolagsverket_clickhouse.node_def.compute_fn.decorated_fn(
            dg.build_asset_context(), _FakeClickhouse(client)
        )
    message = str(exc_info.value)
    assert "5" in message
    assert "7" in message
