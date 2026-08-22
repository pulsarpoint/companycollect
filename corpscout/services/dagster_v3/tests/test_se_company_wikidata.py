from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.wikidata import (
    SE_COMPANY_INFO_WIKIDATA_COLUMNS,
    SE_COMPANY_INFO_WIKIDATA_SQL,
    se_company_info_wikidata_clickhouse,
)
from tests.se_company_ddl import declared_columns


def test_wikidata_select_links_entities_by_orgnr_or_lei() -> None:
    assert list(SE_COMPANY_INFO_WIKIDATA_COLUMNS) == [
        c for c in declared_columns("se_company_info_wikidata") if c != "evidence_hash"
    ]
    sql = SE_COMPANY_INFO_WIKIDATA_SQL
    assert "identifiers.identifier_type = 'se_orgnr'" in sql
    assert "identifiers.identifier_type = 'lei'" in sql
    assert "issuer_scheme = 'lei'" in sql and "is_current = 1" in sql
    assert "FROM corpscout.wikidata_companies AS entities FINAL" in sql
    assert "concat('wikidata:', entities.wikidata_id) AS source_record_uid" in sql
    assert "entities.resolved_at AS observed_at" in sql
    assert "NOT EXISTS" not in sql  # dedupe is publish_with_stage's job now


def test_wikidata_asset_depends_on_entities_and_the_register() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(
        dg.AssetKey("se_company_info_wikidata_clickhouse")
    )
    assert asset.parent_keys == {
        dg.AssetKey("wikidata_companies"),
        dg.AssetKey("sweden_company_companies_clickhouse"),
    }
    assert asset.group_name == "se_company_wikidata"


class _FakeClient:
    """Records executed SQL; answers system.tables and a scripted count queue."""

    def __init__(self, *, existing_tables: set[str], answers: list[list[tuple]]) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.existing_tables = existing_tables
        self.answers = answers

    def execute(self, sql: str, params: Any = None) -> list[tuple]:
        self.executed.append((sql, params))
        stripped = sql.strip()
        if "system.tables" in sql:
            requested = tuple(params["tables"])
            return [(table,) for table in requested if table in self.existing_tables]
        if stripped.upper().startswith("SELECT"):
            return self.answers.pop(0)
        return []


class _FakeClickhouse:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[_FakeClient]:
        yield self._client


def test_wikidata_asset_publishes_new_versions_only_via_left_anti_join() -> None:
    client = _FakeClient(
        existing_tables={
            "wikidata_companies",
            "wikidata_company_identifiers",
            "company_identifier",
            "se_companies",
            "se_company_info_wikidata",
        },
        answers=[[(2, 0)], [(10,)], [(1,)], [(11,)]],  # staged/invalid, existing, anti-join, total
    )

    result = se_company_info_wikidata_clickhouse.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), _FakeClickhouse(client)
    )

    insert_sql = next(
        sql
        for sql, _ in client.executed
        if sql.strip().startswith("INSERT INTO `corpscout`.`se_company_info_wikidata`")
    )
    assert "LEFT ANTI JOIN" in insert_sql
    assert result.metadata["appended_count"] == 1
    assert result.metadata["total_count"] == 11
