from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.esef import (
    SE_COMPANY_INFO_ESEF_COLUMNS,
    SE_COMPANY_INFO_ESEF_SQL,
    se_company_info_esef_clickhouse,
)
from tests.se_company_ddl import declared_columns, projection_aliases


def test_esef_select_keeps_swedish_issuers_with_a_description() -> None:
    assert list(SE_COMPANY_INFO_ESEF_COLUMNS) == [
        c for c in declared_columns("se_company_info_esef") if c != "evidence_hash"
    ]
    sql = SE_COMPANY_INFO_ESEF_SQL
    assert projection_aliases(sql) == list(SE_COMPANY_INFO_ESEF_COLUMNS)
    assert "FROM corpscout.esef_document_company_information AS info" in sql
    # A LEFT join: the documents table only supplies entity_name, which no rule reads,
    # so a missing document row must not drop an otherwise valid description. Its
    # entity_name is non-Nullable in the source DDL, so the miss still needs the ifNull
    # guard (it is NULL under join_use_nulls = 1, and the artifact column is String).
    assert (
        "LEFT JOIN corpscout.esef_source_documents AS documents ON documents.source_document_id = info.source_document_id"
        in sql
    )
    assert "INNER JOIN corpscout.esef_source_documents" not in sql
    assert "ifNull(documents.entity_name, '') AS entity_name" in sql
    assert "info.country_iso2 = 'SE'" in sql and "match(info.company_id, '^([0-9]{10}|[0-9]{12})$')" in sql
    assert "trim(info.company_description) != ''" in sql
    assert "info.source_record_uid AS source_record_uid" in sql
    # Stable tie-break: resolved_at DESC alone can tie between extraction runs;
    # model_provider/model_name/prompt_version are real columns on this table
    # (confirmed against the migration DDL) and make the "newest" pick
    # deterministic instead of it flipping (and evidence_hash with it) between
    # runs when timestamps collide.
    assert (
        "ORDER BY info.resolved_at DESC, info.model_provider, info.model_name, info.prompt_version"
        in sql
    )
    assert "LIMIT 1 BY info.company_id, info.source_record_uid" in sql
    assert "NOT EXISTS" not in sql  # dedupe is publish_with_stage's job now


def test_esef_asset_depends_on_the_document_information_asset() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(
        dg.AssetKey("se_company_info_esef_clickhouse")
    )
    assert asset.parent_keys == {
        dg.AssetKey("esef_document_company_information_clickhouse"),
        dg.AssetKey("esef_source_documents_clickhouse"),
    }
    assert asset.group_name == "se_company_esef"


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


def test_esef_asset_publishes_new_versions_only_via_left_anti_join() -> None:
    client = _FakeClient(
        existing_tables={
            "esef_document_company_information",
            "esef_source_documents",
            "se_company_info_esef",
        },
        answers=[[(2, 0)], [(10,)], [(1,)], [(11,)]],  # staged/invalid, existing, anti-join, total
    )

    result = se_company_info_esef_clickhouse.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), _FakeClickhouse(client)
    )

    insert_sql = next(
        sql
        for sql, _ in client.executed
        if sql.strip().startswith("INSERT INTO `corpscout`.`se_company_info_esef`")
    )
    assert "LEFT ANTI JOIN" in insert_sql
    assert result.metadata["appended_count"] == 1
    assert result.metadata["total_count"] == 11
