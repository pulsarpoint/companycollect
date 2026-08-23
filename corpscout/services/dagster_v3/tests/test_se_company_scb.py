from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.scb import (
    SE_COMPANY_INFO_SCB_COLUMNS,
    SE_COMPANY_INFO_SCB_SQL,
    se_company_info_scb_clickhouse,
)
from tests.se_company_ddl import declared_columns, projection_aliases


def test_scb_select_projects_envelope_then_payload_in_table_order() -> None:
    assert list(SE_COMPANY_INFO_SCB_COLUMNS) == [
        c for c in declared_columns("se_company_info_scb") if c != "evidence_hash"
    ]
    sql = SE_COMPANY_INFO_SCB_SQL
    assert projection_aliases(sql) == list(SE_COMPANY_INFO_SCB_COLUMNS)
    assert "FROM corpscout.se_companies AS companies FINAL" in sql
    assert (
        "ifNull(nullIf(companies.scb_source_record_uid, ''), companies.bolagsverket_source_record_uid) AS source_record_uid"
        in sql
    )
    # observed_at is when THIS artifact observed the version, not the register's own
    # stamp: se_companies.updated_from_raw_at is a single constant for a whole bulk load
    # (one value across all 3.5M rows) and is older than every published resolved_at, so
    # a version appended under it can never be seen as newer than the row it replaces --
    # build_changed_companies_sql would return nothing and the change would never publish.
    assert "now64(3, 'UTC') AS observed_at" in sql
    # (the industries CTE still tie-breaks on ITS updated_from_raw_at -- that one orders
    # SNI rows within a company, it never stamps the envelope)
    assert "companies.updated_from_raw_at" not in sql
    assert "%(source_run_id)s AS source_run_id" in sql
    # industries is pre-aggregated to one row per company_id in its own CTE, then
    # joined 1:1 -- no outer GROUP BY over every companies column is needed to
    # undo a fan-out. The tie-break is a tuple so a same-timestamp tie can't flip
    # the pick (and evidence_hash with it) between runs.
    assert (
        "argMaxIf(industries.sni_code, (industries.updated_from_raw_at, industries.sni_code), industries.is_primary = 1)"
        in sql
    )
    assert (
        "argMaxIf(industries.nace_rev2_class_code, (industries.updated_from_raw_at, industries.nace_rev2_class_code), industries.is_primary = 1)"
        in sql
    )
    assert "GROUP BY industries.company_id" in sql
    assert "LEFT JOIN industries ON industries.company_id = companies.company_id" in sql
    # The English activity description is the translator's, read exactly the way
    # corpscout.se_companies_translated reads it: one explicit language pair (a second
    # target language must never replace the English text), newest by version, keyed by
    # cityHash64 of the source text. The join miss reads as '' under either
    # join_use_nulls setting, which is what info_rules treats as "no translation yet".
    assert (
        "WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description'\n"
        "          AND source_lang = 'sv' AND target_lang = 'en'"
    ) in sql
    assert "argMax(translated_text, version) AS translated_text" in sql
    assert "GROUP BY source_text_hash" in sql
    assert ") AS act ON act.source_text_hash = cityHash64(ifNull(companies.activity_description, ''))" in sql
    assert "ifNull(act.translated_text, '') AS activity_description_en" in sql
    assert "GROUP BY\n        companies.company_id" not in sql  # the wide outer GROUP BY is gone
    assert "NOT EXISTS" not in sql  # dedupe is publish_with_stage's job now
    assert "match(companies.company_id, '^([0-9]{10}|[0-9]{12})$')" in sql


def test_scb_asset_reads_the_register_and_writes_its_own_table() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(
        dg.AssetKey("se_company_info_scb_clickhouse")
    )
    assert asset.parent_keys == {
        dg.AssetKey("sweden_company_companies_clickhouse"),
        dg.AssetKey("sweden_company_industries_clickhouse"),
        # The translator service writes corpscout.text_translations outside Dagster; this
        # asset is the one that enqueues the Swedish descriptions and waits for it.
        dg.AssetKey("sweden_company_translation_load"),
    }
    assert asset.group_name == "se_company_scb"
    assert asset.metadata["table"] == "corpscout.se_company_info_scb"


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


def test_scb_asset_publishes_new_versions_only_via_left_anti_join() -> None:
    client = _FakeClient(
        existing_tables={"se_companies", "se_industries", "text_translations", "se_company_info_scb"},
        answers=[[(2, 0)], [(10,)], [(1,)], [(11,)]],  # staged/invalid, existing, anti-join, total
    )

    result = se_company_info_scb_clickhouse.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), _FakeClickhouse(client)
    )

    insert_sql = next(
        sql
        for sql, _ in client.executed
        if sql.strip().startswith("INSERT INTO `corpscout`.`se_company_info_scb`")
    )
    assert "LEFT ANTI JOIN" in insert_sql
    assert result.metadata["appended_count"] == 1
    assert result.metadata["total_count"] == 11
