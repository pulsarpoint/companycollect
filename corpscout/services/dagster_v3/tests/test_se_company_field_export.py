"""The registry export: rows match the module, the asset seeds them in one insert."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.fields.export import (
    GROUP_NAME,
    registry_rows,
    se_company_field_registry_clickhouse,
)
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, field_by_name, field_names
from dagster_v3.defs.se_company.fields.sql import render_projection_sql, render_resolve_sql
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_FIELD_REGISTRY_COLUMNS

RENDERED_AT = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def test_one_row_per_field_plus_the_projection_row() -> None:
    rows = registry_rows(INFO_REGISTRY, rendered_at=RENDERED_AT)
    assert [row["field"] for row in rows] == [*field_names(INFO_REGISTRY), "*"]
    for row in rows:
        assert tuple(row) == SE_COMPANY_FIELD_REGISTRY_COLUMNS  # dict order == positional insert list
        assert (row["datatype"], row["country"], row["registry_version"], row["version"]) == (
            "info", "SE", "se-info-v1", RENDERED_AT)


def test_field_rows_carry_the_registry_and_the_generated_statement() -> None:
    rows = {row["field"]: row for row in registry_rows(INFO_REGISTRY, rendered_at=RENDERED_AT)}
    website = rows["website"]
    assert website["sources"] == ["domains", "wikidata"]
    assert (website["value_type"], website["display_group"], website["structured"], website["python_only"]) == (
        "url", "scale", False, False)
    assert (website["policy_name"], website["policy_version"]) == ("source_precedence", "source_precedence-v1")
    assert website["resolve_sql"] == render_resolve_sql(INFO_REGISTRY, field_by_name(INFO_REGISTRY, "website"))
    assert rows["employee_count"]["structured"] is True


def test_the_projection_row_carries_the_wide_statement() -> None:
    row = registry_rows(INFO_REGISTRY, rendered_at=RENDERED_AT)[-1]
    assert (row["field"], row["value_type"], row["display_group"], row["structured"], row["python_only"]) == (
        "*", "projection", "", False, False)
    assert (row["sources"], row["policy_name"], row["policy_version"]) == ([], "", "")
    assert row["resolve_sql"] == render_projection_sql(INFO_REGISTRY)


class _FakeClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[tuple[Any, ...]]]] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        if "system.tables" in sql:
            return [("se_company_field_registry",)]
        self.inserts.append((sql, list(params)))
        return []


class _FakeClickhouse:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[_FakeClient]:
        yield self._client


def test_the_asset_seeds_every_row_through_one_insert() -> None:
    client = _FakeClient()
    result = se_company_field_registry_clickhouse.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), _FakeClickhouse(client)
    )
    (sql, rows), = client.inserts
    assert sql == (
        "INSERT INTO corpscout.se_company_field_registry ("
        + ", ".join(SE_COMPANY_FIELD_REGISTRY_COLUMNS) + ") VALUES"
    )
    assert len(rows) == len(INFO_REGISTRY.fields) + 1
    assert rows[0][:3] == ("info", "SE", "legal_name") and rows[-1][2:4] == ("*", "projection")
    versions = {row[-1] for row in rows}
    assert len(versions) == 1 and next(iter(versions)).tzinfo is UTC  # one version per export
    assert result.metadata["rows"] == len(rows) and result.metadata["registry_version"] == "se-info-v1"
    assert result.metadata["fields"] == len(INFO_REGISTRY.fields)


def test_the_asset_is_wired_into_the_definitions() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    node = repository.asset_graph.get(dg.AssetKey("se_company_field_registry_clickhouse"))
    assert node.group_name == GROUP_NAME == "se_company_fields"
    assert node.parent_keys == set()
