"""The registry-driven resolve asset: the server-side parameter encoding, the
registry-statement loader, the changed-company scan (SQL pinned as text; executed in
test_se_company_field_resolve_clickhouse_local.py), the batch loop through the
scripted FakeClient, and the Definitions wiring."""

import re
from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.se_company.fields import resolve
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, field_names
from dagster_v3.defs.se_company.fields.resolve import (
    EPOCH_SQL,
    PROJECTION_FIELD,
    SELECTION_COLUMNS,
    SELECTION_REASONS,
    SECompanyFieldResolveConfig,
    ServerSideLiteral,
    build_batch_stats_sql,
    build_changed_companies_sql,
    build_registry_statements_sql,
    clickhouse_stamp,
    load_registry_statements,
    open_resolve_client,
    server_array,
    server_params,
    split_insert_header,
)
from dagster_v3.defs.se_company.fields.sql import render_projection_sql
from tests.se_company_ddl import declared_columns
from tests.test_se_company_common import FakeClient

NOW = datetime(2026, 9, 2, 10, 0, 0, tzinfo=UTC)
HANDELSBANKEN = "5020077862"
OTHER_COMPANY = "5560125220"
SOLE_TRADER = "196408233412"
PUBLISHED_AT = f"ifNull(published.resolved_at, {EPOCH_SQL})"


def test_server_side_literals_are_escaped_exactly_once_for_the_driver() -> None:
    """clickhouse-driver's server_side_params path quotes a non-str value's str() as-is
    and escapes a str twice, so the array literal is handed over as a non-str whose
    str() carries ONE level of escaping (verified against 26.5, see the module doc)."""
    plain = server_array(["5020077862", "5560125220"])
    assert plain.text == "['5020077862','5560125220']"
    assert str(plain) == "[\\'5020077862\\',\\'5560125220\\']"
    tricky = server_array(["it's", "a\\b"])
    assert tricky.text == "['it\\'s','a\\\\b']"
    assert str(tricky) == "[\\'it\\\\\\'s\\',\\'a\\\\\\\\b\\']"
    assert server_array(()).text == "[]" and str(server_array(())) == "[]"
    assert server_array(["x"]) == ServerSideLiteral("['x']") and server_array(["x"]) != ServerSideLiteral("['y']")
    assert repr(ServerSideLiteral("['x']")) == "ServerSideLiteral(\"['x']\")"

    params = server_params(company_ids=[HANDELSBANKEN], field="legal_name", source_run_id="run",
                           resolved_at=NOW, page_size=20_000, all_companies=0)
    assert params == {"company_ids": ServerSideLiteral("['5020077862']"), "field": "legal_name",
                      "source_run_id": "run", "resolved_at": "2026-09-02 10:00:00.000",
                      "page_size": 20_000, "all_companies": 0}
    assert clickhouse_stamp(NOW) == "2026-09-02 10:00:00.000"


def test_open_resolve_client_builds_a_server_side_params_client_from_the_resource(monkeypatch) -> None:
    built: list[dict] = []

    class _Client:
        def __init__(self, **kwargs) -> None:
            built.append(kwargs)
            self.disconnected = False

        def disconnect(self) -> None:
            self.disconnected = True

    monkeypatch.setattr(resolve, "Client", _Client)
    resource = ClickhouseResource(host="ch.local", port=9440, user="u", password="p", database="corpscout",
                                  secure=True, settings={"max_execution_time": 600})
    with open_resolve_client(resource) as client:
        assert isinstance(client, _Client) and client.disconnected is False
    assert built == [{"host": "ch.local", "port": 9440, "user": "u", "password": "p", "database": "corpscout",
                      "secure": True, "settings": {"max_execution_time": 600, "server_side_params": True}}]
    assert client.disconnected is True


def test_split_insert_header_reads_the_projection_target_and_columns() -> None:
    header = split_insert_header(
        "INSERT INTO corpscout.se_company_info (\n    company_id, legal_name,\n    `status`\n)\n"
        "WITH x AS (SELECT 1)\nSELECT company_id, legal_name, status FROM x")
    assert header.table == "corpscout.se_company_info"
    assert header.columns == ("company_id", "legal_name", "status")
    assert header.body == "WITH x AS (SELECT 1)\nSELECT company_id, legal_name, status FROM x"
    with pytest.raises(ValueError, match="INSERT INTO"):
        split_insert_header("SELECT 1")


def test_the_projection_header_names_every_wide_column_in_ddl_order() -> None:
    """The stage INSERT binds the projection's SELECT positionally to the header's
    column list, so that list must be the deployed table minus its MATERIALIZED hash."""
    header = split_insert_header(render_projection_sql(INFO_REGISTRY))
    assert header.table == "corpscout.se_company_info"
    assert list(header.columns) == [c for c in declared_columns("se_company_info") if c != "evidence_set_hash"]


def test_changed_companies_sql_reads_candidates_decisions_published_and_versions() -> None:
    sql = build_changed_companies_sql(INFO_REGISTRY)
    assert sql.startswith("WITH current_registry AS (")
    # The registry table is the version authority: what a company was resolved WITH.
    assert "argMax(registry_version, version) AS registry_version" in sql
    assert "argMax(policy_version, version) AS policy_version" in sql
    assert f"WHERE datatype = 'info' AND country = 'SE' AND field != '{PROJECTION_FIELD}'" in sql
    # candidates replaces the old artifacts CTE: max(extracted_at) IS the version column,
    # so no FINAL; the register-name gate rides on the same aggregate.
    assert "SELECT company_id, max(extracted_at) AS latest_extracted_at," in sql
    assert "countIf(field = 'legal_name' AND source IN ('bolagsverket', 'scb')) > 0 AS has_register_name" in sql
    assert ("FROM corpscout.se_company_field_candidate\n"
            "    WHERE ({all_companies:UInt8} = 1 OR company_id IN {company_ids:Array(String)})") in sql
    assert "se_company_field_candidate FINAL" not in sql
    # The decisions CTE keeps its alias: the backoffice Pipeline page mirrors this SQL.
    assert ("SELECT company_id, max(created_at) AS latest_correction_at\n"
            "    FROM corpscout.se_company_info_field_value") in sql
    assert "FROM corpscout.se_company_info AS final FINAL" in sql
    assert ("FROM corpscout.se_company_field AS resolved FINAL\n"
            "    INNER JOIN current_registry ON current_registry.field = resolved.field") in sql
    assert ("toUInt8(countIf(resolved.registry_version != current_registry.registry_version\n"
            "                        OR resolved.policy_version != current_registry.policy_version) > 0)"
            " AS version_changed") in sql
    assert "WHERE candidates.has_register_name\n  AND (" in sql
    assert "ifNull(published.company_id, '') = ''\n     OR " in sql
    assert (f"OR ({{resolve_all:UInt8}} = 1 AND {PUBLISHED_AT} < "
            "parseDateTime64BestEffort({resolve_all_before:String}, 3, 'UTC'))") in sql
    assert f"OR candidates.latest_extracted_at > {PUBLISHED_AT}" in sql
    assert f"OR ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {PUBLISHED_AT}" in sql
    assert "OR ifNull(versions.version_changed, 0) = 1" in sql
    # Every LEFT JOIN miss goes through ifNull (join_use_nulls = 1 safety).
    assert "> published.resolved_at" not in sql and "ledger.latest_correction_at >" not in sql
    assert "versions.version_changed = 1" not in sql
    assert "AND candidates.company_id > {after_company_id:String}" in sql
    assert sql.endswith("ORDER BY candidates.company_id\nLIMIT {page_size:UInt32}")
    # Server-side placeholders only; no model terms survive from the old scan.
    assert "%(" not in sql and "pending_model" not in sql and "multi_source" not in sql


def test_the_scan_projects_why_each_company_was_selected() -> None:
    sql = build_changed_companies_sql(INFO_REGISTRY)
    assert SELECTION_REASONS == ("never_published", "new_candidates", "decision_pending", "version_changed")
    assert SELECTION_COLUMNS == ("company_id", *SELECTION_REASONS)
    projected = re.search(r"SELECT candidates\.company_id AS company_id,\n(.*?)\nFROM candidates", sql, re.DOTALL)
    assert projected is not None
    assert [line.split(" AS ")[-1].strip() for line in projected.group(1).split(",\n")] == list(SELECTION_REASONS)
    assert "ifNull(published.company_id, '') = '' AS never_published" in sql
    assert f"candidates.latest_extracted_at > {PUBLISHED_AT} AS new_candidates" in sql
    assert f"ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {PUBLISHED_AT} AS decision_pending" in sql
    assert "ifNull(versions.version_changed, 0) = 1 AS version_changed" in sql


PROJECTION_STATEMENT = (
    "INSERT INTO corpscout.se_company_info (company_id, legal_name, source_record_uids)\n"
    "SELECT company_id, value AS legal_name, [source_record_uid] AS source_record_uids\n"
    "FROM corpscout.se_company_field FINAL\n"
    "WHERE field = 'legal_name' AND company_id IN {company_ids:Array(String)}")


def _registry_rows(version: str = INFO_REGISTRY.version, *, drop: str = "") -> list[tuple]:
    """Scripted answer for build_registry_statements_sql: (field, resolve_sql,
    policy_version, registry_version), alphabetical like the real ORDER BY. The fake
    statements name their field in the SQL text so a test can read the order back."""
    rows = [(name, f"INSERT INTO corpscout.se_company_field SELECT '{name}' AS field, "
                   "arrayJoin({company_ids:Array(String)}) AS company_id, {field:String} AS f, "
                   "{source_run_id:String} AS source_run_id, {resolved_at:DateTime64(3, 'UTC')} AS resolved_at",
             "source_precedence-v1", version)
            for name in field_names(INFO_REGISTRY) if name != drop]
    rows.append((PROJECTION_FIELD, PROJECTION_STATEMENT, "", version))
    return sorted(rows)


def test_registry_statements_sql_and_loader_refuse_a_stale_or_partial_export() -> None:
    sql = build_registry_statements_sql(INFO_REGISTRY)
    assert sql == (
        "SELECT field,\n"
        "    argMax(resolve_sql, version) AS resolve_sql,\n"
        "    argMax(policy_version, version) AS policy_version,\n"
        "    argMax(registry_version, version) AS registry_version\n"
        "FROM corpscout.se_company_field_registry\n"
        "WHERE datatype = 'info' AND country = 'SE'\n"
        "GROUP BY field\n"
        "ORDER BY field")

    statements = load_registry_statements(FakeClient(answers=[_registry_rows()]), INFO_REGISTRY)
    assert statements.registry_version == INFO_REGISTRY.version
    assert list(statements.resolve_sql) == list(field_names(INFO_REGISTRY))  # registry order, not alphabetical
    assert statements.projection_sql == PROJECTION_STATEMENT

    with pytest.raises(ValueError, match="materialize se_company_field_registry_clickhouse first"):
        load_registry_statements(FakeClient(answers=[_registry_rows(version="se-info-v0")]), INFO_REGISTRY)
    with pytest.raises(ValueError, match=r"no row for \['website'\]"):
        load_registry_statements(FakeClient(answers=[_registry_rows(drop="website")]), INFO_REGISTRY)
    with pytest.raises(ValueError, match="no row for"):
        load_registry_statements(FakeClient(answers=[[]]), INFO_REGISTRY)


def test_batch_stats_sql_counts_this_runs_rows_per_field_source_and_decision() -> None:
    assert build_batch_stats_sql() == (
        "SELECT field, source, toUInt8(decision_id IS NOT NULL) AS from_decision, count() AS rows\n"
        "FROM corpscout.se_company_field\n"
        "WHERE source_run_id = {source_run_id:String} AND company_id IN {company_ids:Array(String)}\n"
        "GROUP BY field, source, from_decision\n"
        "ORDER BY field, source, from_decision")


def test_the_config_defaults_to_a_preview_and_caps_the_batch() -> None:
    config = SECompanyFieldResolveConfig()
    assert config.execute is False and config.company_ids == [] and config.fields == []
    assert config.max_companies is None and config.company_batch_size == 20_000
    assert config.resolve_all is False and config.resolve_all_before is None
    with pytest.raises(ValueError):
        SECompanyFieldResolveConfig(company_batch_size=20_001)
    with pytest.raises(ValueError):
        SECompanyFieldResolveConfig(max_companies=0)
