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


# --- the batch loop -------------------------------------------------------------------

from dagster_v3.defs.se_company.fields.resolve import (  # noqa: E402
    RESOLVE_ASSET,
    FieldStats,
    materialize_se_company_fields,
)

IDS = ServerSideLiteral("['5020077862']")
RESOLVED_AT = "2026-09-02 10:00:00.000"
PUBLISH_ANSWERS = [[(1, 0)], [(0,)], [(1,)]]  # stage validation, existing count, final count


def _context(run_id: str = "run") -> SimpleNamespace:
    """What materialize_se_company_fields reads off the asset context."""
    logged: list[tuple] = []
    return SimpleNamespace(run_id=run_id, log=SimpleNamespace(info=lambda *args: logged.append(args)),
                           logged=logged)


def _selected(company_id: str, **flags: int) -> tuple:
    unknown = set(flags) - set(SELECTION_REASONS)
    assert not unknown, f"not scan reasons: {sorted(unknown)}"
    return (company_id, *(int(flags.get(name, 0)) for name in SELECTION_REASONS))


def _scans(client: FakeClient) -> list[tuple[str, dict]]:
    return [(sql, params) for sql, params in client.executed if sql.startswith("WITH current_registry AS (")]


def _resolve_inserts(client: FakeClient) -> list[tuple[str, dict]]:
    return [(sql, params) for sql, params in client.executed
            if sql.startswith("INSERT INTO corpscout.se_company_field ")]


def test_a_batch_runs_every_field_statement_in_registry_order_then_the_projection_then_the_counts() -> None:
    client = FakeClient(answers=[
        _registry_rows(),
        [_selected(HANDELSBANKEN, never_published=1, new_candidates=1)],
        *PUBLISH_ANSWERS,
        [("description_sv", "reviewer", 1, 1), ("legal_name", "bolagsverket", 0, 1)],  # batch stats
    ])
    summary = materialize_se_company_fields(
        _context(), client, SECompanyFieldResolveConfig(execute=True, company_batch_size=2),
        registry=INFO_REGISTRY, now=NOW)

    statements = [sql for sql, _ in client.executed]
    inserts = _resolve_inserts(client)
    assert [re.search(r"SELECT '([a-z_]+)' AS field", sql).group(1) for sql, _ in inserts] == list(field_names(INFO_REGISTRY))
    for name, (sql, params) in zip(field_names(INFO_REGISTRY), inserts, strict=True):
        # The statement text reaches the server untouched; the values travel beside it.
        assert "{company_ids:Array(String)}" in sql and "{resolved_at:DateTime64(3, 'UTC')}" in sql
        assert params == {"company_ids": IDS, "field": name, "source_run_id": "run", "resolved_at": RESOLVED_AT}
    # All fields, THEN the projection through the stage, THEN the counts.
    stage = next(i for i, sql in enumerate(statements) if sql.startswith("CREATE TABLE `corpscout`.`_tmp_se_company_info_"))
    assert stage > statements.index(inserts[-1][0])
    stage_sql, stage_params = client.executed[stage + 1]
    assert stage_sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_")
    assert "(company_id,\n    legal_name,\n    source_record_uids)\n" in stage_sql
    assert stage_sql.endswith("WHERE field = 'legal_name' AND company_id IN {company_ids:Array(String)}")
    assert "INSERT INTO corpscout.se_company_info" not in stage_sql  # the header was split off
    assert stage_params == {"company_ids": IDS}
    assert "countIf(trim(legal_name) = '' OR empty(source_record_uids))" in statements[stage + 2]
    assert any(sql.startswith("INSERT INTO `corpscout`.`se_company_info` (company_id,") for sql in statements)
    drop = next(i for i, sql in enumerate(statements) if sql.startswith("DROP TABLE IF EXISTS"))
    stats_sql, stats_params = client.executed[-1]
    assert len(statements) - 1 > drop
    assert stats_sql.startswith("SELECT field, source, toUInt8(decision_id IS NOT NULL) AS from_decision")
    assert stats_params == {"company_ids": IDS, "source_run_id": "run"}
    assert all(settings is None for settings in client.settings_calls)

    assert summary.companies_selected == 1 and summary.companies_published == 1
    assert summary.per_reason == {"never_published": 1, "new_candidates": 1, "decision_pending": 0, "version_changed": 0}
    assert summary.per_field["legal_name"] == FieldStats(rows=1, from_decision=0, per_source={"bolagsverket": 1}, no_row=0)
    assert summary.per_field["description_sv"] == FieldStats(rows=1, from_decision=1, per_source={"reviewer": 1}, no_row=0)
    assert summary.per_field["website"] == FieldStats(rows=0, from_decision=0, per_source={}, no_row=1)
    assert set(summary.per_field) == set(field_names(INFO_REGISTRY))
    assert summary.preview is False and summary.stopped_at_cap is False
    assert summary.registry_version == INFO_REGISTRY.version and summary.source_run_id == "run"


def test_a_preview_scans_everything_and_writes_nothing() -> None:
    client = FakeClient(answers=[
        _registry_rows(),
        [_selected(HANDELSBANKEN, never_published=1, new_candidates=1, decision_pending=1),
         _selected(OTHER_COMPANY, version_changed=1)],
        [],  # page 2: exhausted
    ])
    summary = materialize_se_company_fields(
        _context(), client, SECompanyFieldResolveConfig(company_batch_size=2), registry=INFO_REGISTRY, now=NOW)

    assert all(sql.lstrip().upper().startswith(("SELECT", "WITH")) for sql, _ in client.executed)
    assert len(_scans(client)) == 2 and _resolve_inserts(client) == []
    assert _scans(client)[0][1] == {"company_ids": ServerSideLiteral("[]"), "all_companies": 1, "resolve_all": 0,
                                    "resolve_all_before": RESOLVED_AT, "after_company_id": "", "page_size": 2}
    assert _scans(client)[1][1]["after_company_id"] == OTHER_COMPANY
    assert summary.preview is True and summary.companies_selected == 2 and summary.companies_published == 0
    assert summary.per_field == {}
    assert summary.per_reason == {"never_published": 1, "new_candidates": 1, "decision_pending": 1, "version_changed": 1}
    metadata = summary.metadata()
    assert metadata["preview"] is True and metadata["companies_selected"] == 2
    assert {reason: metadata[reason] for reason in SELECTION_REASONS} == summary.per_reason
    assert metadata["company_scope"] == [] and metadata["registry_version"] == INFO_REGISTRY.version
    assert isinstance(metadata["per_field"], dg.JsonMetadataValue)


def test_the_scan_is_paged_by_keyset_and_stops_on_a_short_page_or_at_the_cap() -> None:
    client = FakeClient(answers=[
        _registry_rows(),
        [_selected(OTHER_COMPANY), _selected(HANDELSBANKEN)],  # page 1: full
        [_selected("5567890123")],                             # page 2: short -> stop
    ])
    summary = materialize_se_company_fields(
        _context(), client, SECompanyFieldResolveConfig(company_batch_size=2), registry=INFO_REGISTRY, now=NOW)
    scans = _scans(client)
    assert len(scans) == 2 and summary.companies_selected == 3 and summary.stopped_at_cap is False
    assert [params["after_company_id"] for _, params in scans] == ["", HANDELSBANKEN]
    assert [params["page_size"] for _, params in scans] == [2, 2]

    capped = FakeClient(answers=[_registry_rows(), [_selected(HANDELSBANKEN)]])
    context = _context()
    summary = materialize_se_company_fields(
        context, capped, SECompanyFieldResolveConfig(company_batch_size=1, max_companies=1),
        registry=INFO_REGISTRY, now=NOW)
    assert summary.stopped_at_cap is True and len(_scans(capped)) == 1
    assert any("max_companies cap" in str(entry[0]) for entry in context.logged)


def test_resolve_all_binds_its_cutoff_and_an_explicit_scope_is_chunked() -> None:
    def _first_scan_params(config: SECompanyFieldResolveConfig) -> dict:
        client = FakeClient(answers=[_registry_rows(), []])
        materialize_se_company_fields(_context(), client, config, registry=INFO_REGISTRY, now=NOW)
        return _scans(client)[0][1]

    # Always bound, resolve_all or not -- parseDateTime64BestEffort('') would be an error.
    assert _first_scan_params(SECompanyFieldResolveConfig())["resolve_all"] == 0
    assert _first_scan_params(SECompanyFieldResolveConfig())["resolve_all_before"] == RESOLVED_AT
    on = _first_scan_params(SECompanyFieldResolveConfig(resolve_all=True))
    assert on["resolve_all"] == 1 and on["resolve_all_before"] == RESOLVED_AT
    explicit = _first_scan_params(SECompanyFieldResolveConfig(resolve_all=True, resolve_all_before="2026-08-23 18:30:00"))
    assert explicit["resolve_all_before"] == "2026-08-23 18:30:00"

    scope = [f"55600000{index:02d}" for index in range(7)]
    chunked = FakeClient(answers=[_registry_rows(), [], [], []])  # one empty scan per chunk
    summary = materialize_se_company_fields(
        _context(), chunked, SECompanyFieldResolveConfig(company_ids=scope, company_batch_size=3),
        registry=INFO_REGISTRY, now=NOW)
    scans = _scans(chunked)
    assert [params["company_ids"] for _, params in scans] == [
        server_array(scope[0:3]), server_array(scope[3:6]), server_array(scope[6:7])]
    assert all(params["all_companies"] == 0 and params["after_company_id"] == "" for _, params in scans)
    assert summary.company_scope == tuple(scope)
    # A twelve-digit sole-trader id is a valid scope.
    materialize_se_company_fields(
        _context(), FakeClient(answers=[_registry_rows(), []]),
        SECompanyFieldResolveConfig(company_ids=[SOLE_TRADER]), registry=INFO_REGISTRY, now=NOW)


def test_a_fields_subset_runs_only_those_statements_and_still_projects() -> None:
    client = FakeClient(answers=[
        _registry_rows(), [_selected(HANDELSBANKEN)], *PUBLISH_ANSWERS,
        [("website", "domains", 0, 1)],
    ])
    summary = materialize_se_company_fields(
        _context(), client,
        SECompanyFieldResolveConfig(execute=True, fields=["website", "legal_name"], company_batch_size=2),
        registry=INFO_REGISTRY, now=NOW)
    names = [re.search(r"SELECT '([a-z_]+)' AS field", sql).group(1) for sql, _ in _resolve_inserts(client)]
    assert names == [name for name in field_names(INFO_REGISTRY) if name in {"website", "legal_name"}]  # registry order
    assert any(sql.startswith("CREATE TABLE `corpscout`.`_tmp_se_company_info_") for sql, _ in client.executed)
    assert set(summary.per_field) == {"website", "legal_name"}
    assert summary.per_field["legal_name"] == FieldStats(rows=0, from_decision=0, per_source={}, no_row=1)

    with pytest.raises(ValueError, match=r"Not registry fields: \['bogus'\]"):
        materialize_se_company_fields(
            _context(), FakeClient(answers=[]), SECompanyFieldResolveConfig(fields=["bogus"]),
            registry=INFO_REGISTRY, now=NOW)


def test_a_stale_registry_export_stops_the_run_before_the_scan() -> None:
    client = FakeClient(answers=[_registry_rows(version="se-info-v0")])
    with pytest.raises(ValueError, match="materialize se_company_field_registry_clickhouse first"):
        materialize_se_company_fields(
            _context(), client, SECompanyFieldResolveConfig(execute=True), registry=INFO_REGISTRY, now=NOW)
    assert _scans(client) == []


def test_the_asset_is_declared_with_its_deps_group_and_tables() -> None:
    from dagster_v3.defs.se_company.fields.resolve import (
        CANDIDATE_ASSETS,
        REGISTRY_ASSET,
        se_company_field_resolved_clickhouse,
    )

    assert se_company_field_resolved_clickhouse.key == dg.AssetKey(RESOLVE_ASSET)
    spec = se_company_field_resolved_clickhouse.specs_by_key[dg.AssetKey(RESOLVE_ASSET)]
    assert spec.group_name == "se_company_fields"
    assert {dep.asset_key for dep in spec.deps} == {dg.AssetKey(REGISTRY_ASSET), *(dg.AssetKey(n) for n in CANDIDATE_ASSETS)}
    assert spec.metadata["table"] == "corpscout.se_company_field"
    assert spec.metadata["wide_table"] == "corpscout.se_company_info"
    assert spec.kinds == {"clickhouse", "python"}
