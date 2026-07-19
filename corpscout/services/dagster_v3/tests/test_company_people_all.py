import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.company_people.assets import (
    CompanyPeopleClickhouseExportConfig,
    _company_people_all_quality_sql,
    build_company_people_all_insert_sql,
    replace_company_people_all_clickhouse,
)
from dagster_v3.defs.company_people.tables import (
    COMPANY_PEOPLE_ALL_COLUMNS,
    COMPANY_PEOPLE_ALL_TABLE,
    PEOPLE_SOURCES,
    QUALIFIED_COMPANY_PEOPLE_ALL_TABLE,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"

_STAGE_TABLE = "`corpscout`.`_tmp_company_people_all_test`"


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text(encoding="utf-8")


def _built_sql() -> str:
    return build_company_people_all_insert_sql(_STAGE_TABLE)


def test_company_people_all_table_and_columns_constants() -> None:
    assert COMPANY_PEOPLE_ALL_TABLE == "company_people_all"
    assert QUALIFIED_COMPANY_PEOPLE_ALL_TABLE == "corpscout.company_people_all"
    assert COMPANY_PEOPLE_ALL_COLUMNS == (
        "country_iso2",
        "company_id",
        "company_name",
        "first_name",
        "last_name",
        "full_name_normalized",
        "role_original",
        "role_kind",
        "signatory_kind",
        "fiscal_year",
        "identifier_kind",
        "identifier_value",
        "source",
        "source_statement_key",
        "resolved_at",
    )


def test_migration_000145_covers_company_people_all_columns_in_order() -> None:
    """Contract test (mirrors officers.py's migration-column contract): every
    column in ``COMPANY_PEOPLE_ALL_COLUMNS`` must appear, in order, in
    migration 000145's up SQL -- pins the Python columns tuple to the
    ClickHouse schema so the two can never silently drift apart.
    """
    up_sql = _migration_sql("000145_corpscout_company_people_all.up.sql")
    down_sql = _migration_sql("000145_corpscout_company_people_all.down.sql")

    assert "CREATE DATABASE IF NOT EXISTS corpscout" in up_sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.company_people_all" in up_sql

    last_index = -1
    for column_name in COMPANY_PEOPLE_ALL_COLUMNS:
        marker = f"    {column_name} "
        index = up_sql.index(marker)
        assert index > last_index, (
            f"column {column_name!r} is out of order relative to "
            "COMPANY_PEOPLE_ALL_COLUMNS in the migration file"
        )
        last_index = index

    assert "country_iso2 LowCardinality(String)" in up_sql
    assert "role_kind LowCardinality(String)" in up_sql
    assert "signatory_kind LowCardinality(String)" in up_sql
    assert "identifier_kind LowCardinality(String)" in up_sql
    assert "source LowCardinality(String)" in up_sql
    assert "fiscal_year Int32" in up_sql
    assert "resolved_at DateTime64(3, 'UTC')" in up_sql
    assert (
        "INDEX idx_people_name full_name_normalized TYPE ngrambf_v1(3, 65536, 3, 7) "
        "GRANULARITY 4" in up_sql
    )
    assert "ENGINE = MergeTree" in up_sql
    assert (
        "ORDER BY (full_name_normalized, country_iso2, company_id, fiscal_year)"
        in up_sql
    )
    assert "DROP TABLE IF EXISTS corpscout.company_people_all" in down_sql


def test_people_sources_registry_has_one_se_entry() -> None:
    assert set(PEOPLE_SOURCES) == {"se_xbrl_signatures"}


def test_company_people_all_insert_sql_targets_stage_table_with_explicit_columns() -> (
    None
):
    sql = _built_sql()

    assert sql.startswith(f"INSERT INTO {_STAGE_TABLE} (")
    columns_block = sql[: sql.index(")\n")]
    for column_name in COMPANY_PEOPLE_ALL_COLUMNS:
        assert column_name in columns_block


def test_company_people_all_insert_sql_unions_every_people_source() -> None:
    sql = _built_sql()

    for select_sql in PEOPLE_SOURCES.values():
        assert select_sql in sql
    if len(PEOPLE_SOURCES) > 1:
        assert "UNION ALL" in sql


def test_se_xbrl_signatures_select_joins_se_companies_for_company_name() -> None:
    sql = PEOPLE_SOURCES["se_xbrl_signatures"]

    assert "FROM corpscout.se_company_officers AS o" in sql
    assert (
        "LEFT JOIN corpscout.se_companies AS c ON c.registration_number = o.company_id"
        in sql
    )
    assert "coalesce(c.legal_name, '')" in sql
    assert "'SE' AS country_iso2" in sql
    assert "'se_xbrl_signatures' AS source" in sql
    assert "'' AS identifier_kind" in sql
    assert "'' AS identifier_value" in sql
    assert (
        "lowerUTF8(trim(concat(o.first_name, ' ', o.last_name))) AS full_name_normalized"
        in sql
    )


def test_se_xbrl_signatures_select_dedupes_one_row_per_person_and_signatory_kind() -> (
    None
):
    """Locks in the plan's dedupe contract: one row per (company_id,
    fiscal_year, first_name, last_name, signatory_kind), preferring a
    resolved role_kind over 'unknown' via argMax -- the same preference
    shape as the plan's Task 4 backoffice officers query -- with
    statement_key appended as an explicit tuple tiebreak (Finding 1) so two
    grouped rows that both have a known role_kind still resolve to one
    deterministic winning row instead of an arbitrary ClickHouse pick.
    """
    sql = PEOPLE_SOURCES["se_xbrl_signatures"]

    assert (
        "GROUP BY o.company_id, o.fiscal_year, o.first_name, o.last_name, "
        "o.signatory_kind" in sql
    )
    tiebreak = "(o.role_kind != 'unknown', o.statement_key)"
    assert f"argMax(o.role_original, {tiebreak}) AS role_original" in sql
    assert f"argMax(o.role_kind, {tiebreak}) AS role_kind" in sql
    assert f"argMax(o.statement_key, {tiebreak}) AS source_statement_key" in sql


def _simulate_role_argmax(rows: list[dict[str, str]]) -> dict[str, str]:
    """Pure-Python mirror of the SE source SELECT's
    ``argMax(x, (role_kind != 'unknown', statement_key))`` tiebreak (Finding
    1) -- reimplemented here, not imported, because this module's tests are
    string-level with no live ClickHouse to execute the real SQL against.
    Rows are assumed to already share one (company_id, fiscal_year,
    first_name, last_name, signatory_kind) GROUP BY key, exactly the
    scenario the module docstring's tiebreak note reasons about. ClickHouse
    ``argMax`` picks the row with the greatest comparator value (ties broken
    lexicographically for a tuple), which is exactly what ``max(..., key=)``
    does here.
    """
    winner = max(
        rows,
        key=lambda row: (row["role_kind"] != "unknown", row["statement_key"]),
    )
    return {
        "role_original": winner["role_original"],
        "role_kind": winner["role_kind"],
        "source_statement_key": winner["statement_key"],
    }


def test_role_argmax_simulation_breaks_known_role_ties_deterministically() -> None:
    """Locks in Finding 1's fix: two rows in the same (company, fiscal_year,
    person, signatory_kind) GROUP BY group can both carry a known role_kind
    (e.g. the same person signing under an original filing and again under a
    later-amended filing within the same fiscal year) -- a bare
    ``argMax(x, role_kind != 'unknown')`` condition ties on both rows and
    ClickHouse gives no deterministic winner among ties on a plain boolean.
    The ``(role_kind != 'unknown', statement_key)`` tuple breaks that tie
    deterministically by preferring the greater statement_key, and
    role_original/role_kind/source_statement_key must all come from that
    SAME winning row rather than being resolved independently.
    """
    rows = [
        {
            "statement_key": "se-2023-stmt-v1",
            "role_original": "Styrelseledamot",
            "role_kind": "board_member",
        },
        {
            "statement_key": "se-2023-stmt-v2",
            "role_original": "Verkställande direktör",
            "role_kind": "ceo",
        },
    ]
    assert rows[1]["statement_key"] > rows[0]["statement_key"]

    result = _simulate_role_argmax(rows)

    assert result == {
        "role_original": "Verkställande direktör",
        "role_kind": "ceo",
        "source_statement_key": "se-2023-stmt-v2",
    }

    # Order-independence: the winner must not depend on input row order,
    # since ClickHouse's argMax does not guarantee scan order either.
    assert _simulate_role_argmax(list(reversed(rows))) == result


def test_company_people_all_quality_sql_counts_countries_companies_sources() -> None:
    sql = _company_people_all_quality_sql(_STAGE_TABLE)

    assert sql.startswith("SELECT")
    assert f"FROM {_STAGE_TABLE}" in sql
    assert "count() AS row_count" in sql
    assert "uniqExact(country_iso2) AS country_count" in sql
    assert "uniqExact(company_id) AS company_count" in sql
    assert "uniqExact(source) AS source_count" in sql


def test_company_people_all_sql_has_no_bare_percent_placeholder_leaks() -> None:
    # Only resolved_at is a driver placeholder; no other %(name)s markers.
    sql = _built_sql()

    placeholders = set(re.findall(r"%\([a-zA-Z_]+\)s", sql))
    assert placeholders == {"%(resolved_at)s"}


def test_company_people_all_sql_survives_percent_formatting_with_driver_params() -> (
    None
):
    """Regression test mirroring officers.py's %-format round-trip check:
    ``replace_company_people_all_clickhouse`` calls ``client.execute(sql,
    {...})`` with a params dict, so clickhouse_driver Python-%-formats the
    whole query text against that dict. There are no LIKE/ILIKE literals in
    this SELECT today, so there is nothing to double -- this test just locks
    in that the round-trip doesn't blow up and leaves no stray '%(...)s'.
    """
    sql = _built_sql()
    params = {
        "resolved_at": datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        "source_run_id": "run-1",
    }

    formatted = sql % params

    assert "%(resolved_at)s" not in formatted
    assert re.findall(r"%\([a-zA-Z_]+\)s", formatted) == []


class _FakeCompanyPeopleClickHouseClient:
    def __init__(
        self,
        *,
        quality_row: tuple[object, ...],
        existing_row_count: int = 0,
    ) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.insert_parameters: dict[str, object] = {}
        self.quality_row = quality_row
        self.existing_row_count = existing_row_count

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
        if "AS row_count" in sql:
            return [self.quality_row]
        # The shrink guard's pre-EXCHANGE existing-row-count check has no
        # alias, so it must be checked AFTER "AS row_count" above.
        if sql.startswith("SELECT count() FROM"):
            return [(self.existing_row_count,)]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def _patch_clickhouse(
    monkeypatch, client: _FakeCompanyPeopleClickHouseClient
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeCompanyPeopleClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


_DEFAULT_QUALITY_ROW = (5000, 3, 3000, 1)


def test_replace_company_people_all_is_atomic_and_reports_counts(monkeypatch) -> None:
    client = _FakeCompanyPeopleClickHouseClient(quality_row=_DEFAULT_QUALITY_ROW)
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_company_people_all_clickhouse(
        clickhouse=resource,
        source_run_id="people-run",
        resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
    )

    assert client.table_checks == [
        ("company_people_all", "se_company_officers", "se_companies"),
    ]
    assert any(statement.startswith("CREATE TABLE") for statement in client.statements)
    assert any(statement.startswith("INSERT INTO") for statement in client.statements)
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")

    assert client.insert_parameters["source_run_id"] == "people-run"
    assert client.insert_parameters["resolved_at"] == datetime(
        2026, 7, 19, 15, 0, tzinfo=UTC
    )

    assert metadata["row_count"] == 5000
    assert metadata["country_count"] == 3
    assert metadata["company_count"] == 3000
    assert metadata["source_count"] == 1
    assert metadata["table"] == QUALIFIED_COMPANY_PEOPLE_ALL_TABLE
    assert metadata["source_run_id"] == "people-run"


def test_replace_company_people_all_refuses_to_swap_an_empty_stage(
    monkeypatch,
) -> None:
    client = _FakeCompanyPeopleClickHouseClient(quality_row=(0, 0, 0, 0))
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="produced no rows"):
        replace_company_people_all_clickhouse(
            clickhouse=resource,
            source_run_id="people-run",
            resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        )

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    # Cleanup still runs (finally block) even on a refused replace.
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_company_people_all_refuses_shrink_below_half(monkeypatch) -> None:
    quality_row = (49, *_DEFAULT_QUALITY_ROW[1:])
    client = _FakeCompanyPeopleClickHouseClient(
        quality_row=quality_row, existing_row_count=100
    )
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="Refusing to replace ClickHouse table"):
        replace_company_people_all_clickhouse(
            clickhouse=resource,
            source_run_id="people-run",
            resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        )

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_company_people_all_allow_shrink_overrides_guard(monkeypatch) -> None:
    quality_row = (1, *_DEFAULT_QUALITY_ROW[1:])
    client = _FakeCompanyPeopleClickHouseClient(
        quality_row=quality_row, existing_row_count=100
    )
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_company_people_all_clickhouse(
        clickhouse=resource,
        source_run_id="people-run",
        resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        allow_shrink=True,
    )

    assert metadata["row_count"] == 1
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


def test_company_people_clickhouse_export_config_defaults_allow_shrink_false() -> None:
    config = CompanyPeopleClickhouseExportConfig()
    assert config.allow_shrink is False


def test_company_people_all_clickhouse_asset_is_wired_correctly() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()

    node = repo.asset_graph.get(dg.AssetKey("company_people_all_clickhouse"))
    assert node.group_name == "company_people_all"
    assert node.pools == set()
    assert node.partitions_def is None
    assert node.parent_keys == {dg.AssetKey("se_company_officers_clickhouse")}


def test_company_people_all_schedule_running_daily_cron() -> None:
    from dagster_v3.defs.company_people.assets import company_people_all_schedule

    # Daily at 07:45 UTC -- offset from companies_all_schedule's 07:15
    # (Europe/Oslo) to avoid contention.
    assert company_people_all_schedule.cron_schedule == "45 7 * * *"
    assert company_people_all_schedule.execution_timezone == "UTC"
    assert (
        company_people_all_schedule.default_status == dg.DefaultScheduleStatus.RUNNING
    )


def test_company_people_all_defs_include_job_and_schedule() -> None:
    from dagster_v3.defs.company_people.assets import defs

    job_names = {job.name for job in defs.jobs}
    assert "company_people_all_job" in job_names

    schedule_names = {schedule.name for schedule in defs.schedules}
    assert "company_people_all_schedule" in schedule_names
