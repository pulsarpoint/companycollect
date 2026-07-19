import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.officers import (
    QUALIFIED_SE_COMPANY_OFFICERS_TABLE,
    SE_COMPANY_OFFICERS_COLUMNS,
    SE_COMPANY_OFFICERS_TABLE,
    _officers_quality_sql,
    build_officers_insert_sql,
    replace_se_company_officers_clickhouse,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"

_STAGE_TABLE = "`corpscout`.`_tmp_se_company_officers_test`"


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text(encoding="utf-8")


def _built_sql() -> str:
    return build_officers_insert_sql(_STAGE_TABLE)


def test_officers_table_and_columns_constants() -> None:
    assert SE_COMPANY_OFFICERS_TABLE == "se_company_officers"
    assert QUALIFIED_SE_COMPANY_OFFICERS_TABLE == "corpscout.se_company_officers"
    assert SE_COMPANY_OFFICERS_COLUMNS == (
        "company_id",
        "fiscal_year",
        "statement_key",
        "signatory_kind",
        "person_seq",
        "first_name",
        "last_name",
        "role_original",
        "role_kind",
        "resolved_at",
    )


def test_migration_000143_covers_officers_columns_in_order() -> None:
    """Contract test (mirrors history.py's migration-column contract): every
    column in ``SE_COMPANY_OFFICERS_COLUMNS`` must appear, in order, in
    migration 000143's up SQL -- pins the Python columns tuple to the
    ClickHouse schema so the two can never silently drift apart.
    """
    up_sql = _migration_sql("000143_corpscout_se_company_officers.up.sql")
    down_sql = _migration_sql("000143_corpscout_se_company_officers.down.sql")

    assert "CREATE DATABASE IF NOT EXISTS corpscout" in up_sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_officers" in up_sql

    last_index = -1
    for column_name in SE_COMPANY_OFFICERS_COLUMNS:
        marker = f"    {column_name} "
        index = up_sql.index(marker)
        assert index > last_index, (
            f"column {column_name!r} is out of order relative to "
            "SE_COMPANY_OFFICERS_COLUMNS in the migration file"
        )
        last_index = index

    assert "signatory_kind LowCardinality(String)" in up_sql
    assert "person_seq UInt16" in up_sql
    assert "role_kind LowCardinality(String)" in up_sql
    assert "resolved_at DateTime64(3, 'UTC')" in up_sql
    assert "ENGINE = MergeTree" in up_sql
    assert (
        "ORDER BY (company_id, fiscal_year, statement_key, signatory_kind, person_seq)"
        in up_sql
    )
    assert "DROP TABLE IF EXISTS corpscout.se_company_officers" in down_sql


def test_officers_insert_sql_targets_stage_table_with_explicit_columns() -> None:
    sql = _built_sql()

    assert sql.startswith(f"INSERT INTO {_STAGE_TABLE} (")
    columns_block = sql[: sql.index(")\nWITH")]
    for column_name in SE_COMPANY_OFFICERS_COLUMNS:
        assert column_name in columns_block


def test_officers_sql_contains_the_four_concept_triples() -> None:
    sql = _built_sql()

    for first, last, role in (
        (
            "UnderskriftHandlingTilltalsnamn",
            "UnderskriftHandlingEfternamn",
            "UnderskriftHandlingRoll",
        ),
        (
            "UnderskriftArsredovisningForetradareTilltalsnamn",
            "UnderskriftArsredovisningForetradareEfternamn",
            "UnderskriftArsredovisningForetradareForetradarroll",
        ),
        (
            "UnderskriftFaststallelseintygForetradareTilltalsnamn",
            "UnderskriftFaststallelseintygForetradareEfternamn",
            "UnderskriftFaststallelseintygForetradareForetradarroll",
        ),
        (
            "UnderskriftRevisionsberattelseRevisorTilltalsnamn",
            "UnderskriftRevisionsberattelseRevisorEfternamn",
            "UnderskriftRevisionsberattelseRevisorTitel",
        ),
    ):
        assert f"'{first}'" in sql
        assert f"'{last}'" in sql
        assert f"'{role}'" in sql


def test_officers_sql_running_sum_person_grouping() -> None:
    sql = _built_sql()

    assert "sum(is_first_name) OVER (" in sql
    assert "PARTITION BY statement_key, signatory_kind" in sql
    assert "ORDER BY fact_ordinal" in sql
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in sql
    assert "AS person_seq" in sql
    assert "WHERE person_seq > 0" in sql
    assert (
        "GROUP BY company_id, fiscal_year, statement_key, signatory_kind, person_seq"
        in sql
    )


def _simulate_person_grouping(rows: list[dict[str, object]]) -> list[dict[str, str]]:
    """Pure-Python mirror of ``build_officers_insert_sql``'s running-sum
    person grouping (``sum(is_first_name) OVER (PARTITION BY statement_key,
    signatory_kind ORDER BY fact_ordinal ...) AS person_seq``, then
    ``GROUP BY ... person_seq`` collapsing via ``anyIf``), reimplemented
    here -- not imported -- because this module's tests are string-level
    with no live ClickHouse to execute the real SQL against. Rows are
    assumed to already share one (statement_key, signatory_kind) partition,
    which is exactly the scenario the module docstring's "Grouping-window
    safety" section reasons about.
    """
    groups: dict[int, dict[str, str]] = {}
    person_seq = 0
    for row in sorted(rows, key=lambda r: r["fact_ordinal"]):
        if row["is_first_name"]:
            person_seq += 1
        if person_seq == 0:
            continue
        group = groups.setdefault(
            person_seq, {"first_name": "", "last_name": "", "role_original": ""}
        )
        if row["is_first_name"]:
            group["first_name"] = row["value"]
        if row["is_last_name"]:
            group["last_name"] = row["value"]
        if row["is_role"]:
            group["role_original"] = row["value"]
    return [groups[seq] for seq in sorted(groups)]


def test_person_grouping_simulation_separates_board_signature_blocks_cleanly() -> None:
    """Locks in the reasoning in the module docstring's "Grouping-window
    safety" section: ``UnderskriftHandling*`` and
    ``UnderskriftArsredovisningForetradare*`` both classify to
    ``signatory_kind = 'board_signature'`` and therefore share ONE window
    partition, but ``fact_ordinal`` is genuine document order and iXBRL
    renders each signature block as a contiguous run of facts -- so a
    Handling-block person immediately followed by a DIFFERENT
    Arsredovisning-block person still collapses into two distinct person
    groups with no cross-attribution of names/roles between them.
    """
    fact_rows: list[dict[str, object]] = [
        # UnderskriftHandling* triple -- person "Anna Andersson".
        {
            "fact_ordinal": 1,
            "is_first_name": True,
            "is_last_name": False,
            "is_role": False,
            "value": "Anna",
        },
        {
            "fact_ordinal": 2,
            "is_first_name": False,
            "is_last_name": True,
            "is_role": False,
            "value": "Andersson",
        },
        {
            "fact_ordinal": 3,
            "is_first_name": False,
            "is_last_name": False,
            "is_role": True,
            "value": "Styrelseledamot",
        },
        # UnderskriftArsredovisningForetradare* triple -- a DIFFERENT
        # person, "Bo Bergqvist", in the same statement/signatory_kind.
        {
            "fact_ordinal": 4,
            "is_first_name": True,
            "is_last_name": False,
            "is_role": False,
            "value": "Bo",
        },
        {
            "fact_ordinal": 5,
            "is_first_name": False,
            "is_last_name": True,
            "is_role": False,
            "value": "Bergqvist",
        },
        {
            "fact_ordinal": 6,
            "is_first_name": False,
            "is_last_name": False,
            "is_role": True,
            "value": "Verkställande direktör",
        },
    ]

    groups = _simulate_person_grouping(fact_rows)

    assert len(groups) == 2
    assert groups[0] == {
        "first_name": "Anna",
        "last_name": "Andersson",
        "role_original": "Styrelseledamot",
    }
    assert groups[1] == {
        "first_name": "Bo",
        "last_name": "Bergqvist",
        "role_original": "Verkställande direktör",
    }


def test_officers_sql_signatory_kind_classification() -> None:
    sql = _built_sql()

    assert "LIKE 'UnderskriftRevisionsberattelseRevisor%%'" in sql
    assert "'auditor'" in sql
    assert "LIKE 'UnderskriftFaststallelseintygForetradare%%'" in sql
    assert "'certification'" in sql
    assert "'board_signature'" in sql


def test_officers_sql_role_kind_mapping_covers_expected_vocabulary() -> None:
    sql = _built_sql()

    for role_kind in (
        "chairman",
        "ceo",
        "deputy_board_member",
        "board_member",
        "liquidator",
        "auditor",
        "unknown",
        "other",
    ):
        assert f"'{role_kind}'" in sql

    assert "ILIKE '%%ordförande%%'" in sql
    assert "ILIKE '%%verkställande direktör%%'" in sql
    assert "ILIKE 'VD%%'" in sql
    assert "role_original = ''" in sql


def test_officers_quality_sql_counts_null_fiscal_year_rows() -> None:
    """Locks in the module docstring's "Null fiscal year handling" section:
    rows with no resolved ``report_period_end`` get ``fiscal_year = 0`` and
    are KEPT (not filtered, unlike ``history.py``), so the quality metadata
    must surface how many rows carry that placeholder -- alongside
    ``unknown_role_count``, following the exact same ``countIf`` pattern.
    """
    sql = _officers_quality_sql(_STAGE_TABLE)

    assert sql.startswith("SELECT")
    assert f"FROM {_STAGE_TABLE}" in sql
    assert "countIf(role_kind = 'unknown') AS unknown_role_count" in sql
    assert "countIf(fiscal_year = 0) AS null_fiscal_year_count" in sql


def test_officers_sql_empty_role_rows_are_kept_not_dropped() -> None:
    # Real filings sometimes omit the role facts entirely while still
    # carrying a name -- HAVING only requires a name, not a role.
    sql = _built_sql()

    assert "HAVING first_name != '' OR last_name != ''" in sql


def test_officers_sql_survives_percent_formatting_with_driver_params() -> None:
    """Regression test mirroring metrics.py's %-format round-trip check:
    ``replace_se_company_officers_clickhouse`` calls
    ``client.execute(sql, {...})`` with a params dict (to carry
    ``resolved_at``), so clickhouse_driver Python-%-formats the whole query
    text against that dict. Every doubled ``%%`` LIKE/ILIKE literal must
    survive as a single ``%`` once formatted.
    """
    sql = _built_sql()
    params = {
        "resolved_at": datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        "source_run_id": "run-1",
    }

    formatted = sql % params

    assert "LIKE 'UnderskriftRevisionsberattelseRevisor%'" in formatted
    assert "LIKE 'UnderskriftFaststallelseintygForetradare%'" in formatted
    assert "ILIKE '%ordförande%'" in formatted
    assert "ILIKE '%verkställande direktör%'" in formatted
    assert "ILIKE 'VD%'" in formatted
    assert "%%" not in formatted


def test_officers_sql_has_no_bare_percent_placeholder_leaks() -> None:
    # Only resolved_at is a driver placeholder; no other %(name)s markers.
    sql = _built_sql()

    placeholders = set(re.findall(r"%\([a-zA-Z_]+\)s", sql))
    assert placeholders == {"%(resolved_at)s"}


class _FakeOfficersClickHouseClient:
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
        # The shrink guard's pre-EXCHANGE existing-row-count check (see
        # guard_against_clickhouse_table_shrink in clickhouse.py) has no
        # alias, so it must be checked AFTER "AS row_count" above.
        if sql.startswith("SELECT count() FROM"):
            return [(self.existing_row_count,)]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def _patch_clickhouse(
    monkeypatch, client: _FakeOfficersClickHouseClient
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeOfficersClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


_DEFAULT_QUALITY_ROW = (500, 300, 250, 350, 100, 50, 20, 15)


def test_replace_se_company_officers_is_atomic_and_reports_counts(monkeypatch) -> None:
    client = _FakeOfficersClickHouseClient(quality_row=_DEFAULT_QUALITY_ROW)
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_se_company_officers_clickhouse(
        clickhouse=resource,
        source_run_id="officers-run",
        resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
    )

    assert client.table_checks == [
        ("se_company_officers", "se_financial_facts"),
    ]
    assert any(statement.startswith("CREATE TABLE") for statement in client.statements)
    assert any(statement.startswith("INSERT INTO") for statement in client.statements)
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")

    assert client.insert_parameters["source_run_id"] == "officers-run"
    assert client.insert_parameters["resolved_at"] == datetime(
        2026, 7, 19, 15, 0, tzinfo=UTC
    )

    assert metadata["row_count"] == 500
    assert metadata["company_count"] == 300
    assert metadata["statement_count"] == 250
    assert metadata["board_signature_count"] == 350
    assert metadata["certification_count"] == 100
    assert metadata["auditor_signatory_count"] == 50
    assert metadata["unknown_role_count"] == 20
    assert metadata["null_fiscal_year_count"] == 15
    assert metadata["table"] == QUALIFIED_SE_COMPANY_OFFICERS_TABLE
    assert metadata["source_run_id"] == "officers-run"


def test_replace_se_company_officers_refuses_to_swap_an_empty_stage(
    monkeypatch,
) -> None:
    client = _FakeOfficersClickHouseClient(quality_row=(0, 0, 0, 0, 0, 0, 0, 0))
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="produced no rows"):
        replace_se_company_officers_clickhouse(
            clickhouse=resource,
            source_run_id="officers-run",
            resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        )

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    # Cleanup still runs (finally block) even on a refused replace.
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_se_company_officers_refuses_shrink_below_half(monkeypatch) -> None:
    quality_row = (49, *_DEFAULT_QUALITY_ROW[1:])
    client = _FakeOfficersClickHouseClient(
        quality_row=quality_row, existing_row_count=100
    )
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="Refusing to replace ClickHouse table"):
        replace_se_company_officers_clickhouse(
            clickhouse=resource,
            source_run_id="officers-run",
            resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        )

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_se_company_officers_allow_shrink_overrides_guard(monkeypatch) -> None:
    quality_row = (1, *_DEFAULT_QUALITY_ROW[1:])
    client = _FakeOfficersClickHouseClient(
        quality_row=quality_row, existing_row_count=100
    )
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_se_company_officers_clickhouse(
        clickhouse=resource,
        source_run_id="officers-run",
        resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        allow_shrink=True,
    )

    assert metadata["row_count"] == 1
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


def test_se_company_officers_clickhouse_asset_is_wired_correctly() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()

    node = repo.asset_graph.get(dg.AssetKey("se_company_officers_clickhouse"))
    assert node.group_name == "sweden_financial"
    assert node.pools == set()
    assert node.partitions_def is None
    assert node.parent_keys == {dg.AssetKey("sweden_financial_facts_clickhouse")}

    # The officers table should refresh whenever metrics/history refresh: it
    # lives in the same (currently unscheduled, manually/backfill-triggered)
    # clickhouse job selection as reports/facts/metrics/history.
    clickhouse_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_clickhouse_job"
        ).asset_layer.executable_asset_keys
    }
    assert "se_company_officers_clickhouse" in clickhouse_job_asset_keys
