from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.company_people.identity import (
    COUNTRY_PERSON_COLUMNS,
    COUNTRY_PERSON_CORRECTION_TABLE,
    COUNTRY_PERSON_IDENTIFIER_COLUMNS,
    COUNTRY_PERSON_IDENTIFIER_TABLE,
    COUNTRY_PERSON_MATCH_COLUMNS,
    COUNTRY_PERSON_MATCH_TABLE,
    COUNTRY_PERSON_OBSERVATION_COLUMNS,
    COUNTRY_PERSON_OBSERVATION_TABLE,
    COUNTRY_PERSON_TABLE,
    CountryPeopleIdentityConfig,
    build_country_person_identifier_insert_sql,
    build_country_person_insert_sql,
    build_country_person_match_insert_sql,
    build_country_person_observation_insert_sql,
    country_person_correction_cursor,
    country_people_identity_schedule,
    country_person_correction_sensor,
    replace_country_people_identity_clickhouse,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"

_OBSERVATION_STAGE = "`corpscout`.`_tmp_country_person_observation_test`"
_MATCH_STAGE = "`corpscout`.`_tmp_country_person_match_test`"
_IDENTIFIER_STAGE = "`corpscout`.`_tmp_country_person_identifier_test`"
_PERSON_STAGE = "`corpscout`.`_tmp_country_person_test`"
_CORRECTION_TABLE = "`corpscout`.`country_person_correction`"
_CURRENT_PERSON_TABLE = "`corpscout`.`country_person`"


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text(encoding="utf-8")


def test_country_people_migration_partitions_all_identity_tables_by_country() -> None:
    up_sql = _migration_sql("000239_corpscout_country_people_identity.up.sql")
    down_sql = _migration_sql("000239_corpscout_country_people_identity.down.sql")

    for table in (
        COUNTRY_PERSON_TABLE,
        COUNTRY_PERSON_OBSERVATION_TABLE,
        COUNTRY_PERSON_IDENTIFIER_TABLE,
        COUNTRY_PERSON_MATCH_TABLE,
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in up_sql
        assert f"DROP TABLE IF EXISTS corpscout.{table}" in down_sql

    assert up_sql.count("PARTITION BY country_iso2") == 4
    assert "ORDER BY (country_iso2, person_id)" in up_sql
    assert "ORDER BY (country_iso2, observation_id)" in up_sql
    assert (
        "ORDER BY (country_iso2, source, identifier_kind, identifier_value, observation_id)"
        in up_sql
    )
    assert "ORDER BY (country_iso2, observation_id, person_id)" in up_sql


def test_country_person_corrections_are_an_immutable_country_partitioned_ledger() -> (
    None
):
    up_sql = _migration_sql("000240_corpscout_country_person_corrections.up.sql")
    down_sql = _migration_sql("000240_corpscout_country_person_corrections.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.country_person_correction" in up_sql
    assert "PARTITION BY country_iso2" in up_sql
    assert (
        "ifNull(observation_id, toUUID('00000000-0000-0000-0000-000000000000'))"
        in up_sql
    )
    assert "observation_id Nullable(UUID)" in up_sql
    assert "supersedes_correction_id Nullable(UUID)" in up_sql
    assert "ReplacingMergeTree" not in up_sql
    assert "DROP TABLE IF EXISTS corpscout.country_person_correction" in down_sql


def test_country_people_column_contracts_follow_migration_order() -> None:
    up_sql = _migration_sql("000239_corpscout_country_people_identity.up.sql")
    contracts = {
        COUNTRY_PERSON_TABLE: COUNTRY_PERSON_COLUMNS,
        COUNTRY_PERSON_OBSERVATION_TABLE: COUNTRY_PERSON_OBSERVATION_COLUMNS,
        COUNTRY_PERSON_IDENTIFIER_TABLE: COUNTRY_PERSON_IDENTIFIER_COLUMNS,
        COUNTRY_PERSON_MATCH_TABLE: COUNTRY_PERSON_MATCH_COLUMNS,
    }

    for table, columns in contracts.items():
        table_sql = up_sql.split(
            f"CREATE TABLE IF NOT EXISTS corpscout.{table}", maxsplit=1
        )[1].split("ENGINE =", maxsplit=1)[0]
        positions = [table_sql.index(f"    {column} ") for column in columns]
        assert positions == sorted(positions)

def test_observation_sql_preserves_source_person_provenance() -> None:
    sql = build_country_person_observation_insert_sql(_OBSERVATION_STAGE)

    assert "o.statement_key, '|', o.signatory_kind, '|', toString(o.person_seq)" in sql
    assert "o.statement_key AS source_record_id" in sql
    assert "concat(o.signatory_kind, '|', toString(o.person_seq))" in sql
    assert "o.statement_key AS source_statement_key" in sql
    assert "'SE' AS country_iso2" in sql
    assert "'se_xbrl_signatures' AS source" in sql


def test_match_sql_never_uses_a_name_outside_country_and_company() -> None:
    sql = build_country_person_match_insert_sql(
        _MATCH_STAGE,
        _OBSERVATION_STAGE,
        _CORRECTION_TABLE,
    )

    assert "'country_person|identifier|'" in sql
    assert "'country_person|company_name|'" in sql
    assert "country_iso2, '|', company_id, '|', observed_name_normalized" in sql
    assert "'country_person|observation|'" in sql
    assert "same_name_in_statement_block = 1" in sql
    assert "'same_company_name'" in sql
    assert "'singleton'" in sql
    assert "observation_corrections" in sql
    assert "person_corrections" in sql
    assert "'reviewed_merge'" in sql
    assert "concat('reviewed_', b.observation_correction_kind)" in sql


def test_identifier_and_person_sql_join_on_country_and_observation() -> None:
    identifier_sql = build_country_person_identifier_insert_sql(
        _IDENTIFIER_STAGE,
        _OBSERVATION_STAGE,
        _MATCH_STAGE,
    )
    person_sql = build_country_person_insert_sql(
        _PERSON_STAGE,
        _OBSERVATION_STAGE,
        _MATCH_STAGE,
        _CORRECTION_TABLE,
        _CURRENT_PERSON_TABLE,
    )

    for sql in (identifier_sql, person_sql):
        assert "m.country_iso2 = o.country_iso2" in sql
        assert "m.observation_id = o.observation_id" in sql
    assert (
        "WHERE o.identifier_kind != '' AND o.identifier_value != ''" in identifier_sql
    )
    assert "GROUP BY o.country_iso2, m.person_id" in person_sql
    assert "'merged' AS resolution_status" in person_sql
    assert "c.reviewed_person_id AS merged_into_person_id" in person_sql


class _FakeCountryPeopleClient:
    def __init__(
        self,
        *,
        quality_row: tuple[object, ...],
        existing_row_count: int = 0,
    ) -> None:
        self.quality_row = quality_row
        self.existing_row_count = existing_row_count
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []

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
        if sql.startswith("CREATE TABLE") or sql.startswith("INSERT INTO"):
            return []
        if "AS observation_count" in sql:
            return [self.quality_row]
        if sql.startswith("SELECT count() FROM"):
            return [(self.existing_row_count,)]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def _patch_clickhouse(
    monkeypatch: pytest.MonkeyPatch,
    client: _FakeCountryPeopleClient,
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeCountryPeopleClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def test_country_people_replace_builds_and_exchanges_all_four_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeCountryPeopleClient(quality_row=(3, 3, 0, 1, 1, 0, 0, 1, 0))
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_country_people_identity_clickhouse(
        clickhouse=resource,
        source_run_id="identity-run",
        resolved_at=datetime(2026, 8, 1, 10, 0, tzinfo=UTC),
    )

    assert client.table_checks == [
        (
            COUNTRY_PERSON_OBSERVATION_TABLE,
            COUNTRY_PERSON_MATCH_TABLE,
            COUNTRY_PERSON_IDENTIFIER_TABLE,
            COUNTRY_PERSON_TABLE,
            COUNTRY_PERSON_CORRECTION_TABLE,
            "se_financial_report_signatories",
            "se_companies",
        )
    ]
    assert sum(sql.startswith("CREATE TABLE") for sql in client.statements) == 4
    assert sum(sql.startswith("INSERT INTO") for sql in client.statements) == 4
    assert sum(sql.startswith("EXCHANGE TABLES") for sql in client.statements) == 4
    assert sum(sql.startswith("DROP TABLE") for sql in client.statements) == 4
    assert metadata["observation_count"] == 3
    assert metadata["person_count"] == 1
    assert metadata["provisional_person_count"] == 1
    assert metadata["source_run_id"] == "identity-run"


def test_country_people_replace_rejects_incomplete_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeCountryPeopleClient(quality_row=(3, 2, 0, 1, 1, 0, 0, 1, 0))
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="exactly one match row per observation"):
        replace_country_people_identity_clickhouse(
            clickhouse=resource,
            source_run_id="identity-run",
            resolved_at=datetime(2026, 8, 1, 10, 0, tzinfo=UTC),
        )

    assert not any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)
    assert sum(sql.startswith("DROP TABLE") for sql in client.statements) == 4


def test_country_people_config_requires_explicit_shrink_override() -> None:
    assert CountryPeopleIdentityConfig().allow_shrink is False


def test_country_person_correction_cursor_advances_with_the_ledger_row_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    correction_id = "44444444-4444-4444-8444-444444444444"

    class FakeCorrectionClient(_FakeCountryPeopleClient):
        def __init__(self) -> None:
            super().__init__(quality_row=())

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
        ) -> list[tuple[object, ...]]:
            assert "FROM `corpscout`.`country_person_correction`" in sql
            assert params is None
            return [(3, correction_id)]

    resource = _patch_clickhouse(monkeypatch, FakeCorrectionClient())

    assert country_person_correction_cursor(resource) == f"3:{correction_id}"


def test_country_people_assets_and_schedule_are_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    for asset_key in (
        "country_person_observations_clickhouse",
        "country_person_matches_clickhouse",
        "country_person_identifiers_clickhouse",
        "country_people_clickhouse",
    ):
        node = repository.asset_graph.get(dg.AssetKey(asset_key))
        assert node.group_name == "country_people"

    assert country_people_identity_schedule.cron_schedule == "0 8 * * *"
    assert country_people_identity_schedule.execution_timezone == "UTC"
    assert (
        country_people_identity_schedule.default_status
        == dg.DefaultScheduleStatus.RUNNING
    )
    assert (
        country_person_correction_sensor.default_status
        == dg.DefaultSensorStatus.RUNNING
    )
    assert country_person_correction_sensor.minimum_interval_seconds == 60
