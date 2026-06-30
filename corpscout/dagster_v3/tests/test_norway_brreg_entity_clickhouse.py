from __future__ import annotations

from datetime import date
from datetime import datetime

import polars as pl

from dagster_v3.defs.norway_brreg.assets.entity_clickhouse import (
    apply_entity_update_parquets_to_clickhouse,
    replace_entity_snapshot_parquets_in_clickhouse,
)
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
    ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
    ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES,
    ENTITY_NORMALIZED_TABLE_NO_WEBSITES,
    ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
)
from dagster_v3.defs.norway_resolved import tables as no_tables


def test_replace_entity_snapshot_parquets_replaces_clickhouse_entity_tables() -> None:
    storage = FakeEntityStorage(
        snapshot_tables={
            ENTITY_NORMALIZED_TABLE_NO_COMPANIES: _no_companies_frame("1000", "2000"),
            ENTITY_NORMALIZED_TABLE_NO_WEBSITES: _no_websites_frame("1000"),
            ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES: _no_industries_frame("1000"),
        }
    )
    client = FakeClickHouseClient()

    row_counts = replace_entity_snapshot_parquets_in_clickhouse(
        storage=storage,
        clickhouse_client=client,
        run_id="run-1",
    )

    assert storage.snapshot_read_calls == [
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
        ENTITY_NORMALIZED_TABLE_NO_WEBSITES,
        ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES,
    ]
    assert row_counts == {
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES: 2,
        ENTITY_NORMALIZED_TABLE_NO_WEBSITES: 1,
        ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES: 1,
    }
    assert _statement_count(client, "CREATE TABLE `corpscout`.`_tmp_no_companies_") == 1
    assert _statement_count(client, "CREATE TABLE `corpscout`.`_tmp_no_websites_") == 1
    assert _statement_count(client, "CREATE TABLE `corpscout`.`_tmp_no_industries_") == 1
    assert _statement_count(client, "EXCHANGE TABLES `corpscout`.`_tmp_no_companies_") == 1
    assert _statement_count(client, "EXCHANGE TABLES `corpscout`.`_tmp_no_websites_") == 1
    assert _statement_count(client, "EXCHANGE TABLES `corpscout`.`_tmp_no_industries_") == 1


def test_apply_entity_update_parquets_deletes_affected_orgs_then_inserts_replacements() -> None:
    storage = FakeEntityStorage(
        update_tables={
            ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: _org_list_frame(
                ("1000", "changed"),
                ("3000", "removed"),
            ),
            ENTITY_NORMALIZED_TABLE_REMOVED_ORGS: _org_list_frame(("3000", "removed")),
            ENTITY_NORMALIZED_TABLE_NO_COMPANIES: _no_companies_frame("1000"),
            ENTITY_NORMALIZED_TABLE_NO_WEBSITES: _no_websites_frame("1000"),
            ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES: _no_industries_frame("1000"),
        }
    )
    client = FakeClickHouseClient()

    row_counts = apply_entity_update_parquets_to_clickhouse(
        storage=storage,
        clickhouse_client=client,
        partition_date="2026-06-29",
    )

    assert storage.update_read_calls == [
        ("2026-06-29", ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS),
        ("2026-06-29", ENTITY_NORMALIZED_TABLE_REMOVED_ORGS),
        ("2026-06-29", ENTITY_NORMALIZED_TABLE_NO_COMPANIES),
        ("2026-06-29", ENTITY_NORMALIZED_TABLE_NO_WEBSITES),
        ("2026-06-29", ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES),
    ]
    assert row_counts == {
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: 2,
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS: 1,
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES: 1,
        ENTITY_NORMALIZED_TABLE_NO_WEBSITES: 1,
        ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES: 1,
    }
    assert any(
        sql.startswith("CREATE TABLE `corpscout`.`_tmp_no_affected_orgs_")
        and "ENGINE = Memory" in sql
        for sql, _params in client.events
    )
    assert any(
        database == "corpscout"
        and table.startswith("_tmp_no_affected_orgs_")
        and rows == [("1000",), ("3000",)]
        and columns == ("org_number",)
        for database, table, rows, columns in client.row_insert_calls
    )
    delete_positions = [
        index
        for index, (sql, _params) in enumerate(client.events)
        if sql.startswith("ALTER TABLE `corpscout`.`no_")
        and " DELETE WHERE `org_number` IN " in sql
        and "SETTINGS mutations_sync = 1" in sql
    ]
    target_insert_positions = [
        index
        for index, (sql, _params) in enumerate(client.events)
        if sql.startswith("INSERT INTO `corpscout`.`no_")
    ]
    assert len(delete_positions) == 3
    assert len(target_insert_positions) == 3
    assert max(delete_positions) < min(target_insert_positions)
    assert any(sql.startswith("DROP TABLE IF EXISTS `corpscout`.`_tmp_no_affected_orgs_") for sql, _ in client.events)


class FakeEntityStorage:
    def __init__(
        self,
        *,
        snapshot_tables: dict[str, pl.DataFrame] | None = None,
        update_tables: dict[str, pl.DataFrame] | None = None,
    ) -> None:
        self.snapshot_tables = snapshot_tables or {}
        self.update_tables = update_tables or {}
        self.snapshot_read_calls: list[str] = []
        self.update_read_calls: list[tuple[str, str]] = []

    def read_normalized_snapshot_table(self, table_name: str) -> pl.DataFrame:
        self.snapshot_read_calls.append(table_name)
        return self.snapshot_tables[table_name]

    def read_normalized_update_table(self, partition_date: str, table_name: str) -> pl.DataFrame:
        self.update_read_calls.append((partition_date, table_name))
        return self.update_tables[table_name]


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.events: list[tuple[str, object | None]] = []
        self.row_insert_calls: list[
            tuple[str | None, str, list[tuple[object, ...]], tuple[str, ...]]
        ] = []

    def execute(self, sql: str, params: object | None = None) -> list[tuple[str]]:
        self.events.append((sql, params))
        return []

    def insert_rows(
        self,
        table: str,
        rows: list[tuple[object, ...]],
        columns: tuple[str, ...] | list[str],
        database: str | None = None,
    ) -> None:
        self.row_insert_calls.append((database, table, rows, tuple(columns)))


def _statement_count(client: FakeClickHouseClient, prefix: str) -> int:
    return sum(1 for sql, _params in client.events if sql.startswith(prefix))


def _no_companies_frame(*org_numbers: str) -> pl.DataFrame:
    return _frame(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_COMPANIES_TABLE],
        [
            {
                "org_number": org_number,
                "country_iso2": "NO",
                "name": f"Company {org_number}",
                "name_normalized": f"company {org_number}",
                "registration_date": date(2020, 1, 2),
                "incorporation_date": date(2020, 1, 1),
                "lifecycle_status": "active",
                "is_active": True,
                "legal_form_code": "AS",
                "legal_form_description_original": "Aksjeselskap",
                "articles_purpose_original": "Purpose",
                "activity_text_original": "Activity",
                "primary_website_url": f"https://{org_number}.example.no",
                "primary_website_host": f"{org_number}.example.no",
                "source_system": "norway_brregenhet",
                "source_run_id": "run-1",
                "source_record_id": org_number,
                "resolved_at": datetime(2026, 6, 29, 12, 0, 0),
            }
            for org_number in org_numbers
        ],
    )


def _no_websites_frame(*org_numbers: str) -> pl.DataFrame:
    return _frame(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_WEBSITES_TABLE],
        [
            {
                "org_number": org_number,
                "website_url": f"https://{org_number}.example.no",
                "website_normalized_url": f"https://{org_number}.example.no",
                "website_host": f"{org_number}.example.no",
                "root_domain": "example.no",
                "website_path": None,
                "registered_on": None,
                "ended_on": None,
                "is_current": True,
                "is_primary": True,
                "source_system": "norway_brregenhet",
                "source_run_id": "run-1",
                "source_record_id": org_number,
                "resolved_at": datetime(2026, 6, 29, 12, 0, 0),
            }
            for org_number in org_numbers
        ],
    )


def _no_industries_frame(*org_numbers: str) -> pl.DataFrame:
    return _frame(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_INDUSTRIES_TABLE],
        [
            {
                "org_number": org_number,
                "source_industry_code": "62.010",
                "source_industry_code_set": "NACE_REV_2",
                "description_original": "Programmeringstjenester",
                "description_language": "no",
                "description_en": None,
                "description_translated_at": None,
                "description_translation_provider": None,
                "description_translation_model": None,
                "nace_revision": "NACE_REV_2",
                "nace_code": "62.010",
                "nace_normalized_code": "62010",
                "nace_mapping_method": "direct_code",
                "nace_mapping_status": "mapped",
                "is_primary": True,
                "source_system": "norway_brregenhet",
                "source_run_id": "run-1",
                "source_record_id": org_number,
                "resolved_at": datetime(2026, 6, 29, 12, 0, 0),
            }
            for org_number in org_numbers
        ],
    )


def _org_list_frame(*rows: tuple[str, str]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "org_number": org_number,
                "change_type": change_type,
                "source_change_type": change_type,
                "updated_at": "2026-06-29T12:00:00.000Z",
                "update_id": index,
            }
            for index, (org_number, change_type) in enumerate(rows, start=1)
        ],
        schema={
            "org_number": pl.Utf8,
            "change_type": pl.Utf8,
            "source_change_type": pl.Utf8,
            "updated_at": pl.Utf8,
            "update_id": pl.Int64,
        },
    )


def _frame(columns: tuple[str, ...], rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame([{column: row.get(column) for column in columns} for row in rows])
