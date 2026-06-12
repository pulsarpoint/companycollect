from datetime import datetime, timezone

import dagster as dg

from dagster_corpscout.sources.finland.prh_ytj.explorer_cache import (
    COMPANY_EXPLORER_CACHE_TABLE,
    build_company_explorer_cache_insert_query,
    refresh_company_explorer_cache,
)

SOURCE_PREFIX = ["sources", "finland", "prh_ytj"]


class FakeQueryResult:
    result_rows = [(819443,)]


class FakeClient:
    def __init__(self):
        self.commands = []
        self.queries = []

    def command(self, sql):
        self.commands.append(sql)

    def query(self, sql):
        self.queries.append(sql)
        return FakeQueryResult()


class FakeClickHouse:
    database = "corpscout_sources"

    def __init__(self):
        self.client_object = FakeClient()

    def client(self):
        return self.client_object


def test_build_company_explorer_cache_insert_query_joins_nace_mapping():
    query = build_company_explorer_cache_insert_query(
        database="corpscout_sources",
        target_table="fi_prhytj_company_explorer_cache_refresh_test",
        refreshed_at=datetime(2026, 6, 12, 6, 0, 1, 123000, tzinfo=timezone.utc),
    )

    assert "INSERT INTO `corpscout_sources`.`fi_prhytj_company_explorer_cache_refresh_test`" in query
    assert "FROM `corpscout_sources`.`fi_prhytj_company_explorer` AS explorer" in query
    assert "LEFT JOIN `corpscout_sources`.`fi_prhytj_industry_nace_mappings` AS industry_mapping" in query
    assert "industry_mapping.`nace_code` AS `nace_code`" in query
    assert "industry_mapping.`mapping_status` AS `nace_mapping_status`" in query
    assert "toDateTime64('2026-06-12 06:00:01.123', 3, 'UTC') AS `refreshed_at`" in query


def test_refresh_company_explorer_cache_uses_atomic_table_exchange(monkeypatch):
    fake = FakeClickHouse()
    refreshed_at = datetime(2026, 6, 12, 6, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "dagster_corpscout.sources.finland.prh_ytj.explorer_cache._temp_table_name",
        lambda: "fi_prhytj_company_explorer_cache_refresh_test",
    )

    result = refresh_company_explorer_cache(fake, refreshed_at=refreshed_at)

    assert result.cache_table == "corpscout_sources.fi_prhytj_company_explorer_cache"
    assert result.rows == 819443
    assert result.refreshed_at == refreshed_at
    assert fake.client_object.commands[0] == (
        "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_explorer_cache_refresh_test`"
    )
    assert fake.client_object.commands[1] == (
        "CREATE TABLE `corpscout_sources`.`fi_prhytj_company_explorer_cache_refresh_test` "
        "AS `corpscout_sources`.`fi_prhytj_company_explorer_cache`"
    )
    assert fake.client_object.commands[2] == (
        "TRUNCATE TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_explorer_cache_refresh_test`"
    )
    assert fake.client_object.commands[-2] == (
        "EXCHANGE TABLES `corpscout_sources`.`fi_prhytj_company_explorer_cache` "
        "AND `corpscout_sources`.`fi_prhytj_company_explorer_cache_refresh_test`"
    )
    assert fake.client_object.commands[-1] == (
        "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_explorer_cache_refresh_test`"
    )
    assert fake.client_object.queries == [
        "SELECT count() FROM `corpscout_sources`.`fi_prhytj_company_explorer_cache_refresh_test`"
    ]


def test_company_explorer_cache_asset_is_registered():
    from dagster_corpscout.definitions import defs

    assert defs.get_assets_def(dg.AssetKey([*SOURCE_PREFIX, "company_explorer_cache"])) is not None
    assert COMPANY_EXPLORER_CACHE_TABLE == "fi_prhytj_company_explorer_cache"
