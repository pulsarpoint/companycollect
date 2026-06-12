from datetime import datetime, timezone

import pytest

from dagster_corpscout.sources.finland_prhytj.industry_mapping import (
    INDUSTRY_NACE_MAPPING_TABLE,
    build_industry_nace_mapping_insert_query,
    refresh_industry_nace_mappings,
)


class FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, reference_rows=42, mapping_counts=(17, 15, 2)):
        self.commands = []
        self.queries = []
        self.reference_rows = reference_rows
        self.mapping_counts = mapping_counts

    def command(self, sql):
        self.commands.append(sql)

    def query(self, sql):
        self.queries.append(sql)
        if "FROM `corpscout_reference`.`nace_codes`" in sql:
            return FakeQueryResult([(self.reference_rows,)])
        return FakeQueryResult([self.mapping_counts])


class FakeClickHouse:
    database = "corpscout_sources"

    def __init__(self, reference_rows=42, mapping_counts=(17, 15, 2)):
        self.client_object = FakeClient(
            reference_rows=reference_rows,
            mapping_counts=mapping_counts,
        )

    def client(self):
        return self.client_object


def test_build_industry_nace_mapping_insert_query_uses_source_lines_and_reference_nace():
    query = build_industry_nace_mapping_insert_query(
        database="corpscout_sources",
        target_table="fi_prhytj_industry_nace_mappings_refresh_test",
        mapped_at=datetime(2026, 6, 12, 8, 1, 2, 345000, tzinfo=timezone.utc),
    )

    assert "INSERT INTO `corpscout_sources`.`fi_prhytj_industry_nace_mappings_refresh_test`" in query
    assert "`corpscout_sources`.`fi_prhytj_business_lines`" in query
    assert "`corpscout_sources`.`fi_prhytj_business_line_descriptions`" in query
    assert "`corpscout_reference`.`nace_codes`" in query
    assert "source_code_set = 'TOIMI4', '2.1'" in query
    assert "source_code_set = 'TOIMI3', '2'" in query
    assert "'toimi_5_digit_prefix'" in query
    assert "'unsupported_code_set'" in query
    assert "toDateTime64('2026-06-12 08:01:02.345', 3, 'UTC') AS `mapped_at`" in query


def test_refresh_industry_nace_mappings_uses_atomic_table_exchange(monkeypatch):
    fake = FakeClickHouse(mapping_counts=(17, 15, 2))
    mapped_at = datetime(2026, 6, 12, 8, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "dagster_corpscout.sources.finland_prhytj.industry_mapping._temp_table_name",
        lambda: "fi_prhytj_industry_nace_mappings_refresh_test",
    )

    result = refresh_industry_nace_mappings(fake, mapped_at=mapped_at)

    assert result.mapping_table == "corpscout_sources.fi_prhytj_industry_nace_mappings"
    assert result.rows == 17
    assert result.mapped_rows == 15
    assert result.unmapped_rows == 2
    assert result.mapped_at == mapped_at
    assert fake.client_object.commands[0] == (
        "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_industry_nace_mappings_refresh_test`"
    )
    assert fake.client_object.commands[1] == (
        "CREATE TABLE `corpscout_sources`.`fi_prhytj_industry_nace_mappings_refresh_test` "
        "AS `corpscout_sources`.`fi_prhytj_industry_nace_mappings`"
    )
    assert fake.client_object.commands[2] == (
        "TRUNCATE TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_industry_nace_mappings_refresh_test`"
    )
    assert fake.client_object.commands[-2] == (
        "EXCHANGE TABLES `corpscout_sources`.`fi_prhytj_industry_nace_mappings` "
        "AND `corpscout_sources`.`fi_prhytj_industry_nace_mappings_refresh_test`"
    )
    assert fake.client_object.commands[-1] == (
        "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_industry_nace_mappings_refresh_test`"
    )


def test_refresh_industry_nace_mappings_requires_reference_nace_rows():
    fake = FakeClickHouse(reference_rows=0)

    with pytest.raises(ValueError, match="active NACE class reference rows"):
        refresh_industry_nace_mappings(fake)


def test_industry_nace_mappings_asset_is_registered():
    import dagster as dg

    from dagster_corpscout.definitions import defs

    assert defs.get_assets_def(dg.AssetKey(["finland_prhytj", "industry_nace_mappings"])) is not None
    assert INDUSTRY_NACE_MAPPING_TABLE == "fi_prhytj_industry_nace_mappings"
