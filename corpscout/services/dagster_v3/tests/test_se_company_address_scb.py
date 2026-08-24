import dagster as dg

from dagster_v3.defs.se_company.scb import (
    SE_COMPANY_ADDRESS_SCB_COLUMNS,
    SE_COMPANY_ADDRESS_SCB_SQL,
)
from tests.se_company_ddl import declared_columns, projection_aliases


def test_columns_and_projection_match_the_migration() -> None:
    assert list(SE_COMPANY_ADDRESS_SCB_COLUMNS) == [
        column for column in declared_columns("se_company_address_scb") if column != "evidence_hash"
    ]
    assert projection_aliases(SE_COMPANY_ADDRESS_SCB_SQL) == list(SE_COMPANY_ADDRESS_SCB_COLUMNS)


def test_the_select_reads_only_the_scb_rows() -> None:
    sql = SE_COMPANY_ADDRESS_SCB_SQL
    assert "addresses.source = 'scb'" in sql
    assert "addresses.has_address = 1" in sql
    assert "now64(3, 'UTC') AS observed_at" in sql
    # SCB does not distinguish visiting from postal; the type travels as the register
    # recorded it rather than being renamed here.
    assert "toString(addresses.address_type) AS address_type" in sql


def test_the_two_scb_assets_are_separate_and_write_separate_tables() -> None:
    from dagster_v3.definitions import defs as load_defs

    graph = load_defs().get_repository_def().asset_graph
    address = graph.get(dg.AssetKey("se_company_address_scb_clickhouse"))
    info = graph.get(dg.AssetKey("se_company_info_scb_clickhouse"))
    assert address.parent_keys == {dg.AssetKey("sweden_company_addresses_clickhouse")}
    assert address.group_name == info.group_name == "se_company_scb"
    assert address.metadata["table"] == "corpscout.se_company_address_scb"
    assert info.metadata["table"] == "corpscout.se_company_info_scb"
