import dagster as dg

from dagster_v3.defs.se_company.bolagsverket import (
    SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS,
    SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL,
)
from tests.se_company_ddl import declared_columns, projection_aliases


def test_columns_are_the_migration_order_minus_the_materialized_hash() -> None:
    assert list(SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS) == [
        column for column in declared_columns("se_company_address_bolagsverket")
        if column != "evidence_hash"
    ]


def test_the_trailing_projection_binds_positionally_to_those_columns() -> None:
    """publish_with_stage inserts positionally: a swapped pair of same-typed columns
    here would transpose values with an otherwise-green suite."""
    assert projection_aliases(SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL) == list(
        SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS
    )


def test_the_select_reads_only_this_source_and_only_real_addresses() -> None:
    sql = SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL
    assert "FROM corpscout.se_company_addresses_current AS addresses" in sql
    assert "addresses.source = 'bolagsverket'" in sql
    assert "addresses.has_address = 1" in sql
    assert "addresses.post_town AS city" in sql
    assert "toString(addresses.address_fingerprint) AS address_fingerprint" in sql
    assert "match(addresses.company_id, '^([0-9]{10}|[0-9]{12})$')" in sql


def test_observed_at_is_append_time_not_the_bulk_load_stamp() -> None:
    """updated_from_raw_at is one constant per weekly load; a version stamped with it
    would never look newer than the final row it replaces."""
    sql = SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL
    assert "now64(3, 'UTC') AS observed_at" in sql
    assert "updated_from_raw_at" not in sql


def test_the_asset_reads_the_source_layer_and_writes_its_own_table() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(
        dg.AssetKey("se_company_address_bolagsverket_clickhouse"))
    assert asset.parent_keys == {dg.AssetKey("sweden_company_addresses_clickhouse")}
    assert asset.group_name == "se_company_bolagsverket"
    assert asset.metadata["table"] == "corpscout.se_company_address_bolagsverket"
