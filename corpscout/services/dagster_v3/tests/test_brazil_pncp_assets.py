import dagster as dg

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.assets import (
    CONTRACTS_PARTITIONS,
    brazil_pncp_backfill_job,
    defs,
)


def test_partitions_are_monthly_from_pncps_first_contracts() -> None:
    """Monthly rather than daily because deep pagination works: a 340-page month
    can be read in one run. A depth cap would have forced ~1,700 daily
    partitions instead of ~55."""
    keys = CONTRACTS_PARTITIONS.get_partition_keys()

    assert keys[0] == "2022-01-01"
    assert all(key.endswith("-01") for key in keys[:12])
    assert len(keys) > 50


def test_every_asset_throttles_its_own_backfill() -> None:
    """One partition per run, so a backfill of 55 months does not open 55
    concurrent runs against a rate-limited API."""
    for asset in defs.assets:
        spec = asset.specs_by_key[asset.key]
        policy = asset.backfill_policy
        assert policy is not None, asset.key
        assert policy.max_partitions_per_run == 1, asset.key
        assert spec.partitions_def == CONTRACTS_PARTITIONS


def test_the_duckdb_writers_share_one_pool() -> None:
    """A DuckDB writer excludes readers across processes, so every asset that
    opens the file must serialise on the same pool."""
    by_key = {a.key.to_user_string(): a for a in defs.assets}
    for name in ("brazil_pncp_contracts_duckdb", "brazil_pncp_contracts_clickhouse"):
        node = by_key[name].op
        assert node.pool == tables.DUCKDB_POOL, name


def test_export_waits_for_the_download() -> None:
    by_key = {a.key.to_user_string(): a for a in defs.assets}
    export = by_key["brazil_pncp_contracts_clickhouse"]
    deps = {d.asset_key for d in export.specs_by_key[export.key].deps}
    assert deps == {dg.AssetKey("brazil_pncp_contracts_duckdb")}


def test_the_backfill_is_not_scheduled() -> None:
    """It loads history once and is then a repair tool. Re-fetching a 340-page
    month to pick up one day's contracts would be absurd when a day is 12."""
    assert brazil_pncp_backfill_job.name == "brazil_pncp_backfill_job"
    assert list(defs.schedules or []) == []
