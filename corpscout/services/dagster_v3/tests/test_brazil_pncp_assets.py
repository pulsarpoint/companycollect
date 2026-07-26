import inspect

import dagster as dg

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.assets import (
    CONTRACTS_PARTITIONS,
    brazil_pncp_backfill_job,
    brazil_pncp_contracts_duckdb,
    brazil_pncp_page_cache_cleanup,
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
    for name in (
        "brazil_pncp_raw_pages_s3",
        "brazil_pncp_contracts_duckdb",
        "brazil_pncp_contracts_clickhouse",
    ):
        node = by_key[name].op
        assert node.pool == tables.DUCKDB_POOL, name


def test_the_chain_runs_snapshot_then_normalise_then_export() -> None:
    by_key = {a.key.to_user_string(): a for a in defs.assets}

    def deps(name):
        a = by_key[name]
        return {d.asset_key for d in a.specs_by_key[a.key].deps}

    assert deps("brazil_pncp_raw_pages_s3") == set()
    assert deps("brazil_pncp_contracts_duckdb") == {
        dg.AssetKey("brazil_pncp_raw_pages_s3")
    }
    assert deps("brazil_pncp_contracts_clickhouse") == {
        dg.AssetKey("brazil_pncp_contracts_duckdb")
    }


def test_normalisation_reads_the_snapshot_not_local_scratch() -> None:
    """Re-fetching a month is hundreds of rate-limited requests and the full
    history ~6,400, so changing how contracts are normalised must never mean
    paying for the fetch again -- and must work on a machine that never did the
    download. Local pages are scratch, S3 is the record."""
    source = inspect.getsource(brazil_pncp_contracts_duckdb.op.compute_fn.decorated_fn)

    assert "list_keys" in source
    assert "download_file" in source
    assert "PAGE_SCRATCH" not in source


def test_the_snapshot_outlives_the_cleanup() -> None:
    """Only local scratch is deleted. Dropping the S3 copy would leave the raw
    data existing nowhere, recoverable only by re-fetching."""
    source = inspect.getsource(
        brazil_pncp_page_cache_cleanup.op.compute_fn.decorated_fn
    )

    assert "PAGE_SCRATCH" in source
    # Nothing here may remove the snapshot -- only local files.
    assert "delete_keys" not in source
    assert "shutil.rmtree" in source


def test_the_backfill_is_not_scheduled() -> None:
    """It loads history once and is then a repair tool. Re-fetching a 340-page
    month to pick up one day's contracts would be absurd when a day is 12."""
    assert brazil_pncp_backfill_job.name == "brazil_pncp_backfill_job"
    assert list(defs.schedules or []) == []
