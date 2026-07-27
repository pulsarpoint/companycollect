import inspect

import dagster as dg

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.assets import (
    CONTRACTS_PARTITIONS,
    brazil_pncp_backfill_job,
    brazil_pncp_contracts_duckdb,
    brazil_pncp_page_cache_cleanup,
    brazil_pncp_raw_pages_s3,
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
    # USD conversion sits between normalisation and export, so the exported
    # rows carry a rate rather than needing a second pass over ClickHouse.
    assert deps("brazil_pncp_contracts_usd") == {
        dg.AssetKey("brazil_pncp_contracts_duckdb")
    }
    assert deps("brazil_pncp_contracts_clickhouse") == {
        dg.AssetKey("brazil_pncp_contracts_usd")
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


def test_the_download_retries_so_an_overnight_backfill_survives() -> None:
    """A transient failure already happened inside the first 80 pages; over the
    ~6,400 a full backfill costs they are routine. Without a retry the run is
    found stalled in the morning rather than done, and the whole point of the
    per-page cache is that retrying resumes rather than re-fetches."""
    by_key = {a.key.to_user_string(): a for a in defs.assets}
    policy = by_key["brazil_pncp_raw_pages_s3"].op.retry_policy

    assert policy is not None
    assert policy.max_retries == 3
    # Long enough to outlast a rate-limit window, not a network blip.
    assert policy.delay == 60
    assert policy.backoff == dg.Backoff.EXPONENTIAL


def test_the_download_resumes_from_the_snapshot_not_only_local_scratch() -> None:
    """download_partition resumes at (pages on disk + 1), and on any machine
    that did not itself fetch this month that disk is empty -- a fresh deploy,
    another host, or a month whose scratch the cleanup asset already cleared.
    Without hydrating from S3 first, re-materializing a snapshotted month
    silently re-paid for every page: ~3,000 rate-limited requests to rebuild the
    37 months already stored."""
    source = inspect.getsource(brazil_pncp_raw_pages_s3.op.compute_fn.decorated_fn)
    # Everything before the fetch call. Hydrating after it would do nothing, so
    # the check is that these appear in this half of the function.
    before_fetch = source.split("download_partition(")[0]

    assert "list_keys" in before_fetch
    assert "download_file" in before_fetch
    # The metadata has to distinguish the two, or a month that quietly
    # re-downloaded looks identical to one served from the snapshot.
    assert "pages_fetched_from_api" in source


def test_a_retry_resumes_from_the_pages_already_paid_for() -> None:
    """The retry above is only cheap because of this: the downloader counts the
    page files already on disk and starts after them. If it ever restarted at
    page 1, three retries of a 340-page month would cost 1,360 requests."""
    from dagster_v3.defs.brazil_pncp.resources import download_partition

    source = inspect.getsource(download_partition)

    assert "resume_from = len(existing) + 1" in source


def test_the_backfill_is_not_scheduled() -> None:
    """It loads history once and is then a repair tool. Re-fetching a 340-page
    month to pick up one day's contracts would be absurd when a day is 12."""
    assert brazil_pncp_backfill_job.name == "brazil_pncp_backfill_job"
    assert list(defs.schedules or []) == []
