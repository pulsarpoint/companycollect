"""The rolling daily window.

The distinction this file exists to protect: the backfill REPLACEs a month, the
daily job APPENDs a slice of one. Getting that backwards deletes three weeks of
contracts and leaves a full-looking table behind.
"""

import inspect
from datetime import date

import dagster as dg
import pytest

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.assets import (
    brazil_pncp_daily_clickhouse,
    brazil_pncp_daily_job,
    brazil_pncp_daily_pages_s3,
    brazil_pncp_daily_schedule,
    defs,
)
from dagster_v3.defs.brazil_pncp.clickhouse import insert_contracts_clickhouse
from dagster_v3.defs.brazil_pncp.resources import trailing_window


def test_the_window_is_seven_inclusive_days() -> None:
    """Inclusive at both ends, like month_bounds -- verified arithmetically
    against the live API rather than assumed."""
    assert trailing_window(date(2026, 7, 20)) == ("20260714", "20260720")
    assert trailing_window(date(2026, 3, 3)) == ("20260225", "20260303")  # month edge
    assert trailing_window(date(2026, 1, 2)) == ("20251227", "20260102")  # year edge
    assert trailing_window(date(2026, 7, 20), days=1) == ("20260720", "20260720")


def test_a_zero_day_window_is_refused() -> None:
    with pytest.raises(ValueError, match="at least one day"):
        trailing_window(date(2026, 7, 20), days=0)


def test_the_daily_export_appends_and_never_replaces_a_partition() -> None:
    """The window is a slice of a month. REPLACE PARTITION 202607 with seven
    days of it would delete the other three weeks, and the monthly export's
    guard would not catch it -- that only fires on an EMPTY input, and this
    batch is full."""
    # The docstring explains why it must not, so check the code, not the prose.
    source = inspect.getsource(insert_contracts_clickhouse)
    body = source.split('"""')[2]

    assert "REPLACE PARTITION" not in body
    assert "INSERT INTO" in body


def test_the_monthly_export_still_replaces() -> None:
    """The contrast that makes the above meaningful: a month rebuilt from its
    complete snapshot must replace, or re-running it doubles the month."""
    from dagster_v3.defs.brazil_pncp.clickhouse import export_contracts_clickhouse

    assert "REPLACE PARTITION" in inspect.getsource(export_contracts_clickhouse)


def test_an_empty_window_is_a_broken_fetch_not_a_quiet_week() -> None:
    """Brazil does not go a week without awarding a contract. Appending nothing
    removes nothing, so the risk is not data loss -- it is a silently degraded
    feed that looks healthy for as long as nobody counts."""
    assert "refusing to treat" in inspect.getsource(insert_contracts_clickhouse)
    assert "refusing to treat that as a quiet week" in inspect.getsource(
        brazil_pncp_daily_pages_s3.op.compute_fn.decorated_fn
    )


def test_both_endpoints_are_read() -> None:
    """/contratos cannot see an amendment to a contract published last year;
    /contratos/atualizacao is the only feed that carries it."""
    source = inspect.getsource(brazil_pncp_daily_pages_s3.op.compute_fn.decorated_fn)

    assert "CONTRACTS_BY_PUBLICATION_PATH" in source
    assert "CONTRACTS_BY_UPDATE_PATH" in source


def test_the_two_endpoints_pages_do_not_overwrite_each_other() -> None:
    """Both number their pages from 1, so page-00001.jsonl exists twice. Landing
    them in one directory unqualified would silently drop a whole endpoint."""
    from dagster_v3.defs.brazil_pncp.assets import brazil_pncp_daily_duckdb

    source = inspect.getsource(brazil_pncp_daily_duckdb.op.compute_fn.decorated_fn)

    assert 'f"{endpoint}-{Path(key).name}"' in source


def test_the_daily_window_is_kept_out_of_the_monthly_snapshot_prefix() -> None:
    """The monthly prefix is contiguous pages 1..N of one month, and the
    backfill's resume logic depends on that. A rolling window's pages are a
    different slicing and would break the contiguity check."""
    assert tables.S3_DAILY_PREFIX != tables.S3_RAW_PREFIX
    source = inspect.getsource(brazil_pncp_daily_pages_s3.op.compute_fn.decorated_fn)
    assert "S3_DAILY_PREFIX" in source
    assert "S3_RAW_PREFIX" not in source


def test_the_daily_chain_is_scheduled_and_the_backfill_is_not() -> None:
    schedules = {schedule.name for schedule in defs.schedules or []}
    assert schedules == {"brazil_pncp_daily_schedule"}
    assert brazil_pncp_daily_schedule.job.name == brazil_pncp_daily_job.name
    # Staggered off the hour, per the guidelines: at 100+ sources every schedule
    # on :00 is a thundering herd against one Postgres.
    assert brazil_pncp_daily_schedule.cron_schedule == "20 4 * * *"


def test_the_daily_assets_share_the_sources_pool() -> None:
    """The daily chain and a repair backfill can be launched at once. Both open
    the same DuckDB file, and a writer excludes readers across processes."""
    by_key = {a.key.to_user_string(): a for a in defs.assets}
    for name in (
        "brazil_pncp_daily_pages_s3",
        "brazil_pncp_daily_duckdb",
        "brazil_pncp_daily_usd",
        "brazil_pncp_daily_clickhouse",
    ):
        assert by_key[name].op.pool == tables.DUCKDB_POOL, name


def test_the_daily_chain_runs_in_order() -> None:
    by_key = {a.key.to_user_string(): a for a in defs.assets}

    def deps(name):
        asset = by_key[name]
        return {d.asset_key for d in asset.specs_by_key[asset.key].deps}

    assert deps("brazil_pncp_daily_pages_s3") == set()
    assert deps("brazil_pncp_daily_duckdb") == {dg.AssetKey("brazil_pncp_daily_pages_s3")}
    assert deps("brazil_pncp_daily_usd") == {dg.AssetKey("brazil_pncp_daily_duckdb")}
    assert deps("brazil_pncp_daily_clickhouse") == {dg.AssetKey("brazil_pncp_daily_usd")}


def test_the_daily_window_is_not_partitioned() -> None:
    """A rolling window is not a partition: every run reads an overlapping seven
    days, so partition keys would claim a per-period ownership that does not
    exist."""
    by_key = {a.key.to_user_string(): a for a in defs.assets}
    for name in (
        "brazil_pncp_daily_pages_s3",
        "brazil_pncp_daily_duckdb",
        "brazil_pncp_daily_usd",
        "brazil_pncp_daily_clickhouse",
    ):
        asset = by_key[name]
        assert asset.specs_by_key[asset.key].partitions_def is None, name


def test_the_daily_download_retries() -> None:
    """213 pages a run against a rate limit with no Retry-After. Without this a
    transient failure means a day silently missing from the feed."""
    policy = brazil_pncp_daily_pages_s3.op.retry_policy

    assert policy is not None
    assert policy.max_retries == 3


def test_a_retry_rereads_the_window_rather_than_reusing_yesterdays_pages() -> None:
    """The retry above re-enters the asset. Scratch from a previous day would be
    resumed past, so the window would be read as already complete and the new
    days never fetched."""
    source = inspect.getsource(brazil_pncp_daily_pages_s3.op.compute_fn.decorated_fn)

    assert "shutil.rmtree(scratch, ignore_errors=True)" in source


def test_the_clickhouse_step_asserts_its_tables_exist() -> None:
    source = inspect.getsource(brazil_pncp_daily_clickhouse.op.compute_fn.decorated_fn)

    assert "assert_clickhouse_tables_exist" in source
    assert "br_companies" in source
