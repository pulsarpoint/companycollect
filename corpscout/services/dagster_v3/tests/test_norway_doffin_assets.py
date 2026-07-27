"""Wiring and schema contract for the Doffin ingest."""

import inspect
from pathlib import Path

import dagster as dg
import pytest

from dagster_v3.defs.norway_doffin import tables
from dagster_v3.defs.norway_doffin.assets import (
    NOTICE_PARTITIONS,
    defs,
    norway_doffin_notices_s3,
)
from dagster_v3.defs.norway_doffin.clickhouse import (
    candidate_stage_ddl,
    export_notices_clickhouse,
    notices_insert_sql,
)
from dagster_v3.defs.norway_doffin.resources import (
    assert_slice_is_filtered_and_complete,
    month_bounds,
)


def _migration_sql() -> str:
    root = Path(__file__).resolve().parents[3]
    return (
        root / "clickhouse" / "migrations" / "000198_corpscout_no_doffin_notices.up.sql"
    ).read_text()


def test_partitions_are_monthly_from_doffins_earliest_notices() -> None:
    """Nothing exists before 2018 -- 2006..2013 all return zero -- and the
    busiest month measured is 378 notices against a 1,000-result ceiling."""
    keys = NOTICE_PARTITIONS.get_partition_keys()

    assert keys[0] == "2018-01-01"
    assert all(key.endswith("-01") for key in keys[:12])


def test_the_partition_maps_to_issue_date_not_publication_date() -> None:
    """There is no publication-date filter in the API at all, so issue date is
    the only key a partition can actually be served on."""
    assert month_bounds("2025-11-01") == ("2025-11-01", "2025-11-30")
    assert month_bounds("2024-02-01") == ("2024-02-01", "2024-02-29")  # leap year


def test_an_ignored_date_filter_is_caught_rather_than_loaded() -> None:
    """The most dangerous behaviour in this API: a misspelt parameter returns
    HTTP 200 and the whole register, so a backfill would load the same 31,097
    notices into every partition while looking perfectly healthy."""
    with pytest.raises(RuntimeError, match="date filter was ignored"):
        assert_slice_is_filtered_and_complete(
            {"numHitsTotal": 31097, "numHitsAccessible": 1000},
            issue_date_from="2025-11-01",
            issue_date_to="2025-11-30",
            unfiltered_total=31097,
        )


def test_a_slice_beyond_the_result_ceiling_is_refused() -> None:
    """numHitsAccessible caps at 1,000 with no marker in the response, so a
    busier-than-expected month would silently lose its tail."""
    with pytest.raises(RuntimeError, match="result ceiling"):
        assert_slice_is_filtered_and_complete(
            {"numHitsTotal": 1400, "numHitsAccessible": 1000},
            issue_date_from="2025-11-01",
            issue_date_to="2025-11-30",
        )


def test_a_normal_slice_passes_and_returns_its_total() -> None:
    assert (
        assert_slice_is_filtered_and_complete(
            {"numHitsTotal": 256, "numHitsAccessible": 256},
            issue_date_from="2025-11-01",
            issue_date_to="2025-11-30",
            unfiltered_total=31097,
        )
        == 256
    )


def test_the_snapshot_reuses_xml_it_already_holds() -> None:
    """~300 notices a month. A re-materialization after a parser change must not
    re-fetch them, which is the same rule ted_monthly_snapshot follows."""
    source = inspect.getsource(norway_doffin_notices_s3.op.compute_fn.decorated_fn)

    assert "exists(" in source
    assert "reused" in source


def test_normalisation_reads_the_snapshot_not_the_api() -> None:
    """Changing how a notice is projected must never mean paying for the fetch
    again, and must work on a machine that never did the download."""
    from dagster_v3.defs.norway_doffin.assets import norway_doffin_notices_duckdb

    source = inspect.getsource(norway_doffin_notices_duckdb.op.compute_fn.decorated_fn)

    assert "read_bytes" in source
    assert "fetch_notice_xml" not in source
    assert "iter_search_hits" not in source


def test_every_asset_throttles_its_own_backfill() -> None:
    for asset in defs.assets:
        policy = asset.backfill_policy
        assert policy is not None, asset.key
        assert policy.max_partitions_per_run == 1, asset.key
        assert asset.specs_by_key[asset.key].partitions_def == NOTICE_PARTITIONS


def test_every_asset_shares_the_sources_duckdb_pool() -> None:
    """A DuckDB writer excludes readers across processes, so every step that
    opens the file serialises on one pool -- including the S3 fetch, which shares
    it so a backfill and the daily chain cannot interleave destructively."""
    for asset in defs.assets:
        assert asset.op.pool == tables.DUCKDB_POOL, asset.key


def test_the_chain_runs_fetch_then_normalise_then_convert_then_export() -> None:
    by_key = {a.key.to_user_string(): a for a in defs.assets}

    def deps(name):
        asset = by_key[name]
        return {d.asset_key for d in asset.specs_by_key[asset.key].deps}

    assert deps("norway_doffin_notices_s3") == set()
    assert deps("norway_doffin_notices_duckdb") == {
        dg.AssetKey("norway_doffin_notices_s3")
    }
    assert deps("norway_doffin_notices_usd") == {
        dg.AssetKey("norway_doffin_notices_duckdb")
    }
    assert deps("norway_doffin_notices_clickhouse") == {
        dg.AssetKey("norway_doffin_notices_usd")
    }


def test_the_migration_covers_every_exported_column() -> None:
    """The migration owns the schema and the export must not drift from it."""
    sql = _migration_sql()

    assert f"CREATE TABLE IF NOT EXISTS corpscout.{tables.NOTICES_TABLE}" in sql
    for column in tables.NOTICES_COLUMNS:
        assert f"    {column} " in sql, column


def test_every_value_metric_has_an_original_a_usd_and_a_currency() -> None:
    """The storage rule: convert all of them, not just the one the view reads."""
    sql = _migration_sql()

    for metric, _ in tables.VALUE_METRICS:
        for suffix in ("_amount_original", "_amount_usd", "_currency"):
            assert f"    {metric}{suffix} " in sql, f"{metric}{suffix}"


def test_the_sort_key_carries_no_nullable_column() -> None:
    """allow_nullable_key is off, so a Nullable in ORDER BY fails the migration
    rather than the query."""
    sql = _migration_sql()

    assert "ORDER BY (company_id, doffin_id, lot_id, winner_ordinal)" in sql
    for column in ("company_id", "doffin_id", "lot_id"):
        assert f"    {column} String" in sql
    assert "    winner_ordinal Int32" in sql


def test_a_foreign_winner_is_labelled_rather_than_called_unmatched() -> None:
    """'we could not find this Norwegian company' and 'this company is not
    Norwegian' are different facts, and 34 of one November's 505 rows are the
    second."""
    sql = notices_insert_sql(target_table="t", stage_table="s")

    assert "'foreign_winner'" in sql
    assert "'unmatched_company'" in sql
    assert "'no_winner_named'" in sql


def test_the_company_join_is_restricted_to_this_batch() -> None:
    """ClickHouse materialises the whole right side of a join; joining
    no_companies unrestricted reads the entire register per partition."""
    sql = notices_insert_sql(target_table="t", stage_table="s")

    assert "WHERE org_number IN (" in sql


def test_the_stage_matches_the_candidate_contract() -> None:
    ddl = candidate_stage_ddl("stage_src")

    for column in tables.CANDIDATE_COLUMNS:
        assert f"{column} " in ddl
    assert "cpv_codes Array(String)" in ddl


def test_a_partition_key_that_is_not_a_date_is_refused() -> None:
    """It is interpolated into DDL, which cannot be parameterised."""
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        export_notices_clickhouse(
            duckdb_connection=None,
            clickhouse_client=None,
            partition="2025-11-01'; DROP TABLE x --",
        )


def test_the_view_becomes_a_union_with_ted() -> None:
    """Norway was TED-only; Doffin joins it as a second branch, the way Sweden's
    and Finland's views are already unions."""
    sql = _migration_sql()

    assert "UNION ALL" in sql
    assert "FROM corpscout.no_doffin_notices AS d FINAL" in sql
    assert "'norway_doffin_procurement'" in sql
    # Only resolved winners reach the country view; the rest stay on the source
    # table, where the source page can still show them.
    assert "d.company_match_status = 'exact'" in sql


def test_the_view_names_the_business_term_behind_each_number() -> None:
    """A displayed figure has to state its own origin, or it cannot be checked
    against the notice."""
    sql = _migration_sql()

    assert "'BT-720 PayableAmount'" in sql
    assert "'BT-161 TotalAmount'" in sql
    # The estimate is deliberately NOT surfaced as a contract value.
    assert "BT-27" not in sql.split("UNION ALL")[1]
