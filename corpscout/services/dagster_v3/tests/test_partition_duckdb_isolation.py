"""Per-partition DuckDB files, so an export cannot read another month's rows.

Brazil lost 36 of 37 months on 2026-07-28 because normalise did `create or
replace table contract_candidates` in ONE shared file, and the export read it
unfiltered and trusted it to hold the month it was asked for. All 37 _duckdb runs
finished a day before the first _clickhouse run, so every export read the same
leftover 2025-01 rows and `REPLACE PARTITION <month> FROM stage` deleted each
target partition and copied stage's absent same-named partition into it --
reported by ClickHouse as success.

norway_doffin and estonia_rhr_procurement had the identical shape and survived
only because their backfills happened to interleave normalise and export per
partition (Doffin 2024-03: duckdb 12:11:41, clickhouse 12:13:33). They were safe
by launch pattern, not by construction.

Putting the partition in the FILE PATH -- as ted_procurement already does --
removes the class rather than detecting it: opening the wrong month means opening
a different file, which cannot happen when the path is derived from the same
partition key the export replaces.
"""

import importlib

import pytest

MODULES = [
    "norway_doffin",
    "estonia_rhr_procurement",
    "latvia_iub_procurement",
    "slovakia_uvo_procurement",
]


@pytest.mark.parametrize("module_name", MODULES)
def test_each_partition_gets_its_own_duckdb_file(module_name: str) -> None:
    tables = importlib.import_module(f"dagster_v3.defs.{module_name}.tables")

    assert hasattr(tables, "partition_duckdb_path"), (
        f"{module_name} has no partition_duckdb_path: its assets share one DuckDB "
        f"file, so a stale buffer can be exported as another partition"
    )
    first = tables.partition_duckdb_path("2024-03-01")
    second = tables.partition_duckdb_path("2024-04-01")

    assert first != second
    # The partition must be recoverable from the path, so an operator looking at
    # the directory can tell which month a file holds.
    assert "2024-03" in str(first)
    assert "2024-04" in str(second)


@pytest.mark.parametrize("module_name", MODULES)
def test_the_partition_is_validated_before_it_reaches_a_path(module_name: str) -> None:
    """The value is interpolated into a filesystem path, so a malformed or
    traversing partition key must be refused rather than silently writing
    outside the data directory."""
    tables = importlib.import_module(f"dagster_v3.defs.{module_name}.tables")

    for bad in ("", "../escape", "2024-3-1", "not-a-date"):
        with pytest.raises(ValueError):
            tables.partition_duckdb_path(bad)


@pytest.mark.parametrize("module_name", MODULES)
def test_no_asset_still_opens_a_shared_partitioned_duckdb(module_name: str) -> None:
    """A single module-level DUCKDB_PATH handed to every partitioned asset is the
    shape that lost Brazil's months. Once partition_duckdb_path exists, nothing
    partitioned may keep using the shared file."""
    source = importlib.import_module(f"dagster_v3.defs.{module_name}.assets").__file__
    assert source is not None
    text = open(source, encoding="utf-8").read()

    # Must open a per-partition file. Checked against the callables rather than
    # the words "partition_duckdb_path", which also appear in prose -- the first
    # version of this test passed on a module purely because of its comment.
    assert (
        "open_partition_duckdb(" in text or "require_partition_duckdb(" in text
    ), f"{module_name}/assets.py opens no per-partition DuckDB file"

    # And must retain no route back to a shared one.
    assert "DUCKDB_PATH" not in text, (
        f"{module_name} still refers to a module-level shared DuckDB path"
    )
    assert "DuckDBResource" not in text, (
        f"{module_name} still injects the shared DuckDB resource, which is the "
        f"shape that let an export read another partition's rows"
    )
