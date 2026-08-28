"""One DuckDB file per partition, so an export cannot read another month's rows.

Why this exists, in full, because the failure was silent and expensive:

On 2026-07-28 Brazil's PNCP chain materialized 37 monthly partitions
(2022-01..2025-01) and left 36 of them EMPTY in ClickHouse. Partition 2024-03
alone had really produced 65,218 rows. Every run reported success.

The cause was a shared DuckDB file. `normalize.py` did
``create or replace table contract_candidates``, so the file held whichever month
ran last -- scratch, not per-partition input -- and the export read it unfiltered,
trusting it to be the month it had been asked for. All 37 normalise runs finished
on 07-27 (05:59-09:26) and the first export started 07-28 16:12, a 31-hour gap,
so all 37 exports read the same leftover 2025-01 rows.

``ALTER TABLE t REPLACE PARTITION 202403 FROM stage`` then resolved as "drop the
target's 202403, copy stage's 202403 in". The stage held January rows, so its
202403 partition was absent, and an absent source partition is a legal
replace-with-nothing in ClickHouse -- not an error. Each statement deleted a
populated partition and wrote nothing, successfully.

``norway_doffin`` and ``estonia_rhr_procurement`` carried the identical shape and
never fired, purely because their backfills interleaved normalise and export per
partition (Doffin 2024-03: duckdb 12:11:41, clickhouse 12:13:33 -- two minutes
apart). They were safe by launch pattern, not by construction, and a single
phase-by-phase backfill would have emptied 99 partitions of Norwegian history.

Putting the partition in the file path removes the class instead of detecting it,
which is what ``ted_procurement`` already does: reading the wrong month means
opening a different file, and the path is derived from the same partition key the
export replaces. A missing file then fails loudly ("normalise has not run for
this partition") where a shared buffer silently succeeded with the wrong data.
"""

import re
from pathlib import Path

# Mirrors ted_procurement's layout (data/<source>/duckdb/partition_key=<key>/),
# so an operator can tell which month a file holds by looking at the directory.
DUCKDB_ROOT = Path("data")

# Most callers use monthly or weekly ISO-date keys. Annual bulk sources use a
# four-digit year so the source archive and Dagster partition have the same key.
_PARTITION_KEY = re.compile(r"(?:\d{4}|\d{4}-\d{2}-\d{2})")


def partition_duckdb_path(*, source: str, partition: str) -> Path:
    """The DuckDB file for one partition of one source.

    The partition key is interpolated into a filesystem path, so it is validated
    rather than trusted: it reaches here from Dagster and is therefore already
    controlled, but validating means a future caller cannot make this the one
    path-traversal site in the codebase. Norway's export already validates the
    same value before interpolating it into DDL, for the same reason.
    """
    if not _PARTITION_KEY.fullmatch(partition):
        raise ValueError(
            f"partition must be YYYY or YYYY-MM-DD, got {partition!r} -- it is "
            "used as a directory name"
        )
    return (
        DUCKDB_ROOT / source / "duckdb" / f"partition_key={partition}" / "data.duckdb"
    )


def open_partition_duckdb(*, source: str, partition: str):
    """Connect to a partition's DuckDB file, creating its directory.

    Callers own closing the connection. Kept next to the path helper so a module
    converting away from a shared resource has one obvious thing to call.
    """
    import duckdb

    path = partition_duckdb_path(source=source, partition=partition)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def require_partition_duckdb(*, source: str, partition: str):
    """Connect to a partition's DuckDB file, refusing to create it.

    For steps downstream of normalise. A missing file means normalise has not run
    for this partition, which under the shared-file design was invisible: the
    step read the previous partition's rows and published them as this one.
    """
    import duckdb

    path = partition_duckdb_path(source=source, partition=partition)
    if not path.exists():
        raise ValueError(
            f"No DuckDB file for partition {partition} at {path}. Its normalise "
            f"step has not run, or ran before this source moved to per-partition "
            f"files -- re-run it for this partition rather than exporting "
            f"whatever another partition left behind."
        )
    return duckdb.connect(str(path))
