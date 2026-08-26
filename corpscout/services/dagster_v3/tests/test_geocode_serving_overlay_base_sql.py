"""The SERVING overlay's precise base is swappable, and by default it is the store read.

`build_served_geocodes_sql` fills unmatched/ambiguous/postal_box identities (the
fallback-eligible statuses -- FALLBACK_ELIGIBLE_STATUSES) with coarse centroids over a
PRECISE base. That base is `build_current_geocodes_sql` by default -- the self-ranking
read straight off the versioned store -- which is what the overlay's own clickhouse-local
correctness test seeds and depends on. But that read is expensive (it re-ranks 2.09M raw
rows), so the SERVED VIEW (migration, Task 5b) wraps the ALREADY materialized fast serving
table instead, handed in through `base_sql`.

These are pure-string unit tests: no engine, no docker. They pin the two facts the swap
turns on -- default is the store read, and a supplied base_sql replaces it verbatim while
the overlay machinery around it is unchanged -- so the correctness suite stays the
authority on the overlay's row-level behavior.
"""

import re

import pytest

from dagster_v3.defs.sweden_company.geocode_serving_overlay import (
    FAST_SERVING_TABLE,
    build_served_geocodes_sql,
    fast_serving_base_sql,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
)

STORE = QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE  # corpscout.se_address_geocodes


def _reads_store(sql: str) -> bool:
    """The raw store as a WHOLE table name -- `se_address_geocodes` but never the fast
    `se_address_geocodes_current`, of which it is a prefix (a trailing `_` blocks \\b)."""
    return re.search(re.escape(STORE) + r"\b", sql) is not None


def test_default_base_is_the_versioned_store_read() -> None:
    """No base_sql -> the overlay wraps build_current_geocodes_sql, which reads the raw
    store. This is the base the clickhouse-local correctness suite relies on."""
    served = build_served_geocodes_sql()
    # The store's two-stage read leaves its fingerprints: the raw store table and the
    # per-identity rank both appear.
    assert _reads_store(served)
    assert "LIMIT 1 BY address_id" in served


def test_a_supplied_base_replaces_the_store_read_and_is_read_from() -> None:
    """Pass a base SELECT over a distinctive table and the overlay reads THAT, not the
    store: build_current_geocodes_sql's raw-store read is gone entirely."""
    base_table = "corpscout.some_fast_serving_table"
    base = f"SELECT address_id FROM {base_table}"
    served = build_served_geocodes_sql(base_sql=base)

    assert base_table in served
    # The store read is not present at all -- the base was swapped, not appended.
    assert not _reads_store(served)
    assert "LIMIT 1 BY address_id" not in served


def test_the_supplied_base_appears_verbatim_inside_the_base_subquery() -> None:
    """The base is dropped in untouched as `FROM ( <base_sql> ) AS base` -- so whatever
    read the caller hands in is exactly the read the overlay fills the gaps in."""
    base = "SELECT address_id FROM corpscout.some_fast_serving_table"
    served = build_served_geocodes_sql(base_sql=base)
    assert base in served


def test_fast_serving_base_reads_the_materialized_current_table() -> None:
    """The helper the served VIEW uses reads the fast MergeTree (migration 000320), not the
    store -- one servable row per identity already, so a plain projection is the whole base."""
    base = fast_serving_base_sql()
    assert base.startswith("SELECT")
    assert f"FROM {FAST_SERVING_TABLE}" in base
    assert not _reads_store(base)
    # It carries no rank of its own: the fast table is already one row per address_id.
    assert "LIMIT 1 BY" not in base

    served = build_served_geocodes_sql(base_sql=base)
    assert FAST_SERVING_TABLE in served
    assert not _reads_store(served)


def test_a_supplied_base_refuses_an_address_filter() -> None:
    """address_filter_sql threads into the DEFAULT store read only. Combined with an
    explicit base it is a caller error -- the base already fixes what is read -- so it is
    refused rather than silently dropped."""
    with pytest.raises(ValueError, match="base_sql"):
        build_served_geocodes_sql(
            base_sql="SELECT address_id FROM t",
            address_filter_sql="address_id = 'x'",
        )


def test_default_base_still_threads_the_address_filter() -> None:
    """With no base_sql the filter reaches the store read exactly as before the swap."""
    served = build_served_geocodes_sql(address_filter_sql="address_id = 'x'")
    assert "WHERE address_id = 'x'" in served
    assert _reads_store(served)
