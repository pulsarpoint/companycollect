import re

from dagster_v3.defs.company_contracts import tables
from dagster_v3.defs.company_contracts.rollup_sql import build_rollup_select


def test_supplier_columns_are_bound_only_where_the_source_has_them() -> None:
    """The plain facts table has no winner_registered_id -- selecting it would
    fail at query time for six of the eight countries."""
    plain = build_rollup_select(source_table="t", has_supplier_detail=False)
    awards = build_rollup_select(source_table="t", has_supplier_detail=True)
    assert "argMin('', if(" in plain
    # 'exact', NOT '': a source that publishes no match status has nothing to
    # disagree with, and the page renders that as an exact match. Blanking it
    # changes what six countries display.
    assert "argMin('exact', if(" in plain
    assert "argMin(winner_registered_id, if(" in awards
    assert "argMin(winner_match_status, if(" in awards


def test_carries_no_filter_or_limit() -> None:
    """Filters move to the read side and the asset writes every row -- a LIMIT
    here would silently truncate the table.

    Narrower than it reads: the per-country predicate is wrapped around the
    source table in assets.py, outside this function, so this proves the
    aggregation carries no filter of its OWN.
    """
    sql = build_rollup_select(source_table="t", has_supplier_detail=True)
    assert "LIMIT" not in sql.upper()
    assert " WHERE " not in sql.upper()


def test_unwraps_the_usd_sentinel_before_storing_it() -> None:
    """-1.0 distinguishes 'no source reported USD' from a genuine zero while
    max() collapses the per-source values. Stored, it stops being a sentinel and
    starts being a number: `amount_usd <= X` is true for it, so every contract
    without a USD figure would join every max-amount filter, and an ascending
    sort would lead with them instead of trailing.
    """
    sql = build_rollup_select(source_table="t", has_supplier_detail=True)
    assert "-1.0" in sql
    assert "nullIf(max(priority), -1.0)" in sql


def test_projects_columns_in_table_order() -> None:
    """The INSERT names its columns, so a SELECT in a different order writes
    source_url into publication_date -- and ClickHouse accepts it."""
    sql = build_rollup_select(source_table="t", has_supplier_detail=True)
    outer = sql.split("FROM")[0]
    expected = [
        column
        for column in tables.ROLLUP_COLUMNS
        # Literals the asset supplies at their own positions.
        if column not in ("country_code", "resolved_at")
    ]
    assert ["contract_ref", *re.findall(r"AS (\w+)", outer)] == expected
