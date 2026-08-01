from dagster_v3.defs.company_contracts.rollup_sql import build_rollup_select


def test_supplier_columns_only_where_the_source_has_them() -> None:
    """The plain shape carries no winner_registered_id -- selecting it would
    fail at query time for six of the eight countries."""
    plain = build_rollup_select(source_table="t", has_supplier_detail=False)
    awards = build_rollup_select(source_table="t", has_supplier_detail=True)
    assert "winner_registered_id," not in plain.split("FROM")[1]
    assert "winner_registered_id" in awards


def test_carries_no_filter_or_limit() -> None:
    """Filters move to the read side and the asset writes every row -- a LIMIT
    here would silently truncate the table."""
    sql = build_rollup_select(source_table="t", has_supplier_detail=True)
    assert "LIMIT" not in sql.upper()
    assert " WHERE " not in sql.upper()


def test_keeps_the_usd_sentinel() -> None:
    """-1.0 distinguishes 'no source reported USD' from a genuine zero."""
    sql = build_rollup_select(source_table="t", has_supplier_detail=True)
    assert "-1.0" in sql
