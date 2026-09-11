"""Slice 0 of the financial entity (spec 2026-09-11 section 3): the USD twins of
se_ratsit_financial_periods. Column pairs, the SQL the asset runs, and the run function
against a fake client and fake rates."""

from dagster_v3.defs.sweden_ratsit import financial_usd
from dagster_v3.defs.sweden_ratsit.financial_usd import (
    AMOUNT_COLUMNS,
    JOIN_TABLE_PREFIX,
    MONETARY_PRESENT_SQL,
    PER_EMPLOYEE_COLUMNS,
    QUALIFIED_PERIODS_TABLE,
    RATE_DATE_SQL,
    USD_PAIRS,
    join_insert_sql,
    join_table_ddl,
    join_table_name,
    mutation_status_sql,
    pending_rate_dates_sql,
    usd_update_sql,
)


def test_usd_pairs_cover_the_twenty_monetary_columns_and_nothing_else() -> None:
    assert len(AMOUNT_COLUMNS) == 18
    assert AMOUNT_COLUMNS[0] == "revenue_amount"
    assert "balance_sheet_total_amount" in AMOUNT_COLUMNS
    assert PER_EMPLOYEE_COLUMNS == (
        ("personnel_cost_per_employee_msek", "personnel_cost_per_employee_usd"),
        ("revenue_per_employee_msek", "revenue_per_employee_usd"),
    )
    assert len(USD_PAIRS) == 20
    natives = [native for native, _, _ in USD_PAIRS]
    assert "average_salary" not in natives
    assert not any(native.endswith("_percent") for native in natives)
    for native, usd, scale_sql in USD_PAIRS:
        if native.endswith("_msek"):
            assert usd == native.removesuffix("_msek") + "_usd"
            assert scale_sql == "1000000"
        else:
            assert usd == f"{native}_usd"
            assert scale_sql == financial_usd.UNIT_SCALE_SQL
    assert financial_usd.UNIT_SCALE_SQL == (
        "multiIf(monetary_unit = 'MSEK', 1000000, monetary_unit = 'TSEK', 1000, 1)"
    )


def test_rate_date_and_pending_scan() -> None:
    assert RATE_DATE_SQL == "ifNull(period_end, makeDate32(fiscal_year, 12, 31))"
    assert MONETARY_PRESENT_SQL.startswith("(revenue_amount IS NOT NULL OR ")
    assert MONETARY_PRESENT_SQL.endswith("revenue_per_employee_msek IS NOT NULL)")
    sql = pending_rate_dates_sql()
    assert sql.startswith(f"SELECT {RATE_DATE_SQL} AS rate_date, count() AS rows")
    assert f"FROM {QUALIFIED_PERIODS_TABLE} FINAL" in sql
    assert f"WHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL}" in sql
    assert sql.endswith("GROUP BY rate_date\nORDER BY rate_date")


def test_join_table_is_qualified_run_scoped_and_a_join_engine() -> None:
    name = join_table_name("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    assert name == f"{JOIN_TABLE_PREFIX}a1b2c3d4e5f67890abcdef1234567890"
    assert name.startswith("corpscout.")
    ddl = join_table_ddl(name)
    assert ddl == (
        f"CREATE TABLE {name} (rate_date Date32, fx_rate Decimal(38, 12), "
        "fx_rate_date Date32, fx_source String) ENGINE = Join(ANY, LEFT, rate_date)"
    )
    assert join_insert_sql(name) == (
        f"INSERT INTO {name} (rate_date, fx_rate, fx_rate_date, fx_source) VALUES"
    )


def test_update_sql_names_every_pair_the_fx_columns_and_the_pending_guard() -> None:
    name = join_table_name("run-1")
    sql = usd_update_sql(name)
    assert sql.startswith(f"ALTER TABLE {QUALIFIED_PERIODS_TABLE} UPDATE\n")
    for native, usd, scale_sql in USD_PAIRS:
        assert (
            f"{usd} = multiplyDecimal({native} * {scale_sql}, "
            f"joinGet('{name}', 'fx_rate', {RATE_DATE_SQL}), 6)"
        ) in sql, usd
    assert f"fx_rate_to_usd = joinGet('{name}', 'fx_rate', {RATE_DATE_SQL})" in sql
    assert f"fx_rate_date = joinGet('{name}', 'fx_rate_date', {RATE_DATE_SQL})" in sql
    assert f"fx_source = joinGet('{name}', 'fx_source', {RATE_DATE_SQL})" in sql
    assert (
        f"WHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL} "
        f"AND {RATE_DATE_SQL} IN (SELECT rate_date FROM {name})"
    ) in sql
    assert sql.endswith("SETTINGS mutations_sync = 0")
    assert usd_update_sql(name, mutations_sync=2).endswith("SETTINGS mutations_sync = 2")
    # Every assignment is one line and the fx columns come last, so a reader can diff it.
    assert sql.count("\n") == 20 + 3 + 1


def test_mutation_status_sql_is_bound_by_table_and_join_name() -> None:
    sql = mutation_status_sql()
    assert "FROM system.mutations" in sql
    assert "database = %(database)s AND table = %(table)s AND command LIKE %(pattern)s" in sql
    assert "ORDER BY create_time DESC LIMIT 1" in sql
