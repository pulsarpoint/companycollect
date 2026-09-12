"""The financial entity's suggestion target and scan shape (spec section 7)."""

from dagster_v3.defs.se_company.financial import suggestions, tables


def test_the_source_supplies_every_column_but_the_four_stamps() -> None:
    assert suggestions.STAMPED_COLUMNS == ("suggestion_id", "suggested_at", "source_run_id", "extractor_version")
    assert len(suggestions.FINANCIAL_SELECT_COLUMNS) == 59
    assert set(suggestions.FINANCIAL_SELECT_COLUMNS) | set(suggestions.STAMPED_COLUMNS) == set(tables.SUGGESTION_COLUMNS)
    assert suggestions.FINANCIAL_STATE_COLUMNS == tuple(c for c in suggestions.FINANCIAL_SELECT_COLUMNS if c not in ("company_id", "source"))
    assert len(suggestions.FINANCIAL_STATE_COLUMNS) == 57
    assert "period_key" in suggestions.FINANCIAL_STATE_COLUMNS and "source_record_uid" in suggestions.FINANCIAL_STATE_COLUMNS


def test_the_target_inserts_every_table_column_and_hashes_the_period_key() -> None:
    target = suggestions.FINANCIAL_TARGET
    assert target.qualified_table == "corpscout.se_company_financial_suggestion"
    assert set(target.insert_columns) == set(tables.SUGGESTION_COLUMNS)
    assert target.insert_columns == (*suggestions.FINANCIAL_SELECT_COLUMNS, *suggestions.STAMPED_COLUMNS)
    assert target.with_sql == "WITH (SELECT now64(3, 'UTC')) AS stamp\n"
    assert "candidate.period_key, '\\n', toString(stamp))))) AS suggestion_id, stamp AS suggested_at" in target.trailing_select_sql
    assert "%(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version" in target.trailing_select_sql
    assert target.asset_prefix == "se_company_financial_suggestions_"
    assert target.scratch_prefix == "corpscout._tmp_financial_scope_"


def test_a_live_row_has_a_figure_or_employees() -> None:
    assert suggestions.LIVE_ROW_PREDICATE.startswith("(revenue_amount_original IS NOT NULL OR ")
    assert suggestions.LIVE_ROW_PREDICATE.endswith(" OR employees IS NOT NULL)")
    assert suggestions.LIVE_ROW_PREDICATE.count(" IS NOT NULL") == 41


def test_a_tombstone_copies_the_key_and_its_two_check_columns_and_nulls_every_value() -> None:
    values = suggestions.tombstone_values("ratsit")
    assert set(values) == set(suggestions.FINANCIAL_SELECT_COLUMNS)
    assert values["period_key"] == "stored.period_key" and values["scope"] == "stored.scope" and values["period_end"] == "stored.period_end"
    assert values["source"] == "'ratsit'" and values["amount_scale"] == "toUInt32(1)" and values["period_end_derived"] == "toUInt8(0)"
    assert values["currency"] == "CAST(NULL AS Nullable(String))"
    for column in tables.SUGGESTION_VALUE_COLUMNS:
        assert values[column].startswith("CAST(NULL AS Nullable("), column
    scan = suggestions.scan_for("ratsit")
    assert scan.key_column == "period_key" and scan.tombstone_columns == ("period_key", "scope", "period_end")


def test_period_months_is_the_rounded_month_count_or_null() -> None:
    assert suggestions.period_months_sql("a", "b") == (
        "if(a IS NULL OR b IS NULL, CAST(NULL AS Nullable(UInt16)), toUInt16(round((dateDiff('day', a, b) + 1) / 30.4375)))"
    )
    assert suggestions.universe_join_sql("m").endswith("ON universe.company_id = m.company_id")
