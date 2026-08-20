from pathlib import Path

from dagster_v3.defs.company_financials_latest.tables import (
    COMPANY_FINANCIALS_LATEST_COLUMNS,
    COMPANY_FINANCIALS_LATEST_COUNTRIES,
    COMPANY_FINANCIALS_LATEST_TABLES,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def _migration_sql() -> str:
    return "\n".join(
        (
            (
                MIGRATIONS_DIR / "000137_corpscout_company_financials_latest.up.sql"
            ).read_text(encoding="utf-8"),
            (
                MIGRATIONS_DIR / "000209_corpscout_fr_financial_and_enrichments.up.sql"
            ).read_text(encoding="utf-8"),
        )
    )


def test_migration_creates_every_summary_table_with_full_schema() -> None:
    sql = _migration_sql()
    assert len(COMPANY_FINANCIALS_LATEST_TABLES) == 9
    for table in COMPANY_FINANCIALS_LATEST_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in sql
    for column in COMPANY_FINANCIALS_LATEST_COLUMNS:
        assert f"    {column} " in sql


def test_build_latest_insert_sql_covers_every_country() -> None:
    from dagster_v3.defs.company_financials_latest.sql import (
        SOURCES,
        build_latest_insert_sql,
    )

    assert set(SOURCES) == set(COMPANY_FINANCIALS_LATEST_COUNTRIES)

    # every column except resolved_at must be an explicit alias in the SELECT body;
    # resolved_at is generated (now64(3)) rather than sourced from an upstream column.
    aliased_columns = COMPANY_FINANCIALS_LATEST_COLUMNS[:-1]

    for code in COMPANY_FINANCIALS_LATEST_COUNTRIES:
        select_sql = build_latest_insert_sql(code)

        source_table = SOURCES[code]["table"]
        assert f"corpscout.{source_table}" in select_sql

        source_id = SOURCES[code]["id"]
        assert "AS company_id" in select_sql
        assert source_id in select_sql

        for column in aliased_columns:
            assert f"AS {column}" in select_sql, (
                f"{code}: missing alias for {column!r} in generated SQL"
            )


def test_build_latest_insert_sql_qualifies_resolved_at_tiebreak() -> None:
    """The wide template's ORDER BY must reference the source-table
    ``resolved_at`` column, not the constant ``now64(3) AS resolved_at``
    alias in the SELECT list. ClickHouse resolves a bare ``resolved_at`` in
    ORDER BY to that alias (shadowing the source column), silently making the
    tiebreak a no-op. Qualifying with the source table name forces resolution
    to the real per-row column -- derived here from ``SOURCES`` so all seven
    wide countries are covered without hand-listing table names.
    """
    from dagster_v3.defs.company_financials_latest.sql import (
        SOURCES,
        build_latest_insert_sql,
    )

    for code in COMPANY_FINANCIALS_LATEST_COUNTRIES:
        if code == "br":
            continue  # BR's hand-built SELECT has no {tiebreak}-style placeholder
        source_table = SOURCES[code]["table"]
        select_sql = build_latest_insert_sql(code)
        expected = f"`{source_table}`.resolved_at DESC"
        assert expected in select_sql, (
            f"{code}: expected qualified tiebreak {expected!r} in generated SQL"
        )
        # And the bare, shadowable form must not appear anywhere in ORDER BY.
        order_by_line = next(
            line
            for line in select_sql.splitlines()
            if line.strip().startswith("ORDER BY")
        )
        assert (
            "ORDER BY fiscal_year DESC NULLS LAST, resolved_at DESC"
            not in order_by_line
        )


def test_sweden_latest_prefers_reported_metrics_over_comparatives() -> None:
    from dagster_v3.defs.company_financials_latest.sql import build_latest_insert_sql

    sql = build_latest_insert_sql("se")

    assert "observation_kind = 'reported' DESC" in sql
    assert "source_fiscal_year DESC NULLS LAST" in sql


def test_build_latest_insert_sql_rejects_unknown_code() -> None:
    from dagster_v3.defs.company_financials_latest.sql import build_latest_insert_sql

    try:
        build_latest_insert_sql("zz")
    except KeyError, ValueError:
        pass
    else:
        raise AssertionError("expected build_latest_insert_sql('zz') to raise")


def test_upstream_keys_cover_every_country() -> None:
    from dagster_v3.defs.company_financials_latest.assets import UPSTREAM_KEYS

    assert set(UPSTREAM_KEYS) == set(COMPANY_FINANCIALS_LATEST_COUNTRIES)
    for code, keys in UPSTREAM_KEYS.items():
        assert keys, f"{code}: UPSTREAM_KEYS must list at least one upstream asset"


def test_company_financials_latest_assets_match_tables() -> None:
    from dagster_v3.defs.company_financials_latest.assets import (
        company_financials_latest_assets,
    )

    asset_names = {
        asset.key.to_user_string() for asset in company_financials_latest_assets
    }
    expected_names = {
        f"{code}_company_financials_latest_clickhouse"
        for code in COMPANY_FINANCIALS_LATEST_COUNTRIES
    }
    assert asset_names == expected_names


def test_company_financials_latest_defs_include_job_and_schedule() -> None:
    from dagster_v3.defs.company_financials_latest.assets import defs

    job_names = {job.name for job in defs.jobs}
    assert "company_financials_latest_job" in job_names

    schedule_names = {schedule.name for schedule in defs.schedules}
    assert "company_financials_latest_schedule" in schedule_names


def test_norway_select_excludes_quality_flagged_statements() -> None:
    from dagster_v3.defs.company_financials_latest.sql import build_latest_insert_sql

    sql = build_latest_insert_sql("no")
    assert "WHERE quality_flag = ''" in sql

    # The plausibility filter is Norway-specific; other sources carry no
    # quality_flag column and must not reference it.
    for code in COMPANY_FINANCIALS_LATEST_COUNTRIES:
        if code != "no":
            assert "quality_flag" not in build_latest_insert_sql(code)
