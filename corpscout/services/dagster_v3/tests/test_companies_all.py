import dagster as dg
import pytest

from dagster_v3.defs.companies_all.sql import SOURCES, build_country_insert_select
from dagster_v3.defs.companies_all.tables import (
    COMPANIES_ALL_COLUMNS,
    COMPANIES_ALL_COUNTRIES,
    COMPANIES_ALL_TABLE,
)

# The 11 companies-export keys, verified (2026-07-18) against `uv run dg list
# defs` output -- these matched the plan's Ground truth list exactly, no
# fixes needed.
EXPECTED_COMPANIES_EXPORT_KEYS = {
    "norway_brreg_entities_snapshot_clickhouse",
    "norway_brreg_entity_updates_clickhouse",
    "finland_ytj_resolved_clickhouse",
    "sweden_company_companies_clickhouse",
    "estonia_ar_clickhouse_companies",
    "latvia_ur_clickhouse_companies",
    "uk_companies_house_clickhouse_companies",
    "france_sirene_clickhouse_companies",
    "brazil_comp_rfb_clickhouse_companies",
    "czech_ares_clickhouse_companies",
    "slovakia_rpo_clickhouse_companies",
}

# Additions found by checking (per the brief) whether any country exports its
# industries table from an asset NOT already in the companies-export list --
# each verified against `uv run dg list defs` (2026-07-18). no/fi are
# deliberately absent: their no_industries/fi_industries tables are exported
# TOGETHER with their companies tables by the assets already listed above
# (norway_brreg_entities_snapshot_clickhouse / norway_brreg_entity_updates_
# clickhouse; finland_ytj_resolved_clickhouse).
EXPECTED_INDUSTRIES_EXPORT_KEYS = {
    "sweden_company_industries_clickhouse",
    "estonia_ar_clickhouse_industries",
    "uk_companies_house_clickhouse_industries",
    "france_sirene_clickhouse_industries",
    "czech_ares_clickhouse_industries",
    "slovakia_rpo_clickhouse_industries",
    "brazil_comp_rfb_clickhouse_establishments",
    "brazil_comp_cnae_to_nace_clickhouse",
    "nace_categories_clickhouse",
    "latvia_ur_nace_classification",
}

EXPECTED_FINANCIALS_LATEST_EXPORT_KEYS = {
    f"{code}_company_financials_latest_clickhouse"
    for code in ("no", "fi", "se", "ee", "lv", "gb", "br", "sk")
}

# fr/cz have no financials-latest summary table (verified live via
# system.columns 2026-07-18: neither fr_company_financials_latest nor
# cz_company_financials_latest exists).
NO_FINANCIALS_COUNTRIES = ("fr", "cz")


def test_companies_all_table_name() -> None:
    assert COMPANIES_ALL_TABLE == "companies_all"


def test_companies_all_columns_match_global_constraints_order() -> None:
    assert COMPANIES_ALL_COLUMNS == (
        "country_code",
        "company_id",
        "name",
        "name_normalized",
        "is_active",
        "status",
        "legal_form",
        "place",
        "size",
        "industry_code",
        "industry_label",
        "revenue_usd",
        "fiscal_year",
        "employees",
        "has_financials",
        "resolved_at",
    )


def test_companies_all_countries_match_sources() -> None:
    assert set(COMPANIES_ALL_COUNTRIES) == set(SOURCES)
    assert len(COMPANIES_ALL_COUNTRIES) == 10


@pytest.mark.parametrize("code", COMPANIES_ALL_COUNTRIES)
def test_build_country_insert_select_mentions_companies_table(code: str) -> None:
    select = build_country_insert_select(code)
    companies_table = SOURCES[code]["companies_table"]
    assert f"FROM corpscout.{companies_table} AS c" in select


@pytest.mark.parametrize("code", COMPANIES_ALL_COUNTRIES)
def test_build_country_insert_select_aliases_country_code(code: str) -> None:
    select = build_country_insert_select(code)
    assert f"'{code}' AS country_code" in select


@pytest.mark.parametrize("code", COMPANIES_ALL_COUNTRIES)
def test_build_country_insert_select_emits_every_column(code: str) -> None:
    select = build_country_insert_select(code)
    for column in COMPANIES_ALL_COLUMNS:
        assert f"AS {column}" in select, f"{code}: missing alias for {column}"


@pytest.mark.parametrize("code", COMPANIES_ALL_COUNTRIES)
def test_build_country_insert_select_joins_industry_subquery(code: str) -> None:
    select = build_country_insert_select(code)
    industry_join_key = SOURCES[code]["industry_join_key"]
    assert f"LEFT JOIN ({SOURCES[code]['industry_subquery']}) AS ind" in select
    assert f"ON ind.company_id = toString({industry_join_key})" in select


@pytest.mark.parametrize(
    "code", [c for c in COMPANIES_ALL_COUNTRIES if c not in NO_FINANCIALS_COUNTRIES]
)
def test_countries_with_financials_join_the_summary_table(code: str) -> None:
    select = build_country_insert_select(code)
    financials_table = SOURCES[code]["financials_table"]
    financials_join_key = SOURCES[code]["financials_join_key"]
    assert f"LEFT JOIN corpscout.{financials_table} AS fin" in select
    assert f"ON fin.company_id = toString({financials_join_key})" in select
    assert "fin.revenue_amount_usd AS revenue_usd" in select
    assert "fin.fiscal_year AS fiscal_year" in select
    assert "fin.employees AS employees" in select
    assert "toUInt8(fin.company_id != '') AS has_financials" in select
    # Exactly one financials join (nested industry subqueries have their own
    # LEFT JOIN to nace_categories, so a bare "LEFT JOIN" count would double-count).
    assert select.count("AS fin ON") == 1


@pytest.mark.parametrize("code", NO_FINANCIALS_COUNTRIES)
def test_no_financials_countries_use_null_literals(code: str) -> None:
    select = build_country_insert_select(code)
    assert SOURCES[code]["financials_table"] is None
    assert "CAST(NULL AS Nullable(Float64)) AS revenue_usd" in select
    assert "CAST(NULL AS Nullable(Int32)) AS fiscal_year" in select
    assert "CAST(NULL AS Nullable(Float64)) AS employees" in select
    assert "toUInt8(0) AS has_financials" in select
    # No financials join at all -- "fin" never appears as an alias.
    assert "AS fin ON" not in select
    assert "fin." not in select


def test_se_industry_and_financials_join_key_is_qualified_company_id() -> None:
    select = build_country_insert_select("se")
    assert "ON ind.company_id = toString(c.company_id)" in select
    assert "ON fin.company_id = toString(c.company_id)" in select
    # The public-facing id column still comes from registration_number, but
    # it must never leak into the join keys (se_companies has its OWN
    # internal `company_id`, distinct from `registration_number`).
    assert "toString(registration_number) AS company_id" in select
    assert "ON ind.company_id = toString(registration_number)" not in select
    assert "ON fin.company_id = toString(registration_number)" not in select


def test_build_country_insert_select_unknown_code_raises() -> None:
    with pytest.raises(ValueError):
        build_country_insert_select("xx")


def test_companies_all_asset_name_and_kind() -> None:
    from dagster_v3.defs.companies_all.assets import companies_all_clickhouse

    assert companies_all_clickhouse.key == dg.AssetKey("companies_all_clickhouse")
    spec = companies_all_clickhouse.specs_by_key[companies_all_clickhouse.key]
    assert spec.kinds == {"clickhouse"}


def test_companies_all_asset_deps_pinned() -> None:
    from dagster_v3.defs.companies_all.assets import companies_all_clickhouse

    spec = companies_all_clickhouse.specs_by_key[companies_all_clickhouse.key]
    actual_deps = {dep.asset_key for dep in spec.deps}

    expected_keys = (
        EXPECTED_COMPANIES_EXPORT_KEYS
        | EXPECTED_INDUSTRIES_EXPORT_KEYS
        | EXPECTED_FINANCIALS_LATEST_EXPORT_KEYS
    )
    assert len(expected_keys) == 11 + 10 + 8
    expected = {dg.AssetKey(key) for key in expected_keys}
    assert actual_deps == expected


def test_companies_all_schedule_running_daily_cron() -> None:
    from dagster_v3.defs.companies_all.assets import companies_all_schedule

    assert companies_all_schedule.cron_schedule == "15 7 * * *"
    assert companies_all_schedule.execution_timezone == "Europe/Oslo"
    assert companies_all_schedule.default_status == dg.DefaultScheduleStatus.RUNNING


def test_companies_all_defs_include_job_and_schedule() -> None:
    from dagster_v3.defs.companies_all.assets import defs

    job_names = {job.name for job in defs.jobs}
    assert "companies_all_job" in job_names

    schedule_names = {schedule.name for schedule in defs.schedules}
    assert "companies_all_schedule" in schedule_names


_COMPANIES_TABLE_TO_CODE = {
    SOURCES[code]["companies_table"]: code for code in COMPANIES_ALL_COUNTRIES
}


class _FakeReplaceClient:
    """Fakes just enough of the ClickHouse client for
    ``_replace_companies_all_table``: stage create/drop/exchange are no-ops,
    the per-leg INSERT is a no-op, and the two ``SELECT count()`` queries
    (staged rows for the leg, source-table rows) are answered from canned
    per-country counts.
    """

    def __init__(
        self, *, source_counts: dict[str, int], stage_counts: dict[str, int]
    ) -> None:
        self.source_counts = source_counts
        self.stage_counts = stage_counts
        self.statements: list[str] = []

    def execute(
        self, sql: str, params: dict[str, object] | None = None
    ) -> list[tuple[int]]:
        self.statements.append(sql)
        if sql.startswith("CREATE TABLE") or sql.startswith("INSERT INTO"):
            return []
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        if "WHERE country_code = %(code)s" in sql:
            code = str(params["code"])  # type: ignore[index]
            return [(self.stage_counts[code],)]
        if sql.startswith("SELECT count() FROM corpscout."):
            table = sql.removeprefix("SELECT count() FROM corpscout.")
            code = _COMPANIES_TABLE_TO_CODE[table]
            return [(self.source_counts[code],)]
        raise AssertionError(sql)


class _NoOpLog:
    def info(self, *args: object, **kwargs: object) -> None:
        pass


@pytest.mark.parametrize("empty_code", ["no", "sk"])
def test_replace_companies_all_table_refuses_empty_source_country(
    empty_code: str,
) -> None:
    from dagster_v3.defs.companies_all.assets import _replace_companies_all_table

    # Every leg staged as many rows as its source has -- except the source
    # register for `empty_code`, which has gone to 0 rows. Staging then also
    # produces 0 rows for that leg, so the OLD exact-equality guard
    # (country_rows != source_count) sees 0 == 0 and passes silently,
    # letting the country vanish from companies_all with no error. The new
    # guard must refuse the whole build instead, naming the empty country.
    source_counts = {code: 100 for code in COMPANIES_ALL_COUNTRIES}
    source_counts[empty_code] = 0
    stage_counts = dict(source_counts)

    client = _FakeReplaceClient(source_counts=source_counts, stage_counts=stage_counts)

    try:
        _replace_companies_all_table(client, log=_NoOpLog())
    except ValueError as exc:
        message = str(exc)
        assert empty_code in message
        assert "0 rows" in message
    else:
        raise AssertionError("expected ValueError for empty source country")

    # The stage table must still be cleaned up even though the build was
    # refused (the `finally: DROP TABLE IF EXISTS` path).
    assert any(
        stmt.startswith("DROP TABLE IF EXISTS") for stmt in client.statements
    )
    # The build must refuse BEFORE reaching a later country's leg.
    later_codes = COMPANIES_ALL_COUNTRIES[
        COMPANIES_ALL_COUNTRIES.index(empty_code) + 1 :
    ]
    for later_code in later_codes:
        companies_table = SOURCES[later_code]["companies_table"]
        assert not any(
            stmt == f"SELECT count() FROM corpscout.{companies_table}"
            for stmt in client.statements
        )
