import pytest

from dagster_v3.defs.companies_all.sql import SOURCES, build_country_insert_select
from dagster_v3.defs.companies_all.tables import (
    COMPANIES_ALL_COLUMNS,
    COMPANIES_ALL_COUNTRIES,
    COMPANIES_ALL_TABLE,
)

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
