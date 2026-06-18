from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

DBT_DIR = (
    Path(__file__).parents[1]
    / "src" / "dagster_v3" / "defs" / "norway_resolved" / "dbt"
)


def _seed_norway_brreg_source(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("create schema if not exists norway_brreg")
        conn.execute(
            """
            create table norway_brreg.entities as select * from (values
              (
                'NO', 'norway_brreg', 'run-1', '1000', 'hash1',
                '1000', 'Active One AS', '2020-01-02', '2020-01-01',
                'WWW.ActiveOne.NO/about', 'AS', 'Aksjeselskap', 'Limited liability company',
                '62.010', 'Programmeringstjenester', 'Computer programming activities',
                '70.100', 'Hovedkontortjenester', 'Activities of head offices',
                '', '', '',
                'active', true
              ),
              (
                'NO', 'norway_brreg', 'run-1', '2000', 'hash2',
                '2000', 'Inactive Two AS', '2018-03-04', '',
                '', 'AS', 'Aksjeselskap', 'Limited liability company',
                '', '', '',
                '', '', '',
                '', '', '',
                'inactive', false
              )
            ) as t(
              country_iso2, source_slug, source_run_id, source_record_id, source_payload_hash,
              org_number, legal_name, registration_date, incorporation_date,
              website, legal_form_code, legal_form_description_original,
              legal_form_description_en,
              nace1_code, nace1_description_original, nace1_description_en,
              nace2_code, nace2_description_original, nace2_description_en,
              nace3_code, nace3_description_original, nace3_description_en,
              status, is_active
            )
            """
        )
        conn.execute(
            """
            create table norway_brreg.financial_statements (
              country_iso2 varchar,
              source_slug varchar,
              source_run_id varchar,
              source_record_id varchar,
              source_payload_hash varchar,
              org_number varchar,
              legal_name varchar,
              last_submitted_accounts_year varchar,
              filing_id bigint,
              journal_number varchar,
              accounts_type varchar,
              legal_form_code varchar,
              is_parent_company boolean,
              period_start_date date,
              period_end_date date,
              fiscal_year bigint,
              currency varchar,
              liquidation_accounts boolean,
              statement_layout varchar,
              is_not_audited boolean,
              opted_out_audit boolean,
              is_small_enterprise boolean,
              accounting_rules varchar,
              operating_revenue_amount_original decimal(38, 6),
              operating_revenue_amount_usd decimal(38, 6),
              operating_costs_amount_original decimal(38, 6),
              operating_costs_amount_usd decimal(38, 6),
              operating_result_amount_original decimal(38, 6),
              operating_result_amount_usd decimal(38, 6),
              net_financial_items_amount_original decimal(38, 6),
              net_financial_items_amount_usd decimal(38, 6),
              pretax_result_amount_original decimal(38, 6),
              pretax_result_amount_usd decimal(38, 6),
              net_result_amount_original decimal(38, 6),
              net_result_amount_usd decimal(38, 6),
              total_assets_amount_original decimal(38, 6),
              total_assets_amount_usd decimal(38, 6),
              current_assets_amount_original decimal(38, 6),
              current_assets_amount_usd decimal(38, 6),
              fixed_assets_amount_original decimal(38, 6),
              fixed_assets_amount_usd decimal(38, 6),
              equity_amount_original decimal(38, 6),
              equity_amount_usd decimal(38, 6),
              total_debt_amount_original decimal(38, 6),
              total_debt_amount_usd decimal(38, 6),
              current_liabilities_amount_original decimal(38, 6),
              current_liabilities_amount_usd decimal(38, 6),
              long_term_liabilities_amount_original decimal(38, 6),
              long_term_liabilities_amount_usd decimal(38, 6),
              fx_rate_to_usd decimal(38, 12),
              fx_rate_date date,
              fx_source varchar,
              source_url varchar
            )
            """
        )
        conn.execute(
            """
            insert into norway_brreg.financial_statements values (
              'NO', 'norway_brregregnskap', 'run-2', '1000-fin-2024', 'hash-fin-1',
              '1000', 'Active One AS', '2024', 123, '2024/1', 'annual',
              'AS', false, '2024-01-01'::date, '2024-12-31'::date, 2024, 'NOK',
              false, 'small', false, false, true, 'regnskapsloven',
              1000.00, 95.00,
              700.00, 66.50,
              300.00, 28.50,
              -10.00, -0.95,
              290.00, 27.55,
              210.00, 19.95,
              5000.00, 475.00,
              2000.00, 190.00,
              3000.00, 285.00,
              2500.00, 237.50,
              2500.00, 237.50,
              1000.00, 95.00,
              1500.00, 142.50,
              0.095, '2024-12-31'::date, 'ECB EXR',
              'https://data.brreg.no/regnskapsregisteret/regnskap/1000'
            )
            """
        )


def _dbt_build(db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NORWAY_BRREG_DUCKDB_PATH", str(db_path))
    result = dbtRunner().invoke(
        ["build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    )
    assert result.success, result.exception


def test_no_companies_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "source.duckdb"
    _seed_norway_brreg_source(db)
    _dbt_build(db, monkeypatch)

    with duckdb.connect(str(db), read_only=True) as conn:
        rows = conn.execute(
            "select org_number, name, name_normalized, is_active, primary_website_host "
            "from norway_resolved.no_companies order by org_number"
        ).fetchall()

    assert rows == [
        ("1000", "Active One AS", "active one as", True, "www.activeone.no"),
        ("2000", "Inactive Two AS", "inactive two as", False, None),
    ]


def test_no_websites_model_contains_root_domain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = tmp_path / "source.duckdb"
    _seed_norway_brreg_source(db)
    _dbt_build(db, monkeypatch)

    with duckdb.connect(str(db), read_only=True) as conn:
        rows = conn.execute(
            "select org_number, website_normalized_url, website_host, root_domain, "
            "is_current, is_primary "
            "from norway_resolved.no_websites order by org_number"
        ).fetchall()

    assert rows == [
        (
            "1000",
            "https://www.activeone.no/about",
            "www.activeone.no",
            "activeone.no",
            True,
            True,
        )
    ]


def test_no_industries_model_splits_nace_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = tmp_path / "source.duckdb"
    _seed_norway_brreg_source(db)
    _dbt_build(db, monkeypatch)

    with duckdb.connect(str(db), read_only=True) as conn:
        rows = conn.execute(
            "select org_number, source_industry_code, description_original, description_en, "
            "nace_normalized_code, is_primary "
            "from norway_resolved.no_industries order by org_number, source_industry_code"
        ).fetchall()

    assert rows == [
        (
            "1000",
            "62.010",
            "Programmeringstjenester",
            "Computer programming activities",
            "62010",
            True,
        ),
        (
            "1000",
            "70.100",
            "Hovedkontortjenester",
            "Activities of head offices",
            "70100",
            False,
        ),
    ]


def test_no_financial_statements_preserves_original_and_usd_amounts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = tmp_path / "source.duckdb"
    _seed_norway_brreg_source(db)
    _dbt_build(db, monkeypatch)

    with duckdb.connect(str(db), read_only=True) as conn:
        row = conn.execute(
            "select org_number, currency, operating_revenue_amount_original, "
            "operating_revenue_amount_usd, fx_rate_to_usd, fx_rate_date "
            "from norway_resolved.no_financial_statements"
        ).fetchone()

    assert row == (
        "1000",
        "NOK",
        Decimal("1000.000000"),
        Decimal("95.000000"),
        Decimal("0.095000000000"),
        date(2024, 12, 31),
    )
