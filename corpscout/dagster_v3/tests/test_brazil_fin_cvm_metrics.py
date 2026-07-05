from decimal import Decimal

import duckdb

from dagster_v3.defs.brazil_financial.cvm.parsing import BRAZIL_CVM_DUCKDB_SCHEMA


def test_build_brazil_fin_cvm_financial_metrics_from_dfp_and_itr() -> None:
    from dagster_v3.defs.brazil_financial.cvm.metrics import (
        FINANCIAL_METRICS_TABLE,
        build_brazil_fin_cvm_financial_metrics,
    )

    connection = duckdb.connect(":memory:")
    _create_statement_rows_table(connection, "dfp_statement_rows", "dfp_year")
    _create_statement_rows_table(connection, "itr_statement_rows", "itr_year")
    _insert_statement_row(
        connection,
        "dfp_statement_rows",
        year_column="dfp_year",
        source_record_id="dfp-v1-assets",
        source_run_id="dfp-run",
        year=2025,
        reference_date="2025-12-31",
        version=1,
        statement_code="BPA",
        statement_name="Balance Sheet Assets",
        account_code="1",
        account_description_original="Ativo Total",
        amount_original="1000",
        amount_usd="200",
        source_row_number=1,
    )
    _insert_statement_row(
        connection,
        "dfp_statement_rows",
        year_column="dfp_year",
        source_record_id="dfp-v2-assets",
        source_run_id="dfp-run",
        year=2025,
        reference_date="2025-12-31",
        version=2,
        statement_code="BPA",
        statement_name="Balance Sheet Assets",
        account_code="1",
        account_description_original="Ativo Total",
        amount_original="1100",
        amount_usd="220",
        source_row_number=2,
    )
    _insert_statement_row(
        connection,
        "dfp_statement_rows",
        year_column="dfp_year",
        source_record_id="dfp-v2-current-liabilities",
        source_run_id="dfp-run",
        year=2025,
        reference_date="2025-12-31",
        version=2,
        statement_code="BPP",
        statement_name="Balance Sheet Liabilities",
        account_code="2.01",
        account_description_original="Passivo Circulante",
        amount_original="300",
        amount_usd="60",
        source_row_number=3,
    )
    _insert_statement_row(
        connection,
        "dfp_statement_rows",
        year_column="dfp_year",
        source_record_id="dfp-v2-non-current-liabilities",
        source_run_id="dfp-run",
        year=2025,
        reference_date="2025-12-31",
        version=2,
        statement_code="BPP",
        statement_name="Balance Sheet Liabilities",
        account_code="2.02",
        account_description_original="Passivo Nao Circulante",
        amount_original="200",
        amount_usd="40",
        source_row_number=4,
    )
    _insert_statement_row(
        connection,
        "dfp_statement_rows",
        year_column="dfp_year",
        source_record_id="dfp-v2-revenue",
        source_run_id="dfp-run",
        year=2025,
        reference_date="2025-12-31",
        version=2,
        statement_code="DRE",
        statement_name="Income Statement",
        account_code="3.01",
        account_description_original="Receita de Venda de Bens e/ou Servicos",
        amount_original="700",
        amount_usd="140",
        source_row_number=5,
    )
    _insert_statement_row(
        connection,
        "dfp_statement_rows",
        year_column="dfp_year",
        source_record_id="dfp-v2-cash",
        source_run_id="dfp-run",
        year=2025,
        reference_date="2025-12-31",
        version=2,
        statement_code="BPA",
        statement_name="Balance Sheet Assets",
        account_code="1.01.01",
        account_description_original="Caixa e Equivalentes de Caixa",
        amount_original="90",
        amount_usd="18",
        source_row_number=6,
    )
    _insert_statement_row(
        connection,
        "itr_statement_rows",
        year_column="itr_year",
        source_record_id="itr-v1-net-income",
        source_run_id="itr-run",
        year=2026,
        reference_date="2026-03-31",
        version=1,
        statement_code="DRE",
        statement_name="Income Statement",
        account_code="3.11",
        account_description_original="Lucro/Prejuizo Consolidado do Periodo",
        amount_original="70",
        amount_usd="14",
        source_row_number=7,
    )

    counts = build_brazil_fin_cvm_financial_metrics(
        duckdb_connection=connection,
        source_run_id="metrics-run",
    )

    assert counts == {"metrics": 6}
    rows = connection.execute(
        f"""
        select source_dataset, source_year, source_record_id, metric_name,
               period_type, version, is_latest_version, source_statement_code,
               source_account_codes, source_statement_record_ids,
               source_statement_row_count,
               amount_original, amount_usd, fx_rate_to_usd, metric_mapping_version
        from {BRAZIL_CVM_DUCKDB_SCHEMA}.{FINANCIAL_METRICS_TABLE}
        order by source_dataset, metric_name, version
        """
    ).fetchall()

    assert (
        "DFP",
        2025,
        "DFP:00000000000191:2025-12-31:2025-01-01:2025-12-31:CON:total_assets:1",
        "total_assets",
        "annual",
        1,
        False,
        "BPA",
        "1",
        "dfp-v1-assets",
        1,
        Decimal("1000.000000"),
        Decimal("200.000000"),
        Decimal("0.200000000000"),
        "br-cvm-financial-metrics-v1",
    ) in rows
    assert (
        "DFP",
        2025,
        "DFP:00000000000191:2025-12-31:2025-01-01:2025-12-31:CON:total_liabilities:2",
        "total_liabilities",
        "annual",
        2,
        True,
        "BPP",
        "2.01|2.02",
        "dfp-v2-current-liabilities|dfp-v2-non-current-liabilities",
        2,
        Decimal("500.000000"),
        Decimal("100.000000"),
        Decimal("0.200000000000"),
        "br-cvm-financial-metrics-v1",
    ) in rows
    assert (
        "ITR",
        2026,
        "ITR:00000000000191:2026-03-31:2026-01-01:2026-03-31:CON:net_income:1",
        "net_income",
        "interim",
        1,
        True,
        "DRE",
        "3.11",
        "itr-v1-net-income",
        1,
        Decimal("70.000000"),
        Decimal("14.000000"),
        Decimal("0.200000000000"),
        "br-cvm-financial-metrics-v1",
    ) in rows


def _create_statement_rows_table(
    connection: duckdb.DuckDBPyConnection, table_name: str, year_column: str
) -> None:
    connection.execute(f"create schema if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table {BRAZIL_CVM_DUCKDB_SCHEMA}.{table_name} (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            {year_column} integer,
            cnpj varchar,
            cnpj_basico varchar,
            company_name varchar,
            cvm_code varchar,
            reference_date date,
            version integer,
            statement_code varchar,
            statement_name varchar,
            consolidation_type varchar,
            grupo_dfp varchar,
            currency varchar,
            scale varchar,
            original_order varchar,
            period_start_date date,
            period_end_date date,
            equity_column varchar,
            account_code varchar,
            account_description_original varchar,
            amount_original decimal(38, 6),
            amount_usd decimal(38, 6),
            fx_rate_to_usd decimal(38, 12),
            fx_rate_date date,
            fx_source varchar,
            fixed_account_flag varchar,
            source_archive_key varchar,
            source_file_name varchar,
            source_row_number integer,
            resolved_at timestamp
        )
        """
    )


def _insert_statement_row(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    *,
    year_column: str,
    source_record_id: str,
    source_run_id: str,
    year: int,
    reference_date: str,
    version: int,
    statement_code: str,
    statement_name: str,
    account_code: str,
    account_description_original: str,
    amount_original: str,
    amount_usd: str,
    source_row_number: int,
) -> None:
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{table_name} by name
        select
            'BR' as country_iso2,
            'brazil_cvm_{table_name[:3]}' as source_slug,
            ? as source_run_id,
            ? as source_record_id,
            ? as {year_column},
            '00.000.000/0001-91' as cnpj,
            '00000000' as cnpj_basico,
            'Example SA' as company_name,
            '12345' as cvm_code,
            cast(? as date) as reference_date,
            ? as version,
            ? as statement_code,
            ? as statement_name,
            'CON' as consolidation_type,
            '' as grupo_dfp,
            'BRL' as currency,
            'MIL' as scale,
            '' as original_order,
            cast(? as date) as period_start_date,
            cast(? as date) as period_end_date,
            '' as equity_column,
            ? as account_code,
            ? as account_description_original,
            cast(? as decimal(38, 6)) as amount_original,
            cast(? as decimal(38, 6)) as amount_usd,
            cast(0.2 as decimal(38, 12)) as fx_rate_to_usd,
            cast(? as date) as fx_rate_date,
            'test-fx' as fx_source,
            'S' as fixed_account_flag,
            'archive.zip' as source_archive_key,
            'statement.csv' as source_file_name,
            ? as source_row_number,
            cast('2026-07-05 00:00:00' as timestamp) as resolved_at
        """,
        [
            source_run_id,
            source_record_id,
            year,
            reference_date,
            version,
            statement_code,
            statement_name,
            "2025-01-01" if table_name == "dfp_statement_rows" else "2026-01-01",
            reference_date,
            account_code,
            account_description_original,
            amount_original,
            amount_usd,
            reference_date,
            source_row_number,
        ],
    )
