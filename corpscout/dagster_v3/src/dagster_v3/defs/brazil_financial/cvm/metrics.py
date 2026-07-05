from collections.abc import Callable
from typing import Any

from dagster_v3.defs.brazil_financial.cvm.itr_parsing import ITR_STATEMENT_ROWS_TABLE
from dagster_v3.defs.brazil_financial.cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    DFP_STATEMENT_ROWS_TABLE,
)

FINANCIAL_METRICS_TABLE = "financial_metrics"
FINANCIAL_METRICS_SOURCE_SLUG = "brazil_cvm_financial_metrics"
FINANCIAL_METRICS_MAPPING_VERSION = "br-cvm-financial-metrics-v1"


def build_brazil_fin_cvm_financial_metrics(
    *,
    duckdb_connection: Any,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build canonical Brazil CVM financial metrics from converted DFP and ITR rows."""
    duckdb_connection.execute(f"create schema if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}")
    duckdb_connection.execute(
        f"""
        create or replace table {BRAZIL_CVM_DUCKDB_SCHEMA}.{FINANCIAL_METRICS_TABLE} as
        with statement_rows as (
            select
                'DFP' as source_dataset,
                dfp_year as source_year,
                'annual' as period_type,
                *
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE}
            union all
            select
                'ITR' as source_dataset,
                itr_year as source_year,
                'interim' as period_type,
                *
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{ITR_STATEMENT_ROWS_TABLE}
        ),
        direct_mapping as (
            select *
            from (
                values
                    ('revenue', 'Revenue', 'DRE', '3.01', 1),
                    ('net_income', 'Net income', 'DRE', '3.11', 1),
                    ('total_assets', 'Total assets', 'BPA', '1', 1),
                    ('equity', 'Equity', 'BPP', '2.03', 1),
                    (
                        'cash_and_equivalents',
                        'Cash and equivalents',
                        'BPA',
                        '1.01.01',
                        1
                    ),
                    (
                        'operating_cash_flow',
                        'Operating cash flow',
                        'DFC_MI',
                        '6.01',
                        1
                    ),
                    (
                        'operating_cash_flow',
                        'Operating cash flow',
                        'DFC_MD',
                        '6.01',
                        2
                    )
            ) as mapping(
                metric_name,
                metric_label,
                statement_code,
                account_code,
                mapping_priority
            )
        ),
        direct_metric_rows as (
            select
                source_dataset,
                source_year,
                country_iso2,
                cnpj,
                cnpj_basico,
                company_name,
                cvm_code,
                reference_date,
                period_start_date,
                period_end_date,
                period_type,
                version,
                consolidation_type,
                mapping.metric_name,
                mapping.metric_label,
                currency,
                cast(amount_original as decimal(38, 6)) as amount_original,
                cast(amount_usd as decimal(38, 6)) as amount_usd,
                cast(fx_rate_to_usd as decimal(38, 12)) as fx_rate_to_usd,
                fx_rate_date,
                fx_source,
                statement_code as source_statement_code,
                statement_name as source_statement_name,
                account_code as source_account_codes,
                account_description_original as source_account_descriptions_original,
                source_run_id as source_statement_run_ids,
                source_record_id as source_statement_record_ids,
                source_archive_key as source_archive_keys,
                source_file_name as source_file_names,
                1::ubigint as source_statement_row_count,
                source_row_number,
                mapping.mapping_priority
            from statement_rows
            join direct_mapping as mapping using (statement_code, account_code)
            where amount_original is not null
              and currency is not null
              and currency <> ''
            qualify row_number() over (
                partition by
                    source_dataset,
                    cnpj,
                    cvm_code,
                    reference_date,
                    period_start_date,
                    period_end_date,
                    version,
                    consolidation_type,
                    mapping.metric_name
                order by mapping.mapping_priority, source_row_number
            ) = 1
        ),
        liability_metric_rows as (
            select
                source_dataset,
                source_year,
                any_value(country_iso2) as country_iso2,
                cnpj,
                any_value(cnpj_basico) as cnpj_basico,
                any_value(company_name) as company_name,
                cvm_code,
                reference_date,
                period_start_date,
                period_end_date,
                period_type,
                version,
                consolidation_type,
                'total_liabilities' as metric_name,
                'Total liabilities' as metric_label,
                any_value(currency) as currency,
                cast(sum(amount_original) as decimal(38, 6)) as amount_original,
                cast(sum(amount_usd) as decimal(38, 6)) as amount_usd,
                case
                    when sum(amount_original) is not null
                     and sum(amount_original) <> 0
                     and sum(amount_usd) is not null
                    then cast(sum(amount_usd) / sum(amount_original) as decimal(38, 12))
                    else cast(max(fx_rate_to_usd) as decimal(38, 12))
                end as fx_rate_to_usd,
                max(fx_rate_date) as fx_rate_date,
                any_value(fx_source) as fx_source,
                'BPP' as source_statement_code,
                any_value(statement_name) as source_statement_name,
                string_agg(account_code, '|' order by account_code) as source_account_codes,
                string_agg(
                    account_description_original,
                    '|'
                    order by account_code
                ) as source_account_descriptions_original,
                string_agg(source_run_id, '|' order by account_code) as source_statement_run_ids,
                string_agg(source_record_id, '|' order by account_code) as source_statement_record_ids,
                string_agg(source_archive_key, '|' order by account_code) as source_archive_keys,
                string_agg(source_file_name, '|' order by account_code) as source_file_names,
                count(*)::ubigint as source_statement_row_count,
                min(source_row_number) as source_row_number,
                1 as mapping_priority
            from statement_rows
            where statement_code = 'BPP'
              and account_code in ('2.01', '2.02')
              and amount_original is not null
              and currency is not null
              and currency <> ''
            group by
                source_dataset,
                source_year,
                cnpj,
                cvm_code,
                reference_date,
                period_start_date,
                period_end_date,
                period_type,
                version,
                consolidation_type
        ),
        candidate_metric_rows as (
            select * from direct_metric_rows
            union all
            select * from liability_metric_rows
        ),
        latest_flagged as (
            select
                *,
                version = max(version) over (
                    partition by
                        source_dataset,
                        cnpj,
                        cvm_code,
                        reference_date,
                        period_start_date,
                        period_end_date,
                        consolidation_type,
                        metric_name
                ) as is_latest_version
            from candidate_metric_rows
        )
        select
            country_iso2,
            '{FINANCIAL_METRICS_SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            source_dataset
                || ':'
                || regexp_replace(cnpj, '[^0-9]', '', 'g')
                || ':'
                || cast(reference_date as varchar)
                || ':'
                || coalesce(cast(period_start_date as varchar), '')
                || ':'
                || coalesce(cast(period_end_date as varchar), '')
                || ':'
                || consolidation_type
                || ':'
                || metric_name
                || ':'
                || cast(version as varchar) as source_record_id,
            source_dataset,
            cast(source_year as integer) as source_year,
            cnpj,
            cnpj_basico,
            company_name,
            cvm_code,
            reference_date,
            period_start_date,
            period_end_date,
            period_type,
            cast(version as integer) as version,
            is_latest_version,
            consolidation_type,
            metric_name,
            metric_label,
            upper(currency) as currency,
            amount_original,
            amount_usd,
            fx_rate_to_usd,
            fx_rate_date,
            fx_source,
            source_statement_code,
            source_statement_name,
            source_account_codes,
            source_account_descriptions_original,
            source_statement_run_ids,
            source_statement_record_ids,
            source_archive_keys,
            source_file_names,
            source_statement_row_count,
            '{FINANCIAL_METRICS_MAPPING_VERSION}' as metric_mapping_version,
            cast(now() as timestamp) as resolved_at
        from latest_flagged
        """,
        [source_run_id],
    )
    metrics = int(
        duckdb_connection.execute(
            f"select count(*) from {BRAZIL_CVM_DUCKDB_SCHEMA}.{FINANCIAL_METRICS_TABLE}"
        ).fetchone()[0]
    )
    if log is not None:
        log("Built Brazil CVM financial metrics: metrics=%s", metrics)
    return {"metrics": metrics}
