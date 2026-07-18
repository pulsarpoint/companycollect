import dagster as dg

from dagster_v3.defs.norway_brreg_financial.assets import (
    ANNUAL_ACCOUNT_DUCKDB_PATH,
    norway_brreg_annual_account_documents_duckdb,
    norway_brreg_annual_account_documents_json,
    norway_brreg_annual_account_pdf_cleanup,
    norway_brreg_annual_account_pdfs,
    norway_brreg_annual_account_fact_mappings_duckdb,
    norway_brreg_annual_account_facts_clickhouse,
    norway_brreg_annual_account_facts_duckdb,
    norway_brreg_annual_account_facts_usd_duckdb,
    norway_brreg_annual_account_metrics_clickhouse,
    norway_brreg_annual_account_metrics_duckdb,
    norway_brreg_annual_account_reports_clickhouse,
    norway_brreg_financial_bootstrap_responses_json,
    norway_brreg_financial_bootstrap_responses_parquet,
    norway_brreg_financial_responses_updates_json,
    norway_brreg_financial_responses_updates_parquet,
    norway_brreg_financial_statements_snapshot_clickhouse,
    norway_brreg_financial_statements_snapshot_parquet,
    norway_brreg_financial_statements_snapshot_usd_parquet,
    norway_brreg_financial_statements_updates_clickhouse,
    norway_brreg_financial_statements_updates_parquet,
    norway_brreg_financial_statements_updates_usd_parquet,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
)


defs = dg.Definitions(
    assets=[
        norway_brreg_annual_account_pdfs,
        norway_brreg_annual_account_documents_json,
        norway_brreg_annual_account_pdf_cleanup,
        norway_brreg_annual_account_documents_duckdb,
        norway_brreg_annual_account_facts_duckdb,
        norway_brreg_annual_account_fact_mappings_duckdb,
        norway_brreg_annual_account_facts_usd_duckdb,
        norway_brreg_annual_account_metrics_duckdb,
        norway_brreg_annual_account_reports_clickhouse,
        norway_brreg_annual_account_facts_clickhouse,
        norway_brreg_annual_account_metrics_clickhouse,
        norway_brreg_financial_bootstrap_responses_json,
        norway_brreg_financial_bootstrap_responses_parquet,
        norway_brreg_financial_responses_updates_json,
        norway_brreg_financial_responses_updates_parquet,
        norway_brreg_financial_statements_snapshot_parquet,
        norway_brreg_financial_statements_updates_parquet,
        norway_brreg_financial_statements_snapshot_usd_parquet,
        norway_brreg_financial_statements_updates_usd_parquet,
        norway_brreg_financial_statements_snapshot_clickhouse,
        norway_brreg_financial_statements_updates_clickhouse,
    ],
    resources={
        "norway_brreg_financial_storage": NorwayBrregFinancialParquetStorageResource(),
        "norway_brreg_annual_accounts_duckdb": duckdb_resource(
            ANNUAL_ACCOUNT_DUCKDB_PATH
        ),
    },
)
