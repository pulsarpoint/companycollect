"""Contracts for the ESEF filings source (filings.xbrl.org).

Column orders below are load-bearing against migration
000149_corpscout_esef_filings.up.sql — each *_EXPORT_COLUMNS tuple must match
that migration's column order exactly, minus `resolved_at` (ClickHouse
defaults it via `DEFAULT now64(3)`). See tests/test_esef_filings_client.py for
the contract test that greps the migration file and asserts this.

`esef_financial_metrics` (the fourth table in the migration) is the derived
IFRS metrics table built entirely in ClickHouse by
`esef_filings/metrics.py` (Task 6) -- see that module for the mapping and
selection rules.
"""

SOURCE_SLUG = "esef_filings"
DLT_DATASET_NAME = "esef_filings"

# DuckDB (esef_filings.*) staging table names -- shared by assets.py (which
# builds them) and publish.py (which reads them for the ClickHouse export).
FILINGS_INDEX_TABLE = "filings_index"
FACTS_TABLE = "facts"
QUALIFIED_FILINGS_INDEX_TABLE = f"{DLT_DATASET_NAME}.{FILINGS_INDEX_TABLE}"
QUALIFIED_FACTS_TABLE = f"{DLT_DATASET_NAME}.{FACTS_TABLE}"

ESEF_DATABASE = "corpscout"
ESEF_FILINGS_TABLE = "esef_filings"
ESEF_FACTS_TABLE = "esef_facts"
ESEF_ENTITY_REGISTRY_MAP_TABLE = "esef_entity_registry_map"
ESEF_FINANCIAL_METRICS_TABLE = "esef_financial_metrics"

QUALIFIED_ESEF_FILINGS_TABLE = f"{ESEF_DATABASE}.{ESEF_FILINGS_TABLE}"
QUALIFIED_ESEF_FACTS_TABLE = f"{ESEF_DATABASE}.{ESEF_FACTS_TABLE}"
QUALIFIED_ESEF_ENTITY_REGISTRY_MAP_TABLE = (
    f"{ESEF_DATABASE}.{ESEF_ENTITY_REGISTRY_MAP_TABLE}"
)
QUALIFIED_ESEF_FINANCIAL_METRICS_TABLE = (
    f"{ESEF_DATABASE}.{ESEF_FINANCIAL_METRICS_TABLE}"
)

# corpscout.esef_filings column order minus resolved_at (CH default).
ESEF_FILINGS_EXPORT_COLUMNS = (
    "lei",
    "entity_name",
    "fxo_id",
    "country",
    "period_end",
    "date_added",
    "processed_at",
    "json_url",
    "package_url",
    "report_url",
    "viewer_url",
    "package_sha256",
    "error_count",
    "warning_count",
    "inconsistency_count",
    "has_json_facts",
    "source_url",
    "source_run_id",
)

# corpscout.esef_facts column order minus resolved_at (CH default).
ESEF_FACTS_EXPORT_COLUMNS = (
    "lei",
    "fxo_id",
    "period_end",
    "fact_id",
    "concept_qname",
    "concept_namespace",
    "concept_local_name",
    "period_start",
    "period_instant",
    "period_duration_end",
    "unit",
    "currency",
    "value_kind",
    "raw_value",
    "amount_original",
    "decimals",
    "dimensions",
    "language",
    "source_run_id",
)

# corpscout.esef_entity_registry_map column order minus resolved_at (CH default).
ESEF_ENTITY_MAP_EXPORT_COLUMNS = (
    "lei",
    "country_iso2",
    "registry_id_raw",
    "registry_id",
    "match_source",
    "source_run_id",
)

# corpscout.esef_financial_metrics column order minus resolved_at (CH default).
# Derived entirely in ClickHouse by esef_filings/metrics.py (Task 6) -- see
# that module for the mapping/selection rules this column order feeds.
ESEF_FINANCIAL_METRICS_EXPORT_COLUMNS = (
    "lei",
    "entity_name",
    "fxo_id",
    "country",
    "scope",
    "fiscal_year",
    "period_start",
    "period_end",
    "currency",
    "revenue_amount_original",
    "revenue_amount_usd",
    "operating_profit_amount_original",
    "operating_profit_amount_usd",
    "profit_loss_amount_original",
    "profit_loss_amount_usd",
    "total_assets_amount_original",
    "total_assets_amount_usd",
    "equity_amount_original",
    "equity_amount_usd",
    "liabilities_amount_original",
    "liabilities_amount_usd",
    "cash_amount_original",
    "cash_amount_usd",
    "employees",
    "mapped_fact_count",
    "source_fact_count",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "viewer_url",
    "source_run_id",
)
