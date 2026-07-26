CLICKHOUSE_DATABASE = "corpscout"
GROUP_NAME = "company_signals"

# Contracts themselves are views owned by the migration -- one per country plus
# a cross-country merge -- so there is no evidence or summary table to write.
# Coverage is the exception: it carries editorial prose about what a country's
# sources miss, which no query over the rows can derive.
SIGNAL_COVERAGE_TABLE = "company_signal_coverage"
COUNTRY_CONTRACTS_VIEW_SUFFIX = "_government_contracts"
COMPANY_CONTRACTS_VIEW = "company_government_contracts"

SIGNAL_COVERAGE_COLUMNS = (
    "country_code",
    "signal_name",
    "coverage_status",
    "coverage_from",
    "coverage_to",
    "source_slugs",
    "source_updated_at",
    "resolved_at",
    "caveat",
)
