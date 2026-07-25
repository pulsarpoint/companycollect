CLICKHOUSE_DATABASE = "corpscout"
GROUP_NAME = "company_signals"

GOVERNMENT_CONTRACT_EVIDENCE_TABLE = "company_government_contract_evidence"
GOVERNMENT_CONTRACT_SUMMARY_TABLE = "company_government_contract_summary"
SIGNAL_COVERAGE_TABLE = "company_signal_coverage"

GOVERNMENT_CONTRACT_EVIDENCE_COLUMNS = (
    "country_code",
    "company_id",
    "evidence_id",
    "source_slugs",
    "source_references",
    "source_urls",
    "publication_date",
    "buyer_name",
    "title",
    "agreement_type",
    "source_updated_at",
    "resolved_at",
)

GOVERNMENT_CONTRACT_SUMMARY_COLUMNS = (
    "country_code",
    "company_id",
    "public_award_count",
    "public_award_last_date",
    "source_slugs",
    "source_updated_at",
    "resolved_at",
)

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
