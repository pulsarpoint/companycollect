CLICKHOUSE_DATABASE = "corpscout"
GROUP_NAME = "company_identifier"

COMPANY_IDENTIFIER_TABLE = "company_identifier"
QUALIFIED_COMPANY_IDENTIFIER_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{COMPANY_IDENTIFIER_TABLE}"
)

LEI_ISSUER_SCHEME = "lei"

COMPANY_IDENTIFIER_COLUMNS = (
    "issuer_scheme",
    "issuer_id",
    "country_code",
    "company_id",
    "match_method",
    "match_confidence",
    "registration_authority_id",
    "registered_as_raw",
    "company_id_normalized",
    "entity_status",
    "registration_status",
    "is_current",
    "successor_issuer_id",
    "first_seen_date",
    "last_seen_date",
    "source_run_id",
    "resolved_at",
)
