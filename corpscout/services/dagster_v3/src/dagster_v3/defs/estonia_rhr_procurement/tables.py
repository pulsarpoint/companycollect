COUNTRY_CODE = "EE"
SOURCE_SLUG = "estonia_rhr_procurement"
GROUP_NAME = "estonia_rhr_procurement"

SOURCE_HOMEPAGE = "https://riigihanked.riik.ee/rhr-web/#/open-data"
SOURCE_API_ROOT = "https://riigihanked.riik.ee/rhr/api/public/v1/opendata"
SOURCE_LICENCE = "CC BY-SA 3.0 EE"
PARTITION_START_DATE = "2024-01-01"

S3_BUCKET = "source-estonia-rhr-procurement"
S3_AWARDS_PREFIX = "contract-award-notices"
S3_TED_INDEX_PREFIX = "ted-index"
S3_MANIFEST_PREFIX = "manifests"

DUCKDB_FILE_NAME = "estonia_rhr_procurement_source.duckdb"
DUCKDB_SCHEMA = "estonia_rhr_procurement"
DUCKDB_POOL = "estonia_rhr_procurement_duckdb"

CLICKHOUSE_DATABASE = "corpscout"
NOTICES_TABLE = "ee_rhr_procurement_notices"
LOTS_TABLE = "ee_rhr_procurement_lots"
WINNER_CANDIDATES_TABLE = "ee_rhr_procurement_winner_candidates"
WINNERS_TABLE = "ee_rhr_procurement_winners"

NOTICES_CURRENT_VIEW = "ee_rhr_procurement_notices_current"
LOTS_CURRENT_VIEW = "ee_rhr_procurement_lots_current"
WINNERS_CURRENT_VIEW = "ee_rhr_procurement_winners_current"

NOTICES_COLUMNS = (
    "country_code",
    "source_slug",
    "source_run_id",
    "notice_version_id",
    "notice_id",
    "version_id",
    "changed_notice_version_id",
    "procedure_id",
    "notice_type",
    "notice_subtype",
    "publication_date",
    "buyer_name",
    "buyer_id_raw",
    "buyer_reg_code",
    "title",
    "cpv_code",
    "ted_publication_number",
    "ted_publication_date",
    "directive_governed",
    "total_value_amount_original",
    "total_value_currency",
    "estimated_value_amount_original",
    "estimated_value_currency",
    "framework_maximum_amount_original",
    "framework_maximum_currency",
    "framework_total_maximum_amount_original",
    "framework_total_maximum_currency",
    "framework_total_approximate_amount_original",
    "framework_total_approximate_currency",
    "source_url",
    "source_object_key",
    "source_retrieved_at",
    "resolved_at",
    "partition_key",
)

LOTS_COLUMNS = (
    "country_code",
    "source_slug",
    "source_run_id",
    "notice_version_id",
    "lot_id",
    "lot_title",
    "estimated_value_amount_original",
    "estimated_value_currency",
    "framework_maximum_amount_original",
    "framework_maximum_currency",
    "framework_value_maximum_amount_original",
    "framework_value_maximum_currency",
    "framework_value_reestimated_amount_original",
    "framework_value_reestimated_currency",
    "lower_tender_amount_original",
    "lower_tender_currency",
    "higher_tender_amount_original",
    "higher_tender_currency",
    "settled_contract_count",
    "settled_contracts_json",
    "publication_date",
    "source_object_key",
    "resolved_at",
    "partition_key",
)

WINNER_CANDIDATE_COLUMNS = (
    "country_code",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "notice_version_id",
    "notice_id",
    "procedure_id",
    "lot_id",
    "tender_id",
    "winner_ordinal",
    "winner_name",
    "winner_id_raw",
    "winner_reg_code",
    "winner_country",
    "awarded_amount_original",
    "awarded_amount_eur",
    "awarded_amount_usd",
    "awarded_currency",
    "subcontracting_amount_original",
    "subcontracting_amount_eur",
    "subcontracting_amount_usd",
    "subcontracting_currency",
    "awarded_value_attributable",
    "publication_date",
    "source_object_key",
    "resolved_at",
    "partition_key",
    "match_eligibility",
)

WINNERS_COLUMNS = (
    "company_id",
    "company_match_status",
    *WINNER_CANDIDATE_COLUMNS,
)

QUALIFIED_WINNERS_TABLE = f"{CLICKHOUSE_DATABASE}.{WINNERS_TABLE}"
