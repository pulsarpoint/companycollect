from dagster_v3.defs.common.partition_duckdb import (
    partition_duckdb_path as _partition_duckdb_path,
)

COUNTRY_CODE = "SK"
SOURCE_SLUG = "slovakia_uvo_procurement"
GROUP_NAME = "slovakia_uvo_procurement"

BULLETIN_URL = "https://www.uvo.gov.sk/vestnik-a-registre/vestnik"
DETAIL_URL_TEMPLATE = (
    "https://www.uvo.gov.sk/vestnik-a-registre/vestnik/oznamenie/detail/{notice_id}"
)
SOURCE_LICENCE = "Machine-reuse approval managed operationally"
PARTITION_START_DATE = "2024-01-01"

S3_BUCKET = "source-slovakia-uvo-procurement"
S3_ISSUE_PREFIX = "issues"
S3_DETAIL_PREFIX = "notices"
S3_MANIFEST_PREFIX = "manifests"

# Superseded by partition_duckdb_path: kept so an operator can still find
# the pre-2026-07-30 shared file this source used to write.
LEGACY_SHARED_DUCKDB_FILE_NAME = "slovakia_uvo_procurement_source.duckdb"
DUCKDB_SCHEMA = "slovakia_uvo_procurement"
DUCKDB_POOL = "slovakia_uvo_procurement_duckdb"
CANDIDATES_TABLE = "notice_winner_candidates"

CLICKHOUSE_DATABASE = "corpscout"
NOTICES_TABLE = "sk_uvo_procurement_notices"
QUALIFIED_NOTICES_TABLE = f"{CLICKHOUSE_DATABASE}.{NOTICES_TABLE}"

CANDIDATE_COLUMNS = (
    "country_code",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "uvo_notice_id",
    "bulletin_number",
    "bulletin_code",
    "publication_date",
    "procedure_id",
    "notice_version_id",
    "buyer_name",
    "buyer_ico",
    "title",
    "cpv_code",
    "lot_id",
    "lot_title",
    "winner_ordinal",
    "winner_name",
    "winner_id_raw",
    "winner_ico",
    "winner_country",
    "contract_conclusion_date",
    "awarded_amount_eur",
    "awarded_amount_usd",
    "awarded_currency",
    "lowest_tender_amount_eur",
    "highest_tender_amount_eur",
    "notice_value_amount_eur",
    "received_tenders",
    "directive_governed",
    "source_url",
    "source_object_key",
    "source_retrieved_at",
    "resolved_at",
    "partition_key",
    "match_eligibility",
)

NOTICES_COLUMNS = (
    "company_id",
    "company_match_status",
    *CANDIDATE_COLUMNS,
)


def partition_duckdb_path(partition: str):
    """This source's DuckDB file for one monthly partition.

    Per-partition rather than one shared file: see
    defs/common/partition_duckdb.py for the 36 months this shape cost
    Brazil's PNCP chain.
    """
    return _partition_duckdb_path(source="slovakia_uvo_procurement", partition=partition)
