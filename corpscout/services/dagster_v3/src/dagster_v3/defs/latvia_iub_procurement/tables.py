from dagster_v3.defs.common.partition_duckdb import (
    partition_duckdb_path as _partition_duckdb_path,
)

COUNTRY_CODE = "LV"
SOURCE_SLUG = "latvia_iub_procurement"
GROUP_NAME = "latvia_iub_procurement"

SOURCE_BASE_URL = "https://open.iub.gov.lv/data/notice"
CATALOG_URL = "https://www.iub.gov.lv/en/open-data"
SOURCE_LICENCE = "CC0 1.0"
PARTITION_START_DATE = "2023-10-01"

S3_BUCKET = "source-latvia-iub-procurement"
S3_NOTICE_PREFIX = "notice"
S3_MANIFEST_PREFIX = "manifests"

# Superseded by partition_duckdb_path: kept so an operator can still find
# the pre-2026-07-30 shared file this source used to write.
LEGACY_SHARED_DUCKDB_FILE_NAME = "latvia_iub_procurement_source.duckdb"
DUCKDB_SCHEMA = "latvia_iub_procurement"
DUCKDB_POOL = "latvia_iub_procurement_duckdb"

CLICKHOUSE_DATABASE = "corpscout"
NOTICES_TABLE = "lv_iub_notices"
LOTS_TABLE = "lv_iub_notice_lots"
WINNER_CANDIDATES_TABLE = "lv_iub_notice_winner_candidates"
WINNERS_TABLE = "lv_iub_notice_winners"
EXECUTIONS_TABLE = "lv_iub_contract_executions"

NOTICES_CURRENT_VIEW = "lv_iub_notices_current"
LOTS_CURRENT_VIEW = "lv_iub_notice_lots_current"
WINNERS_CURRENT_VIEW = "lv_iub_notice_winners_current"
EXECUTIONS_CURRENT_VIEW = "lv_iub_contract_executions_current"

QUALIFIED_WINNERS_TABLE = f"{CLICKHOUSE_DATABASE}.{WINNERS_TABLE}"

NOTICES_COLUMNS = (
    "country_code",
    "source_slug",
    "source_run_id",
    "notice_id",
    "cloned_from",
    "previous_identifier",
    "procedure_id",
    "form_type",
    "notice_type",
    "publication_date",
    "buyer_name",
    "buyer_regcode",
    "title",
    "cpv_code",
    "legal_basis",
    "directive_governed",
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
    "notice_id",
    "lot_id",
    "lot_sequence",
    "lot_title",
    "decision_date",
    "winner_selection_status",
    "estimated_value_amount_eur",
    "lowest_tender_amount_eur",
    "highest_tender_amount_eur",
    "received_tenders",
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
    "notice_id",
    "procedure_id",
    "lot_id",
    "contract_id",
    "winner_ordinal",
    "party_ordinal",
    "winner_name",
    "winner_id_raw",
    "winner_regcode",
    "winner_country",
    "is_natural_person",
    "tender_value_amount_eur",
    "tender_value_amount_usd",
    "tender_value_attributable",
    "contract_conclusion_date",
    "contract_title",
    "contract_url",
    "publication_date",
    "buyer_name",
    "buyer_regcode",
    "notice_title",
    "cpv_code",
    "legal_basis",
    "directive_governed",
    "source_url",
    "source_object_key",
    "source_retrieved_at",
    "resolved_at",
    "partition_key",
    "match_eligibility",
)

WINNERS_COLUMNS = (
    "company_id",
    "company_match_status",
    *WINNER_CANDIDATE_COLUMNS,
)

EXECUTIONS_COLUMNS = (
    "country_code",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "notice_id",
    "procedure_id",
    "contract_id",
    "winner_ordinal",
    "party_ordinal",
    "winner_name",
    "winner_id_raw",
    "winner_regcode",
    "winner_country",
    "is_natural_person",
    "tender_value_amount_eur",
    "contract_conclusion_date",
    "actual_end_date",
    "contract_title",
    "publication_date",
    "buyer_name",
    "buyer_regcode",
    "notice_title",
    "source_url",
    "source_object_key",
    "source_retrieved_at",
    "resolved_at",
    "partition_key",
)


def partition_duckdb_path(partition: str):
    """This source's DuckDB file for one monthly partition.

    Per-partition rather than one shared file: see
    defs/common/partition_duckdb.py for the 36 months this shape cost
    Brazil's PNCP chain.
    """
    return _partition_duckdb_path(source="latvia_iub_procurement", partition=partition)
