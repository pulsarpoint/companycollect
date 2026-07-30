"""Contracts for the Norway Doffin procurement source.

See docs/norway_doffin-design.md. Two facts from there shape everything here:

* The search endpoint returns at most **1,000** results per query, so the ingest
  is sliced by ``issueDate`` -- the only date the API filters on. Publication
  date is not filterable, so partitions key on issue date.
* The search endpoint publishes **no realized value**, only ``estimatedValue``.
  The money lives in the eForms XML behind ``/v2/download/{doffinId}``, which is
  why this is a two-stage ingest rather than one.
"""

from __future__ import annotations

from dagster_v3.defs.common.partition_duckdb import (
    partition_duckdb_path as _partition_duckdb_path,
)

COUNTRY_CODE = "NO"
SOURCE_SLUG = "norway_doffin_procurement"
GROUP_NAME = "norway_doffin"

API_BASE_URL = "https://api.doffin.no/public"
SEARCH_PATH = "/v2/search"
DOWNLOAD_PATH_TEMPLATE = "/v2/download/{doffin_id}"
# The human-facing address for a notice, for source_url.
NOTICE_URL_TEMPLATE = "https://doffin.no/notices/{doffin_id}"

# The subscription key header. Primary is used; the secondary exists so one can
# be rotated while the other serves, not as a pair to load-balance across.
API_KEY_HEADER = "Ocp-Apim-Subscription-Key"
API_KEY_ENV = "DOF_NOTICE_PRIM_API_KEY"

# Measured 2026-07-27. 100 is the documented maximum; the 20 in the API's own
# example is just an example.
MAX_HITS_PER_PAGE = 100
# numHitsAccessible = min(numHitsTotal, 1000) on every query. The busiest month
# measured is 378 award notices, so monthly slices sit well under it -- but the
# ingest asserts rather than assumes (see resources.assert_under_result_ceiling).
RESULT_CEILING = 1000

# ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT is the award notice: 31,097 of 157,055.
# RESULT is NOT a tighter filter, it is a wider one (42,636) that sweeps in the
# 8,276 cancelled notices -- the design doc guessed the opposite before measuring.
AWARD_NOTICE_TYPE = "ANNOUNCEMENT_OF_CONCLUSION_OF_CONTRACT"

# Nothing exists before 2018: 2006..2013 all return zero.
PARTITION_START_DATE = "2018-01-01"

S3_BUCKET = "source-norway-doffin"
S3_SEARCH_PREFIX = "search"
S3_NOTICE_PREFIX = "notices"

# Superseded by partition_duckdb_path: kept only so an operator can find the
# pre-2026-07-30 shared file that this source used to write.
LEGACY_SHARED_DUCKDB_FILE_NAME = "norway_doffin_source.duckdb"
DUCKDB_SCHEMA = "norway_doffin"
DUCKDB_POOL = "norway_doffin_duckdb"
RAW_NOTICES_TABLE = "raw_notices"
CANDIDATES_TABLE = "notice_winner_candidates"

CLICKHOUSE_DATABASE = "corpscout"
NOTICES_TABLE = "no_doffin_notices"
QUALIFIED_NOTICES_TABLE = f"{CLICKHOUSE_DATABASE}.{NOTICES_TABLE}"
CONTRACTS_VIEW = "no_government_contracts"

# Every monetary figure Doffin publishes, as (metric, what it actually is).
# Each becomes <metric>_amount_original + <metric>_amount_usd + <metric>_currency,
# per the currency guideline: convert all of them, not just the one the view
# reads, or the rest are answerable in NOK alone.
#
# They stay in separate columns and are never coalesced. An estimate and a
# realized award are different claims, and notice 2026-109693 shows why it
# matters: it estimates 2,500,000 against a realized 1,485,571.
VALUE_METRICS: tuple[tuple[str, str], ...] = (
    # BT-720, the per-winner realized amount. This is THE contract value, and it
    # is the reason the XML has to be fetched at all.
    ("value", "BT-720 tender value, realized, per winner"),
    # BT-161, the notice's total realized value. Repeated across the notice's
    # rows, as TED's total_value is.
    ("notice_value", "BT-161 notice value, realized, per notice"),
    # BT-27, an ESTIMATE. Never merged into either of the above.
    ("estimated_value", "BT-27 estimated value, per notice"),
)


def value_columns() -> tuple[str, ...]:
    """The three columns each monetary metric contributes, in schema order."""
    return tuple(
        column
        for metric, _ in VALUE_METRICS
        for column in (
            f"{metric}_amount_original",
            f"{metric}_amount_usd",
            f"{metric}_currency",
        )
    )


# Grain: one row per (notice, lot, winner) -- the same shape as TED and Hilma.
CANDIDATE_COLUMNS: tuple[str, ...] = (
    "country_code",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_url",
    "doffin_id",
    "contract_folder_id",
    "notice_type",
    "notice_status",
    "issue_date",
    "publication_date",
    "deadline_date",
    "buyer_name",
    "buyer_org_number",
    "notice_title",
    "notice_description",
    "cpv_codes",
    "location_ids",
    "lot_id",
    "lot_heading",
    "winner_ordinal",
    "winner_name",
    "winner_org_number_raw",
    "winner_org_number",
    "winner_country",
    *value_columns(),
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "directive_governed",
    "received_tenders",
    "award_result",
    "partition_key",
    "source_retrieved_at",
    "resolved_at",
)

# The export resolves the winner against no_companies and adds the match status.
NOTICES_COLUMNS: tuple[str, ...] = (
    "company_id",
    "company_match_status",
    *CANDIDATE_COLUMNS,
)


def partition_duckdb_path(partition: str):
    """This source's DuckDB file for one monthly partition.

    Per-partition rather than one shared file: see
    defs/common/partition_duckdb.py for the 36 months this shape cost Brazil.
    """
    return _partition_duckdb_path(source="norway_doffin", partition=partition)
