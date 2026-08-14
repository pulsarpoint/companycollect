from datetime import datetime
from typing import Any

from dagster_v3.defs.sweden_company import address_canonicalization, shared_addresses

COMPANY_ADDRESSES_SERVING_TABLE = "se_company_addresses_serving_current"
QUALIFIED_DUCKDB_COMPANY_ADDRESSES_SERVING_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{COMPANY_ADDRESSES_SERVING_TABLE}"
)
QUALIFIED_CLICKHOUSE_COMPANY_ADDRESSES_SERVING_TABLE = (
    f"{address_canonicalization.CLICKHOUSE_DATABASE}.{COMPANY_ADDRESSES_SERVING_TABLE}"
)

COMPANY_ADDRESSES_SERVING_COLUMNS = (
    "company_id",
    "address_id",
    "canonical_address_key",
    "address_types",
    "address_sources",
    "link_evidence_count",
    "link_first_observed_at",
    "link_last_observed_at",
    "review_status",
    "reviewed_at",
    "reviewed_by",
    "review_note",
    "address_identity_run_id",
    "address_identity_built_at",
    "serving_run_id",
    "served_at",
)


def replace_sweden_company_addresses_serving(
    *,
    connection: Any,
    serving_run_id: str,
    served_at: datetime,
) -> dict[str, int]:
    """Build the company-keyed relationship index from shared address links."""
    connection.execute("begin transaction")
    try:
        connection.execute(
            f"""
            create or replace table {
                QUALIFIED_DUCKDB_COMPANY_ADDRESSES_SERVING_TABLE
            } as
            select
                link.company_id,
                link.address_id,
                link.canonical_address_key,
                link.address_types,
                link.address_sources,
                link.evidence_count as link_evidence_count,
                link.first_observed_at as link_first_observed_at,
                link.last_observed_at as link_last_observed_at,
                link.review_status,
                link.reviewed_at,
                link.reviewed_by,
                link.review_note,
                link.address_identity_run_id,
                link.address_identity_built_at,
                ?::varchar as serving_run_id,
                ?::timestamptz as served_at
            from {shared_addresses.QUALIFIED_COMPANY_ADDRESS_LINKS_TABLE} link
            """,
            [serving_run_id, served_at],
        )
        _assert_serving_output_is_complete(connection)
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise

    [(rows, reviewed)] = connection.execute(
        f"""
        select
            count(*),
            count(*) filter (where review_status != 'unreviewed')
        from {QUALIFIED_DUCKDB_COMPANY_ADDRESSES_SERVING_TABLE}
        """
    ).fetchall()
    return {
        "company_addresses": int(rows),
        "reviewed_links": int(reviewed),
    }


def _assert_serving_output_is_complete(connection: Any) -> None:
    [(link_rows,)] = connection.execute(
        f"select count(*) from {shared_addresses.QUALIFIED_COMPANY_ADDRESS_LINKS_TABLE}"
    ).fetchall()
    [
        (
            serving_rows,
            unique_rows,
            serving_runs,
            address_identity_runs,
        )
    ] = connection.execute(
        f"""
        select
            count(*),
            count(distinct (company_id, address_id)),
            count(distinct serving_run_id),
            count(distinct address_identity_run_id)
        from {QUALIFIED_DUCKDB_COMPANY_ADDRESSES_SERVING_TABLE}
        """
    ).fetchall()
    if int(link_rows) != int(serving_rows) or int(serving_rows) != int(unique_rows):
        raise ValueError("Every company-address link must have one serving row")
    if int(serving_runs) != 1:
        raise ValueError("The company-address serving table must contain one build run")
    if int(address_identity_runs) != 1:
        raise ValueError("The company-address serving table must use one link run")
