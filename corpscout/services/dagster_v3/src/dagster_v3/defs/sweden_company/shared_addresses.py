from collections.abc import Sequence
from datetime import datetime
from typing import Any

from dagster_v3.defs.sweden_company import address_canonicalization

SHARED_ADDRESSES_TABLE = "se_addresses_current"
COMPANY_ADDRESS_LINKS_TABLE = "se_company_address_links_current"
QUALIFIED_SHARED_ADDRESSES_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{SHARED_ADDRESSES_TABLE}"
)
QUALIFIED_COMPANY_ADDRESS_LINKS_TABLE = (
    f"{address_canonicalization.ENRICHMENT_SCHEMA}.{COMPANY_ADDRESS_LINKS_TABLE}"
)
QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE = (
    f"{address_canonicalization.CLICKHOUSE_DATABASE}.{SHARED_ADDRESSES_TABLE}"
)
QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE = (
    f"{address_canonicalization.CLICKHOUSE_DATABASE}.{COMPANY_ADDRESS_LINKS_TABLE}"
)

SHARED_ADDRESS_COLUMNS = (
    "address_id",
    "canonical_display_address",
    "representative_address_source",
    "street_address",
    "street_name",
    "house_number",
    "unit",
    "postal_code",
    "post_town",
    "country_code",
    "address_kind",
    "normalized_street",
    "normalized_postal_code",
    "normalized_post_town",
    "address_types",
    "address_sources",
    "company_count",
    "evidence_count",
    "first_observed_at",
    "last_observed_at",
    "address_identity_run_id",
    "address_identity_built_at",
)

COMPANY_ADDRESS_LINK_COLUMNS = (
    "company_id",
    "address_id",
    "canonical_address_key",
    "address_types",
    "address_sources",
    "evidence_count",
    "first_observed_at",
    "last_observed_at",
    "review_status",
    "reviewed_at",
    "reviewed_by",
    "review_note",
    "address_identity_run_id",
    "address_identity_built_at",
)


def replace_sweden_shared_addresses(
    *,
    connection: Any,
    company_address_link_reviews: Sequence[Sequence[object]],
    address_identity_run_id: str,
    address_identity_built_at: datetime,
) -> dict[str, int]:
    """Build country-level addresses and company links from canonical evidence."""
    _replace_company_address_link_review_snapshot(
        connection,
        company_address_link_reviews,
    )
    connection.execute("begin transaction")
    try:
        _create_company_address_links(
            connection=connection,
            address_identity_run_id=address_identity_run_id,
            address_identity_built_at=address_identity_built_at,
        )
        _create_shared_addresses(
            connection=connection,
            address_identity_run_id=address_identity_run_id,
            address_identity_built_at=address_identity_built_at,
        )
        _assert_shared_address_invariants(connection)
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise

    shared_addresses = _count(connection, QUALIFIED_SHARED_ADDRESSES_TABLE)
    company_address_links = _count(
        connection,
        QUALIFIED_COMPANY_ADDRESS_LINKS_TABLE,
    )
    return {
        "shared_addresses": shared_addresses,
        "company_address_links": company_address_links,
        "shared_company_links": company_address_links - shared_addresses,
    }


def _replace_company_address_link_review_snapshot(
    connection: Any,
    review_rows: Sequence[Sequence[object]],
) -> None:
    connection.execute(
        """
        create or replace temporary table _sweden_company_address_link_reviews (
            company_id varchar,
            address_id varchar,
            review_status varchar,
            reviewed_at timestamptz,
            reviewed_by varchar,
            review_note varchar
        )
        """
    )
    if review_rows:
        connection.executemany(
            """
            insert into _sweden_company_address_link_reviews
            values (?, ?, ?, ?, ?, ?)
            """,
            review_rows,
        )


def _create_company_address_links(
    *,
    connection: Any,
    address_identity_run_id: str,
    address_identity_built_at: datetime,
) -> None:
    connection.execute(
        f"""
        create or replace table {QUALIFIED_COMPANY_ADDRESS_LINKS_TABLE} as
        with canonical_links as (
            select
                canonical.company_id,
                sha256(concat_ws(
                    chr(31),
                    canonical.country_code,
                    canonical.normalized_street,
                    canonical.normalized_postal_code,
                    canonical.normalized_post_town
                )) as address_id,
                canonical.canonical_address_key,
                canonical.representative_address_source,
                canonical.street_address,
                canonical.canonical_display_address,
                canonical.address_types,
                canonical.address_sources,
                canonical.member_count::uinteger as evidence_count,
                min(members.source_observed_at) as first_observed_at,
                max(members.source_observed_at) as last_observed_at
            from {address_canonicalization.QUALIFIED_CANONICAL_ADDRESSES_TABLE}
                canonical
            join {address_canonicalization.QUALIFIED_ADDRESS_MEMBERS_TABLE} members
                using (company_id, canonical_address_key)
            group by all
        ), links as (
            select
                company_id,
                address_id,
                first(
                    canonical_address_key
                    order by
                        evidence_count desc,
                        (street_address != upper(street_address)) desc,
                        (representative_address_source = 'bolagsverket') desc,
                        length(canonical_display_address) desc,
                        canonical_address_key
                ) as canonical_address_key,
                list_sort(list_distinct(flatten(list(address_types))))
                    as address_types,
                list_sort(list_distinct(flatten(list(address_sources))))
                    as address_sources,
                sum(evidence_count)::uinteger as evidence_count,
                min(first_observed_at) as first_observed_at,
                max(last_observed_at) as last_observed_at
            from canonical_links
            group by company_id, address_id
        )
        select
            links.*,
            coalesce(review.review_status, 'unreviewed') as review_status,
            review.reviewed_at,
            coalesce(review.reviewed_by, '') as reviewed_by,
            coalesce(review.review_note, '') as review_note,
            ?::varchar as address_identity_run_id,
            ?::timestamptz as address_identity_built_at
        from links
        left join _sweden_company_address_link_reviews review
            using (company_id, address_id)
        """,
        [address_identity_run_id, address_identity_built_at],
    )


def _create_shared_addresses(
    *,
    connection: Any,
    address_identity_run_id: str,
    address_identity_built_at: datetime,
) -> None:
    connection.execute(
        f"""
        create or replace table {QUALIFIED_SHARED_ADDRESSES_TABLE} as
        with candidates as (
            select
                links.address_id,
                canonical.*,
                links.address_types as link_address_types,
                links.address_sources as link_address_sources,
                links.evidence_count as link_evidence_count,
                links.first_observed_at,
                links.last_observed_at,
                row_number() over (
                    partition by links.address_id
                    order by
                        canonical.member_count desc,
                        (canonical.street_address != upper(canonical.street_address))
                            desc,
                        (canonical.representative_address_source = 'bolagsverket')
                            desc,
                        length(canonical.canonical_display_address) desc,
                        canonical.company_id,
                        canonical.canonical_address_key
                ) as representative_rank
            from {QUALIFIED_COMPANY_ADDRESS_LINKS_TABLE} links
            join {address_canonicalization.QUALIFIED_CANONICAL_ADDRESSES_TABLE}
                canonical using (company_id, canonical_address_key)
        )
        select
            address_id,
            array_to_string(list_filter([
                first(street_address order by representative_rank),
                trim(concat_ws(
                    ' ',
                    first(postal_code order by representative_rank),
                    first(post_town order by representative_rank)
                )),
                case
                    when first(country_code order by representative_rank) = 'SE'
                        then ''
                    else first(country_code order by representative_rank)
                end
            ], value -> trim(value) != ''), ', ') as canonical_display_address,
            first(representative_address_source order by representative_rank)
                as representative_address_source,
            first(street_address order by representative_rank) as street_address,
            first(street_name order by representative_rank) as street_name,
            first(house_number order by representative_rank) as house_number,
            first(unit order by representative_rank) as unit,
            first(postal_code order by representative_rank) as postal_code,
            first(post_town order by representative_rank) as post_town,
            first(country_code order by representative_rank) as country_code,
            first(address_kind order by representative_rank) as address_kind,
            first(normalized_street order by representative_rank)
                as normalized_street,
            first(normalized_postal_code order by representative_rank)
                as normalized_postal_code,
            first(normalized_post_town order by representative_rank)
                as normalized_post_town,
            list_sort(list_distinct(flatten(list(link_address_types))))
                as address_types,
            list_sort(list_distinct(flatten(list(link_address_sources))))
                as address_sources,
            count(distinct company_id)::uinteger as company_count,
            sum(link_evidence_count)::ubigint as evidence_count,
            min(first_observed_at) as first_observed_at,
            max(last_observed_at) as last_observed_at,
            ?::varchar as address_identity_run_id,
            ?::timestamptz as address_identity_built_at
        from candidates
        group by address_id
        """,
        [address_identity_run_id, address_identity_built_at],
    )


def _assert_shared_address_invariants(connection: Any) -> None:
    [(
        expected_link_rows,
        canonical_evidence,
        link_rows,
        unique_link_rows,
        linked_evidence,
    )] = (
        connection.execute(
            f"""
            with expected as (
                select
                    company_id,
                    sha256(concat_ws(
                        chr(31),
                        country_code,
                        normalized_street,
                        normalized_postal_code,
                        normalized_post_town
                    )) as address_id,
                    member_count
                from {address_canonicalization.QUALIFIED_CANONICAL_ADDRESSES_TABLE}
            )
            select
                (select count(distinct (company_id, address_id)) from expected),
                (select sum(member_count) from expected),
                count(*),
                count(distinct (company_id, address_id)),
                sum(evidence_count)
            from {QUALIFIED_COMPANY_ADDRESS_LINKS_TABLE}
            """
        ).fetchall()
    )
    [(address_rows, unique_address_rows, address_evidence, address_companies)] = (
        connection.execute(
            f"""
            select
                count(*),
                count(distinct address_id),
                sum(evidence_count),
                sum(company_count)
            from {QUALIFIED_SHARED_ADDRESSES_TABLE}
            """
        ).fetchall()
    )
    if int(expected_link_rows) != int(link_rows) or int(link_rows) != int(
        unique_link_rows
    ):
        raise ValueError(
            "Every canonical Sweden company address must map to one shared address"
        )
    if int(canonical_evidence) != int(linked_evidence):
        raise ValueError(
            "Company-to-address links must retain all canonical source evidence"
        )
    if int(address_rows) != int(unique_address_rows):
        raise ValueError("Sweden shared address IDs must be unique")
    if int(linked_evidence) != int(address_evidence):
        raise ValueError("Shared Sweden address evidence totals must match link totals")
    if int(link_rows) != int(address_companies):
        raise ValueError("Shared Sweden address company counts must match link rows")


def _count(connection: Any, qualified_table: str) -> int:
    [(count,)] = connection.execute(
        f"select count(*) from {qualified_table}"
    ).fetchall()
    return int(count)
