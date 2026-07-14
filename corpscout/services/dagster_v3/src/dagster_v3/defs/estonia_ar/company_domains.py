from __future__ import annotations

from collections.abc import Callable

from duckdb import DuckDBPyConnection

from dagster_v3.contact_extraction import EMAIL_UNIQUE_CONFIDENCE, WEBSITE_CONFIDENCE
from dagster_v3.defs.estonia_ar import tables
from dagster_v3.defs.estonia_ar.contacts import register_domain_udfs

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
CONTACTS_TABLE = tables.GENERAL_DATA_RAW_TABLE
COMPANY_DOMAINS_TABLE = tables.COMPANY_DOMAINS_TABLE
DOMAINS_SOURCE_SLUG = "estonia_ar"


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_estonia_ar_company_domains(
    *,
    duckdb_connection: DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Deduped per-company domain feeder for the cross-source domain graph.

    One row per (registry_id, domain) from the domain-tagged rows of the
    contacts table (website-derived or email-derived). Website rows carry the
    normalized URL/host and confidence WEBSITE_CONFIDENCE (1.0); email rows
    leave those columns empty and carry EMAIL_UNIQUE_CONFIDENCE (0.9). Refuses
    to replace on empty input.
    """
    contacts = f"{DLT_DATASET_NAME}.{CONTACTS_TABLE}"
    qualified = f"{DLT_DATASET_NAME}.{COMPANY_DOMAINS_TABLE}"
    sql = f"""
        create or replace table {qualified} as
        with src as (
            select
                registry_id,
                source_record_id,
                domain,
                domain_source,
                is_current,
                case when domain_source = 'website' then contact_value else '' end as website_url,
                case when domain_source = 'website'
                     then coalesce(normalized_url(contact_value), '') else '' end as website_normalized_url,
                case when domain_source = 'website'
                     then coalesce(website_host(contact_value), '') else '' end as website_host
            from {contacts}
            where domain <> ''
        ),
        deduped as (
            -- one row per (registry_id, domain): prefer a website source over
            -- email, then a current contact, then a stable order.
            select *, row_number() over (
                partition by registry_id, domain
                order by (domain_source = 'website') desc, is_current desc, website_normalized_url
            ) as rn
            from src
        ),
        picked as (select * from deduped where rn = 1),
        primaried as (
            -- exactly one primary domain per company.
            select *, case when row_number() over (
                partition by registry_id
                order by (domain_source = 'website') desc, is_current desc, length(domain), domain
            ) = 1 then 1 else 0 end as is_primary
            from picked
        )
        select
            'EE' as country_iso2,
            '{DOMAINS_SOURCE_SLUG}' as source_slug,
            {_sql_literal(source_run_id)} as source_run_id,
            source_record_id,
            registry_id,
            domain,
            domain_source,
            '' as validation_method,
            case when domain_source = 'website'
                 then cast({WEBSITE_CONFIDENCE} as double)
                 else cast({EMAIL_UNIQUE_CONFIDENCE} as double) end as confidence,
            website_url,
            website_normalized_url,
            website_host,
            is_current,
            is_primary,
            now() as resolved_at
        from primaried
    """
    register_domain_udfs(duckdb_connection)
    duckdb_connection.execute(sql)
    rows = int(duckdb_connection.execute(f"select count(*) from {qualified}").fetchone()[0])
    websites = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified} where domain_source = 'website'"
        ).fetchone()[0]
    )
    emails = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified} where domain_source = 'email'"
        ).fetchone()[0]
    )
    companies = int(
        duckdb_connection.execute(
            f"select count(distinct registry_id) from {qualified}"
        ).fetchone()[0]
    )
    if rows == 0:
        raise ValueError(
            "Estonia AR contacts produced no company domains; refusing to replace the table"
        )
    counts = {
        "domains": rows,
        "website_domains": websites,
        "email_domains": emails,
        "companies": companies,
    }
    if log is not None:
        log(
            "Built Estonia AR company domains: domains=%s website=%s email=%s companies=%s",
            rows,
            websites,
            emails,
            companies,
        )
    return counts
