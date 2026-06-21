from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import duckdb

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
    database_path: str | Path,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Deduped per-company domain feeder for the cross-source domain graph.

    One row per (reg_code, domain) from the domain-tagged rows of the contacts
    table (website-derived or email-derived). Website rows carry the normalized
    URL/host; email rows leave those empty. Refuses to replace on empty input.
    """
    contacts = f"{DLT_DATASET_NAME}.{CONTACTS_TABLE}"
    qualified = f"{DLT_DATASET_NAME}.{COMPANY_DOMAINS_TABLE}"
    sql = f"""
        create or replace table {qualified} as
        with src as (
            select
                reg_code,
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
            -- one row per (reg_code, domain): prefer a website source over email,
            -- then a current contact, then a stable order.
            select *, row_number() over (
                partition by reg_code, domain
                order by (domain_source = 'website') desc, is_current desc, website_normalized_url
            ) as rn
            from src
        ),
        picked as (select * from deduped where rn = 1),
        primaried as (
            -- exactly one primary domain per company.
            select *, case when row_number() over (
                partition by reg_code
                order by (domain_source = 'website') desc, is_current desc, length(domain), domain
            ) = 1 then 1 else 0 end as is_primary
            from picked
        )
        select
            'EE' as country_iso2,
            '{DOMAINS_SOURCE_SLUG}' as source_slug,
            {_sql_literal(source_run_id)} as source_run_id,
            source_record_id,
            reg_code,
            domain,
            domain_source,
            website_url,
            website_normalized_url,
            website_host,
            is_current,
            is_primary,
            now() as resolved_at
        from primaried
    """
    with duckdb.connect(str(database_path)) as connection:
        register_domain_udfs(connection)
        connection.execute(sql)
        rows = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
        websites = int(
            connection.execute(
                f"select count(*) from {qualified} where domain_source = 'website'"
            ).fetchone()[0]
        )
        emails = int(
            connection.execute(
                f"select count(*) from {qualified} where domain_source = 'email'"
            ).fetchone()[0]
        )
        companies = int(
            connection.execute(
                f"select count(distinct reg_code) from {qualified}"
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
