from __future__ import annotations

from collections.abc import Callable

import duckdb

from dagster_v3.defs.brazil_rfb import tables
from dagster_v3.domains import root_domain

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
DOMAINS_SOURCE_SLUG = "brazil_rfb"
EMAIL_DOMAIN_MAX_COMPANIES = 1
EMAIL_PROVIDER_DENYLIST = frozenset(
    {
        "gmail.com",
        "googlemail.com",
        "outlook.com",
        "hotmail.com",
        "live.com",
        "msn.com",
        "yahoo.com",
        "ymail.com",
        "icloud.com",
        "me.com",
        "mac.com",
        "aol.com",
        "gmx.com",
        "mail.com",
        "zoho.com",
        "proton.me",
        "protonmail.com",
        "fastmail.com",
        "uol.com.br",
        "bol.com.br",
        "terra.com.br",
        "ig.com.br",
        "globo.com",
        "r7.com",
    }
)


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _denylist_sql() -> str:
    items = ", ".join(
        _sql_literal(domain) for domain in sorted(EMAIL_PROVIDER_DENYLIST)
    )
    return items or "''"


def register_domain_udfs(connection: duckdb.DuckDBPyConnection) -> None:
    try:
        connection.remove_function("root_domain")
    except (duckdb.InvalidInputException, duckdb.CatalogException):
        pass
    try:
        connection.create_function(
            "root_domain",
            root_domain,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
    except duckdb.CatalogException as exc:
        if 'Scalar Function with name "root_domain" already exists' not in str(exc):
            raise


def build_brazil_rfb_contact_info(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    contacts_table = f"{DLT_DATASET_NAME}.{tables.COMPANY_CONTACT_INFO_TABLE}"
    establishments_table = f"{DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE}"
    keep_email = (
        "email_root_domain <> '' "
        f"and email_company_count <= {EMAIL_DOMAIN_MAX_COMPANIES} "
        f"and email_root_domain not in ({_denylist_sql()})"
    )

    register_domain_udfs(connection)
    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    connection.execute(
        f"""
        create or replace table {contacts_table} as
        with base as (
            select
                country_iso2,
                source_slug,
                source_run_id as establishment_source_run_id,
                cnpj,
                cnpj_basico,
                'email' as contact_type,
                'Email' as contact_type_en,
                '' as contact_area_code,
                lower(trim(correio_eletronico)) as contact_value,
                case when status_code = '02' then 1 else 0 end as is_current
            from {establishments_table}
            where nullif(trim(correio_eletronico), '') is not null

            union all

            select
                country_iso2,
                source_slug,
                source_run_id as establishment_source_run_id,
                cnpj,
                cnpj_basico,
                'phone' as contact_type,
                'Phone' as contact_type_en,
                trim(ddd_1) as contact_area_code,
                trim(telefone_1) as contact_value,
                case when status_code = '02' then 1 else 0 end as is_current
            from {establishments_table}
            where nullif(trim(telefone_1), '') is not null

            union all

            select
                country_iso2,
                source_slug,
                source_run_id as establishment_source_run_id,
                cnpj,
                cnpj_basico,
                'phone' as contact_type,
                'Phone' as contact_type_en,
                trim(ddd_2) as contact_area_code,
                trim(telefone_2) as contact_value,
                case when status_code = '02' then 1 else 0 end as is_current
            from {establishments_table}
            where nullif(trim(telefone_2), '') is not null

            union all

            select
                country_iso2,
                source_slug,
                source_run_id as establishment_source_run_id,
                cnpj,
                cnpj_basico,
                'fax' as contact_type,
                'Fax' as contact_type_en,
                trim(ddd_fax) as contact_area_code,
                trim(fax) as contact_value,
                case when status_code = '02' then 1 else 0 end as is_current
            from {establishments_table}
            where nullif(trim(fax), '') is not null
        ),
        enriched as (
            select
                *,
                case
                    when contact_type = 'email' and contains(contact_value, '@')
                    then coalesce(
                        root_domain(
                            concat(
                                'https://',
                                lower(trim(regexp_extract(contact_value, '[^@]+$')))
                            )
                        ),
                        ''
                    )
                    else ''
                end as email_root_domain
            from base
        ),
        email_counts as (
            select
                email_root_domain,
                count(distinct cnpj_basico) as email_company_count
            from enriched
            where email_root_domain <> ''
            group by email_root_domain
        )
        select
            country_iso2,
            source_slug,
            {_sql_literal(source_run_id)} as source_run_id,
            concat(cnpj, ':', contact_type, ':', contact_value) as source_record_id,
            cnpj,
            cnpj_basico,
            contact_type,
            contact_type_en,
            contact_area_code,
            contact_value,
            is_current,
            case when {keep_email} then email_root_domain else '' end as root_domain,
            case when {keep_email} then 'email' else '' end as domain_source,
            now() as resolved_at
        from enriched
        left join email_counts using (email_root_domain)
        """
    )
    contacts = int(
        connection.execute(f"select count(*) from {contacts_table}").fetchone()[0]
    )
    email_domains = int(
        connection.execute(
            f"""
            select count(*)
            from {contacts_table}
            where domain_source = 'email'
            """
        ).fetchone()[0]
    )
    companies_with_contacts = int(
        connection.execute(
            f"select count(distinct cnpj_basico) from {contacts_table}"
        ).fetchone()[0]
    )
    if log is not None:
        log(
            "Built Brazil RFB contact info: contacts=%s email_domains=%s companies=%s",
            contacts,
            email_domains,
            companies_with_contacts,
        )
    return {
        "contacts": contacts,
        "email_domains": email_domains,
        "companies_with_contacts": companies_with_contacts,
    }


def build_brazil_rfb_websites(
    *,
    connection: duckdb.DuckDBPyConnection,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    contacts_table = f"{DLT_DATASET_NAME}.{tables.COMPANY_CONTACT_INFO_TABLE}"
    websites_table = f"{DLT_DATASET_NAME}.{tables.WEBSITES_TABLE}"

    connection.execute(
        f"""
        create or replace table {websites_table} as
        with src as (
            select
                country_iso2,
                source_slug,
                source_run_id,
                source_record_id,
                cnpj_basico,
                root_domain,
                domain_source,
                is_current
            from {contacts_table}
            where root_domain <> ''
        ),
        deduped as (
            select
                *,
                row_number() over (
                    partition by cnpj_basico, root_domain
                    order by is_current desc, source_record_id
                ) as rn
            from src
        ),
        picked as (
            select * from deduped where rn = 1
        ),
        primaried as (
            select
                *,
                case when row_number() over (
                    partition by cnpj_basico
                    order by is_current desc, length(root_domain), root_domain
                ) = 1 then 1 else 0 end as is_primary
            from picked
        )
        select
            country_iso2,
            source_slug,
            source_run_id,
            concat('br_websites:', cnpj_basico, ':', root_domain) as source_record_id,
            cnpj_basico,
            root_domain,
            domain_source,
            '' as website_url,
            '' as website_normalized_url,
            '' as website_host,
            is_current,
            is_primary,
            now() as resolved_at
        from primaried
        """
    )
    websites = int(
        connection.execute(f"select count(*) from {websites_table}").fetchone()[0]
    )
    companies_with_websites = int(
        connection.execute(
            f"select count(distinct cnpj_basico) from {websites_table}"
        ).fetchone()[0]
    )

    if log is not None:
        log(
            "Built Brazil RFB websites: websites=%s companies=%s",
            websites,
            companies_with_websites,
        )
    return {
        "websites": websites,
        "companies_with_websites": companies_with_websites,
    }


def build_brazil_rfb_contact_info_and_websites(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    contact_counts = build_brazil_rfb_contact_info(
        connection=connection,
        source_run_id=source_run_id,
        log=log,
    )
    website_counts = build_brazil_rfb_websites(connection=connection, log=log)
    return {
        "contacts": contact_counts["contacts"],
        "websites": website_counts["websites"],
        "email_domains": contact_counts["email_domains"],
        "companies_with_contacts": contact_counts["companies_with_contacts"],
    }
