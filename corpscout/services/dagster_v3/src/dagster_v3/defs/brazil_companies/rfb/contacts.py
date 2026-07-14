from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import duckdb

from dagster_v3.contact_extraction import (
    EMAIL_DOMAIN_MAX_COMPANIES,
    EMAIL_PROVIDER_DENYLIST,
    EMAIL_UNIQUE_CONFIDENCE,
)
from dagster_v3.defs.brazil_companies.rfb import tables
from dagster_v3.defs.brazil_companies.rfb.duckdb_attach import (
    attached_read_only_database,
)
from dagster_v3.domains import root_domain

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
DOMAINS_SOURCE_SLUG = "brazil_rfb"
EMAIL_CONTACT_DOMAINS_TABLE = "email_contact_domains"
EMAIL_ROOT_DOMAIN_MAP_TABLE = "email_root_domain_map"


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
    except duckdb.InvalidInputException, duckdb.CatalogException:
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
    companies_database_path: str | Path,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    contacts_table = f"{DLT_DATASET_NAME}.{tables.COMPANY_CONTACT_INFO_TABLE}"
    email_contact_domains_table = f"{DLT_DATASET_NAME}.{EMAIL_CONTACT_DOMAINS_TABLE}"
    email_root_domain_map_table = f"{DLT_DATASET_NAME}.{EMAIL_ROOT_DOMAIN_MAP_TABLE}"
    keep_email = (
        "email_root_domain <> '' "
        f"and email_company_count <= {EMAIL_DOMAIN_MAX_COMPANIES} "
        f"and email_root_domain not in ({_denylist_sql()})"
    )

    register_domain_udfs(connection)
    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    with attached_read_only_database(
        connection,
        database_path=companies_database_path,
        alias="companies_db",
    ) as companies_alias:
        establishments_table = (
            f"{companies_alias}.{DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE}"
        )
        connection.execute(
            f"""
            create or replace table {email_contact_domains_table} as
            select distinct raw_email_domain
            from (
                select
                    lower(trim(regexp_extract(correio_eletronico, '[^@]+$')))
                    as raw_email_domain
                from {establishments_table}
                where nullif(trim(correio_eletronico), '') is not null
                  and contains(trim(correio_eletronico), '@')
            )
            where raw_email_domain <> ''
            """
        )
        connection.execute(
            f"""
            create or replace table {email_root_domain_map_table} as
            select
                raw_email_domain,
                coalesce(root_domain(concat('https://', raw_email_domain)), '')
                as email_root_domain
            from {email_contact_domains_table}
            """
        )
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
                    lower(trim(regexp_extract(correio_eletronico, '[^@]+$')))
                    as raw_email_domain,
                    case when status_code = '02' then 1 else 0 end as is_current,
                    'correio_eletronico' as source_field
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
                    '' as raw_email_domain,
                    case when status_code = '02' then 1 else 0 end as is_current,
                    'telefone_1' as source_field
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
                    '' as raw_email_domain,
                    case when status_code = '02' then 1 else 0 end as is_current,
                    'telefone_2' as source_field
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
                    '' as raw_email_domain,
                    case when status_code = '02' then 1 else 0 end as is_current,
                    'fax' as source_field
                from {establishments_table}
                where nullif(trim(fax), '') is not null
            ),
            enriched as (
                select
                    base.*,
                    case
                        when base.contact_type = 'email'
                        then coalesce(domain_map.email_root_domain, '')
                        else ''
                    end as email_root_domain
                from base
                left join {email_root_domain_map_table} as domain_map
                    on base.raw_email_domain = domain_map.raw_email_domain
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
                now() as resolved_at,
                source_field
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

    # Canonical company_contacts stage (spec-standard COMPANY_CONTACTS_COLUMNS):
    # company-grain (registry_id/contact_type/contact_value) dedupe of the raw
    # per-establishment facts above, with area code folded into contact_value.
    # Built from ALL rows of the internal stage (facts are validation-independent —
    # a denylisted-provider email still appears here; only the domains stage below
    # filters on domain acceptance).
    company_contacts_stage = f"{DLT_DATASET_NAME}.{tables.COMPANY_CONTACTS_STAGE_TABLE}"
    connection.execute(
        f"""
        create or replace table {company_contacts_stage} as
        with folded as (
            select
                country_iso2,
                source_slug,
                source_run_id,
                source_record_id,
                cnpj,
                cnpj_basico as registry_id,
                contact_type,
                '' as contact_type_raw,
                case
                    when contact_area_code <> '' then contact_area_code || ' ' || contact_value
                    else contact_value
                end as contact_value,
                source_field,
                is_current,
                cast(null as date) as valid_to,
                '' as source_url,
                resolved_at
            from {contacts_table}
        ),
        deduped as (
            select
                *,
                row_number() over (
                    partition by registry_id, contact_type, contact_value
                    order by is_current desc, cnpj, source_field
                ) as rn
            from folded
        )
        select
            country_iso2, source_slug, source_run_id, source_record_id,
            registry_id, contact_type, contact_type_raw, contact_value,
            source_field, is_current, valid_to, source_url, resolved_at
        from deduped
        where rn = 1
        """
    )
    contact_facts = int(
        connection.execute(f"select count(*) from {company_contacts_stage}").fetchone()[0]
    )

    if log is not None:
        log(
            "Built Brazil RFB contact info: contacts=%s email_domains=%s companies=%s "
            "contact_facts=%s",
            contacts,
            email_domains,
            companies_with_contacts,
            contact_facts,
        )
    return {
        "contacts": contacts,
        "email_domains": email_domains,
        "companies_with_contacts": companies_with_contacts,
        "contact_facts": contact_facts,
    }


def build_brazil_rfb_websites(
    *,
    connection: duckdb.DuckDBPyConnection,
    contact_info_database_path: str | Path,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Builds the br_websites feeder table (exported as corpscout.br_websites).
    Internal stage with no consumers — the domain graph reads br_company_domains
    instead. Retire br_websites via a future migration; do not build new consumers."""
    contact_info_path = Path(contact_info_database_path).expanduser()
    if not contact_info_path.is_absolute():
        contact_info_path = contact_info_path.resolve()
    if not contact_info_path.exists():
        raise FileNotFoundError(
            "Brazil RFB websites require the contact-info DuckDB stage at "
            f"{contact_info_path}. Materialize brazil_comp_rfb_contact_info_duckdb "
            "before brazil_comp_rfb_websites_duckdb. If this is a retry after the "
            "contact/websites stage split, rerun brazil_comp_rfb_contact_info_duckdb "
            "first for the same monthly partition so the snapshot-scoped "
            "contact_info.duckdb stage is created."
        )

    websites_table = f"{DLT_DATASET_NAME}.{tables.WEBSITES_TABLE}"

    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    with attached_read_only_database(
        connection,
        database_path=contact_info_path,
        alias="contact_info_db",
    ) as contact_info_alias:
        contacts_table = (
            f"{contact_info_alias}.{DLT_DATASET_NAME}."
            f"{tables.COMPANY_CONTACT_INFO_TABLE}"
        )
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

        # Canonical company_domains stage (spec-standard COMPANY_DOMAINS_COLUMNS):
        # same src/deduped/picked/primaried election as the legacy websites stage
        # above, re-derived from the attached contact-info stage (root_domain <> ''
        # already excludes denylisted-provider and over-shared email domains).
        company_domains_stage = (
            f"{DLT_DATASET_NAME}.{tables.COMPANY_DOMAINS_STAGE_TABLE}"
        )
        connection.execute(
            f"""
            create or replace table {company_domains_stage} as
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
                concat('br_company_domains:', cnpj_basico, ':', root_domain)
                as source_record_id,
                cnpj_basico as registry_id,
                root_domain as domain,
                domain_source,
                '' as validation_method,
                cast({EMAIL_UNIQUE_CONFIDENCE} as double) as confidence,
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
    company_domains = int(
        connection.execute(f"select count(*) from {company_domains_stage}").fetchone()[0]
    )
    primary_domains = int(
        connection.execute(
            f"select coalesce(sum(is_primary), 0) from {company_domains_stage}"
        ).fetchone()[0]
    )
    distinct_domain_registries = int(
        connection.execute(
            f"select count(distinct registry_id) from {company_domains_stage}"
        ).fetchone()[0]
    )
    if primary_domains != distinct_domain_registries:
        raise ValueError(
            "Brazil RFB company_domains primary-domain count "
            f"({primary_domains}) does not match distinct registry count "
            f"({distinct_domain_registries}); refusing to proceed"
        )

    if log is not None:
        log(
            "Built Brazil RFB websites: websites=%s companies=%s company_domains=%s "
            "primary_domains=%s",
            websites,
            companies_with_websites,
            company_domains,
            primary_domains,
        )
    return {
        "websites": websites,
        "companies_with_websites": companies_with_websites,
        "company_domains": company_domains,
        "primary_domains": primary_domains,
    }
