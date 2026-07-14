from __future__ import annotations

import zipfile
from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection

from dagster_v3.contact_extraction import EMAIL_DOMAIN_MAX_COMPANIES, EMAIL_PROVIDER_DENYLIST
from dagster_v3.defs.estonia_ar import resources, tables
from dagster_v3.domains import normalized_url, root_domain, website_host

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
CONTACTS_TABLE = tables.GENERAL_DATA_RAW_TABLE
CONTACTS_SOURCE_SLUG = "estonia_ar_contacts"


def _denylist_sql() -> str:
    """The provider denylist as a SQL IN-list literal."""
    items = ", ".join(
        "'" + d.replace("'", "''") + "'"
        for d in sorted(EMAIL_PROVIDER_DENYLIST)
    )
    return items or "''"


_DOMAIN_UDFS = (
    ("root_domain", root_domain),
    ("normalized_url", normalized_url),
    ("website_host", website_host),
)


def register_domain_udfs(connection: duckdb.DuckDBPyConnection) -> None:
    """Register the shared tldextract-based URL/domain helpers as DuckDB UDFs."""
    for name, fn in _DOMAIN_UDFS:
        try:
            connection.remove_function(name)
        except (duckdb.InvalidInputException, duckdb.CatalogException):
            pass
        connection.create_function(
            name, fn, ["VARCHAR"], "VARCHAR", null_handling="special"
        )

# Only ariregistri_kood + yldandmed.sidevahendid are parsed out of the ~4.5 GB JSON;
# every other (large, nested) field is ignored by read_json's column projection.
_READ_JSON_COLUMNS = (
    "{'ariregistri_kood': 'BIGINT', "
    "'yldandmed': 'STRUCT(sidevahendid STRUCT("
    "liik VARCHAR, liik_tekstina VARCHAR, sisu VARCHAR, lopp_kpv VARCHAR)[])'}"
)


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _extract_single_json(zip_path: Path, dest_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as archive:
        members = [n for n in archive.namelist() if n.lower().endswith(".json")]
        if not members:
            raise ValueError(f"no JSON member found in {zip_path}")
        archive.extract(members[0], dest_dir)
        return dest_dir / members[0]


def _build_contacts_from_json(
    *, duckdb_connection: DuckDBPyConnection, json_path: Path, source_run_id: str
) -> dict[str, int]:
    # Canonical contact_type CASE (spec: contacts standard). ANY code not in
    # EE_CONTACT_TYPE_BY_CODE (e.g. the live-data TELEX code) falls back to the
    # `else 'other'`; contact_type_raw below preserves the source code verbatim
    # regardless of whether it was mapped.
    contact_type_case = "\n                ".join(
        f"when '{code}' then '{canonical}'"
        for code, canonical in resources.EE_CONTACT_TYPE_BY_CODE.items()
    )
    # An email suffix is a company domain only if (a) not a known provider and
    # (b) it maps to <= EMAIL_DOMAIN_MAX_COMPANIES distinct companies — i.e. it is
    # unique to this company. Counting distinct registry_id (not contact rows)
    # drops every provider and shared accounting/formation-agent domain
    # automatically.
    keep_email = (
        f"e.email_domain <> '' "
        f"and ec.company_count <= {EMAIL_DOMAIN_MAX_COMPANIES} "
        f"and e.email_domain not in ({_denylist_sql()})"
    )
    sql = f"""
        create or replace table {DLT_DATASET_NAME}.{CONTACTS_TABLE} as
        with exploded as (
            select
                ariregistri_kood::varchar as reg_code,
                unnest(yldandmed.sidevahendid) as sv
            from read_json(
                {_sql_literal(str(json_path))},
                format = 'array',
                records = true,
                columns = {_READ_JSON_COLUMNS}
            )
        ),
        base as (
            select
                'EE' as country_iso2,
                '{CONTACTS_SOURCE_SLUG}' as source_slug,
                {_sql_literal(source_run_id)} as source_run_id,
                reg_code as source_record_id,
                reg_code as registry_id,
                sv.liik as contact_type_raw,
                case sv.liik
                {contact_type_case}
                else 'other' end as contact_type,
                trim(sv.sisu) as contact_value,
                'sidevahendid' as source_field,
                case when sv.lopp_kpv is null or sv.lopp_kpv = '' then 1 else 0 end as is_current,
                try_strptime(sv.lopp_kpv, '%d.%m.%Y')::date as valid_to,
                {_sql_literal(tables.GENERAL_DATA_URL)} as source_url,
                now() as resolved_at
            from exploded
            where coalesce(trim(sv.sisu), '') <> ''
        ),
        enriched as (
            select
                *,
                case when contact_type = 'website'
                     then coalesce(root_domain(contact_value), '') else '' end as website_domain,
                case when contact_type = 'email' and contains(contact_value, '@')
                     then nullif(lower(trim(regexp_extract(contact_value, '[^@]+$'))), '')
                     else '' end as email_domain_raw
            from base
        ),
        enriched2 as (
            select
                *,
                case when email_domain_raw like '%.%' then email_domain_raw else '' end as email_domain
            from enriched
        ),
        email_counts as (
            select email_domain, count(distinct registry_id) as company_count
            from enriched2 where email_domain <> ''
            group by email_domain
        )
        select
            e.country_iso2, e.source_slug, e.source_run_id, e.source_record_id, e.registry_id,
            e.contact_type, e.contact_type_raw, e.contact_value, e.source_field, e.is_current,
            e.valid_to, e.source_url, e.resolved_at,
            case
                when e.website_domain <> '' then e.website_domain
                when {keep_email} then e.email_domain
                else '' end as domain,
            case
                when e.website_domain <> '' then 'website'
                when {keep_email} then 'email'
                else '' end as domain_source
        from enriched2 e
        left join email_counts ec on e.email_domain = ec.email_domain
    """
    duckdb_connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    register_domain_udfs(duckdb_connection)
    duckdb_connection.execute(sql)
    qualified = f"{DLT_DATASET_NAME}.{CONTACTS_TABLE}"
    contacts = int(duckdb_connection.execute(f"select count(*) from {qualified}").fetchone()[0])
    websites = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified} where domain_source = 'website'"
        ).fetchone()[0]
    )
    email_domains = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified} where domain_source = 'email'"
        ).fetchone()[0]
    )
    return {"contacts": contacts, "websites": websites, "email_domains": email_domains}
