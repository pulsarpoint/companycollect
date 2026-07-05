"""Wikidata canonical-pair derivation: reshapes ``corpscout.wikidata_company_websites``
(LEFT JOIN ``corpscout.wikidata_companies`` for country) into the canonical
``corpscout.wikidata_company_contacts`` / ``corpscout.wikidata_company_domains`` pair
via a ClickHouse-native INSERT-SELECT
(``dagster_v3.contact_extraction.replace_table_from_select``).

``build_contacts_select()``/``build_domains_select()`` are pinned in lock-step with
the Task 2 backfill migration
(``clickhouse/migrations/000099_corpscout_wikidata_canonical_contacts.up.sql``) by
``tests/test_canonical_derivation_assets.py`` — a change to either SELECT body must
be mirrored in the other.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.contact_extraction import (
    COMPANY_CONTACTS_COLUMNS,
    COMPANY_DOMAINS_COLUMNS,
    replace_table_from_select,
)
from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
)
from dagster_v3.defs.wikidata.tables import (
    WIKIDATA_COMPANIES_TABLE,
    WIKIDATA_COMPANY_WEBSITES_TABLE,
)

GROUP_NAME = "wikidata"
WIKIDATA_COMPANY_CONTACTS_TABLE = "wikidata_company_contacts"
WIKIDATA_COMPANY_DOMAINS_TABLE = "wikidata_company_domains"
QUALIFIED_WIKIDATA_COMPANY_CONTACTS_TABLE = (
    f"{RESOLVED_DATABASE}.{WIKIDATA_COMPANY_CONTACTS_TABLE}"
)
QUALIFIED_WIKIDATA_COMPANY_DOMAINS_TABLE = (
    f"{RESOLVED_DATABASE}.{WIKIDATA_COMPANY_DOMAINS_TABLE}"
)
QUALIFIED_WIKIDATA_COMPANY_WEBSITES_TABLE = (
    f"{RESOLVED_DATABASE}.{WIKIDATA_COMPANY_WEBSITES_TABLE}"
)
QUALIFIED_WIKIDATA_COMPANIES_TABLE = f"{RESOLVED_DATABASE}.{WIKIDATA_COMPANIES_TABLE}"


def build_contacts_select() -> str:
    """wikidata_company_websites (LEFT JOIN wikidata_companies for country) ->
    wikidata_company_contacts. Every website row becomes a contact fact,
    confidence-free, is_current=1 always (wikidata carries no current/historical
    distinction here)."""
    return (
        "SELECT ifNull(companies.headquarters_country_iso2, ''), 'wikidata', "
        "websites.source_run_id, websites.source_record_id, websites.wikidata_id, "
        "'website', '', websites.website_url, 'official_website', 1, NULL, '', "
        "now64(3, 'UTC') "
        f"FROM {QUALIFIED_WIKIDATA_COMPANY_WEBSITES_TABLE} AS websites "
        f"LEFT JOIN {QUALIFIED_WIKIDATA_COMPANIES_TABLE} AS companies "
        "ON companies.wikidata_id = websites.wikidata_id"
    )


def build_domains_select() -> str:
    """wikidata_company_websites -> wikidata_company_domains. Dedupes to one row per
    (wikidata_id, domain) via domain_rn = 1, then elects exactly one primary per
    wikidata_id via rn = 1. rn is computed over ALL rows (before the domain_rn dedup
    filter) but is guaranteed to survive it: rn = 1 is the minimal (length(root_domain),
    root_domain, website_normalized_url) row for a wikidata_id, which is necessarily
    also the minimal website_normalized_url row within its own (wikidata_id,
    root_domain) group — i.e. domain_rn = 1 for that same row. Verified live:
    countIf(rn=1) == countIf(rn=1 AND domain_rn=1), zero rn=1 rows lost to the
    domain_rn filter (see task-2-report.md)."""
    return (
        "SELECT country_iso2, 'wikidata', source_run_id, source_record_id, "
        "registry_id, domain, 'website', '', 1.0, website_url, "
        "website_normalized_url, website_host, 1, if(rn = 1, 1, 0), now64(3, 'UTC') "
        "FROM ( "
        "SELECT "
        "ifNull(companies.headquarters_country_iso2, '') AS country_iso2, "
        "websites.source_run_id AS source_run_id, "
        "websites.source_record_id AS source_record_id, "
        "websites.wikidata_id AS registry_id, "
        "websites.root_domain AS domain, "
        "websites.website_url AS website_url, "
        "websites.website_normalized_url AS website_normalized_url, "
        "websites.website_host AS website_host, "
        "row_number() OVER (PARTITION BY websites.wikidata_id ORDER BY "
        "length(websites.root_domain), websites.root_domain, "
        "websites.website_normalized_url) AS rn, "
        "row_number() OVER (PARTITION BY websites.wikidata_id, websites.root_domain "
        "ORDER BY websites.website_normalized_url) AS domain_rn "
        f"FROM {QUALIFIED_WIKIDATA_COMPANY_WEBSITES_TABLE} AS websites "
        f"LEFT JOIN {QUALIFIED_WIKIDATA_COMPANIES_TABLE} AS companies "
        "ON companies.wikidata_id = websites.wikidata_id "
        "WHERE nullIf(trim(websites.root_domain), '') IS NOT NULL "
        ") "
        "WHERE domain_rn = 1"
    )


@dg.asset(
    name="wikidata_clickhouse_canonical_contacts",
    deps=[dg.AssetKey("wikidata_company_seed_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Derives corpscout.wikidata_company_contacts/wikidata_company_domains from "
        "corpscout.wikidata_company_websites (LEFT JOIN wikidata_companies for "
        "country) via a ClickHouse-native INSERT-SELECT, in lock-step with the "
        "Task 2 backfill migration."
    ),
)
def wikidata_clickhouse_canonical_contacts(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=(WIKIDATA_COMPANY_CONTACTS_TABLE, WIKIDATA_COMPANY_DOMAINS_TABLE),
    )
    with clickhouse.get_connection() as client:
        # Contacts first, domains second — write order matters for Phase E.
        contacts_written = replace_table_from_select(
            client,
            qualified_table=QUALIFIED_WIKIDATA_COMPANY_CONTACTS_TABLE,
            columns=COMPANY_CONTACTS_COLUMNS,
            select_sql=build_contacts_select(),
            log=context.log.info,
        )
        domains_written = replace_table_from_select(
            client,
            qualified_table=QUALIFIED_WIKIDATA_COMPANY_DOMAINS_TABLE,
            columns=COMPANY_DOMAINS_COLUMNS,
            select_sql=build_domains_select(),
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"contacts": contacts_written, "domains": domains_written}
    )


defs = dg.Definitions(
    assets=[wikidata_clickhouse_canonical_contacts],
)
