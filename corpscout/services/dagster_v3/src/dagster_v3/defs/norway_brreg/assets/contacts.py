"""Norway Brreg canonical-pair derivation: reshapes ``corpscout.no_websites`` into
the canonical ``corpscout.no_company_contacts`` / ``corpscout.no_company_domains``
pair via a ClickHouse-native INSERT-SELECT
(``dagster_v3.contact_extraction.replace_table_from_select``).

``build_contacts_select()``/``build_domains_select()`` are pinned in lock-step with
the Task 2 backfill migration
(``clickhouse/migrations/000097_corpscout_no_canonical_contacts.up.sql``) by
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
from dagster_v3.defs.norway_brreg.constants import GROUP_NAME
from dagster_v3.defs.norway_brreg.resolved_tables import NO_WEBSITES_TABLE

NO_COMPANY_CONTACTS_TABLE = "no_company_contacts"
NO_COMPANY_DOMAINS_TABLE = "no_company_domains"
QUALIFIED_NO_COMPANY_CONTACTS_TABLE = f"{RESOLVED_DATABASE}.{NO_COMPANY_CONTACTS_TABLE}"
QUALIFIED_NO_COMPANY_DOMAINS_TABLE = f"{RESOLVED_DATABASE}.{NO_COMPANY_DOMAINS_TABLE}"
QUALIFIED_NO_WEBSITES_TABLE = f"{RESOLVED_DATABASE}.{NO_WEBSITES_TABLE}"


def build_contacts_select() -> str:
    """no_websites -> no_company_contacts. no_websites.root_domain is a non-nullable
    String, so (unlike Finland) no ifNull guard is needed."""
    return (
        "SELECT 'NO', 'norway_brreg', source_run_id, source_record_id, org_number, "
        "'website', '', website_url, 'hjemmeside', is_current, ended_on, '', "
        f"now64(3, 'UTC') FROM {QUALIFIED_NO_WEBSITES_TABLE}"
    )


def build_domains_select() -> str:
    """no_websites -> no_company_domains, one row per current-or-past website with a
    non-blank root_domain."""
    return (
        "SELECT 'NO', 'norway_brreg', source_run_id, source_record_id, org_number, "
        "root_domain, 'website', '', 1.0, website_url, website_normalized_url, "
        f"website_host, is_current, is_primary, now64(3, 'UTC') "
        f"FROM {QUALIFIED_NO_WEBSITES_TABLE} "
        "WHERE nullIf(trim(root_domain), '') IS NOT NULL"
    )


@dg.asset(
    name="norway_brreg_clickhouse_canonical_contacts",
    # Both ClickHouse landing paths (manual full snapshot and daily updates) write
    # corpscout.no_websites, so both are upstream of this derivation, mirroring
    # norway_brreg_translation_load's dep wiring.
    deps=[
        dg.AssetKey("norway_brreg_entities_snapshot_clickhouse"),
        dg.AssetKey("norway_brreg_entity_updates_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Derives corpscout.no_company_contacts/no_company_domains from "
        "corpscout.no_websites via a ClickHouse-native INSERT-SELECT, in lock-step "
        "with the Task 2 backfill migration."
    ),
)
def norway_brreg_clickhouse_canonical_contacts(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=(NO_COMPANY_CONTACTS_TABLE, NO_COMPANY_DOMAINS_TABLE),
    )
    with clickhouse.get_connection() as client:
        # Contacts first, domains second — write order matters for Phase E.
        contacts_written = replace_table_from_select(
            client,
            qualified_table=QUALIFIED_NO_COMPANY_CONTACTS_TABLE,
            columns=COMPANY_CONTACTS_COLUMNS,
            select_sql=build_contacts_select(),
            log=context.log.info,
        )
        domains_written = replace_table_from_select(
            client,
            qualified_table=QUALIFIED_NO_COMPANY_DOMAINS_TABLE,
            columns=COMPANY_DOMAINS_COLUMNS,
            select_sql=build_domains_select(),
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"contacts": contacts_written, "domains": domains_written}
    )
