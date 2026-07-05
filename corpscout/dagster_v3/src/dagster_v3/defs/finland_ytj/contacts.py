"""Finland YTJ canonical-pair derivation: reshapes ``corpscout.fi_websites`` into
the canonical ``corpscout.fi_company_contacts`` / ``corpscout.fi_company_domains``
pair via a ClickHouse-native INSERT-SELECT
(``dagster_v3.contact_extraction.replace_table_from_select``).

``build_contacts_select()``/``build_domains_select()`` are pinned in lock-step with
the Task 2 backfill migration
(``clickhouse/migrations/000098_corpscout_fi_canonical_contacts.up.sql``) by
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
from dagster_v3.defs.finland_ytj.resolved_tables import FI_WEBSITES_TABLE

GROUP_NAME = "finland_ytj"
FI_COMPANY_CONTACTS_TABLE = "fi_company_contacts"
FI_COMPANY_DOMAINS_TABLE = "fi_company_domains"
QUALIFIED_FI_COMPANY_CONTACTS_TABLE = f"{RESOLVED_DATABASE}.{FI_COMPANY_CONTACTS_TABLE}"
QUALIFIED_FI_COMPANY_DOMAINS_TABLE = f"{RESOLVED_DATABASE}.{FI_COMPANY_DOMAINS_TABLE}"
QUALIFIED_FI_WEBSITES_TABLE = f"{RESOLVED_DATABASE}.{FI_WEBSITES_TABLE}"


def build_contacts_select() -> str:
    """fi_websites -> fi_company_contacts."""
    return (
        "SELECT 'FI', 'finland_ytj', source_run_id, source_record_id, business_id, "
        "'website', '', website_url, 'website', is_current, ended_on, '', "
        f"now64(3, 'UTC') FROM {QUALIFIED_FI_WEBSITES_TABLE}"
    )


def build_domains_select() -> str:
    """fi_websites -> fi_company_domains. fi_websites.root_domain is Nullable(String)
    (contrast with Norway's non-nullable root_domain), so the domain projection AND
    the filter both go through ifNull(root_domain, '') — the canonical domain column
    is non-nullable String, and a NULL root_domain must not survive the "non-empty
    domain" filter either."""
    return (
        "SELECT 'FI', 'finland_ytj', source_run_id, source_record_id, business_id, "
        "ifNull(root_domain, ''), 'website', '', 1.0, website_url, "
        "website_normalized_url, website_host, is_current, is_primary, "
        f"now64(3, 'UTC') FROM {QUALIFIED_FI_WEBSITES_TABLE} "
        "WHERE nullIf(trim(ifNull(root_domain, '')), '') IS NOT NULL"
    )


@dg.asset(
    name="finland_ytj_clickhouse_canonical_contacts",
    deps=[dg.AssetKey("finland_ytj_resolved_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Derives corpscout.fi_company_contacts/fi_company_domains from "
        "corpscout.fi_websites via a ClickHouse-native INSERT-SELECT, in lock-step "
        "with the Task 2 backfill migration."
    ),
)
def finland_ytj_clickhouse_canonical_contacts(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=(FI_COMPANY_CONTACTS_TABLE, FI_COMPANY_DOMAINS_TABLE),
    )
    with clickhouse.get_connection() as client:
        # Contacts first, domains second — write order matters for Phase E.
        contacts_written = replace_table_from_select(
            client,
            qualified_table=QUALIFIED_FI_COMPANY_CONTACTS_TABLE,
            columns=COMPANY_CONTACTS_COLUMNS,
            select_sql=build_contacts_select(),
            log=context.log.info,
        )
        domains_written = replace_table_from_select(
            client,
            qualified_table=QUALIFIED_FI_COMPANY_DOMAINS_TABLE,
            columns=COMPANY_DOMAINS_COLUMNS,
            select_sql=build_domains_select(),
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"contacts": contacts_written, "domains": domains_written}
    )


defs = dg.Definitions(
    assets=[finland_ytj_clickhouse_canonical_contacts],
)
