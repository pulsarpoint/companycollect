"""Latvia company contacts: domains embedded in legal names.

Latvia UR has no structured contact fields, but ~1.3k companies carry their
domain as the legal name ('SIA "cenuklubs.lv"'). Candidates are extracted
with the shared contact_extraction module (IDN-aware — Latvian domains use
diacritics), validated against CommonCrawl/DNS, and used to atomically
replace the canonical corpscout.lv_company_contacts (all guard-surviving
candidates, validation-independent) and corpscout.lv_company_domains
(validated-only, per-registry primary election) pair. Full recompute per run.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.contact_extraction import (
    CANDIDATE_TEXT_FILTER,
    COMPANY_CONTACTS_COLUMNS,
    COMPANY_DOMAINS_COLUMNS,
    ContactCandidate,
    commoncrawl_domains,
    elect_primary_domains,
    extract_contact_candidates,
    extract_contact_candidates_by_domain,
    iter_company_domain_rows,
    iter_contact_fact_rows,
    merge_domain_candidates,
    replace_contact_table,
    resolve_nameservers_concurrently,
)

LV_CONTACTS_SOURCE_SLUG = "latvia_ur"
QUALIFIED_LV_CONTACTS_TABLE = "corpscout.lv_company_contacts"
QUALIFIED_LV_DOMAINS_TABLE = "corpscout.lv_company_domains"

# Latvia registry id semantics (canonical contact/domain standard — spec:
# docs/superpowers/specs/2026-07-04-company-contacts-domains-standard-design.md,
# Key decisions #1): `registry_id` in the canonical tables carries this source's
# native id (regcode), recorded once here rather than per-row.
REGISTRY_ID_TYPE = "regcode"

SCAN_BATCH_SIZE = 100_000


def build_candidate_scan_sql(*, after_regcode: str, limit: int) -> str:
    return f"""
SELECT regcode, legal_name
FROM corpscout.lv_companies
WHERE match(legal_name, '{CANDIDATE_TEXT_FILTER}')
  AND regcode > '{after_regcode}'
ORDER BY regcode
LIMIT {limit}"""


def extract_latvia_contact_candidates(
    *, regcode: str, legal_name: str
) -> list[ContactCandidate]:
    return extract_contact_candidates(record_id=regcode, text=legal_name)


def replace_latvia_company_contacts_clickhouse(
    *,
    clickhouse_client: Any,
    resolved_at: datetime | None = None,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    resolved_timestamp = resolved_at or datetime.now(UTC)
    candidates_by_domain: dict[str, list[ContactCandidate]] = {}
    scanned = 0
    after = ""
    while True:
        rows = clickhouse_client.execute(
            build_candidate_scan_sql(after_regcode=after, limit=SCAN_BATCH_SIZE)
        )
        if not rows:
            break
        scanned += len(rows)
        row_pairs = [(str(regcode), str(legal_name)) for regcode, legal_name in rows]
        merge_domain_candidates(
            candidates_by_domain,
            extract_contact_candidates_by_domain(row_pairs, home_tlds=frozenset({"lv"})),
        )
        after = row_pairs[-1][0]
        if len(rows) < SCAN_BATCH_SIZE:
            break

    candidate_domains = tuple(sorted(candidates_by_domain))
    found_commoncrawl_domains = commoncrawl_domains(clickhouse_client, candidate_domains)
    nameservers_by_domain = resolve_nameservers_concurrently(
        [domain for domain in candidate_domains if domain not in found_commoncrawl_domains]
    )

    if log is not None:
        log(
            "Built Latvia company contact candidates: scanned=%s domains=%s "
            "commoncrawl_domains=%s dns_domains=%s",
            scanned,
            len(candidate_domains),
            len(found_commoncrawl_domains),
            sum(1 for nameservers in nameservers_by_domain.values() if nameservers),
        )

    fact_rows = iter_contact_fact_rows(
        candidates_by_domain,
        country_iso2="LV",
        source_slug=LV_CONTACTS_SOURCE_SLUG,
        source_field="legal_name",
        resolved_at=resolved_timestamp,
    )
    domain_rows = elect_primary_domains(
        iter_company_domain_rows(
            candidates_by_domain,
            commoncrawl_domains=found_commoncrawl_domains,
            nameservers_by_domain=nameservers_by_domain,
            country_iso2="LV",
            source_slug=LV_CONTACTS_SOURCE_SLUG,
            resolved_at=resolved_timestamp,
        )
    )

    contact_facts = replace_contact_table(
        clickhouse_client,
        qualified_table=QUALIFIED_LV_CONTACTS_TABLE,
        columns=COMPANY_CONTACTS_COLUMNS,
        rows=fact_rows,
        log=log,
    )
    replace_contact_table(
        clickhouse_client,
        qualified_table=QUALIFIED_LV_DOMAINS_TABLE,
        columns=COMPANY_DOMAINS_COLUMNS,
        rows=domain_rows,
        log=log,
    )
    return {
        "contact_facts": contact_facts,
        "domains": len(domain_rows),
        "primary_domains": sum(1 for row in domain_rows if row[13] == 1),
        "commoncrawl_validated": sum(1 for row in domain_rows if row[7] == "commoncrawl"),
        "dns_validated": sum(1 for row in domain_rows if row[7] == "dns"),
    }


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"python", "clickhouse"},
    metadata={
        "contacts_table": QUALIFIED_LV_CONTACTS_TABLE,
        "domains_table": QUALIFIED_LV_DOMAINS_TABLE,
    },
    description=(
        "Extract domains embedded in Latvian legal names ('SIA \"cenuklubs.lv\"'), "
        "validate via CommonCrawl/DNS, and atomically replace the canonical "
        "corpscout.lv_company_contacts (all candidates) and "
        "corpscout.lv_company_domains (validated, elected primary) pair."
    ),
)
def latvia_ur_clickhouse_company_contacts(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        counts = replace_latvia_company_contacts_clickhouse(
            clickhouse_client=client,
            resolved_at=datetime.now(UTC),
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "contacts_table": QUALIFIED_LV_CONTACTS_TABLE,
            "domains_table": QUALIFIED_LV_DOMAINS_TABLE,
        }
    )
