"""Czech ARES company-contact extraction.

Owns only the Czech-specific candidate SQL scan (cz_companies keyset pagination) and
the thin orchestration that wires it into the shared `dagster_v3.contact_extraction`
module, which owns candidate parsing, CommonCrawl/DNS validation, and the atomic
ClickHouse table replace.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from dagster_v3.contact_extraction import (
    CANDIDATE_TEXT_FILTER,
    COMPANY_CONTACTS_COLUMNS,
    COMPANY_DOMAINS_COLUMNS,
    ContactCandidate,
    commoncrawl_domains,
    elect_primary_domains,
    extract_contact_candidates_by_domain,
    iter_company_domain_rows,
    iter_contact_fact_rows,
    merge_domain_candidates,
    replace_contact_table,
    resolve_nameservers_concurrently,
)
from dagster_v3.defs.czech_ares import tables

# Source-name slug, matching the latvia_ur convention (and the defs group name),
# so domain-graph consumers can trace rows back to their source uniformly.
CONTACTS_SOURCE_SLUG = "czech_ares"
CLICKHOUSE_COMPANY_BATCH_SIZE = 100_000
# Not wired into the shared commoncrawl_domains() lookup (it owns its own internal
# query-batch size) — kept as a Czech-facing constant per the module's public
# interface; no shared meaning outside this module.
CLICKHOUSE_QUERY_BATCH_SIZE = 10_000


def load_company_contact_candidate_batch(
    clickhouse_client: Any,
    *,
    batch_size: int = CLICKHOUSE_COMPANY_BATCH_SIZE,
    after_ico: str = "",
) -> list[tuple[str, str]]:
    rows = clickhouse_client.execute(
        f"""
        SELECT ico, name
        FROM {tables.QUALIFIED_COMPANIES_TABLE} FINAL
        WHERE match(name, %(candidate_filter)s)
          AND (%(after_ico)s = '' OR ico > %(after_ico)s)
        ORDER BY ico
        LIMIT %(batch_size)s
        """,
        {
            "candidate_filter": CANDIDATE_TEXT_FILTER,
            "batch_size": batch_size,
            "after_ico": after_ico,
        },
    )
    return [(str(row[0]), str(row[1])) for row in rows]


def replace_czech_company_contacts_clickhouse(
    *,
    clickhouse_client: Any,
    resolved_at: datetime | None = None,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    resolved_timestamp = resolved_at or datetime.now(UTC)
    candidates_by_domain: dict[str, list[ContactCandidate]] = {}
    last_ico = ""
    while True:
        company_rows = load_company_contact_candidate_batch(
            clickhouse_client,
            batch_size=CLICKHOUSE_COMPANY_BATCH_SIZE,
            after_ico=last_ico,
        )
        if not company_rows:
            break
        merge_domain_candidates(
            candidates_by_domain,
            extract_contact_candidates_by_domain(company_rows, home_tlds=frozenset({"cz"})),
        )
        last_ico = company_rows[-1][0]

    candidate_domains = tuple(sorted(candidates_by_domain))
    found_commoncrawl_domains = commoncrawl_domains(clickhouse_client, candidate_domains)
    nameservers_by_domain = resolve_nameservers_concurrently(
        [domain for domain in candidate_domains if domain not in found_commoncrawl_domains]
    )

    if log is not None:
        log(
            "Built Czech company contact candidates: domains=%s commoncrawl_domains=%s dns_domains=%s",
            len(candidate_domains),
            len(found_commoncrawl_domains),
            sum(1 for nameservers in nameservers_by_domain.values() if nameservers),
        )

    fact_rows = iter_contact_fact_rows(
        candidates_by_domain,
        country_iso2="CZ",
        source_slug=CONTACTS_SOURCE_SLUG,
        source_field="name",
        resolved_at=resolved_timestamp,
    )
    domain_rows = elect_primary_domains(
        iter_company_domain_rows(
            candidates_by_domain,
            commoncrawl_domains=found_commoncrawl_domains,
            nameservers_by_domain=nameservers_by_domain,
            country_iso2="CZ",
            source_slug=CONTACTS_SOURCE_SLUG,
            resolved_at=resolved_timestamp,
        )
    )

    contact_facts = replace_contact_table(
        clickhouse_client,
        qualified_table=tables.QUALIFIED_COMPANY_CONTACTS_TABLE,
        columns=COMPANY_CONTACTS_COLUMNS,
        rows=fact_rows,
        log=log,
    )
    replace_contact_table(
        clickhouse_client,
        qualified_table=tables.QUALIFIED_COMPANY_DOMAINS_TABLE,
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
