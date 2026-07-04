"""Czech ARES company-contact extraction.

Owns only the Czech-specific candidate SQL scan (cz_companies keyset pagination) and
the thin orchestration that wires it into the shared `dagster_v3.contact_extraction`
module, which owns candidate parsing, CommonCrawl/DNS validation, and the atomic
ClickHouse table replace.
"""

from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from dagster_v3.contact_extraction import (
    CANDIDATE_TEXT_FILTER,
    ContactCandidate,
    commoncrawl_domains,
    extract_contact_candidates_by_domain,
    iter_valid_contact_rows,
    merge_domain_candidates,
    replace_contact_table,
    resolve_nameservers_concurrently,
)
from dagster_v3.defs.czech_ares import tables

CONTACTS_SOURCE_SLUG = "czech_ares_contact_extraction"
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
            extract_contact_candidates_by_domain(company_rows),
        )
        last_ico = company_rows[-1][0]

    candidate_domains = tuple(sorted(candidates_by_domain))
    found_commoncrawl_domains = commoncrawl_domains(clickhouse_client, candidate_domains)
    nameservers_by_domain = resolve_nameservers_concurrently(
        [domain for domain in candidate_domains if domain not in found_commoncrawl_domains]
    )

    contact_rows = iter_valid_contact_rows(
        candidates_by_domain,
        commoncrawl_domains=found_commoncrawl_domains,
        nameservers_by_domain=nameservers_by_domain,
        source_slug=CONTACTS_SOURCE_SLUG,
        resolved_at=resolved_timestamp,
    )
    if log is not None:
        log(
            "Built Czech company contact candidates: domains=%s commoncrawl_domains=%s dns_domains=%s",
            len(candidate_domains),
            len(found_commoncrawl_domains),
            sum(1 for nameservers in nameservers_by_domain.values() if nameservers),
        )

    counts = {"contacts": 0, "domains": 0, "commoncrawl_validated": 0, "dns_validated": 0}
    seen_domains: set[str] = set()
    replace_contact_table(
        clickhouse_client,
        qualified_table=tables.QUALIFIED_COMPANY_CONTACTS_TABLE,
        columns=tables.CZ_COMPANY_CONTACTS_EXPORT_COLUMNS,
        rows=_tally_contact_rows(contact_rows, counts=counts, seen_domains=seen_domains),
        log=log,
    )
    return counts


def _tally_contact_rows(
    rows: Iterable[tuple],
    *,
    counts: dict[str, int],
    seen_domains: set[str],
) -> Iterable[tuple]:
    """Pass rows through to the writer while tallying per-domain-source counts —
    mirrors the bookkeeping the old private `_insert_contact_rows` used to do
    inline, now that the shared `replace_contact_table` only reports a row total.
    """
    for row in rows:
        counts["contacts"] += 1
        domain, domain_source = row[5], row[6]
        if domain not in seen_domains:
            seen_domains.add(domain)
            counts["domains"] += 1
        if domain_source == "commoncrawl":
            counts["commoncrawl_validated"] += 1
        elif domain_source == "dns":
            counts["dns_validated"] += 1
        yield row
