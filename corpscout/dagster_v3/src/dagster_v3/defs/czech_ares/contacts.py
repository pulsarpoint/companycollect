import re
import uuid
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import dns.exception
import dns.rdatatype
import dns.resolver
import tldextract

from dagster_v3.defs.czech_ares import tables
from dagster_v3.domains import root_domain, website_host

CONTACTS_SOURCE_SLUG = "czech_ares_contact_extraction"
COMMONCRAWL_CONFIDENCE = 0.95
DNS_CONFIDENCE = 0.70
DNS_RESOLVE_WORKERS = 32
CLICKHOUSE_COMPANY_BATCH_SIZE = 100_000
CLICKHOUSE_QUERY_BATCH_SIZE = 10_000
CLICKHOUSE_INSERT_BATCH_SIZE = 50_000
DNS_QUERY_TIMEOUT_SECONDS = 2.0

_EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+\-]+@(?:[A-Z0-9](?:[A-Z0-9\-]{0,61}[A-Z0-9])?\.)+[A-Z0-9](?:[A-Z0-9\-]{0,61}[A-Z0-9])?\b",
    re.IGNORECASE,
)
_DOMAIN_RE = re.compile(
    r"(?<!@)\b(?:https?://)?(?:[A-Z0-9](?:[A-Z0-9\-]{0,61}[A-Z0-9])?\.)+[A-Z0-9](?:[A-Z0-9\-]{0,61}[A-Z0-9])?(?:/[^\s,;)]*)?",
    re.IGNORECASE,
)
_CANDIDATE_NAME_FILTER = (
    "(?i)(@|https?://|www\\.|[A-Za-z0-9][A-Za-z0-9-]*\\.[A-Za-z]{2,})"
)
_TLD_EXTRACT = tldextract.TLDExtract(
    suffix_list_urls=(),
    cache_dir=None,
)


@dataclass(frozen=True)
class ContactCandidate:
    ico: str
    contact_type: str
    contact_value: str
    domain: str


CompanyContactRow = tuple[str, str]


def extract_contact_candidates(*, ico: str, company_name: str) -> list[ContactCandidate]:
    """Extract email and domain candidates embedded in a Czech company name."""
    candidates: list[ContactCandidate] = []
    seen: set[tuple[str, str]] = set()
    extracted: list[tuple[int, str, str, str]] = []

    for match in _EMAIL_RE.finditer(company_name):
        value = match.group(0).lower()
        domain = root_domain(value.rsplit("@", maxsplit=1)[-1])
        if domain:
            extracted.append((match.start(), "email", value, domain))

    for match in _DOMAIN_RE.finditer(company_name):
        value = _normalized_domain_contact_value(match.group(0))
        domain = root_domain(value)
        if domain:
            extracted.append((match.start(), "domain", value, domain))

    for _position, contact_type, contact_value, domain in sorted(extracted):
        _append_candidate(
            candidates,
            seen,
            ico=ico,
            contact_type=contact_type,
            contact_value=contact_value,
            domain=domain,
        )

    return candidates


def extract_contact_candidates_by_domain(
    company_rows: Iterable[CompanyContactRow],
) -> dict[str, list[ContactCandidate]]:
    candidates_by_domain: dict[str, list[ContactCandidate]] = {}
    seen: set[tuple[str, str, str]] = set()
    for ico_raw, company_name_raw in company_rows:
        ico = str(ico_raw)
        company_name = str(company_name_raw)
        for parsed_candidate in extract_contact_candidates(
            ico=ico, company_name=company_name
        ):
            candidate = ContactCandidate(
                ico=parsed_candidate.ico,
                contact_type=parsed_candidate.contact_type,
                contact_value=parsed_candidate.contact_value,
                domain=parsed_candidate.domain,
            )
            key = (candidate.ico, candidate.contact_type, candidate.contact_value)
            if key in seen:
                continue
            seen.add(key)
            candidates_by_domain.setdefault(candidate.domain, []).append(candidate)
    return candidates_by_domain


def iter_valid_contact_rows_from_domain_candidates(
    candidates_by_domain: dict[str, list[ContactCandidate]],
    *,
    commoncrawl_domains: set[str],
    nameservers_by_domain: dict[str, tuple[str, ...]],
    resolved_at: datetime,
) -> Iterable[dict[str, object]]:
    for domain in sorted(candidates_by_domain):
        validation = _validated_domain(
            domain,
            commoncrawl_domains=commoncrawl_domains,
            nameservers_by_domain=nameservers_by_domain,
        )
        if validation is None:
            continue
        domain_source, confidence = validation
        for candidate in candidates_by_domain[domain]:
            yield {
                "source_slug": CONTACTS_SOURCE_SLUG,
                "source_record_id": candidate.ico,
                "ico": candidate.ico,
                "contact_type": candidate.contact_type,
                "contact_value": candidate.contact_value,
                "domain": domain,
                "domain_source": domain_source,
                "confidence": confidence,
                "resolved_at": resolved_at,
            }


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
        _merge_domain_candidates(
            candidates_by_domain,
            extract_contact_candidates_by_domain(company_rows),
        )
        last_ico = company_rows[-1][0]

    candidate_domains = tuple(sorted(candidates_by_domain))
    commoncrawl_domains = _commoncrawl_domains(clickhouse_client, candidate_domains)
    nameservers_by_domain = _resolve_nameservers_concurrently(
        [
            domain
            for domain in candidate_domains
            if domain not in commoncrawl_domains
        ]
    )

    contact_rows = iter_valid_contact_rows_from_domain_candidates(
        candidates_by_domain,
        commoncrawl_domains=commoncrawl_domains,
        nameservers_by_domain=nameservers_by_domain,
        resolved_at=resolved_timestamp,
    )
    if log is not None:
        log(
            "Built Czech company contact candidates: domains=%s commoncrawl_domains=%s dns_domains=%s",
            len(candidate_domains),
            len(commoncrawl_domains),
            sum(1 for nameservers in nameservers_by_domain.values() if nameservers),
        )
    return _replace_contact_table(clickhouse_client, contact_rows)


def nameservers_for_domain(domain: str) -> tuple[str, ...]:
    parent_zone = _parent_zone_for_domain(domain)
    if parent_zone == "":
        return ()
    parent_nameserver_addresses = _parent_nameserver_addresses(parent_zone)
    if not parent_nameserver_addresses:
        return ()
    return _resolve_domain_nameservers_from_parent(
        domain,
        parent_nameserver_addresses,
    )


def _parent_zone_for_domain(domain: str) -> str:
    extracted = _TLD_EXTRACT(domain)
    return extracted.suffix.lower()


def _parent_nameserver_addresses(parent_zone: str) -> tuple[str, ...]:
    return _nameserver_addresses(_recursive_nameservers(parent_zone))


def _recursive_nameservers(domain: str) -> tuple[str, ...]:
    try:
        answers = dns.resolver.resolve(
            domain,
            "NS",
            lifetime=DNS_QUERY_TIMEOUT_SECONDS,
        )
    except dns.exception.DNSException:
        return ()
    return _answer_nameservers(answers)


def _nameserver_addresses(nameservers: Sequence[str]) -> tuple[str, ...]:
    addresses: set[str] = set()
    for nameserver in nameservers:
        for record_type in ("A", "AAAA"):
            try:
                answers = dns.resolver.resolve(
                    nameserver,
                    record_type,
                    lifetime=DNS_QUERY_TIMEOUT_SECONDS,
                )
            except dns.exception.DNSException:
                continue
            addresses.update(str(answer).rstrip(".") for answer in answers)
    return tuple(sorted(addresses))


def _resolve_domain_nameservers_from_parent(
    domain: str,
    parent_nameserver_addresses: Sequence[str],
) -> tuple[str, ...]:
    for address in parent_nameserver_addresses:
        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = [address]
        resolver.timeout = DNS_QUERY_TIMEOUT_SECONDS
        resolver.lifetime = DNS_QUERY_TIMEOUT_SECONDS
        try:
            answers = resolver.resolve(
                domain,
                "NS",
                lifetime=DNS_QUERY_TIMEOUT_SECONDS,
            )
        except dns.resolver.NoAnswer as exc:
            nameservers = _authority_nameservers(exc.response())
            if nameservers:
                return nameservers
            continue
        except dns.exception.DNSException:
            continue
        nameservers = _answer_nameservers(answers)
        if nameservers:
            return nameservers
    return ()


def _answer_nameservers(answers: Any) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                str(getattr(answer, "target", answer)).rstrip(".").lower()
                for answer in answers
            }
        )
    )


def _authority_nameservers(response: Any | None) -> tuple[str, ...]:
    if response is None:
        return ()
    nameservers: set[str] = set()
    for rrset in response.authority:
        if rrset.rdtype != dns.rdatatype.NS:
            continue
        nameservers.update(str(record.target).rstrip(".").lower() for record in rrset)
    return tuple(sorted(nameservers))


def _resolve_nameservers_concurrently(domains: Sequence[str]) -> dict[str, tuple[str, ...]]:
    unique_domains = tuple(dict.fromkeys(domains))
    if not unique_domains:
        return {}

    domains_by_parent_zone: dict[str, list[str]] = {}
    results: dict[str, tuple[str, ...]] = {}
    for domain in unique_domains:
        parent_zone = _parent_zone_for_domain(domain)
        if parent_zone == "":
            results[domain] = ()
            continue
        domains_by_parent_zone.setdefault(parent_zone, []).append(domain)

    parent_addresses_by_zone = {
        parent_zone: _parent_nameserver_addresses(parent_zone)
        for parent_zone in sorted(domains_by_parent_zone)
    }

    with ThreadPoolExecutor(max_workers=DNS_RESOLVE_WORKERS) as executor:
        future_by_domain = {}
        for parent_zone, zone_domains in domains_by_parent_zone.items():
            parent_addresses = parent_addresses_by_zone[parent_zone]
            if not parent_addresses:
                results.update({domain: () for domain in zone_domains})
                continue
            for domain in zone_domains:
                future_by_domain[
                    executor.submit(
                        _resolve_domain_nameservers_from_parent,
                        domain,
                        parent_addresses,
                    )
                ] = domain
        for future in as_completed(future_by_domain):
            domain = future_by_domain[future]
            try:
                results[domain] = tuple(future.result())
            except OSError:
                results[domain] = ()
    return results


def _append_candidate(
    candidates: list[ContactCandidate],
    seen: set[tuple[str, str]],
    *,
    ico: str,
    contact_type: str,
    contact_value: str,
    domain: str,
) -> None:
    key = (contact_type, contact_value)
    if key in seen:
        return
    seen.add(key)
    candidates.append(
        ContactCandidate(
            ico=ico,
            contact_type=contact_type,
            contact_value=contact_value,
            domain=domain,
        )
    )


def _normalized_domain_contact_value(raw: str) -> str:
    stripped = raw.strip().rstrip(".,;:")
    host = website_host(stripped)
    return host or stripped.lower()


def _validated_domain(
    domain: str,
    *,
    commoncrawl_domains: set[str],
    nameservers_by_domain: dict[str, tuple[str, ...]],
) -> tuple[str, float] | None:
    if domain in commoncrawl_domains:
        return "commoncrawl", COMMONCRAWL_CONFIDENCE
    if nameservers_by_domain.get(domain):
        return "dns", DNS_CONFIDENCE
    return None


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
            "candidate_filter": _CANDIDATE_NAME_FILTER,
            "batch_size": batch_size,
            "after_ico": after_ico,
        },
    )
    return [(str(row[0]), str(row[1])) for row in rows]


def _merge_domain_candidates(
    target: dict[str, list[ContactCandidate]],
    source: dict[str, list[ContactCandidate]],
) -> None:
    seen = {
        (domain, candidate.ico, candidate.contact_type, candidate.contact_value)
        for domain, candidates in target.items()
        for candidate in candidates
    }
    for domain, candidates in source.items():
        target_candidates = target.setdefault(domain, [])
        for candidate in candidates:
            key = (domain, candidate.ico, candidate.contact_type, candidate.contact_value)
            if key in seen:
                continue
            seen.add(key)
            target_candidates.append(candidate)


def _commoncrawl_domains(
    clickhouse_client: Any,
    candidate_domains: Sequence[str],
) -> set[str]:
    found: set[str] = set()
    for batch in _batches(candidate_domains, CLICKHOUSE_QUERY_BATCH_SIZE):
        rows = clickhouse_client.execute(
            """
            SELECT DISTINCT root_domain
            FROM corpscout.commoncrawl_domains FINAL
            WHERE root_domain IN %(domains)s
            """,
            {"domains": tuple(batch)},
        )
        found.update(str(row[0]) for row in rows)
    return found


def _replace_contact_table(
    clickhouse_client: Any,
    rows: Iterable[dict[str, object]],
) -> dict[str, int]:
    target = tables.QUALIFIED_COMPANY_CONTACTS_TABLE
    stage = f"corpscout._tmp_{tables.COMPANY_CONTACTS_TABLE_CH}_{uuid.uuid4().hex}"
    primary_error: Exception | None = None
    try:
        clickhouse_client.execute(f"CREATE TABLE {stage} AS {target}")
        counts = _insert_contact_rows(clickhouse_client, stage, rows)
        clickhouse_client.execute(f"EXCHANGE TABLES {stage} AND {target}")
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")
        except Exception:
            if primary_error is None:
                raise
    return counts


def _insert_contact_rows(
    clickhouse_client: Any,
    qualified_table: str,
    rows: Iterable[dict[str, object]],
) -> dict[str, int]:
    columns = tables.CZ_COMPANY_CONTACTS_EXPORT_COLUMNS
    counts = {
        "contacts": 0,
        "domains": 0,
        "commoncrawl_validated": 0,
        "dns_validated": 0,
    }
    seen_domains: set[str] = set()
    batch: list[dict[str, object]] = []

    def flush() -> None:
        if not batch:
            return
        value_rows = [tuple(row[column] for column in columns) for row in batch]
        insert_rows = getattr(clickhouse_client, "insert_rows", None)
        if callable(insert_rows):
            insert_rows(
                qualified_table.rsplit(".", maxsplit=1)[-1],
                value_rows,
                columns=columns,
                database=qualified_table.split(".", maxsplit=1)[0],
            )
        else:
            clickhouse_client.execute(
                f"INSERT INTO {qualified_table} ({_column_list(columns)}) VALUES",
                value_rows,
            )
        batch.clear()

    for row in rows:
        counts["contacts"] += 1
        domain = str(row["domain"])
        if domain not in seen_domains:
            seen_domains.add(domain)
            counts["domains"] += 1
        if row["domain_source"] == "commoncrawl":
            counts["commoncrawl_validated"] += 1
        elif row["domain_source"] == "dns":
            counts["dns_validated"] += 1
        batch.append(row)
        if len(batch) >= CLICKHOUSE_INSERT_BATCH_SIZE:
            flush()
    flush()
    return counts


def _column_list(columns: Sequence[str]) -> str:
    return ", ".join(f"`{column}`" for column in columns)


def _batches(values: Sequence[Any], batch_size: int) -> Iterable[Sequence[Any]]:
    for offset in range(0, len(values), batch_size):
        yield values[offset : offset + batch_size]
