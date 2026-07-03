from __future__ import annotations

import re
import socket
import uuid
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.czech_ares import tables
from dagster_v3.domains import root_domain, website_host

CONTACTS_SOURCE_SLUG = "czech_ares_contact_extraction"
COMMONCRAWL_CONFIDENCE = 0.95
DNS_CONFIDENCE = 0.70
DNS_RESOLVE_WORKERS = 32
CLICKHOUSE_QUERY_BATCH_SIZE = 10_000
CLICKHOUSE_INSERT_BATCH_SIZE = 50_000

_EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+\-]+@(?:[A-Z0-9](?:[A-Z0-9\-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,63}\b",
    re.IGNORECASE,
)
_DOMAIN_RE = re.compile(
    r"(?<!@)\b(?:https?://)?(?:[A-Z0-9](?:[A-Z0-9\-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,63}(?:/[^\s,;)]*)?",
    re.IGNORECASE,
)
_CANDIDATE_NAME_FILTER = (
    "(?i)(@|https?://|www\\.|[A-Za-z0-9][A-Za-z0-9-]*\\.[A-Za-z]{2,})"
)


@dataclass(frozen=True)
class ContactCandidate:
    ico: str
    company_name: str
    contact_type: str
    contact_value: str
    domain: str


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
            company_name=company_name,
            contact_type=contact_type,
            contact_value=contact_value,
            domain=domain,
        )

    return candidates


def build_contact_rows_from_company_rows(
    company_rows: Iterable[tuple[str, str, str]],
    *,
    commoncrawl_domains: set[str],
    resolve_domain: Callable[[str], bool],
    source_run_id: str,
    resolved_at: datetime,
    source_url: str = tables.RES_DATA_URL,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    validation_cache: dict[str, tuple[str, float] | None] = {}

    for ico, company_name, candidate_text in company_rows:
        for candidate in extract_contact_candidates(
            ico=str(ico), company_name=str(candidate_text)
        ):
            validation = _validated_domain(
                candidate.domain,
                commoncrawl_domains=commoncrawl_domains,
                resolve_domain=resolve_domain,
                validation_cache=validation_cache,
            )
            if validation is None:
                continue
            domain_source, confidence = validation
            rows.append(
                {
                    "country_iso2": "CZ",
                    "source_slug": CONTACTS_SOURCE_SLUG,
                    "source_run_id": source_run_id,
                    "source_record_id": str(ico),
                    "ico": str(ico),
                    "company_name": str(company_name),
                    "contact_type": candidate.contact_type,
                    "contact_value": candidate.contact_value,
                    "domain": candidate.domain,
                    "domain_source": domain_source,
                    "confidence": confidence,
                    "source_url": source_url,
                    "resolved_at": resolved_at,
                }
            )

    return rows


def replace_czech_company_contacts_clickhouse(
    *,
    clickhouse_client: Any,
    source_run_id: str,
    resolved_at: datetime | None = None,
    log: Callable[..., object] | None = None,
    resolve_domain: Callable[[str], bool] | None = None,
) -> dict[str, int]:
    resolved_timestamp = resolved_at or datetime.now(UTC)
    company_rows = _candidate_company_rows(clickhouse_client)
    candidate_domains = _candidate_domains(company_rows)
    commoncrawl_domains = _commoncrawl_domains(clickhouse_client, candidate_domains)
    if resolve_domain is None:
        dns_results = _resolve_domains_concurrently(
            [
                domain
                for domain in candidate_domains
                if domain not in commoncrawl_domains
            ]
        )

        def resolver(domain: str) -> bool:
            return dns_results.get(domain, False)
    else:
        resolver = resolve_domain
    contact_rows = build_contact_rows_from_company_rows(
        company_rows,
        commoncrawl_domains=commoncrawl_domains,
        resolve_domain=resolver,
        source_run_id=source_run_id,
        resolved_at=resolved_timestamp,
    )
    if log is not None:
        log(
            "Built Czech company contact rows: candidates=%s valid_rows=%s commoncrawl_domains=%s",
            len(candidate_domains),
            len(contact_rows),
            len(commoncrawl_domains),
        )
    return _replace_contact_table(clickhouse_client, contact_rows)


def domain_resolves(domain: str) -> bool:
    try:
        socket.getaddrinfo(domain, None)
    except OSError:
        return False
    return True


def resolve_domains_concurrently(domain: str) -> bool:
    return _resolve_domains_concurrently((domain,)).get(domain, False)


def _resolve_domains_concurrently(domains: Sequence[str]) -> dict[str, bool]:
    unique_domains = tuple(dict.fromkeys(domains))
    if not unique_domains:
        return {}
    results: dict[str, bool] = {}
    with ThreadPoolExecutor(max_workers=DNS_RESOLVE_WORKERS) as executor:
        future_by_domain = {
            executor.submit(domain_resolves, domain): domain
            for domain in unique_domains
        }
        for future in as_completed(future_by_domain):
            domain = future_by_domain[future]
            try:
                results[domain] = bool(future.result())
            except OSError:
                results[domain] = False
    return results


def _append_candidate(
    candidates: list[ContactCandidate],
    seen: set[tuple[str, str]],
    *,
    ico: str,
    company_name: str,
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
            company_name=company_name,
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
    resolve_domain: Callable[[str], bool],
    validation_cache: dict[str, tuple[str, float] | None],
) -> tuple[str, float] | None:
    if domain in validation_cache:
        return validation_cache[domain]
    if domain in commoncrawl_domains:
        validation_cache[domain] = ("commoncrawl", COMMONCRAWL_CONFIDENCE)
        return validation_cache[domain]
    if resolve_domain(domain):
        validation_cache[domain] = ("dns", DNS_CONFIDENCE)
        return validation_cache[domain]
    validation_cache[domain] = None
    return None


def _candidate_company_rows(clickhouse_client: Any) -> list[tuple[str, str, str]]:
    rows = clickhouse_client.execute(
        f"""
        SELECT ico, name, name
        FROM {tables.QUALIFIED_COMPANIES_TABLE} FINAL
        WHERE match(name, %(candidate_filter)s)
        """,
        {"candidate_filter": _CANDIDATE_NAME_FILTER},
    )
    return [(str(row[0]), str(row[1]), str(row[2])) for row in rows]


def _candidate_domains(company_rows: Sequence[tuple[str, str, str]]) -> tuple[str, ...]:
    domains = {
        candidate.domain
        for ico, _company_name, candidate_text in company_rows
        for candidate in extract_contact_candidates(ico=ico, company_name=candidate_text)
    }
    return tuple(sorted(domains))


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
    rows: Sequence[dict[str, object]],
) -> dict[str, int]:
    target = tables.QUALIFIED_COMPANY_CONTACTS_TABLE
    stage = f"corpscout._tmp_{tables.COMPANY_CONTACTS_TABLE_CH}_{uuid.uuid4().hex}"
    primary_error: Exception | None = None
    try:
        clickhouse_client.execute(f"CREATE TABLE {stage} AS {target}")
        _insert_contact_rows(clickhouse_client, stage, rows)
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
    return {
        "contacts": len(rows),
        "domains": len({str(row["domain"]) for row in rows}),
        "commoncrawl_validated": sum(
            1 for row in rows if row["domain_source"] == "commoncrawl"
        ),
        "dns_validated": sum(1 for row in rows if row["domain_source"] == "dns"),
    }


def _insert_contact_rows(
    clickhouse_client: Any,
    qualified_table: str,
    rows: Sequence[dict[str, object]],
) -> None:
    if not rows:
        return
    columns = tables.CZ_COMPANY_CONTACTS_EXPORT_COLUMNS
    for batch in _batches(tuple(rows), CLICKHOUSE_INSERT_BATCH_SIZE):
        value_rows = [tuple(row[column] for column in columns) for row in batch]
        insert_rows = getattr(clickhouse_client, "insert_rows", None)
        if callable(insert_rows):
            insert_rows(
                qualified_table.rsplit(".", maxsplit=1)[-1],
                value_rows,
                columns=columns,
                database=qualified_table.split(".", maxsplit=1)[0],
            )
            continue
        clickhouse_client.execute(
            f"INSERT INTO {qualified_table} ({_column_list(columns)}) VALUES",
            value_rows,
        )


def _column_list(columns: Sequence[str]) -> str:
    return ", ".join(f"`{column}`" for column in columns)


def _batches(values: Sequence[Any], batch_size: int) -> Iterable[Sequence[Any]]:
    for offset in range(0, len(values), batch_size):
        yield values[offset : offset + batch_size]
