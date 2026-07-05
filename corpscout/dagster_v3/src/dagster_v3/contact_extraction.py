"""Shared contact-candidate extraction from free text (emails + domains, IDN-aware),
CommonCrawl/DNS domain validation, and the atomic stage/EXCHANGE table replacers
(`replace_contact_table` for Python-materialized rows, `replace_table_from_select`
for a CH-native INSERT-SELECT).

Per-country modules own their candidate SQL, id semantics, and table names.
"""

import os
import re
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import dns.exception
import dns.rdatatype
import dns.resolver

from dagster_v3.domains import root_domain, website_host

COMMONCRAWL_CONFIDENCE = 0.95
DNS_CONFIDENCE = 0.70
DNS_RESOLVE_WORKERS = 32
DNS_QUERY_TIMEOUT_SECONDS = 2.0
CLICKHOUSE_INSERT_BATCH_SIZE = 50_000

# Spec-owned vocabularies (docs/superpowers/specs/2026-07-04-company-contacts-
# domains-standard-design.md). Closed sets — extend only via the spec.
CONTACT_TYPE_VALUES = frozenset(
    {"email", "phone", "mobile", "fax", "website", "domain_in_name", "other"}
)
DOMAIN_SOURCE_VALUES = frozenset({"website", "email", "name_embedded"})
VALIDATION_METHOD_VALUES = frozenset({"", "commoncrawl", "dns"})
WEBSITE_CONFIDENCE = 1.0
EMAIL_UNIQUE_CONFIDENCE = 0.9

# Union of the Estonia + Brazil provider denylists (18 common to both + 24
# unique to Estonia's RU/LV/EE webmail coverage + 6 unique to Brazil's .com.br
# portals = 48 entries) — one source of truth; countries swap imports in their
# conversion phases (drift-guarded by tests until then).
EMAIL_PROVIDER_DENYLIST = frozenset({
    "aol.com", "apollo.lv", "bk.ru", "bol.com.br", "eesti.ee", "fastmail.com",
    "globo.com", "gmail.com", "gmx.com", "gmx.de", "gmx.net", "googlemail.com",
    "hey.com", "hot.ee", "hotmail.com", "icloud.com", "ig.com.br", "inbox.lv",
    "inbox.ru", "internet.ru", "list.ru", "live.com", "mac.com", "mail.com",
    "mail.ee", "mail.ru", "me.com", "msn.com", "neti.ee", "one.lv", "online.ee",
    "outlook.com", "pm.me", "proton.me", "protonmail.com", "r7.com",
    "rambler.ru", "rocketmail.com", "starman.ee", "terra.com.br", "tutanota.com",
    "uol.com.br", "ya.ru", "yahoo.com", "yandex.com", "yandex.ru", "ymail.com",
    "zoho.com",
})
EMAIL_DOMAIN_MAX_COMPANIES = 1

COMPANY_CONTACTS_COLUMNS = (
    "country_iso2", "source_slug", "source_run_id", "source_record_id",
    "registry_id", "contact_type", "contact_type_raw", "contact_value",
    "source_field", "is_current", "valid_to", "source_url", "resolved_at",
)
COMPANY_DOMAINS_COLUMNS = (
    "country_iso2", "source_slug", "source_run_id", "source_record_id",
    "registry_id", "domain", "domain_source", "validation_method", "confidence",
    "website_url", "website_normalized_url", "website_host",
    "is_current", "is_primary", "resolved_at",
)

_CONTACT_TYPE_BY_CANDIDATE_TYPE = {"domain": "domain_in_name", "email": "email"}

# Internal batching size for CommonCrawl domain-membership lookups. Not part of the
# public interface (the Czech-specific CLICKHOUSE_QUERY_BATCH_SIZE/
# CLICKHOUSE_COMPANY_BATCH_SIZE constants stay in czech_ares — they also govern the
# per-country candidate-row scan batch size, which has no shared meaning here).
_COMMONCRAWL_QUERY_BATCH_SIZE = 10_000

# Label classes gain the Latin-1 Supplement + Latin Extended-A range (À-ſ) so IDN
# domains (e.g. Latvian ā/č/š/ž/ģ/ķ/ļ/ņ) are recognized as candidates alongside ASCII.
EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+\-]+@(?:[A-Z0-9À-ſ](?:[A-Z0-9À-ſ\-]{0,61}[A-Z0-9À-ſ])?\.)+"
    r"[A-Z0-9À-ſ](?:[A-Z0-9À-ſ\-]{0,61}[A-Z0-9À-ſ])?\b",
    re.IGNORECASE,
)
DOMAIN_RE = re.compile(
    r"(?<!@)\b(?:https?://)?(?:[A-Z0-9À-ſ](?:[A-Z0-9À-ſ\-]{0,61}[A-Z0-9À-ſ])?\.)+"
    r"[A-Z0-9À-ſ](?:[A-Z0-9À-ſ\-]{0,61}[A-Z0-9À-ſ])?(?:/[^\s,;)]*)?",
    re.IGNORECASE,
)
# ClickHouse RE2 match() prefilter — broader than the Python regexes above, just cheap
# enough to cut down rows before Python-side parsing. RE2 supports \p{L} (any Unicode
# letter) for the bare-domain alternative's leading label; the TLD stays [A-Za-z]{2,}
# since real TLDs (ccTLD or gTLD) are always ASCII, even for IDN domains (the TLD is
# never punycode-encoded on its own).
CANDIDATE_TEXT_FILTER = "(?i)(@|https?://|www\\.|[\\p{L}0-9][\\p{L}0-9-]*\\.[A-Za-z]{2,})"

# Dotted abbreviations in legal names (e.g. "V.J.V.Ltd", "NADA TRADING CO.LTD.") parse
# as real domains because .ltd/.co/.group/etc. are real TLDs. These labels, when they
# make up the WHOLE matched host, mean "abbreviation", never a real company domain.
_LEGAL_FORM_LABELS = frozenset(
    {"co", "ltd", "inc", "pte", "gmbh", "llc", "plc", "corp", "limited", "sia", "as", "ik"}
)

# Academic/professional titles that form junk domains under real TLDs (.ing is a
# gTLD, so "Dr.Ing." parses as dr.ing) — same all-labels rule as legal forms.
_TITLE_LABELS = frozenset(
    {"dr", "dipl", "eur", "rndr", "mudr", "judr", "phdr", "ing", "sc",
     "phd", "mba", "bsc", "msc", "mgr", "bc"}
)


@dataclass(frozen=True)
class ContactCandidate:
    record_id: str
    contact_type: str
    contact_value: str
    domain: str


def idna_ascii(domain: str) -> str | None:
    """ASCII (punycode) form of a possibly-IDN domain; None if unencodable."""
    try:
        import idna

        return idna.encode(domain).decode("ascii")
    except Exception:  # noqa: BLE001 - any encode failure means "not a resolvable IDN"
        # idna.encode() enforces IDNA2008's ban on hyphens in a label's 3rd/4th
        # position (avoids collisions with the "xn--" ACE prefix) even for domains
        # that are already plain ASCII and perfectly valid DNS names, e.g.
        # "aa--bb.com" (empirically confirmed against idna==3.18). Fall back to the
        # untouched input for those; a non-ASCII domain idna can't encode has no
        # ASCII form at all.
        if domain and domain.isascii():
            return domain
        return None


def extract_contact_candidates(
    *, record_id: str, text: str, home_tlds: frozenset[str] = frozenset()
) -> list[ContactCandidate]:
    """Extract email and domain candidates embedded in free text (IDN-aware).

    `home_tlds` exempts a country's own ccTLD (e.g. {"cz"}, {"lv"}) from the
    single-char-nonhome abbreviation guard below — see `_rejected_as_abbreviation`.
    """
    candidates: list[ContactCandidate] = []
    seen: set[tuple[str, str]] = set()
    extracted: list[tuple[int, str, str, str]] = []

    for match in EMAIL_RE.finditer(text):
        value = match.group(0).lower()
        email_domain = value.rsplit("@", maxsplit=1)[-1]
        domain = root_domain(email_domain)
        if not domain:
            continue
        if _rejected_as_abbreviation(
            labels=email_domain.split("."),
            registrable=domain,
            home_tlds=home_tlds,
            check_initials=False,
        ):
            continue
        extracted.append((match.start(), "email", value, domain))

    for match in DOMAIN_RE.finditer(text):
        value = _normalized_domain_contact_value(match.group(0))
        domain = root_domain(value)
        if not domain:
            continue
        if _rejected_as_abbreviation(
            labels=value.split("."),
            registrable=domain,
            home_tlds=home_tlds,
            check_initials=True,
        ):
            continue
        extracted.append((match.start(), "domain", value, domain))

    for _position, contact_type, contact_value, domain in sorted(extracted):
        _append_candidate(
            candidates,
            seen,
            record_id=record_id,
            contact_type=contact_type,
            contact_value=contact_value,
            domain=domain,
        )

    return candidates


def extract_contact_candidates_by_domain(
    rows: Iterable[tuple[str, str]],
    *,
    home_tlds: frozenset[str] = frozenset(),
) -> dict[str, list[ContactCandidate]]:
    candidates_by_domain: dict[str, list[ContactCandidate]] = {}
    seen: set[tuple[str, str, str]] = set()
    for record_id_raw, text_raw in rows:
        record_id = str(record_id_raw)
        text = str(text_raw)
        for parsed_candidate in extract_contact_candidates(
            record_id=record_id, text=text, home_tlds=home_tlds
        ):
            candidate = ContactCandidate(
                record_id=parsed_candidate.record_id,
                contact_type=parsed_candidate.contact_type,
                contact_value=parsed_candidate.contact_value,
                domain=parsed_candidate.domain,
            )
            key = (candidate.record_id, candidate.contact_type, candidate.contact_value)
            if key in seen:
                continue
            seen.add(key)
            candidates_by_domain.setdefault(candidate.domain, []).append(candidate)
    return candidates_by_domain


def merge_domain_candidates(
    into: dict[str, list[ContactCandidate]],
    new: dict[str, list[ContactCandidate]],
) -> None:
    seen = {
        (domain, candidate.record_id, candidate.contact_type, candidate.contact_value)
        for domain, candidates in into.items()
        for candidate in candidates
    }
    for domain, candidates in new.items():
        target_candidates = into.setdefault(domain, [])
        for candidate in candidates:
            key = (domain, candidate.record_id, candidate.contact_type, candidate.contact_value)
            if key in seen:
                continue
            seen.add(key)
            target_candidates.append(candidate)


def iter_contact_fact_rows(
    candidates_by_domain: dict[str, list[ContactCandidate]],
    *,
    country_iso2: str,
    source_slug: str,
    source_field: str,
    resolved_at: datetime,
    source_run_id: str = "",
) -> Iterator[tuple]:
    """Canonical <src>_company_contacts rows (COMPANY_CONTACTS_COLUMNS order) for
    ALL guard-surviving candidates — facts are validation-independent.
    """
    for domain in sorted(candidates_by_domain):
        for candidate in candidates_by_domain[domain]:
            yield (
                country_iso2,
                source_slug,
                source_run_id,
                candidate.record_id,
                candidate.record_id,
                _CONTACT_TYPE_BY_CANDIDATE_TYPE[candidate.contact_type],
                "",                      # contact_type_raw: no source label distinct from ours
                candidate.contact_value,
                source_field,
                1,                       # is_current
                None,                    # valid_to
                "",                      # source_url
                resolved_at,
            )


def iter_company_domain_rows(
    candidates_by_domain: dict[str, list[ContactCandidate]],
    *,
    commoncrawl_domains: set[str],
    nameservers_by_domain: dict[str, tuple[str, ...]],
    country_iso2: str,
    source_slug: str,
    resolved_at: datetime,
    source_run_id: str = "",
) -> Iterator[tuple]:
    """Canonical <src>_company_domains rows (COMPANY_DOMAINS_COLUMNS order) for
    validated domains only, one row per (registry_id, domain). is_primary is 0 —
    apply elect_primary_domains() on the collected rows.
    """
    for domain in sorted(candidates_by_domain):
        validation = _validated_domain(
            domain,
            commoncrawl_domains=commoncrawl_domains,
            nameservers_by_domain=nameservers_by_domain,
        )
        if validation is None:
            continue
        validation_method, confidence = validation
        seen_registry_ids: set[str] = set()
        for candidate in candidates_by_domain[domain]:
            if candidate.record_id in seen_registry_ids:
                continue
            seen_registry_ids.add(candidate.record_id)
            yield (
                country_iso2,
                source_slug,
                source_run_id,
                candidate.record_id,
                candidate.record_id,
                domain,
                "name_embedded",
                validation_method,
                confidence,
                "", "", "",              # website columns: not a website-sourced domain
                1,                       # is_current
                0,                       # is_primary — election pass sets the winner
                resolved_at,
            )


def elect_primary_domains(rows: Iterable[tuple]) -> list[tuple]:
    """Set is_primary=1 on exactly one row per registry_id (spec election rule:
    website-sourced first, then is_current, then confidence, then shortest domain,
    then alphabetical). Rows are COMPANY_DOMAINS_COLUMNS-ordered tuples.
    """
    materialized = [list(row) for row in rows]
    best_index_by_registry: dict[str, int] = {}
    for index, row in enumerate(materialized):
        registry_id = row[4]
        current_best = best_index_by_registry.get(registry_id)
        if current_best is None or _election_key(row) > _election_key(materialized[current_best]):
            best_index_by_registry[registry_id] = index
    for index in best_index_by_registry.values():
        materialized[index][13] = 1
    return [tuple(row) for row in materialized]


def _election_key(row: Sequence[Any]) -> tuple:
    domain = row[5]
    return (row[6] == "website", bool(row[12]), row[8], -len(domain), _reverse_alpha(domain))


def _reverse_alpha(domain: str) -> tuple[int, ...]:
    # max() with this key picks the alphabetically FIRST domain on final tie.
    return tuple(-ord(ch) for ch in domain)


def pinned_dns_servers() -> tuple[str, ...]:
    """Resolver IPs from CONTACT_EXTRACTION_DNS_SERVERS (comma-separated), or ().

    When set, DNS validation runs in pinned stub mode: plain NS/SOA queries
    against ONLY these servers, instead of the default parent-zone walk that
    queries arbitrary authoritative nameservers directly. Use a dedicated
    recursive resolver (e.g. unbound on a host you control) when the local
    network's resolver path is flaky or rate-limits sustained lookups.
    """
    raw = os.environ.get("CONTACT_EXTRACTION_DNS_SERVERS", "")
    return tuple(server.strip() for server in raw.split(",") if server.strip())


def _stub_resolver(servers: Sequence[str]) -> dns.resolver.Resolver:
    resolver = dns.resolver.Resolver(configure=False)
    resolver.nameservers = list(servers)
    resolver.timeout = DNS_QUERY_TIMEOUT_SECONDS
    resolver.lifetime = DNS_QUERY_TIMEOUT_SECONDS
    return resolver


def _pinned_stub_nameservers(ascii_domain: str, servers: Sequence[str]) -> tuple[str, ...]:
    """Validate a domain via plain stub queries against the pinned servers.

    NS answer wins; on NoAnswer (NS lives at a zone cut above), an SOA answer
    still proves the name exists in DNS — slightly stricter than the parent
    walk (lame delegations SERVFAIL here), acceptable for a validation gate.
    """
    resolver = _stub_resolver(servers)
    try:
        return _answer_nameservers(resolver.resolve(ascii_domain, "NS"))
    except dns.resolver.NoAnswer:
        pass
    except dns.exception.DNSException:
        return ()
    try:
        answers = resolver.resolve(ascii_domain, "SOA")
    except dns.exception.DNSException:
        return ()
    return tuple(
        sorted({str(getattr(answer, "mname", answer)).rstrip(".").lower() for answer in answers})
    )


def nameservers_for_domain(domain: str) -> tuple[str, ...]:
    ascii_domain = idna_ascii(domain)
    if ascii_domain is None:
        return ()
    pinned = pinned_dns_servers()
    if pinned:
        return _pinned_stub_nameservers(ascii_domain, pinned)
    parent_zone = _parent_zone_for_domain(ascii_domain)
    if parent_zone == "":
        return ()
    parent_nameserver_addresses = _parent_nameserver_addresses(parent_zone)
    if not parent_nameserver_addresses:
        return ()
    return _resolve_domain_nameservers_from_parent(
        ascii_domain,
        parent_nameserver_addresses,
    )


def _parent_zone_for_domain(domain: str) -> str:
    # No new tldextract.TLDExtract instance here (per module contract) — derive the
    # public suffix from the shared root_domain() instead. root_domain() returns
    # "<registrable-label>.<public-suffix>" (e.g. "example.co.uk"), so the public
    # suffix is everything after the first label, which is exactly tldextract's
    # `.suffix` for any real (non-bare-suffix) domain. Empirically verified equivalent
    # to tldextract(domain).suffix across single- and multi-label suffixes (cz, co.uk,
    # com, lv). Bare-suffix inputs (e.g. domain == "cz" itself) return "" here instead
    # of the suffix itself, but domains reaching this function always come from
    # root_domain()-validated candidates, so a bare suffix never occurs in practice.
    registrable = root_domain(domain)
    if not registrable or "." not in registrable:
        return ""
    return registrable.split(".", maxsplit=1)[-1]


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


def resolve_nameservers_concurrently(domains: Sequence[str]) -> dict[str, tuple[str, ...]]:
    """Resolve nameservers for each of `domains` concurrently.

    Mirrors `nameservers_for_domain`'s explicit `idna_ascii` encoding (rather than
    relying on dnspython's implicit IDNA2003 handling) so the actual DNS query uses
    the IDNA2008 punycode form; a domain `idna_ascii` can't encode resolves to `()`.
    The returned dict is keyed by the ORIGINAL (unicode) domain in `domains`, since
    that's the form callers use to key their candidates.
    """
    unique_domains = tuple(dict.fromkeys(domains))
    if not unique_domains:
        return {}

    results: dict[str, tuple[str, ...]] = {}
    ascii_domain_by_original: dict[str, str] = {}
    for domain in unique_domains:
        ascii_domain = idna_ascii(domain)
        if ascii_domain is None:
            results[domain] = ()
            continue
        ascii_domain_by_original[domain] = ascii_domain

    pinned = pinned_dns_servers()
    if pinned:
        with ThreadPoolExecutor(max_workers=DNS_RESOLVE_WORKERS) as executor:
            future_by_domain = {
                executor.submit(_pinned_stub_nameservers, ascii_domain, pinned): original
                for original, ascii_domain in ascii_domain_by_original.items()
            }
            for future in as_completed(future_by_domain):
                results[future_by_domain[future]] = future.result()
        return results

    domains_by_parent_zone: dict[str, list[str]] = {}
    for original_domain, ascii_domain in ascii_domain_by_original.items():
        parent_zone = _parent_zone_for_domain(ascii_domain)
        if parent_zone == "":
            results[original_domain] = ()
            continue
        domains_by_parent_zone.setdefault(parent_zone, []).append(original_domain)

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
            for original_domain in zone_domains:
                future_by_domain[
                    executor.submit(
                        _resolve_domain_nameservers_from_parent,
                        ascii_domain_by_original[original_domain],
                        parent_addresses,
                    )
                ] = original_domain
        for future in as_completed(future_by_domain):
            original_domain = future_by_domain[future]
            try:
                results[original_domain] = tuple(future.result())
            except OSError:
                results[original_domain] = ()
    return results


def _append_candidate(
    candidates: list[ContactCandidate],
    seen: set[tuple[str, str]],
    *,
    record_id: str,
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
            record_id=record_id,
            contact_type=contact_type,
            contact_value=contact_value,
            domain=domain,
        )
    )


def _normalized_domain_contact_value(raw: str) -> str:
    stripped = raw.strip().rstrip(".,;:")
    host = website_host(stripped)
    return host or stripped.lower()


def _rejected_as_abbreviation(
    *,
    labels: Sequence[str],
    registrable: str,
    home_tlds: frozenset[str],
    check_initials: bool,
) -> bool:
    """True if `labels` (the raw matched host's dot-separated labels, lowercased)
    look like a dotted abbreviation rather than a real domain:

    1. initials: >=2 single-character labels in the raw match (e.g. "v.j.v.ltd") —
       DOMAIN candidates only; an email's local part legitimately uses initials
       (j.k.novak@firma.cz), so callers skip this clause for emails.
    2. legal-form / title: every label is a bare legal-form abbreviation (e.g.
       "co.ltd", "pte.ltd") OR every label is an academic/professional title
       token (e.g. "dr.ing", from "Dr.Ing. Jan Novák" — .ing is a real gTLD) —
       neither is ever a real registrable domain. Also applied to the
       registrable domain's own labels, so a longer match like "vinse.co.ltd"
       (from "HD VinSe.co.ltd,s.r.o.") can't attribute co.ltd either. The two
       label sets are kept separate (not unioned) so a mixed abbreviation like
       "co.dr" still fails both all()-checks rather than being conflated.
    3. single-char-nonhome: the registrable domain's first label is a single
       character (e.g. "a.group", "v.ltd") and its suffix isn't one of the
       country's own home TLDs (a hypothetical "b.cz" for Czech survives).
    """
    if not labels:
        return False
    if check_initials and sum(1 for label in labels if len(label) == 1) >= 2:
        return True
    if all(label in _LEGAL_FORM_LABELS for label in labels) or all(
        label in _TITLE_LABELS for label in labels
    ):
        return True
    if all(label in _LEGAL_FORM_LABELS for label in registrable.split(".")) or all(
        label in _TITLE_LABELS for label in registrable.split(".")
    ):
        return True
    registrable_first, _dot, registrable_suffix = registrable.partition(".")
    if len(registrable_first) == 1 and registrable_suffix not in home_tlds:
        return True
    return False


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


def commoncrawl_domains(
    clickhouse_client: Any,
    domains: Sequence[str],
) -> set[str]:
    """Which of `domains` are known to CommonCrawl, tried in both unicode and idna
    (punycode) form — a hit on either form validates the (unicode) input domain."""
    query_form_by_domain: dict[str, str] = {}
    for domain in domains:
        query_form_by_domain[domain] = domain
        ascii_domain = idna_ascii(domain)
        if ascii_domain is not None and ascii_domain != domain:
            query_form_by_domain[ascii_domain] = domain

    query_forms = tuple(query_form_by_domain)
    found: set[str] = set()
    for batch in _batches(query_forms, _COMMONCRAWL_QUERY_BATCH_SIZE):
        rows = clickhouse_client.execute(
            """
            SELECT DISTINCT root_domain
            FROM corpscout.commoncrawl_domains FINAL
            WHERE root_domain IN %(domains)s
            """,
            {"domains": tuple(batch)},
        )
        for row in rows:
            matched_domain = query_form_by_domain.get(str(row[0]))
            if matched_domain is not None:
                found.add(matched_domain)
    return found


def replace_contact_table(
    clickhouse_client: Any,
    *,
    qualified_table: str,
    columns: Sequence[str],
    rows: Iterable[tuple],
    batch_size: int = CLICKHOUSE_INSERT_BATCH_SIZE,
    log: Callable[..., object] | None = None,
) -> int:
    """Atomically replace `qualified_table`'s contents via stage table + EXCHANGE
    TABLES. Returns the number of rows written."""
    database, _, bare_table = qualified_table.rpartition(".")
    stage = f"{database}._tmp_{bare_table}_{uuid.uuid4().hex}"
    primary_error: Exception | None = None
    written = 0
    try:
        clickhouse_client.execute(f"CREATE TABLE {stage} AS {qualified_table}")
        written = _insert_contact_rows(
            clickhouse_client,
            stage,
            rows,
            columns=columns,
            batch_size=batch_size,
        )
        clickhouse_client.execute(f"EXCHANGE TABLES {stage} AND {qualified_table}")
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")
        except Exception:
            if primary_error is None:
                raise
    if log is not None:
        log(
            "Replaced contact table %s: rows_written=%s",
            qualified_table,
            written,
        )
    return written


def replace_table_from_select(
    clickhouse_client: Any,
    *,
    qualified_table: str,
    columns: Sequence[str],
    select_sql: str,
    log: Callable[..., object] | None = None,
) -> int:
    """Atomically replace `qualified_table`'s contents from a SELECT over other
    ClickHouse tables (stage CREATE AS target -> INSERT INTO stage (cols) SELECT
    -> EXCHANGE TABLES -> drop stage). CH-native, INSERT-SELECT sibling of
    `replace_contact_table` (which batches Python-materialized rows) for the
    canonical-pair derivation assets, which read straight from other ClickHouse
    tables; per-source SELECTs are duplicated in their backfill migrations by
    design. Returns the row count read back after the exchange."""
    database, _, bare_table = qualified_table.rpartition(".")
    stage = f"{database}._tmp_{bare_table}_{uuid.uuid4().hex}"
    clickhouse_client.execute(f"CREATE TABLE {stage} AS {qualified_table}")
    try:
        clickhouse_client.execute(
            f"INSERT INTO {stage} ({', '.join(columns)}) {select_sql}"
        )
        clickhouse_client.execute(f"EXCHANGE TABLES {stage} AND {qualified_table}")
    finally:
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")
    written = int(clickhouse_client.execute(f"SELECT count() FROM {qualified_table}")[0][0])
    if log is not None:
        log("Replaced %s from select: rows=%s", qualified_table, written)
    return written


def _insert_contact_rows(
    clickhouse_client: Any,
    qualified_table: str,
    rows: Iterable[tuple],
    *,
    columns: Sequence[str],
    batch_size: int,
) -> int:
    written = 0
    batch: list[tuple] = []

    def flush() -> None:
        nonlocal written
        if not batch:
            return
        insert_rows = getattr(clickhouse_client, "insert_rows", None)
        if callable(insert_rows):
            insert_rows(
                qualified_table.rsplit(".", maxsplit=1)[-1],
                list(batch),
                columns=columns,
                database=qualified_table.split(".", maxsplit=1)[0],
            )
        else:
            clickhouse_client.execute(
                f"INSERT INTO {qualified_table} ({_column_list(columns)}) VALUES",
                list(batch),
            )
        written += len(batch)
        batch.clear()

    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            flush()
    flush()
    return written


def _column_list(columns: Sequence[str]) -> str:
    return ", ".join(f"`{column}`" for column in columns)


def _batches(values: Sequence[Any], batch_size: int) -> Iterable[Sequence[Any]]:
    for offset in range(0, len(values), batch_size):
        yield values[offset : offset + batch_size]
