"""Batch registration of central identities, without company/source-index writes."""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cache
from hashlib import sha256
from urllib.parse import urlsplit

import tldextract

from corpscout_identity.coordination import (
    DOMAIN_COLUMNS,
    PAGE_COLUMNS,
    WEBSITE_COLUMNS,
    inventory_publication_lock,
)
from corpscout_identity.urls import page_identity

MAX_REGISTRATION_BATCH = 10000
REGISTRATION_SETTINGS = {"async_insert": 0, "max_execution_time": 120, "max_threads": 4}
HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


@cache
def domain_extractor():
    # Match the existing private-suffix policy. Use the package's locked snapshot,
    # not the network or a user's stale external PSL cache.
    return tldextract.TLDExtract(
        suffix_list_urls=(), cache_dir=None, include_psl_private_domains=True
    )


@dataclass(frozen=True)
class DomainIdentity:
    root_domain: str
    domain_id: str


@dataclass(frozen=True)
class WebsiteIdentity:
    root_domain: str
    domain_id: str
    website_origin: str
    website_id: str
    page_url: str
    page_id: str


@dataclass(frozen=True)
class WebsiteObservation:
    identity: WebsiteIdentity
    discovered_at: datetime
    observed_at: datetime | None
    fetched_at: datetime | None


def identify_domain(host: str) -> DomainIdentity:
    canonical = host.rstrip(".").lower().encode("idna").decode("ascii")
    if len(canonical) > 253 or not all(
        HOST_LABEL.fullmatch(label) for label in canonical.split(".")
    ):
        raise ValueError("Expected a valid domain hostname")
    extracted = domain_extractor()(canonical)
    root = extracted.top_domain_under_public_suffix
    if not root and extracted.is_private:
        # A provider's own homepage is valid even when its hostname is listed
        # as a private suffix. Tenant hosts still retain their private boundaries.
        root = domain_extractor()(
            canonical, include_psl_private_domains=False
        ).top_domain_under_public_suffix
    if not root:
        raise ValueError(
            "Hostname has no registrable domain in the pinned public suffix list"
        )
    return DomainIdentity(root, sha256(root.encode("utf-8")).hexdigest())


def identify_website(url: str) -> WebsiteIdentity:
    origin, page = page_identity(url)
    host = urlsplit(origin).hostname
    if host is None:
        raise ValueError("Website URL has no hostname")
    parent = identify_domain(host)
    return WebsiteIdentity(
        parent.root_domain,
        parent.domain_id,
        origin,
        sha256(origin.encode("utf-8")).hexdigest(),
        page,
        sha256(page.encode("utf-8")).hexdigest(),
    )


def validate_timestamp(stamp: datetime) -> None:
    if stamp.tzinfo is None or stamp.utcoffset() is None or stamp.timestamp() <= 0:
        raise ValueError(
            "Identity evidence timestamps must be timezone-aware and after the Unix epoch"
        )


def register_domains(
    client,
    identities: Sequence[DomainIdentity],
    *,
    source: str,
    discovered_at: datetime,
    run_id: str,
    processing_url: str | None = None,
) -> dict:
    """Register domain-only claims without inventing a website or scheme."""
    if len(identities) > MAX_REGISTRATION_BATCH:
        raise ValueError("Identity registration batch exceeds 10000 records")
    if not source or not run_id:
        raise ValueError("Identity registration requires a source and run ID")
    validate_timestamp(discovered_at)
    for item in identities:
        if item != identify_domain(item.root_domain):
            raise ValueError("Domain identity does not match its canonical parent")
    if not identities:
        return {"domains_inserted": 0}
    rows = {
        item.root_domain: (
            item.root_domain,
            [source],
            discovered_at,
            discovered_at,
            run_id,
        )
        for item in identities
    }
    with inventory_publication_lock(processing_url=processing_url):
        count = register_missing(client, table="domains", rows=rows)
    return {"domains_inserted": count}


def register_websites(
    client,
    observations: Sequence[WebsiteObservation],
    *,
    source: str,
    run_id: str,
    processing_url: str | None = None,
) -> dict:
    """Create missing domain/website/page parents and verify them before returning.

    This only registers identities. Existing evidence timestamps are not overwritten
    by a retry or an older observation; inventory folds update their summaries later.
    Result publishers must retain durable results if this call fails.
    """
    if len(observations) > MAX_REGISTRATION_BATCH:
        raise ValueError("Identity registration batch exceeds 10000 records")
    if not source or not run_id:
        raise ValueError("Identity registration requires a source and run ID")
    domains, websites, pages = {}, {}, {}
    for observation in observations:
        item = observation.identity
        if item != identify_website(item.page_url):
            raise ValueError(
                "Website identity or domain reference does not match its URL"
            )
        validate_timestamp(observation.discovered_at)
        for stamp in (observation.observed_at, observation.fetched_at):
            if stamp is not None:
                validate_timestamp(stamp)
        if observation.fetched_at is not None and (
            observation.observed_at is None
            or observation.fetched_at > observation.observed_at
        ):
            raise ValueError(
                "Successful fetch time requires matching observation evidence"
            )
        seen = observation.discovered_at
        domain = domains.get(item.root_domain)
        domains[item.root_domain] = (
            item.root_domain,
            [source],
            min(domain[2], seen) if domain else seen,
            max(domain[3], seen) if domain else seen,
            run_id,
        )
        for target, key, identity in (
            (websites, item.website_origin, (item.root_domain, item.website_origin)),
            (
                pages,
                item.page_url,
                (item.root_domain, item.website_origin, item.page_url),
            ),
        ):
            previous = target.get(key)
            offset = len(identity)
            observed = [
                stamp
                for stamp in (
                    observation.observed_at,
                    previous[offset + 3] if previous else None,
                )
                if stamp is not None
            ]
            fetched = [
                stamp
                for stamp in (
                    observation.fetched_at,
                    previous[offset + 4] if previous else None,
                )
                if stamp is not None
            ]
            target[key] = (
                *identity,
                [source],
                min(previous[offset + 1], seen) if previous else seen,
                max(previous[offset + 2], seen) if previous else seen,
                max(observed) if observed else None,
                max(fetched) if fetched else None,
                run_id,
            )
    if not observations:
        return {"domains_inserted": 0, "websites_inserted": 0, "pages_inserted": 0}
    with inventory_publication_lock(processing_url=processing_url):
        # Preflight all existing identities before writing any missing parent. A
        # corrupt stored site must not partially register an otherwise valid batch.
        for table, rows in (
            ("domains", domains),
            ("websites", websites),
            ("pages", pages),
        ):
            existing_identities(client, table=table, rows=rows)
        counts = {
            table + "_inserted": register_missing(client, table=table, rows=rows)
            for table, rows in (
                ("domains", domains),
                ("websites", websites),
                ("pages", pages),
            )
        }
    return counts


def existing_identities(client, *, table: str, rows: dict) -> set[str]:
    """Read bounded natural keys and verify materialized IDs and parent membership."""
    key, identity_columns = {
        "domains": ("root_domain", ("root_domain", "domain_id")),
        "websites": (
            "website_origin",
            ("website_origin", "root_domain", "website_id", "domain_id"),
        ),
        "pages": (
            "page_url",
            (
                "page_url",
                "root_domain",
                "website_origin",
                "page_id",
                "website_id",
                "domain_id",
            ),
        ),
    }[table]
    found = set()
    for record in client.execute(
        f"SELECT {','.join(identity_columns)} FROM corpscout.{table} WHERE {key} IN %(keys)s",
        {"keys": tuple(rows)},
        settings=REGISTRATION_SETTINGS,
    ):
        natural = record[0]
        if natural in found:
            raise ValueError(
                f"Duplicate central {table} identity; repair inventory before publishing"
            )
        if table == "domains":
            item = identify_domain(natural)
            expected = (item.root_domain, item.domain_id)
        else:
            item = identify_website(natural)
            expected = (
                (item.website_origin, item.root_domain, item.website_id, item.domain_id)
                if table == "websites"
                else (
                    item.page_url,
                    item.root_domain,
                    item.website_origin,
                    item.page_id,
                    item.website_id,
                    item.domain_id,
                )
            )
        if (
            tuple(record) != expected
            or record[1 if table != "domains" else 0] != rows[natural][0]
        ):
            raise ValueError(
                f"Mismatched central {table} identity; repair inventory before publishing"
            )
        found.add(natural)
    return found


def register_missing(client, *, table: str, rows: dict) -> int:
    columns = {
        "domains": DOMAIN_COLUMNS,
        "websites": WEBSITE_COLUMNS,
        "pages": PAGE_COLUMNS,
    }[table]
    found = existing_identities(client, table=table, rows=rows)
    missing = [value for key, value in rows.items() if key not in found]
    if missing:
        client.execute(
            f"INSERT INTO corpscout.{table} ({','.join(columns)}) VALUES",
            missing,
            settings=REGISTRATION_SETTINGS,
        )
    if existing_identities(client, table=table, rows=rows) != set(rows):
        raise ValueError(
            f"Central {table} registration is incomplete; retain results and retry"
        )
    return len(missing)
