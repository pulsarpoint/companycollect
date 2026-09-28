"""Register claim parents in batches, then publish source-owned current evidence."""

from corpscout_identity.registration import (
    MAX_REGISTRATION_BATCH,
    WebsiteObservation,
    identify_domain,
    identify_website,
    register_domains,
    register_websites,
)

from dagster_v3.defs.se_company.domain import tables


def insert_claims(client, rows: list[dict]) -> None:
    # Preflight the entire page before writing anything. Domain-only evidence does
    # not manufacture a website from a display URL synthesized by an extractor.
    claims, domains, websites = [], {}, []
    for row in rows:
        if row["source"] == "wikidata" and row["website_url"]:
            # Historical Wikidata exports sometimes used the full host as root,
            # or prepended HTTPS to an already-schemed official URL. Preserve
            # source/slot/evidence, deriving the central parent from that URL.
            url = row["website_url"]
            if url.startswith(("https://http://", "https://https://")):
                url = url.removeprefix("https://")
            website = identify_website(url)
            row = {**row, "root_domain": website.root_domain, "website_url": url}
        identity = identify_domain(row["root_domain"])
        if identity.root_domain != row["root_domain"]:
            raise ValueError("Claim root must be a canonical registrable domain")
        if row.get("domain_id", identity.domain_id) != identity.domain_id:
            raise ValueError("Claim domain reference does not match its evidence")
        website_id = row.get("website_id")
        if row["source"] == "crawler_lookup":
            website_id = row["slot"]
        if website_id is not None or (
            row["source"] == "wikidata" and row["website_url"]
        ):
            website = identify_website(row["website_url"])
            if website.domain_id != identity.domain_id or (
                website_id is not None and website_id != website.website_id
            ):
                raise ValueError(
                    "Claim website reference does not match its domain/URL"
                )
            website_id = website.website_id
            websites.append(
                WebsiteObservation(website, row["suggested_at"], None, None)
            )
        domains[identity.domain_id] = identity
        claims.append(
            {**row, "domain_id": identity.domain_id, "website_id": website_id}
        )
    if not claims:
        return
    identities = list(domains.values())
    # A source retry retains original claim stamps. Registration never overwrites
    # existing parent metadata and never writes global contribution memberships.
    stamp = min(row["suggested_at"] for row in claims)
    run_id = claims[0]["source_run_id"] or "domain-claim-backfill"
    for offset in range(0, len(identities), MAX_REGISTRATION_BATCH):
        register_domains(
            client,
            identities[offset : offset + MAX_REGISTRATION_BATCH],
            source=tables.SOURCE_TABLE,
            discovered_at=stamp,
            run_id=run_id,
        )
    for offset in range(0, len(websites), MAX_REGISTRATION_BATCH):
        register_websites(
            client,
            websites[offset : offset + MAX_REGISTRATION_BATCH],
            source=tables.SOURCE_TABLE,
            run_id=run_id,
        )
    for offset in range(0, len(claims), MAX_REGISTRATION_BATCH):
        client.execute(
            f"INSERT INTO corpscout.{tables.SOURCE_TABLE} ({','.join(tables.SOURCE_COLUMNS)}) VALUES",
            [
                tuple(row[column] for column in tables.SOURCE_COLUMNS)
                for row in claims[offset : offset + MAX_REGISTRATION_BATCH]
            ],
            settings={"async_insert": 0},
        )
