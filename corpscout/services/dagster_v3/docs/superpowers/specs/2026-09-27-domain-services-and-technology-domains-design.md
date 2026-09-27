# Domain services (service type + provider) and technology_domains — design

Status: DRAFT for owner review (2026-09-27)
Module: `dagster_v3/defs/technology_catalog` (+ a new `domain_services` module)

## Why

The technology pipeline flattens infrastructure into Wappalyzer technology names,
and the service is taken from the technology's catalog category instead of from
the evidence. Live examples (2026-09-27):

| Evidence | Labelled today | Actually means |
|---|---|---|
| NS → `*.ns.cloudflare.com` | "Cloudflare" (CDN) **and** "Cloudflare DNS" | DNS hosting only; the site may not be proxied |
| NS → Route 53 | "Amazon Route 53" **and** "Amazon Web Services" (PaaS) | DNS hosting; says nothing about where the site runs |
| NS/SOA → GoDaddy (5.8M domains) | "GoDaddy" (Hosting) | Registrar default nameservers |
| TXT → Google Search Console (8.5M) | "Google Search Console" (Analytics) | A verification token |

The surrounding tables have their own problems:

- `technology_companies`, `technology_top_domains` and `technology_adoption` are
  weekly copies that should be views or live reads. `technology_top_domains`
  pauses the `se_companies_serving` refresh (`SYSTEM STOP VIEW`) and needs a
  20 GiB cap to build.
- `domain_signal_technologies` was last built 2026-09-06. Nothing schedules it.
- `webtech_domain_technologies_v2` (700k domains) feeds none of the rollups.
- The backoffice domain page never shows DNS evidence.

## Model

Two separate concepts:

1. **Domain services**: infrastructure a domain uses. It is a pair
   `(service_type, provider)`, and the **evidence decides the service type**.
   Cloudflare as a nameserver gives `(dns, Cloudflare)`. A Cloudflare IP or a
   `cf-ray` header gives `(cdn, Cloudflare)` + `(ddos_protection, Cloudflare)`.
2. **Technologies**: software seen on pages (WordPress, React, HubSpot…), keyed
   by the Wappalyzer catalog name as today.

### Service types (owned by us, closed list)

| service_type | Evidence | Phase |
|---|---|---|
| `dns` | apex NS (SOA only as fallback when NS is absent) | 1 |
| `email` | apex MX | 1 |
| `email_security` | apex MX to a gateway (Proofpoint, Mimecast…) | 1 |
| `email_sending` | SPF `include:` hosts, DKIM selector CNAME targets | 1 |
| `saas_verification` | TXT verification tokens. Weak signal, never counted as infrastructure | 1 |
| `cdn`, `ddos_protection`, `waf` | page/header detections of proxy providers (phase 1); A/AAAA in provider IP ranges, CNAME to edge hosts (phase 2) | 1 + 2 |
| `hosting`, `paas`, `iaas` | CNAME to platform hosts (`*.vercel.app`, `*.azurewebsites.net`…) (phase 1); A/AAAA via RDAP owner / provider IP ranges (phase 2) | 1 + 2 |

The DNS record store already holds everything phase 1 and 2 need: A, AAAA,
CNAME, NS, MX, TXT, SOA, HTTPS, SRV, CAA, plus `*._domainkey.*` and `_dmarc.*`
names (sampled bucket 3/128, 2026-09-27).

### Provider key: every detection is labelled

Every detection gets a **provider key** derived mechanically from the evidence
host: its registrable domain (`cutToFirstSignificantSubdomain`). So
`ns1.binero.se` → `binero.se`.

- A key equal to the domain itself → provider `self-hosted`. This generalises
  today's self-hosted-email rule to every service type.
- A key present in the provider mapping → that named provider.
- Otherwise the key is stored as-is and marked **unmapped**. Counts, company
  views and provider lists are complete from the first run. Nothing is dropped
  into "other".

IP-based evidence (phase 2) gets its key from the matched range's provider, or
from the RDAP owner handle when no provider range matches.

### Provider mapping (repo-owned)

`custom/service_providers.json`, versioned like `technologies.json`:

```json
{
  "cloudflare": {
    "name": "Cloudflare",
    "website": "https://www.cloudflare.com",
    "catalog_technology": "Cloudflare",
    "country": "US",
    "keys": ["cloudflare.com", "cloudflare.net"],
    "key_patterns": [],
    "services": ["dns", "cdn", "ddos_protection", "waf"]
  },
  "aws": {
    "name": "Amazon Web Services",
    "keys": ["amazonaws.com", "cloudfront.net"],
    "key_patterns": ["^awsdns-\\d+\\.(com|net|org|co\\.uk)$"]
  }
}
```

- `catalog_technology` reuses the catalog icon and description.
- `key_patterns` exist only for providers that spread across many registrable
  domains (AWS `awsdns-NN.*`, Azure `azure-dns.*`). The long tail needs no
  patterns.
- `services`, when present, restricts which service types the provider can
  carry. It guards against, for example, Cloudflare `saas_verification` TXT
  tokens being read as CDN.
- Signal-specific classification that the key alone can't express (MX gateway
  → `email_security` vs mailbox → `email`) lives in a small per-signal override
  list in the same file.
- The Wappalyzer `dns` fingerprints and `fingerprints.json` are superseded by
  this file.

**Promotion flow:** the backoffice lists unmapped keys ranked by domain count,
with a per-country breakdown (so Swedish providers surface first). Promoting a
key means adding it to the JSON. The next detection run relabels the old rows,
because detection is recomputed from scratch each run. There is no proposals
state machine: the key is deterministic, so there is nothing to approve.
White-label nameservers show the reseller, which is accurate for the `dns`
service. An optional `parent` link can be added later if it matters.

### Page detections of infrastructure

Wappalyzer page/header detections of infrastructure names ("Cloudflare",
"Amazon CloudFront", "Akamai", "Fastly"…) are translated through the mapping
(`catalog_technology` → provider + its proxy service types). They land in
`domain_services`, not `technology_domains`, so they aren't double-counted.
Phase 1 therefore already gives `cdn`/`ddos_protection` for crawled domains.
Phase 2 extends it to every DNS-scanned domain.

## Tables

All three data tables are partitioned by `cityHash64(root_domain) % 128`, so
each is refreshed one bucket at a time with `REPLACE PARTITION` (the existing
detection asset's pattern).

**`domain_service_evidence`**: one row per supporting record, for explaining
detections. Read by the domain page.

```
root_domain, service_type, provider_key, provider_id ('' when unmapped),
signal_type, record_name, evidence, first_seen, last_seen, confidence,
source ('dns'|'page'|'webtech'|'ip_range'|'rdap'), source_run_id, detected_at
ORDER BY (root_domain, service_type, provider_key, signal_type, evidence)
```

**`domain_services`**: one row per `(service_type, provider_key, root_domain)`.
Read by provider pages.

```
service_type, provider_key, provider_id, root_domain, sources Array,
first_seen, last_seen, confidence, harmonic_rank, detected_at
ORDER BY (service_type, provider_key, harmonic_rank, root_domain)
```

**`technology_domains`**: one row per `(technology, root_domain)` for page
software, combining CommonCrawl page detections and
`webtech_domain_technologies_current_v2`.

```
technology, root_domain, sources Array, first_seen, last_seen,
harmonic_rank, detected_at
ORDER BY (technology, harmonic_rank, root_domain)
```

`harmonic_rank` is copied in from `commoncrawl_domain_graph_ranks` (latest
complete release) at build time, with UInt64 max when unranked. Because the
sort key has it right after the group key:

- **"Top domains for X"** = `WHERE technology = X ORDER BY harmonic_rank LIMIT 500`,
  a sorted read.
- **Adoption count** = `count()` over the key prefix, cheap because the prefix
  is contiguous per partition.

This replaces `technology_top_domains` and `technology_adoption`.

**Views:**

- `company_services`: `company_domains` joined to `domain_services`
- `company_technologies`: `company_domains` joined to `technology_domains`
- `unmapped_provider_keys`: key, service_type, domain count, per-country counts

`company_domains` is ~11k rows, so both company views are cheap and every
country appears automatically.

## Assets and orchestration

- `service_providers_clickhouse` (new, unpartitioned): validates and publishes
  the mapping JSON to a small `service_providers` table (stage + EXCHANGE).
  Runs in the weekly `technology_catalog_job`.
- `domain_services_clickhouse` (replaces `domain_signal_technologies_clickhouse`,
  128 partitions, pool `domain_signal_detection`): one candidate pass per bucket
  over `commoncrawl_domain_dns_records`, which extends today's apex MX/TXT/NS/SOA
  and apex+www CNAME candidates with:
  - SPF includes
  - DKIM selector CNAMEs
  - phase 2: A/AAAA

  Also reads the bucket's page and webtech infrastructure detections. Writes
  both `domain_service_evidence` and `domain_services`.
- `technology_domains_clickhouse` (new, 128 partitions, same pool): per bucket,
  combines page + webtech detections for non-infrastructure technologies and
  joins the ranks. The first full build is the one unavoidable pass over the
  12.9B page rows, but split into 128 separately retryable units instead of one
  job. The page table is not partitioned by the same hash, so each unit still
  filters by hash; measure one bucket before the full backfill.
- **Scheduling:** a server-side sensor launches a full 128-bucket backfill after
  a publish whose provider-mapping or catalog hash changed, or when the DNS
  scan cycle completes. Never a local loop, and never alongside other heavy
  materializations. This fixes today's "nothing ever refreshes detection".

## Phase 2: IP evidence

- Weekly assets for official provider ranges (Cloudflare, AWS
  `ip-ranges.json` with service tag, Azure Service Tags, Google `cloud.json`,
  Fastly, Akamai, Bunny, GitHub `meta`, DigitalOcean/Oracle/Linode) →
  `provider_ip_ranges` + an `ip_trie` dictionary.
- Apex/www A/AAAA matching:
  - A match on a proxy provider → `cdn` / `ddos_protection`.
  - A match on a cloud provider → `iaas`/`paas` with the service tag.
  - No match → `hosting` keyed by the RDAP owner from the existing trie.

## Removals (after cut-over is verified)

- Tables: `technology_adoption`, `technology_companies`,
  `technology_top_domains`, `domain_signal_technologies`,
  `technology_fingerprints`.
- Assets `technology_adoption_clickhouse`, `technology_companies_clickhouse`,
  `technology_top_domains_clickhouse` and `domain_signal_technologies_clickhouse`,
  plus the fingerprint extraction in `technology_catalog_clickhouse` and its
  asset check.
- Drops follow the ledger policy (remove the objects from the migration files +
  a DROP script), and happen only after the backoffice reads the new tables.

## Backoffice

- `technologies.server.ts`: adoption, Domains tab and Companies tab read
  `technology_domains` / `company_technologies`.
- New provider pages per `(service_type, provider)`: top domains + companies
  (e.g. `/admin/services/dns/binero.se`).
- Domain page: a "Services" section from `domain_service_evidence` showing the
  evidence, and technologies from `technology_domains` (adds webtech).
- Unmapped-provider review list (read-only; promotion = edit the JSON).

## Open points for the owner

1. First-cut scope as written: phase 1 = DNS/email/sending/verification plus
   CDN/DDoS from page headers; phase 2 = IP ranges. Or pull IP matching into
   phase 1?
2. Promotion via JSON in the repo (reviewed by commit, as proposed), or an
   admin-editable ClickHouse table so promotion happens in the backoffice?
3. Retire the old `webtech_domain_technologies` (v1, 8.9M rows) as part of this
   work, or leave it to the webtech track?
