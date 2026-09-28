# Domain services (service type + provider) and technology_domains — design

Status: APPROVED by owner (2026-09-28), revision 2
Module: `dagster_v3/defs/domain_services` (new); `technology_domains` later in
`dagster_v3/defs/technology_catalog`

Revision 2 (2026-09-28) records the owner's rulings:
- Everything here is **domain data**. How a company relates to a domain is a
  separate concern, and `company_domains` is to be removed. There are no
  company views, company tabs or company counts in this work.
- Provider evidence comes from `provider_recon` (the `provider_services`,
  `provider_rules` and `provider_ip_ranges` tables in ClickHouse). The
  `custom/service_providers.json` mapping file from revision 1 is dropped.
- IP matching against provider ranges is in the first cut.
- A new named provider is added by editing provider-recon's YAML definitions.
  The next daily provider-recon run publishes it.
- The old `webtech_domain_technologies` (v1) is left to the webtech track.

## Why

The technology pipeline flattens infrastructure into Wappalyzer technology
names, and takes the service from the technology's catalog category instead
of from the evidence. Live examples (2026-09-27):

| Evidence | Labelled today | Actually means |
|---|---|---|
| NS → `*.ns.cloudflare.com` | "Cloudflare" (CDN) **and** "Cloudflare DNS" | DNS hosting only; the site may not be proxied |
| NS → Route 53 | "Amazon Route 53" **and** "Amazon Web Services" (PaaS) | DNS hosting; says nothing about where the site runs |
| NS/SOA → GoDaddy (5.8M domains) | "GoDaddy" (Hosting) | Registrar default nameservers |
| TXT → Google Search Console (8.5M) | "Google Search Console" (Analytics) | A verification token |

The surrounding tables have their own problems:

- **Copies that should be reads.** `technology_companies`, `technology_top_domains`
  and `technology_adoption` are weekly copies that should be views or live
  reads.
- **`technology_top_domains` is costly to build.** It pauses the
  `se_companies_serving` refresh and needs a 20 GiB cap.
- **Detection is stale.** `domain_signal_technologies` was last built
  2026-09-06, and nothing schedules it.
- **Webtech is ignored.** `webtech_domain_technologies_v2` (700k domains) feeds
  none of the rollups.
- **No DNS evidence in the UI.** The backoffice domain page never shows it.

## Model

Two separate concepts, both keyed by `root_domain`:

1. **Domain services** are the infrastructure a domain uses, stored as a pair
   `(service_type, provider)`. **The evidence decides the service type.**
   - Cloudflare nameservers give `(dns, Cloudflare)`.
   - A Cloudflare IP gives `(cdn, Cloudflare)`.
2. **Technologies** are the software seen on pages (WordPress, React,
   HubSpot…), keyed by the Wappalyzer catalog name as today.

### Service types (closed list)

| service_type | Evidence |
|---|---|
| `dns` | Apex NS. SOA MNAME is used only when a domain has no NS record. |
| `email` | Apex MX. |
| `email_security` | Apex MX to a gateway, when the provider's service declares it (Proofpoint, Mimecast…). |
| `email_sending` | SPF `include:`/`redirect=` hosts, and DKIM selector CNAME targets. |
| `saas_verification` | TXT verification tokens. A weak signal, never counted as infrastructure. |
| `cdn`, `ddos_protection`, `waf` | Apex/www A/AAAA inside a provider range, or apex/www CNAME to an edge host. |
| `hosting`, `paas`, `iaas` | Apex/www CNAME to a platform host, or A/AAAA inside a cloud provider range. |

The service type of a **named** provider comes from its matched provider-recon
service (`provider_services.service_types`). The service type of an
**unmapped** key comes from the signal, as in the table above; CNAME targets
give `hosting`.

### Provider key: every detection is labelled

Every host-based detection gets a **provider key**: the registrable domain of
the evidence host, `cutToFirstSignificantSubdomain` (verified on the server:
`ns1.binero.se` → `binero.se`, `mx1.example.co.uk` → `example.co.uk`,
`aspmx.l.google.com` → `google.com`). Then:

1. **A provider-recon rule matches the candidate.** The rule gives the
   provider and the service. See Rule evaluation below.
2. **No rule matches, but the key is one of a provider's `provider_keys`.**
   Matching is exact, or by a `*` pattern such as `awsdns-*`. That provider is
   used, with its first service whose `service_types` contains the signal's
   service type. If it has no such service, the provider is still named but
   the service is empty.
3. **The key equals the domain itself.** The provider is `self-hosted`.
4. **Otherwise** the key is stored as-is, with an empty `provider_slug`, and
   the row is **unmapped**. Counts and lists are complete from the first run,
   and nothing is dropped into "other".

IP evidence has no host. It is labelled only by the matched range's provider
and service (see IP matching below). An IP that no range contains gives no
row. Labelling it by its RDAP owner belongs to the IP-enrichment track: the
`rdap_network_trie` covers only 162k looked-up networks.

### Rule evaluation

`provider_rules` of `kind = 'dns'` (94 rules on 2026-09-28) are evaluated in
SQL against the candidates of the same `record_type`:

- **What is matched.** `match_field = 'target'` matches the normalised host;
  `value` matches the TXT value with quotes stripped; `name` matches the
  record name.
- **Matcher types.**
  - `suffix`: the candidate equals the pattern or ends with `.pattern`.
  - `prefix`, `contains`: string matching.
  - `regex`: `match()` (re2).
  - `exists`: the candidate is non-empty.
  - `case_sensitive = 0` lower-cases both sides.
- **Which rules are used.** Only non-removed rules: `status != 'removed'` in
  the `FINAL` read.
- **Several rules match one candidate.** The highest `priority` wins, then the
  highest `confidence`, then `rule_key` as a tie-break. The result is
  deterministic.

The rule set is tiny, so it is cross-joined with each record type's
candidates. Rules of `kind` `asn`, `ptr` and `http` need data the DNS store
doesn't hold (ASN, reverse DNS, page headers), so they aren't used here.

### History

Every candidate carries its DNS record's seen window (`first_seen`,
`last_seen` from `commoncrawl_domain_dns_records`). The record store keeps
records that have disappeared, so the pipeline computes history, not a
snapshot:

- **Host evidence** keeps the record's window. The current rules classify all
  of history, so improving the definitions relabels the past.
- **IP evidence** is accepted only when the record's window overlaps the
  range's validity window, `[first_seen, coalesce(removed_at, today)]`. Ranges
  collected on the first provider-recon run (2026-09-27, the start of the
  range timeline) count as valid from the beginning of time, because earlier
  ranges are unknown. The accepted window is the overlap.
- **Current state.** `domain_services` stores `domain_last_seen`, the newest
  `last_seen` of any of the domain's records in the pass. A service is
  **current** when its `last_seen >= domain_last_seen - 7 days`. The view
  `domain_services_current` applies that rule.

No per-company or per-day snapshots are kept. "Domain X used Azure for three
months last year" is a read of `domain_services`.

## IP matching

**Candidates.** Apex and www A/AAAA values are parsed with `toIPv6OrNull`, with
IPv4 mapped. Unparsable values are dropped. There are about 4M such records per
bucket: bucket 3 on 2026-09-28 held 2.1M apex A, 1.0M www A and 1.2M AAAA.

**Range keys.** `provider_ip_ranges` (194k rows including history) are
expanded into join keys:
- IPv4 ranges get one key per `/16` they cover.
- IPv6 ranges get one key per `/32` they cover.
- A range wider than `/8` (IPv4) or `/20` (IPv6) is refused with a logged
  count, so one bad feed can't explode the join.

**Join.** Candidates are equi-joined to the keys on `(ip_family, key)`, then
filtered with `range_start <= ip AND ip <= range_end` and the window rule.
When ranges overlap (AWS `AMAZON` and `CLOUDFRONT`, for example), the longest
prefix wins, so the most specific service labels the IP.

**Result.** The matched service gives the provider and its `service_types`,
e.g. `aws.cloudfront` gives `cdn`. The evidence row stores the IP, the CIDR
and the feed tag.

## Tables

The data tables are partitioned by `cityHash64(root_domain) % 128`. That
refines the DNS store's `% 16` key, so detection bucket N reads only
record-store partition N % 16, as today's detection asset does. Each bucket is
rebuilt with a stage table and `REPLACE PARTITION`.

**`domain_service_evidence`** has one row per supporting record, and explains
every detection. The domain page reads it.

```
root_domain, service_type, provider_slug ('' when unmapped),
service_key ('' when unmapped or the provider has no matching service),
provider_key, signal_type, record_name, evidence, rule_key ('' for key fallback),
ip_cidr ('' unless IP evidence), feed_tag, first_seen, last_seen, confidence,
source ('dns'|'ip_range'), source_run_id, detected_at
ORDER BY (root_domain, service_type, provider_key, signal_type, evidence)
```

**`domain_services`** has one row per `(service_type, provider_key,
root_domain)`. Provider pages read it.

```
service_type, provider_key, provider_slug, service_keys Array, root_domain,
signal_types Array, first_seen, last_seen, domain_last_seen, confidence (max),
harmonic_rank, source_run_id, detected_at
ORDER BY (service_type, provider_key, harmonic_rank, root_domain)
```

- **Provider key.** For a named provider the key is the `provider_slug`
  (`cloudflare`), so every Cloudflare host collapses into one provider. For an
  unmapped provider it is the registrable domain (`binero.se`); for
  self-hosted it is `self-hosted`.
- **Rank.** `harmonic_rank` is copied from `commoncrawl_domain_graph_ranks`
  (the latest complete release) at build time, and is UInt64 max when a
  domain is unranked.
- **Top domains** for a provider is a sorted read: `WHERE service_type = X AND
  provider_key = Y ORDER BY harmonic_rank LIMIT 500`.
- **Adoption** is a `count()` over the key prefix.

**Views:**
- `domain_services_current`: the rows that are current (see History).
- `unmapped_provider_keys`: key, service_type, domain count, current domain
  count, and the top domains by rank. The review list of providers worth
  adding to provider-recon.

## Assets and orchestration

`domain_services_clickhouse` is a new asset with 128 static partitions
(`hash_000`…`hash_127`) in pool `domain_signal_detection`. Per bucket, in a
single scan of the DNS store's partition:

1. **Candidates** go into a temp table, one row per distinct `(root_domain,
   record_name, signal_type, candidate)` with the window:
   - apex NS, MX and TXT;
   - apex SOA, only when the domain has no NS;
   - apex and www CNAME;
   - `*._domainkey.<root>` CNAME;
   - apex and www A/AAAA.

   SPF includes are split from `v=spf1` TXT values. `domain_last_seen` is
   computed alongside.
2. **Host signals:** rule evaluation, then key fallback, into the evidence
   stage.
3. **IP signals:** the range-key join into the evidence stage.
4. **Aggregate** the evidence stage into the `domain_services` stage, joining
   ranks.
5. **Swap** both tables' partition N with `REPLACE PARTITION`.

It refuses to swap an empty stage when the bucket has DNS records.

The range keys are rebuilt inside each bucket run as a temp table from
`provider_ip_ranges FINAL` (48k ranges, well under a second), so the asset
doesn't depend on another module's helper table.

**Scheduling** is server-side only:
- A sensor launches a full 128-bucket refresh when the provider
  **definitions** change: services, their keys and types, and the active DNS
  rules, hashed into one digest. It deliberately ignores `content_hash`, which
  changes whenever a feed's IP ranges churn (almost daily); range churn is
  picked up by the weekly refresh.
- A weekly schedule does the same for new DNS records.
- A full backfill never starts while one is in flight, and never alongside
  other heavy materializations (pool limit 1).

The old `domain_signal_technologies_clickhouse` stops being scheduled once the
new asset is live, and is removed after cut-over.

## technology_domains (second plan)

`technology_domains` has one row per `(technology, root_domain)` of page
software. It combines CommonCrawl page detections with
`webtech_domain_technologies_v2`, and carries `harmonic_rank`:

```
technology, harmonic_rank, root_domain, sources, first_seen, last_seen
ORDER BY (technology, harmonic_rank, root_domain)
```

`commoncrawl_page_technologies` (12.9B rows) is partitioned by `crawl_id` and
sorted by `root_domain`, so a hash-bucket filter can't prune it, and 128
per-bucket passes would each read the whole table. The build therefore goes
**per crawl**, which is the page table's own partition:

- Each crawl's pass aggregates `(technology, root_domain, min/max resolved_at)`
  into an AggregatingMergeTree target.
- A new crawl adds one pass.
- A new graph release, which changes ranks, triggers a rebuild.
- Webtech v2 is merged in by a small full-refresh pass.

Page detections of infrastructure names (Cloudflare, CloudFront…) stay
technologies here. Mapping them into `domain_services` would need a
`catalog_technologies` field on provider-recon services. That is a later
addition, unnecessary now that IP matching covers CDN detection for every
DNS-scanned domain.

The first step of that plan is to measure one crawl's pass.

## Removals (after cut-over is verified)

- **Tables:** `technology_adoption`, `technology_companies`,
  `technology_top_domains`, `domain_signal_technologies` and
  `technology_fingerprints`.
- **Assets:**
  - the builders of those tables;
  - `domain_signal_technologies_clickhouse`;
  - the fingerprint extraction in `technology_catalog_clickhouse`, with its
    asset check.
- **Process:** drops follow the ledger policy (remove the objects from the
  migration files + a DROP script), and happen only after nothing reads the
  old tables.

## Backoffice

- **Domain page:** a Services section from `domain_service_evidence` (service
  type, provider, the record that proves it, seen window). Technologies come
  from `technology_domains` once it exists.
- **Service provider pages** for each `(service_type, provider_key)` (e.g.
  `/admin/services/dns/cloudflare`, `/admin/services/dns/binero.se`): top
  domains by rank, current and historical counts, and links to evidence.
- **Services overview:** per service type, providers ranked by current domain
  count, with unmapped keys marked. Adding a provider means editing
  provider-recon's YAML.
- **Technology pages:** adoption and the Domains tab read `technology_domains`.
  The Companies tab goes.
