# provider_recon: provider discovery service (stage 1: collection + per-provider JSON)

Status: DRAFT for owner review (2026-09-27)
Location: `corpscout/services/provider_recon` (new, separate service)
Related: `services/dagster_v3/docs/superpowers/specs/2026-09-27-domain-services-and-technology-domains-design.md`.
That spec's `custom/service_providers.json` is superseded by this service's output.

## Scope and staging (owner order)

1. **Port** the useful provider-discovery pieces out of PulsarProtect into this
   service. It is a copy, not a dependency: no imports of PulsarProtect modules,
   no reads of its database or snapshots.
2. **Collection procedures** that produce **one JSON object per provider**.
   This document is about this stage.
3. **Later, not in this doc:** ClickHouse tables, a regular Dagster job, and an
   auto-update pipeline.

## What exists in PulsarProtect (surveyed 2026-09-27)

Neither PulsarProtect codebase contains real provider data or any official
IP-range feed fetcher. What's worth taking is the **model**, the **validation
rules**, the **matcher** and the **AI augmentation prompts**.

| Source | Take | Leave behind |
|---|---|---|
| backoffice-v2 migrations 000009/000118/000150/000155 | provider → service → typed evidence model (aliases, signatures, IP ranges, ASNs, cert identities, DNS rules, HTTP rules, knowledge items); rule columns `matcher_type` (`exact/suffix/prefix/contains/regex/exists`), `match_field`, `confidence`, `priority`, `source_url`, `evidence` | `vendor_id`, `primary_product_id`, nuclei/nmap/wappalyzer/wordpress targets, company exports, admin-task FKs |
| backoffice-v2 `provider_intelligence_validation.go`, `providers.go:1723-1917` | normalisation (CIDR parse, lowercasing, regex compile, dedupe keys) | Postgres CRUD/API plumbing |
| backoffice-v2 `provider_intelligence_exports.go` | the deterministic export contract (stable ordering, sha256 content hash, `version` field). It is the basis of our JSON shape | snapshot tables in Postgres |
| backoffice-v2 candidate tables + apply/reject | the idea: non-authoritative evidence enters as a **candidate** with rationale + source URLs, and is applied after review | 2.6k lines of per-kind CRUD; we need one generic candidate shape |
| backoffice-v2 `ai_augmentation/agents/provider_intelligence/*` (DSPy) | prompts/signatures for service detection, provider discovery and detection notes | they run **without web/search tools**, so their output is model memory. It can only be candidates that must be verified against the cited source URL |
| runner3 `internal/providerintel` (~1.1k LOC) | pattern matcher (exact / prefix / label-aware suffix / contains / glob), TXT owner+value rules with kind override, `rule_id` on every rule, evidence map per match | linear CIDR scan (need a prefix trie), sqlite runtime/injection plumbing, cdncheck and keyword heuristics |
| runner3 `devsnapshot` + contracts `providerintelmodel` | seed patterns (13 TXT verification tokens, MX/CNAME for Google, M365, CloudFront, S3, Cloudflare); capability/trait vocabulary (`cdn, waf, cloud, paas, mail, dns`; `shared_infrastructure`, `origin_obscured`) | the Go module dependency. The types are re-declared here |

Gaps that neither codebase fills, and that this service adds:
- official feed ingestion
- ASN → announced-prefix resolution
- PTR rules
- A/AAAA/SRV/HTTPS/CAA patterns
- a closed service-type list
- evidence provenance per item

## The per-provider JSON object

One document per provider, `providers/<slug>.json`, contract version
`provider-recon/v1`. Every evidence item carries **provenance**
(`source`, `source_url`, `source_version`, `collected_at`). The collector and
the later ClickHouse load can then rank, expire and explain each item.

```json
{
  "version": "provider-recon/v1",
  "slug": "aws",
  "display_name": "Amazon Web Services",
  "category": "cloud",
  "website": "https://aws.amazon.com",
  "country": "US",
  "aliases": ["Amazon", "AWS", "Amazon.com, Inc."],
  "provider_keys": ["amazonaws.com", "cloudfront.net", "awsdns-*.com"],
  "services": [
    {
      "service_key": "aws.cloudfront",
      "display_name": "Amazon CloudFront",
      "service_types": ["cdn"],
      "traits": ["shared_infrastructure", "origin_obscured"],
      "evidence": {
        "ip_ranges": [
          {"cidr": "13.32.0.0/15", "region": "GLOBAL", "feed_tag": "CLOUDFRONT",
           "confidence": 1.0, "source": "official_feed",
           "source_url": "https://ip-ranges.amazonaws.com/ip-ranges.json",
           "source_version": "syncToken=1727400000", "collected_at": "2026-09-27T06:00:00Z"}
        ],
        "asns": [{"asn": 16509, "confidence": 0.6, "source": "peeringdb"}],
        "dns_rules": [
          {"record_type": "CNAME", "match_field": "target", "matcher_type": "suffix",
           "pattern": "cloudfront.net", "confidence": 0.95, "source": "curated"}
        ],
        "http_rules": [
          {"http_part": "header", "header_name": "x-amz-cf-id", "matcher_type": "exists",
           "pattern": "", "confidence": 0.95, "source": "curated"}
        ],
        "ptr_rules": [
          {"matcher_type": "suffix", "pattern": "cloudfront.net", "confidence": 0.9, "source": "curated"}
        ],
        "certificate_identities": []
      }
    }
  ],
  "collection": {
    "collected_at": "2026-09-27T06:00:00Z",
    "collectors": {"aws_ip_ranges": {"status": "ok", "source_version": "syncToken=1727400000", "items": 7412}},
    "content_hash": "sha256:…"
  },
  "candidates": []
}
```

- **`service_types`** uses the closed list from the domain-services spec
  (`dns, email, email_security, email_sending, saas_verification, cdn,
  ddos_protection, waf, hosting, paas, iaas`). The spec has a phase-1/phase-2
  split; this service collects evidence for all of them.
- **`provider_keys`** are the registrable domains (plus glob patterns) that map
  a DNS provider key to this provider.
- **`candidates`** holds unreviewed evidence (AI, mining). It is kept separate
  from `evidence` and never used for detection until applied.
- Output is deterministic: sorted arrays and a content hash.
  - The hash covers identity + evidence only. It excludes the `collection`
    block and each item's `source_version`, so a feed that republishes
    identical ranges under a new sync token does not create history.
  - Items carry `collector`, `source`, `source_url` and `source_version`, but
    no per-item timestamp. Fetch times live in `collection.collectors`.
    (The example above shows `collected_at` on an item; the implementation
    drops it for this reason.)

## Collection procedures (by trust level)

| # | Procedure | Produces | Trust | Cadence |
|---|---|---|---|---|
| 1 | **Curated definitions** (`definitions/<slug>.yaml` in repo): identity, services, service types, provider keys, DNS/HTTP/PTR rules, which collectors to run and how feed tags map to services | everything structural | authoritative (reviewed by commit) | on change |
| 2 | **Official feed collectors**, one small module per feed: AWS `ip-ranges.json`, Azure Service Tags, Google `goog.json`/`cloud.json`, Oracle `public_ip_ranges.json`, GitHub `/meta`, Cloudflare `ips-v4/v6`, Fastly `public-ip-list`, Bunny edge list, DigitalOcean/Linode geofeeds, Vercel/Netlify where published | `ip_ranges` with `feed_tag` → mapped to service by curated `feed_tag_map` | authoritative | daily fetch, output changes only when the feed does |
| 3 | **ASN collectors**: provider → ASNs (PeeringDB org/net, curated list), ASN → announced prefixes (RIPEstat announced-prefixes) | `asns`, and `ip_ranges` with `source: "bgp"` for providers without a feed (Hetzner, OVH, Akamai…) | high for ASN membership; prefixes are provider-level only (no service tag) | weekly |
| 4 | **Mining our own data**: rank unmapped DNS provider keys (NS/MX/CNAME targets), CNAME suffixes and PTRs from the corpscout DNS store | `candidates` (new provider keys, rules) | needs review | weekly |
| 5 | **AI augmentation** (ported DSPy prompts, **with** web fetch of the cited source to verify) | `candidates` with rationale + verified `source_url` | needs review | on demand per provider |

A feed collector failing keeps the previous output for that collector
(status `stale` + age) rather than emptying the provider. This mirrors the
"refuse to replace on empty" rule in dagster_v3.

**Review/apply (stage 1):** a candidate is promoted by editing the curated
definition (a PR). A backoffice review screen can come later, and it needs no
new mechanism.

## Service shape

```
services/provider_recon/
  definitions/<slug>.yaml          curated, owner-reviewed
  collectors/<feed>.py|go          one per official feed / ASN source
  miners/                          candidate generators over corpscout data
  agents/                          ported AI augmentation (phase 2 of this service)
  matcher/                         ported pattern matcher + prefix trie (used by tests
                                   and by the later detection pass)
  cmd: provider-recon collect [--provider aws] [--out dir]
       provider-recon validate
       provider-recon mine
  (generated providers/<slug>/latest.json + history + changes/ → S3)
```

## First slice proposal

- Definitions and collectors for: Cloudflare, AWS, Azure, Google, Fastly, Akamai,
  Bunny, Oracle, GitHub, Microsoft 365, Google Workspace, and the top Swedish
  DNS/email hosts from the unmapped-key ranking (Loopia, Binero, One.com,
  Oderland, GleSYS…).
- Collectors: all official feeds + PeeringDB/RIPEstat.
- Port: validation/normalisation + matcher (+ trie). AI agents deferred.
- Done when `provider-recon collect` produces valid, deterministic JSON for
  every provider in the first slice, and a re-run with unchanged feeds yields an
  identical content hash.

## Decisions (owner, 2026-09-27)

1. **Language: Go.** It matches `cc-dns-scan` and the code being ported. The AI
   agents are the only possible Python exception, and only if porting DSPy to Go
   proves impractical.
2. **Curated definitions are YAML; generated output is JSON.**
   - YAML carries comments explaining each rule and has readable regexes.
   - Loading is strict: typed structs via `go.yaml.in/yaml/v3` with
     unknown-field errors, and every pattern/CIDR is quoted.
   - `provider-recon validate` also checks the files against a JSON Schema,
     which editors can use too.
3. **Generated per-provider JSON lives in S3 only.** The owner chose S3 over a
   git data repo for easier management. Diffs are produced by the service
   itself rather than by git or bucket versioning:
   - `providers/<slug>/latest.json`: the current document.
   - `providers/<slug>/history/<collected_at>.json`: written only when the
     content hash changes.
   - `changes/<run_id>.json`: per-provider added/removed evidence (IP ranges,
     rules, aliases) and the feed versions that moved.
   - Change manifests are kept indefinitely (small); history pruning is deferred
     until it matters.

## Evidence lifecycle and removal (owner, 2026-09-27)

Nothing is deleted the moment it disappears. Every evidence item carries a
lifecycle:

| field | meaning |
|---|---|
| `status` | `active` → `missing` → `removed` (a missing item that reappears goes back to `active`) |
| `first_seen` / `last_seen` | UTC days of the first and latest successful observation |
| `missing_since` | first successful fetch that no longer contained the item |
| `removed_at`, `removal_action` | when and why: `grace_expired`, `accepted`, `definition_removed` |

Rules:
- **Grace period per feed.** `removal_grace_days` in the definition; default
  7, and 14 for Azure because it publishes weekly.
- **Only successful fetches mark items missing.** While a feed fails, nothing
  becomes missing or removed; the collector is `stale`.
- **Missing items still count for detection** during the grace period.
- **A removed item that comes back starts a new instance** (new `first_seen`).
  The removed instance stays, so validity intervals are never merged across a
  gap.
- **Curated items** removed from a definition are removed at once
  (`definition_removed`). So is every item of a service or feed dropped from
  the definition.
- **Mass-removal gate.** `mass_removal_percent` (default 20). When more than
  that share of a feed's active ranges goes missing in one run, the feed is
  `held`:
  - the items are still flagged missing, but their grace clock is frozen;
  - the hold clears by itself when the ranges come back, or with
    `provider-recon accept -provider <slug> -collector <id>`, which marks the
    missing ranges removed with action `accepted`.
- **Early acceptance.** `accept` also works on a feed that isn't held, to
  confirm removals before the grace period ends.
- **Retention.** Removed items stay in `latest.json` for 90 days, then drop
  out. History objects and the ClickHouse timeline keep them forever.
- **Content hash.** Status transitions change the hash, and so history and
  change entries are written. The daily `last_seen` update does not.
- **Upgrade from slice 1.** Items published before lifecycle tracking are
  treated as active since the run that published them.

### Full timeline, not snapshots

The ClickHouse stage stores every range with its validity window
(`first_seen` → `removed_at`, or open while active/missing). Detection joins
each DNS record's seen window with the ranges valid during that window. A
company's provider history ("Azure App Service 2025-06 → 2025-09") is then a
view over its domains. There are no per-company snapshots: snapshots can't
cover the time before they started, and they freeze today's knowledge instead
of being recomputable from better definitions.

**Limit.** The range timeline starts at the first collection (2026-09-27).
Older DNS records are attributed with the earliest ranges we hold. That is
reliable for the big clouds, whose address space rarely changes hands, and
weaker for small hosters. Public archives of AWS range history exist, if
backfill ever matters.

### Collector shape checks

Each collector checks the structure it depends on, and reports a mismatch as
`feed shape changed`. The feed then goes `stale` instead of silently
shrinking. Checks:
- AWS: both `prefixes` and `ipv6_prefixes`.
- Azure: `AzureCloud` and `AzureFrontDoor.Frontend`.
- Google, Cloudflare, Fastly, Bunny: IPv4 and IPv6 both present.
- Oracle: an `OCI` tag.
- GitHub: `pages`.

### Change manifest additions

- `scope`: the command (`collect` or `accept`) and the selected providers.
- `feeds`: every feed's status, item count, per-run churn (added / missing /
  reappeared / removed / purged) and unmapped tags. The churn is what the
  hold thresholds get calibrated from.
- Evidence diffs list lifecycle transitions: `added`, `missing`,
  `reappeared`, `removed` (with action), `purged`, `updated`.
- Run ids carry the command (`<YYYYMMDDTHHMMSSZ>-collect`), so an `accept`
  right after a run can't overwrite that run's manifest.
