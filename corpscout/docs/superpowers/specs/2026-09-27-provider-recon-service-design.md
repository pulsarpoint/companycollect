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
- Output is deterministic: sorted arrays and a content hash, so an unchanged
  feed produces an unchanged file and diffs show real changes.

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
