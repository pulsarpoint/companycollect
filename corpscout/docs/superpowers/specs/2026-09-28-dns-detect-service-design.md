# dns_detect: domain DNS records → services and evidence

Status: DRAFT for owner review (2026-09-28)
Location: `corpscout/services/dns_detect` (Go module `dns_detect`)
Supersedes: the detection SQL in
`services/dagster_v3/docs/superpowers/plans/2026-09-28-domain-services-detection.md`
(parked). The service model, `(service_type, provider)`, is unchanged from
`services/dagster_v3/docs/superpowers/specs/2026-09-27-domain-services-and-technology-domains-design.md`
(revision 2).

## Owner decisions (2026-09-28)

- **A separate Go service.** DNS → services detection is its own Go service,
  not ClickHouse SQL.
- **Input and output.** The input is one domain's full DNS records; the output
  is the services the domain uses, with the evidence for each.
- **Knowledge.** The knowledge base is built from provider-recon's published
  documents (`providers/<slug>/latest.json` in the `provider-recon` bucket):
  services, provider keys, rules, and IP ranges with their lifecycle.
- **Analyzers per record type,** at more than one level. Some records need only
  a pattern match. Others, like SPF, need parsing and computation first.
- **Isolated, injected matching.** Every matcher gets its sources injected, as a
  knowledge object with a method per protocol, and is tested in isolation.
- **Pure SPF.** SPF is evaluated only on what the record itself says. The engine
  makes no DNS lookups: resolving `include:` chains is the DNS scanner's job, and
  the resolved records arrive as ordinary input.
- **Isolation first.** Build and test the engine in isolation. Sliding records
  through time and wiring it into the pipeline come later.

## Goal

A pure function:

```
Detect(domain, records, asOf, knowledge) → []Service (each with []Evidence)
```

The same records, knowledge and `asOf` always give the same result, in the same
order. History is obtained by calling it at different `asOf` dates.

## Architecture

```
dns_detect/
  internal/model       input records, output services/evidence (JSON contract)
  internal/knowledge   compiled, immutable index + loaders
  internal/hosts       host normalisation, registrable domain (public suffix list)
  internal/analyze     one analyzer per record type / protocol
  internal/engine      routing, fallback labelling, aggregation
  cmd/dns-detect       CLI: records JSON in → services JSON out (later: serve)
```

- **No shared code with provider-recon.** `dns_detect` doesn't import
  provider-recon's packages, which are `internal`. It decodes the published
  `provider-recon/v1` JSON with its own types. The contract is the JSON, not
  the Go structs.
- **Knowledge is built once and read-only.** Analyzers receive it as an
  interface and never load anything themselves.

## Input

```json
{
  "domain": "spotify.com",
  "records": [
    {"name": "spotify.com", "type": "NS", "value": "ns-cloud-a2.googledomains.com.",
     "first_seen": "2026-07-15", "last_seen": "2026-09-25"},
    {"name": "spotify.com", "type": "TXT", "value": "\"v=spf1 include:_spf.google.com\" \" ~all\"",
     "first_seen": "2026-07-15", "last_seen": "2026-09-25"}
  ]
}
```

- **Fields.** `type` is the presentation record type, and `value` is the
  presentation RDATA exactly as `commoncrawl_domain_dns_records` stores it
  (quoted TXT strings, MX `priority host`, SOA fields).
- **Seen window.** `first_seen` and `last_seen` are optional. A record without
  them counts as present at every `asOf`.
- **Record names.** Records may belong to any name under the domain: the apex,
  `www.`, `*._domainkey.`, `_dmarc.`, `_amazonses.`, `_mta-sts.`, and so on.
  Analyzers pick the names they understand.

## Time

- **What `asOf` selects.** It keeps the records whose window contains it, and
  the IP ranges valid at it.
- **When a range is valid.** From `first_seen` until `removed_at` (open while
  active). Ranges from the first provider-recon run count as valid since the
  beginning of time, because nothing earlier is known.
- **Rules.** The current rules apply at every `asOf`, so improving a
  definition relabels history.
- **Full history in `latest.json` (owner ruling 2026-09-28).** Provider-recon
  stops purging removed items. It used to drop them 90 days after `removed_at`
  (`RemovedRetentionDays` in `internal/assemble`); now they stay indefinitely
  with their windows.
  - `latest.json` is therefore the complete timeline, and one document answers
    "which provider owned this IP on date X" for any date since the timeline
    started (2026-09-27).
  - Size: 37 documents total 25 MB today, and a removed range adds a few
    hundred bytes, so growth is a few MB a year. Snapshots were rejected: they
    lose ranges that appear and disappear between two snapshots, and every
    re-analysis would need the right snapshot.
  - The loader stays an interface, used for tests and the directory loader.
- **Sliding.** The caller slides through time. A helper, `ChangePoints(records)
  []date`, lists the dates where the record set changes, so evaluating only at
  those dates gives the full timeline.

## Knowledge

It is compiled from every provider document into these structures:

| Structure | Content | Used by |
|---|---|---|
| Services | provider slug, name, country, service key, service types, traits | all analyzers |
| Provider keys | exact registrable domains → provider; glob keys (`awsdns-*`) → provider | the fallback |
| Rules by kind | the rules of each kind (below), each compiled once (regex precompiled, lowercased patterns) | the matching analyzer |
| IP index | a longest-prefix routing table (`gaissmai/bart`, as provider-recon uses) per IP family; each prefix holds its validity windows and service | A/AAAA, SPF `ip4`/`ip6` |

- **Interface.** Analyzers query knowledge through narrow methods, so a test
  can pass a tiny hand-built knowledge base:
  - `MatchHost(kind, host) (Match, bool)`
  - `MatchValue(kind, value) (Match, bool)`
  - `LookupIP(ip, asOf) (Match, bool)`
  - `ProviderForKey(key) (Provider, bool)`
- **Loading.** A loader reads the documents from a directory (tests, CLI) or
  from S3.
- **Refusing bad knowledge.** Compilation fails on a rule no analyzer handles,
  and on an invalid regex. A bad definition is refused at load time, never
  silently ignored.

## Rule kinds (extension of provider-recon's DNS rules)

Today's DNS rules carry `record_type`, `match_field` and `matcher_type`. They
become the following kinds, each owned by one analyzer:

| Kind (`record_type` / `match_field`) | Matched against | Example |
|---|---|---|
| `NS` / `target` | nameserver host | suffix `ns.cloudflare.com` |
| `MX` / `target` | exchange host | suffix `mail.protection.outlook.com` |
| `CNAME` / `target` | apex/www CNAME target | suffix `fastly.net` |
| `TXT` / `value` | apex TXT value, unquoted and joined | prefix `apple-domain-verification=` |
| `TXT` / `name` | the TXT record's name label under the domain | prefix `_amazonses` |
| `SPF` / `include` | each `include:` / `redirect=` host | suffix `_spf.google.com` |
| `DKIM` / `selector` | the DKIM selector | exact `selector1` (Microsoft 365) |
| `DKIM` / `target` | the selector's CNAME target | suffix `dkim.amazonses.com` |
| `DMARC` / `report` | `rua` / `ruf` mailbox domains | suffix `dmarcian.com` |

- **Matcher types:** `exact`, `suffix` (the host equals the pattern or ends
  with `.pattern`), `prefix`, `contains`, `regex` (RE2), `exists`.
- **Resolving several matches:** the highest `priority`, then `confidence`,
  then the rule id.
- **Provider-recon changes:**
  - its validator accepts exactly these kinds and rejects the rest (A, AAAA,
    CAA, HTTPS, SRV and the `all`/`priority` fields are rejected until an
    analyzer exists);
  - its three SPF `contains` rules and its AWS DKIM rule are rewritten into the
    new kinds.

## Analyzers

Each has the same shape and returns detections, each with its evidence:

```go
type Analyzer interface {
    Name() string
    Analyze(d Domain, recs []Record, kb Knowledge) []Detection
}
```

| Analyzer | Reads | Does | Service type of a fallback |
|---|---|---|---|
| `ns` | apex NS | normalise host → NS rules → fallback | `dns` |
| `soa` | apex SOA, only when there is no NS | MNAME host → NS rules → fallback | `dns` |
| `mx` | apex MX | drop null MX (`0 .`); exchange host → MX rules → fallback | `email` |
| `spf` | the apex TXT starting `v=spf1` (more than one is itself evidence, flagged) | parse the mechanisms (below) | `email_sending` |
| `dkim` | `<selector>._domainkey.<domain>` CNAME and TXT | selector → DKIM selector rules; CNAME target → DKIM target rules → fallback | `email_sending` |
| `dmarc` | `_dmarc.<domain>` TXT | parse tags; `rua`/`ruf` mailbox domains → DMARC rules → fallback | `dmarc_reporting` |
| `txt` | apex TXT (not SPF) and `_name` TXT | TXT value/name rules only, no fallback | from the rule |
| `cname` | apex and `www` CNAME | target → CNAME rules → fallback | `hosting` |
| `ip` | apex and `www` A/AAAA | IP index at `asOf`; no match gives no row | from the range's service |

**SPF parsing:**
- The strings are joined, and mechanisms split on whitespace, with qualifiers
  (`+ - ~ ?`) kept.
- `include:host` and `redirect=host` go through SPF include rules, then the
  fallback.
- `ip4:`/`ip6:` go through the IP index, recorded as `email_sending` evidence
  (e.g. a Mailchimp block).
- `a`, `mx` and `ptr` without a host point at the domain itself and are
  recorded as `self-hosted` `email_sending`.
- Macros (`%{…}`) are recorded as unparsed evidence, never guessed at.
- A record over the RFC's 10-lookup budget, counted statically, is recorded as
  a finding. The engine doesn't enforce the limit.

**Service types.** They are the spec's closed list plus `dmarc_reporting`
(new), because the `rua` domain is a reporting service, not a mail host.

## Fallback labelling (the engine)

For a host no rule matched, the engine uses the registrable domain from Go's
public suffix list (`golang.org/x/net/publicsuffix`: `ns1.binero.se` →
`binero.se`, `x.github.io` → `x.github.io`). Then:
1. **Self-hosted** when the key is the domain itself, or the host is under it.
2. **A named provider** when the key matches a provider key; the provider's
   first service of the analyzer's service type is used.
3. **Unmapped** otherwise, and the key is kept.

## Output

```json
{
  "domain": "spotify.com",
  "as_of": "2026-09-25",
  "knowledge_version": "sha256:…",
  "services": [
    {"service_type": "email_sending", "provider_key": "google", "provider_slug": "google",
     "service_keys": ["google.workspace-sending"], "confidence": 1.0,
     "evidence": [
       {"analyzer": "spf", "record_name": "spotify.com", "record_type": "TXT",
        "value": "include:_spf.google.com", "rule_id": "google/google.workspace-sending/spf-include-1",
        "confidence": 1.0}
     ]},
    {"service_type": "email_sending", "provider_key": "sendgrid.net", "provider_slug": "",
     "service_keys": [], "confidence": 0.5, "evidence": [ … ]}
  ],
  "findings": [{"analyzer": "spf", "code": "spf_lookup_budget_exceeded", "detail": "12 lookups"}]
}
```

- **Aggregation.** `services` has one entry per `(service_type, provider_key)`,
  sorted by service type and then provider key. Its confidence is the maximum
  over its evidence.
- **Rule ids** are stable: `<provider>/<service_key>/<kind>-<n>`, so evidence
  can be traced to a definition.
- **Knowledge version.** `knowledge_version` hashes the loaded documents, so a
  stored result names what produced it.
- **Findings** are observations that aren't services, e.g. two SPF records or an
  exceeded lookup budget.

## Testing

- **Per analyzer:** table-driven, with a hand-built knowledge base. Examples:
  SPF with qualifiers, macros, `redirect`, split strings, two SPF records; a
  null MX; SOA with and without NS; a DKIM selector rule against a target rule;
  a DMARC `rua` with several mailboxes; an IP in two nested ranges; an IP in a
  range that became valid after `asOf`.
- **Knowledge:**
  - compiling real provider-recon documents (copied into `testdata/`);
  - the refusal of unknown rule kinds and invalid regexes;
  - the `awsdns-*` glob keys.
- **Engine:**
  - determinism (shuffled input, same output);
  - `ChangePoints`, plus sliding `asOf` over a domain that moved from Binero to
    Loopia.
- **Golden tests:** record sets exported once from ClickHouse for about 10 real
  domains (spotify.com, a Swedish SME on Binero, one on Microsoft 365, one
  behind Cloudflare, one self-hosted), each with an expected output file.
  `go test -update` rewrites them after an intentional change.
- **Checks:** `go test -race ./...`, `go vet` and `gofmt` must be clean.

## Out of scope for the first slices

- **HTTP service and deployment:** `serve` with a batch endpoint, deployed like
  provider-recon.
- **Pipeline wiring:** exporting each bucket's records, streaming them through
  the service, and loading `domain_services` with the parked plan's tables,
  schedule and sensor.
- **More analyzers:** CAA, SRV, HTTPS and MTA-STS.

## Slices

1. **Engine skeleton and host-based analyzers:** the model, knowledge
   (directory loader, host and key indexes), `hosts`, the `ns`/`soa`/`mx`/`cname`
   analyzers, the engine with fallback and aggregation, and the CLI.
0. **Provider-recon keeps history:** remove the purge of removed items,
   with a test that a 400-day-old removed range is still in the document;
   redeploy.
2. **Parsed analyzers:** `spf`, `dkim`, `dmarc` and `txt`, plus the new rule
   kinds in provider-recon (validator, definitions rewritten, redeploy).
3. **IP analyzer and time:** the IP index with windows, `ip`, SPF `ip4`/`ip6`,
   `ChangePoints`, and the golden tests on real domains.
