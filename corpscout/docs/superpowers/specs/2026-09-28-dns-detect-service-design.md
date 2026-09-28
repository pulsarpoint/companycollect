# dns_detect: DNS records → services, resolved per record

Status: APPROVED by owner (2026-09-28), revision 2
Location: inside the provider-recon Go module
(`corpscout/services/provider_recon`): packages `internal/detect/...` and
command `cmd/dns-detect`.
Supersedes: the detection SQL in
`services/dagster_v3/docs/superpowers/plans/2026-09-28-domain-services-detection.md`
(parked). The service model, `(service_type, provider)`, is unchanged from
`services/dagster_v3/docs/superpowers/specs/2026-09-27-domain-services-and-technology-domains-design.md`
(revision 2).

## Owner decisions (2026-09-28)

- **Go, not SQL.** Detection is Go code.
- **Knowledge from provider-recon documents.** It comes from the published
  documents: services, provider keys, rules, and IP ranges with their
  lifecycle.
- **Indefinite retention.** Provider-recon keeps removed items indefinitely,
  so `latest.json` is the complete timeline. Snapshots were rejected.
- **Pure SPF.** SPF is evaluated only on what the record says. The engine
  makes no DNS lookups.
- **Resolution per record.** Each DNS record instance (`record_id`, value,
  seen window) is resolved independently into the services it proves. Results
  are stored per record, and a domain's history is a view over them.
- **Same Go module as provider-recon.** Detection lives in provider-recon's
  module, so rule semantics (`internal/matcher`) and the document types
  (`internal/model`) exist once. It is a separate command and a separate
  process.
- **A worker, not a CLI on the Dagster host.** Running at scale is a worker
  that pulls records from ClickHouse, resolves them and writes results back.
  The bucket is the unit of work, and it can be distributed over machines.
  Dagster triggers and polls, as it does for provider-recon.
- **Isolation first.** Build and test the resolver in isolation.

## Why per record

- **No point-in-time reconstruction.** DNS observations are point scans, and
  record types are scanned on different schedules (SOA every 4–5 days,
  NS/MX/TXT/A every 2–4 weeks; NS and MX were last seen on different dates for
  1.5% of domains in bucket 3). A per-record result needs no reconstruction:
  each record speaks only for its own window.
- **Incremental by nature.** A new scan creates new record instances, and only
  those need resolving. A knowledge change re-resolves the records whose
  `knowledge_version` is older.
- **Evidence for free.** Every result row points at exactly one record.

## Input: one record

```json
{"record_id": "3f2a…", "root_domain": "spotify.com", "name": "spotify.com.", "type": "MX",
 "value": "5 alt1.aspmx.l.google.com.", "first_seen": "2026-07-15", "last_seen": "2026-09-25"}
```

- **Where it comes from.** The `commoncrawl_domain_dns_records` row:
  `record_id` is its hex `record_id`, and `type` and `value` are its
  presentation form.
- **Dates.** `first_seen` and `last_seen` are dates. A missing one leaves that
  side of the window open.

## Output: results per record

`Resolve(record, knowledge) → Output{Results, Findings}`. It is a pure
function: the same record and knowledge always give the same output, in the
same order.

```json
{"record_id": "3f2a…", "root_domain": "spotify.com", "record_name": "spotify.com", "record_type": "MX",
 "analyzer": "mx", "subject": "alt1.aspmx.l.google.com",
 "service_type": "email", "provider_key": "google", "provider_slug": "google",
 "service_key": "google.workspace-mail",
 "rule_id": "google/google.workspace-mail/MX/target suffix google.com", "confidence": 1,
 "fallback": false, "valid_from": "2026-07-15", "valid_to": "2026-09-25"}
```

- **One row per service type.** A record can yield several rows: a Cloudflare
  edge CNAME gives cdn, ddos_protection and waf.
- **The window.** `valid_from`/`valid_to` is the record's window. It can be
  narrower for IP evidence (a range removed mid-window, slice 3).
- **Fallback rows.** `fallback` is true for evidence that only counts when
  better evidence is absent: SOA MNAME is used only where no NS covers the
  same time. The history view applies it.
- **Findings** are per-record observations that aren't services, e.g.
  `null_mx`. Domain-level findings, such as two SPF records, are computed
  later over a domain's records.

## Storage and history (slice 4)

- **`dns_record_services`** holds one row per result above, plus
  `knowledge_version` and `resolved_at`. It is keyed and partitioned so a
  bucket or a set of `record_id`s can be replaced.
- **The history view** gives, per `(root_domain, service_type,
  provider_key)`, intervals `first_seen → last_seen`:
  - It merges a service's rows whose windows overlap or are separated by less
    than the scan gap (default 45 days), so ten scan-instance fragments become
    one interval.
  - It drops `fallback` rows wherever a non-fallback row of the same service
    type covers the same time.
  - Its dates are as precise as the scans: a switch happened somewhere between
    the last scan of the old provider and the first scan of the new one.
- **Current services** are the intervals whose `last_seen` is the domain's
  latest scan of that record type.

## Knowledge

It is compiled once from `[]model.Document` (decoded `latest.json`), is
immutable, and is injected as an interface. Analyzers never load anything:

```go
type Knowledge interface {
    Match(kind Kind, subject string) (Match, bool)   // best rule: priority, confidence, rule id
    ProviderForKey(key string) (Provider, bool)      // exact provider key or glob key
    Version() string                                 // hash of the documents
}
```

- **Patterns** are compiled with provider-recon's `matcher.Compile`: exact,
  suffix (label-aware), prefix, contains, glob, regex and exists. The
  validator and the resolver share one implementation.
- **What is refused:** a document of another contract version, a rule kind
  outside the list below, an invalid pattern, and a provider key claimed by
  two providers.
- **What is skipped:** removed rules and services removed from a definition.
- **Rule ids** describe the rule itself:
  `<provider>/<service_key>/<RECORD_TYPE>/<field> <matcher> <pattern>`, e.g.
  `cloudflare/cloudflare.dns/NS/target suffix ns.cloudflare.com`.

## Rule kinds

| Kind (`record_type` / `match_field`) | Matched against | Example | Slice |
|---|---|---|---|
| `NS` / `target` | nameserver host | suffix `ns.cloudflare.com` | 1 |
| `MX` / `target` | exchange host | suffix `mail.protection.outlook.com` | 1 |
| `CNAME` / `target` | apex/www CNAME target | suffix `fastly.net` | 1 |
| `TXT` / `value` | apex TXT value, unquoted and joined | prefix `apple-domain-verification=` | 2 |
| `TXT` / `name` | TXT record name label under the domain | prefix `_amazonses` | 2 |
| `SPF` / `include` | each `include:`/`redirect=` host | suffix `_spf.google.com` | 2 |
| `DKIM` / `selector` | DKIM selector | exact `selector1` | 2 |
| `DKIM` / `target` | selector CNAME target | suffix `dkim.amazonses.com` | 2 |
| `DMARC` / `report` | `rua`/`ruf` mailbox domain | suffix `dmarcian.com` | 2 |

Slice 2 makes provider-recon's validator accept exactly these kinds, and
rewrites the three SPF `contains` rules and the AWS DKIM rule into them.

## Routing and analyzers

The resolver routes a record by type and by name relative to `root_domain`:

| Record | Analyzer | Fallback service type | Slice |
|---|---|---|---|
| apex NS | `ns` | `dns` | 1 |
| apex SOA | `soa`, MNAME host, `fallback: true` | `dns` | 1 |
| apex MX | `mx`, exchange host; `0 .` gives the finding `null_mx` | `email` | 1 |
| apex / `www` CNAME | `cname` | `hosting` | 1 |
| apex TXT `v=spf1…` | `spf`: include/redirect hosts; `a`/`mx` give self-hosted; `ip4`/`ip6` go to the IP index (slice 3); macros are findings | `email_sending` | 2 |
| apex / `_name` TXT (other) | `txt`, rules only | from the rule | 2 |
| `<selector>._domainkey` CNAME/TXT | `dkim` | `email_sending` | 2 |
| `_dmarc` TXT | `dmarc`, rua/ruf mailbox domains | `dmarc_reporting` | 2 |
| apex / `www` A, AAAA | `ip`, the range index with windows | from the range's service | 3 |
| anything else | none | | |

**Host labelling** is shared by every host-based analyzer
(`resolve.LabelHost`):
1. The best rule of the kind wins, giving one result per service type of its
   service.
2. Otherwise the registrable domain decides (public suffix list,
   `golang.org/x/net/publicsuffix`: `ns1.binero.se` → `binero.se`,
   `x.github.io` kept whole):
   - the domain itself, or a host under it → `self-hosted`;
   - a provider key → that provider, with its first service of the fallback
     type;
   - anything else → unmapped, keeping the key.
3. A host with no registrable domain (an IP literal, garbage) gives nothing.

**Confidence** without a rule: key match 0.8, self-hosted 0.8, unmapped 0.5.

## Running at scale (slice 4)

- **A worker** (`dns-detect work`) pulls records from ClickHouse one bucket at
  a time: `cityHash64(root_domain) % 128`, refining the DNS store's `% 16`
  partitions.
- **Only what's new or stale.** It resolves the records that have no result
  under the current `knowledge_version`, then writes the results back.
- **Claims.** Workers claim buckets through the agreed shared-queue
  coordination, so any number of machines can run.
- **Dagster** triggers the work and polls, the same way
  `provider_recon_documents` drives provider-recon.
- **Deployment:** one worker under systemd on companycollect, next to
  ClickHouse, deployed with provider-recon's Ansible role. Add machines later.

## Testing

- **Knowledge:** compiling the real documents copied into `testdata`;
  precedence; every matcher type through the shared matcher; glob keys;
  removed rules and services; every refusal.
- **Per analyzer:** table-driven, with a fake Knowledge injected.
- **Resolver:** routing (which records go where, what is ignored); windows
  copied; determinism (the same record gives byte-identical output); the
  fallback flag on SOA.
- **Real-data smoke:** real records of a few domains through the CLI with all
  production documents.
- **Golden files** (slice 3): about 10 real domains with expected output.
- **Checks:** `go test -race ./...`, `go vet` and `gofmt` must be clean.

## Slices

0. **Provider-recon keeps removed items indefinitely.** Drop the purge, with
   tests; redeploy.
1. **Resolver core:** hosts, knowledge (reusing the matcher and the model), the
   NS/SOA/MX/CNAME analyzers, `Resolve`, and the CLI (`dns-detect resolve
   -knowledge DIR`: records in, results out, one per line).
2. **Parsed analyzers:** SPF, DKIM, DMARC and TXT, plus the new rule kinds in
   provider-recon (validator, definitions rewritten, redeploy).
3. **IP:** the range index with validity windows (result windows split at
   range changes), SPF `ip4`/`ip6`, and golden files.
4. **Storage and scale:** the `dns_record_services` migration and history
   view, the ClickHouse adapter, the bucket worker and claims, and the Dagster
   trigger/poll asset.
