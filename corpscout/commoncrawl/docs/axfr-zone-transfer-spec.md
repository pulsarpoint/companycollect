# AXFR (DNS Zone Transfer) Probe — Design Spec

> Historical design record. The implemented scanner now lives in the standalone
> [`cc-dns-axfr`](../cc-dns-axfr/) project and consumes delegation summaries produced by
> [`cc-dns-scan`](../cc-dns-scan/); the combined-worker paths and flags below describe the original
> proposal rather than current runtime behavior.

Status: **historical proposal, superseded by the standalone scanner.** Original decisions
2026-07-07. **Revised 2026-07-09** — storage reversed from *infer-and-discard* to *retain-with-provenance*
(§5 + Risk), after the primary purpose was fixed as **technology detection** over
third-party (CommonCrawl-wide) domains rather than authorized per-target scanning.

Add an opportunistic AXFR (full DNS zone transfer) probe to `cc-dns-worker`. Most
servers refuse; the minority that allow it leak the full zone. An open zone yields two
things almost for free:

1. **Enrichment** — every leaked hostname is another DNS record, and many names are
   direct technology tells (`cpanel.example.com`, `asa-fw.example.com`,
   `portal.example.com` → CNAME to a PaaS). These flow into the *same* record table the
   worker already loads, so downstream technology inference gets them at no extra cost.
2. **Security posture** — an open zone transfer is itself a misconfiguration finding,
   surfaced as a per-domain flag.

## Decisions

- **Storage (revised 2026-07-09): retain-with-provenance.** AXFR-discovered resource
  records are persisted into the **existing** `commoncrawl_domain_dns_records` table as
  ordinary rows, tagged with a new `source` discriminator (`query` | `axfr`). Technology
  inference is **not** done in the worker; it runs downstream as SQL/analytical queries
  over ClickHouse after records land. This supersedes the earlier *infer-and-discard*
  decision — see [Storage](#5-storage--retain-with-provenance) and
  [Risk & retention](#risk--retention) for the reasoning and the legal hedge.
- **Inference is downstream, not inline.** The worker's only job is to discover and load
  as many records as possible. Deriving "domain X uses technology Y" is a query-time
  concern over the loaded records, decoupled from the scan. Keeps the worker a dumb,
  fast record producer.
- **Scope:** the probe runs in its **own scheduler lane** (separate `Scheduler`
  instance), never sharing the UDP resolution budget.
- **Rollout:** ship behind `--axfr=false`; enable on a bounded sample to measure real
  hit-rate and transfer-latency tail before wiring into the steady-state loop.

## Why it fits cleanly

`resolveDomain` (`cmd/cc-dns-worker/scan.go`) already produces a fully-populated
`Delegation` (`NS` + `NSIPs`) for every domain before returning — exactly the input AXFR
needs. No new discovery work. And because the worker already loads distinct DNS records
to ClickHouse via a fixed `DNSRecord` → `RecordRow` path, AXFR records need no new sink:
they are just more rows in the table that already exists.

## 1. Probe — `internal/resolve/axfr.go` (new)

TCP-only, using `miekg/dns`'s streaming `dns.Transfer` (independent of the existing
UDP-first `client`/`Exchanger`).

```go
type AXFRResult struct {
    Open      bool     // a server returned zone data (not REFUSED/NOTAUTH/error)
    Server    string   // the NS IP that answered
    Records   int      // RRs seen (up to the cap)
    Truncated bool     // hit a byte/record/time cap before the SOA close
    Zone      []model.DNSRecord // transferred zone, held in memory then persisted (see §5)
}

func ProbeAXFR(ctx context.Context, sched *scheduler.Scheduler, zone string,
    nsIPs []string, caps AXFRCaps) AXFRResult
```

- Build `dns.Msg{}.SetAxfr(zone)`, run `(&dns.Transfer{}).In(msg, addr)`, drain the
  envelope channel into `Zone`, converting each RR through the existing `collect`-style
  mapping so AXFR records share the shape of actively-queried ones.
- Rotate across `nsIPs`: first server that yields data wins; all-REFUSED → `Open:false`.
- Every send goes through `sched.Do(ctx, nsIP, fn)` — paced + breaker-protected — but on
  the dedicated AXFR scheduler, not `authSched`.

## 2. Caps — bound the fat tail (critical)

```go
type AXFRCaps struct {
    MaxRecords int           // e.g. 50000  — stop draining past this
    MaxBytes   int           // e.g. 64<<20 — running sum of envelope sizes
    Deadline   time.Duration // e.g. 20s    — ctx timeout for the whole transfer
}
```

Caps are essential **regardless of the storage decision**: an unbounded or hostile zone
can be hundreds of MB and would stall a worker and blow memory while it is drained.
`Truncated:true` records that a zone hit a cap, so consumers know they saw only a prefix.

## 3. Separate lane

`scanCycle` builds `discSched` + `authSched` today; add a third:

```go
axfrSched := scheduler.New(scheduler.Config{
    PerServerQPS:     cfg.axfrQPS,     // low, e.g. 5
    MaxInFlight:      1,               // one transfer per server at a time
    BreakerThreshold: cfg.breakerThreshold,
    BreakerCooldown:  cfg.breakerCooldown,
})
```

Its own `Scheduler` instance (not a lane inside `authSched`) because AXFR is TCP and
long-lived, while `authSched` is tuned for thousands of tiny UDP round-trips; sharing the
per-server in-flight semaphore would let a multi-second transfer starve record queries.
Additionally gate *aggregate* concurrent transfers with a counting semaphore
(`--axfr-inflight`, e.g. 50) so total held-open TCP connections stay bounded (mind the
box's FD / conntrack limits — see the OS-tuning notes).

## 4. Skip hyperscalers + dedup by NS set

- **Skip hyperscalers.** `providers.go` already has `isHyperscaler(ip)` — currently
  **unexported and in `package scheduler`**, so it must be exported (or a small shared
  helper added) to be callable from `internal/resolve`. If all of a domain's `NSIPs` are
  hyperscaler, skip — they never allow AXFR and are a large share of volume.
- **Dedup by NS set — this is the primary volume saver.** Server-openness is a property
  of the server and repeats across every zone it hosts. Keep a process-level `sync.Map`
  keyed by the sorted-NS-IP tuple; the first domain on a given NS set establishes the
  server verdict. A REFUSED transfer returns *no error* to `fn`, so the circuit breaker
  never trips on refusals — NS-set dedup is the only thing that stops chronic refusers
  from being re-probed, which is where the savings are. (The hyperscaler CIDR list is
  deliberately partial; many managed-DNS providers aren't in it and will be probed once
  per NS set, then remembered as refusing — safe, just one wasted TCP round-trip.)
- **Zone contents are still per-domain.** Dedup suppresses re-probing servers, not the
  transfer of distinct open zones — each open zone runs once.

## 5. Storage — retain-with-provenance

The raw zone is persisted, not discarded. This reverses the original *infer-and-discard*
decision because the primary purpose is technology detection, which is served by the
records themselves, and the near-free enrichment value justifies retention (see
[Risk](#risk--retention) for the legal reasoning and the exposure hedge).

- **Records → existing table.** Each AXFR RR becomes a `model.DNSRecord` and is loaded
  into `corpscout.commoncrawl_domain_dns_records` through the existing distinct-record
  path (AggregatingMergeTree dedup applies as normal). No new records table.
- **New `source` column.** Add `source` (`query` | `axfr`) to `DNSRecord` / `RecordRow`
  and the CH DDL. This is the cheap lever that makes the retention decision reversible: it
  lets downstream weight AXFR-derived signals and, critically, **filter the AXFR-only
  slice** without a re-scan.
- **Corroboration + exposure gating are downstream, not in the worker.** Whether an AXFR
  name is independently discoverable (Certificate Transparency / passive DNS / actively
  resolvable) is *not* computed here — it belongs to the downstream technology-inference
  project, which first needs other data settled (GeoIP for every IP; curated CNAME/IP maps
  for hosting / PaaS / application providers). The worker's only contribution to that later
  filtering is the `source='axfr'` tag. See
  [Out of scope](#out-of-scope--downstream-dependencies).
- **Scan-summary flags.** Add `axfr_open UInt8`, `axfr_records`, `axfr_truncated` to
  `ScanRow` / `corpscout.commoncrawl_domain_dns_scan`. `axfr_open` is the security-posture
  signal and is the always-safe, always-exposable part.
- **Plumbing touched** (so the estimate is honest): `model.go` (`DNSRecord.Source`,
  `ScanRow` axfr fields), `store.go` (SQLite stage schema + `StagedDomains` /
  `SummariesFor` / staged-records queries), `load.go` / CH DDL, and `DomainResult` gains
  an `AXFR AXFRResult` field consumed by the committer.

### Downstream technology inference (separate work, not in this worker)

Technology inference runs as queries over the loaded records — **no inline pass**. Most
tech signal already comes from records collected on every domain (SPF/DMARC/DKIM/CAA/SRV/
MX/NS/CNAME/TXT-verification/MTA-STS/BIMI/DNSKEY — see `records.Plan`); AXFR adds
internal-hostname tells on top for the open-zone minority. That inference layer is its own
component (the natural home for the deferred techdetect→Wappalyzer work, extended from
web-only to DNS/TLS/port signals) and is out of scope for the probe itself. AXFR's job
here is only to *maximize the record set* that layer consumes.

| Record | Signal |
| --- | --- |
| MX | mail platform (Google Workspace, M365, Proofpoint, Mimecast, Zoho) |
| NS | managed DNS (Cloudflare, Route53, NS1, Azure DNS, Akamai) |
| CNAME target | SaaS/CDN/PaaS (CloudFront, Fastly, Zendesk, HubSpot, Shopify, Netlify, Salesforce, Heroku, Vercel) |
| SPF include chain | named vendors (`_spf.google.com`, `spf.protection.outlook.com`, `sendgrid.net`, `mailgun.org`) |
| DKIM selector | mail platform (`google._domainkey`, `selector1._domainkey` M365, `k1._domainkey` Mailchimp) |
| CAA | standardized CA (Let's Encrypt, DigiCert, Sectigo, Google Trust) |
| SRV / autodiscover | `autodiscover`⇒Exchange/M365, `_sip`/`_sipfederationtls`⇒Teams, `_xmpp`⇒chat |
| TXT verification | adopted SaaS (`google-site-verification`, `MS=`, `atlassian-domain-verification`, `stripe-verification`) |
| **AXFR-only hostnames** | **internal tooling / appliances (`cpanel.`, `jenkins.`, `gitlab.`, `jira.`, `vcenter.`, `citrix.`, `asa-fw.`, `vpn-*.`) — the unique AXFR contribution** |

## 6. Flags (all default-off so it ships dark)

```
--axfr               bool   (default false — master switch)
--axfr-qps           float  (default 5)
--axfr-inflight      int    (default 50)
--axfr-max-records   int    (default 50000)
--axfr-max-bytes     int    (default 67108864)
--axfr-timeout       dur    (default 20s)
```

Add matching fields to `scanConfig` and `scanFlags` in `scan.go`.

## 7. Rollout

1. Land `ProbeAXFR` + tests behind `--axfr=false`. Extend the DNS test harness
   (`internal/resolve/testserver_test.go`) to serve AXFR over TCP — a canned zone covering
   both the REFUSED path and a capped successful transfer, TDD-style. (This is TCP-server
   work, more than adding another canned UDP response.)
2. Enable on a bounded `--max-domains` sample on the box; read real hit-rate and p99 transfer
   time from stats.
3. Wire into the steady-state scan loop with tuned caps.

## Cost

- Refused case: 1 extra TCP round-trip per **NS set** (deduped) ≈ low-single-digit % over
  the current ~68 queries/domain.
- Open case: bounded by §2 caps, isolated by §3 lane — a handful of transfers, none able
  to stall the UDP workers.

## Risk — retention

**Purpose:** third-party (CommonCrawl-wide) technology detection — *not* authorized
per-target scanning. This is the same posture as the platform's existing public-web
CommonCrawl enrichment, and it is the established business model of SecurityTrails,
Shodan, Censys, and BuiltWith (third-party DNS/subdomain/technology data without
per-target authorization, operating in both the EU and US). AXFR-as-source is the one
step those services mostly don't take, which is what makes the *internal-only* slice the
contestable part.

Three tiers, three risk levels:

1. **The AXFR request** — low risk, standard. Unauthenticated DNS; if a server answers it
   chose to serve any requester. zonemaster (already integrated), dnsrecon, and nmap all
   do this.
2. **The open-zone-transfer finding (`axfr_open`)** — no exposure, pure defensive value.
   Always safe to compute and expose.
3. **Retaining and productizing the raw zone** — the contestable tier, on two axes:
   - **Access authorization.** An open AXFR is a *misconfiguration*, not an intended
     public channel — so the access is more contestable than a public web page, and in
     some jurisdictions accessing data via a known misconfiguration can be argued to
     exceed authorization (CFAA-type theories US; Computer Misuse Act UK; §202a StGB DE).
     The counter — an unauthenticated protocol operation the server volunteered, no access
     control bypassed — is strong but not settled.
   - **Data protection (GDPR).** Internal hostnames can embed personal data, so retention
     is *processing* even when internal-only. Manageable under a documented
     legitimate-interest basis with data minimization and a retention window, but not
     obligation-free.

**Corroboration lowers the delta.** Much of a zone is independently discoverable via
Certificate Transparency, passive DNS, and the company's own crawled pages. For any name
also in those channels, the incremental risk of retaining it is near zero. The genuinely
contestable slice is the **internal-only names that appear nowhere public** — which is
also the highest-value tech-detection slice.

**Resolution (this spec): retain with the `source` tag; gating is owned elsewhere.**

- The worker persists all AXFR records with `source='axfr'` and sets `axfr_open`. That is
  the full extent of its responsibility.
- **Exposure gating and GDPR-sensitivity classification are out of scope** — they belong
  to a separate, cross-source service (see
  [Out of scope](#out-of-scope--downstream-dependencies)) that spans every producer's
  personal-data risk (DNS internal hostnames, company contact information with personal
  names, etc.), not AXFR alone.
- The `source='axfr'` tag is the hook that lets that service filter the internal-only
  slice downstream — retention now, exposure decided later, no re-scan.
- Freely computable/exposable regardless of that work: the `axfr_open` flag and derived
  technology facts ("uses tech Y") — the low-risk, industry-standard outputs.

## Out of scope — downstream dependencies

Two separate projects consume what this worker produces; neither is built here, and the
AXFR worker must not take on their responsibilities:

- **Cross-source technology inference.** Consumes the loaded DNS records (including
  `source='axfr'`) and derives "domain X uses technology Y". Runs as query-time analytics,
  not inline. Depends on other data settling first: **GeoIP for every IP address**, and
  **curated CNAME/IP → provider maps** across hosting, PaaS, and application providers.
  Computes the public-corroboration signal. This is its own project.
- **GDPR-sensitivity classification service.** A cross-source service that flags
  potential personal data across *all* producers — DNS internal hostnames here, but also
  company contact information with personal names and other sources. Owns exposure gating,
  the legitimate-interest basis, and retention windows. Productionizing raw-zone retention
  depends on this service, tracked separately — not a blocker on landing the probe.
