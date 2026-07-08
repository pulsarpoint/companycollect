# AXFR (DNS Zone Transfer) Probe — Design Spec

Status: **spec only, not implemented.** Decisions recorded 2026-07-07.

Add an opportunistic AXFR (full DNS zone transfer) probe to `cc-dns-worker`. Most
servers refuse; the minority that allow it leak the full zone, which yields both a
security-posture finding (open zone transfer = misconfiguration) and enrichment data
(internal hostnames → technologies, infra layout).

## Decisions

- **Storage (revised 2026-07-07):** **infer-and-discard.** Process the transferred
  zone transiently in memory to derive technology signals, persist and expose only
  the **inferences** (+ the misconfiguration finding). Do **not** retain the raw zone
  (internal hostnames / IPs) in the product store — it's a security risk to the target
  and adds little unique value (see [Risk](#risk--retention)). This supersedes the
  earlier "full zone by default" choice.
- **Scope:** the probe runs in its **own scheduler lane** (separate `Scheduler`
  instance), never sharing the UDP resolution budget.
- **Rollout:** ship behind `--axfr=false`; enable on a bounded sample to measure
  real hit-rate and transfer-latency tail before wiring into the steady-state loop.

## Why it fits cleanly

`resolveDomain` (`cmd/cc-dns-worker/scan.go`) already produces a fully-populated
`Delegation` (`NS` + `NSIPs`) for every domain before returning — exactly the input
AXFR needs. No new discovery work.

## 1. Probe — `internal/resolve/axfr.go` (new)

TCP-only, using `miekg/dns`'s streaming `dns.Transfer` (independent of the existing
UDP-first `client`/`Exchanger`).

```go
type AXFRResult struct {
    Open      bool     // a server returned zone data (not REFUSED/NOTAUTH/error)
    Server    string   // the NS IP that answered
    Records   int      // RRs seen (up to the cap)
    Truncated bool     // hit a byte/record/time cap before the SOA close
    Zone      []model.DNSRecord // transferred zone, in-memory only — NOT persisted (see §5)
}

func ProbeAXFR(ctx context.Context, sched *scheduler.Scheduler, zone string,
    nsIPs []string, caps AXFRCaps) AXFRResult
```

- Build `dns.Msg{}.SetAxfr(zone)`, run `(&dns.Transfer{}).In(msg, addr)`, drain the
  envelope channel into `Zone`. `Zone` is consumed by the inference pass (§5) and then
  dropped — it is never written to the product store.
- Rotate across `nsIPs`: first server that yields data wins; all-REFUSED → `Open:false`.
- Every send goes through `sched.Do(ctx, nsIP, fn)` — paced + breaker-protected — but
  on the dedicated AXFR scheduler, not `authSched`.

## 2. Caps — bound the fat tail (critical)

```go
type AXFRCaps struct {
    MaxRecords int           // e.g. 50000  — stop draining past this
    MaxBytes   int           // e.g. 64<<20 — running sum of envelope sizes
    Deadline   time.Duration // e.g. 20s    — ctx timeout for the whole transfer
}
```

Even though the zone is only held transiently for inference (not persisted), the caps
are essential: an unbounded or hostile zone can be hundreds of MB and would otherwise
stall a worker and blow memory while it's being drained. `Truncated:true` records that a
zone hit a cap, so the inference pass saw only a prefix.

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
long-lived, while `authSched` is tuned for thousands of tiny UDP round-trips; sharing
the per-server in-flight semaphore would let a multi-second transfer starve record
queries. Additionally gate *aggregate* concurrent transfers with a counting semaphore
(`--axfr-inflight`, e.g. 50) so total held-open TCP connections stay bounded.

## 4. Skip hyperscalers + dedup by NS set

- **Skip hyperscalers.** `providers.go` already has `isHyperscaler(ip)`. If all of a
  domain's `NSIPs` are hyperscaler, skip — they never allow AXFR and are a large share
  of volume.
- **Dedup by NS set for the *finding*.** Server-openness is a property of the (server)
  and repeats across every zone it hosts. Keep a process-level `sync.Map` keyed by the
  sorted-NS-IP tuple; the first domain on a given NS set establishes the server-open
  verdict. Note: with full-zone retention the *zone contents* are still per-domain, so
  a transfer runs per open zone — but we skip re-probing servers already known to
  refuse, which is where the volume savings are.

## 5. Storage — infer-and-discard

The raw zone is processed in memory to derive technology signals; only the derived
inferences and the misconfiguration finding are persisted.

- `model.DomainResult` gains an `AXFR AXFRResult` field, but its `Zone` slice is
  **consumed then dropped** — never written to any table.
- **Finding:** emit `PP_DNS_ZONE_TRANSFER_OPEN` (issue-template model) — evidence =
  server IP, zone, record count, `Truncated`. (No leaked hostnames in the finding.)
- **Inferences:** run a DNS→technology inference pass over the zone and persist the
  *results* (vendor/technology + confidence) to the tech-signal store, alongside the
  inferences already derivable from the public Tier-2 records.

### DNS→technology inference (mostly needs no AXFR)

Most tech signal comes from records **already collected** on every domain (the scan
already queries SPF/DMARC/DKIM/CAA/SRV/MTA-STS/BIMI slots — see `records.Plan`):

| Record | Signal |
| --- | --- |
| MX | mail platform (Google Workspace, M365, Proofpoint, Mimecast, Zoho) |
| NS | managed DNS (Cloudflare, Route53, NS1, Azure DNS, Akamai) |
| CNAME target | SaaS/CDN (CloudFront, Fastly, Zendesk, HubSpot, Shopify, Netlify, Salesforce) |
| SPF include chain (root TXT) | named vendors: `_spf.google.com`, `spf.protection.outlook.com`, `sendgrid.net`, `mailgun.org`, `_spf.salesforce.com`, `servers.mcsv.net` |
| DMARC `rua=` | DMARC reporting vendor (Valimail, dmarcian, Agari, Proofpoint) |
| DKIM selector | mail platform (`google._domainkey`, `selector1._domainkey` M365, `k1._domainkey` Mailchimp) |
| CAA | standardized CA (Let's Encrypt, DigiCert, Sectigo, Google Trust) |
| SRV / autodiscover | `autodiscover`⇒Exchange/M365, `_sip`/`_sipfederationtls`⇒Teams, `_xmpp`⇒chat |
| MTA-STS / TLS-RPT / BIMI | security-posture maturity + reporting vendor |
| DNSKEY / DS | DNSSEC adoption (maturity signal) |
| TXT verification | adopted SaaS (`google-site-verification`, `MS=`, `atlassian-domain-verification`, `stripe-verification`, `docusign=`) |
| A/AAAA range | hosting/cloud by ASN (AWS, GCP, Azure, DigitalOcean, OVH) |

AXFR's *unique* contribution is **internal-hostname hints** (`jenkins.`, `gitlab.`,
`jira.`, `sap.`, `vcenter.`, `citrix.`, `vpn-*.`) that never appear publicly. Build the
inference layer on the existing public-record data first (it stands alone and ties into
the existing techdetect/Wappalyzer work); AXFR then feeds extra internal-hostname
signals into the same pass for the open-zone minority.

**Non-contestable subdomain source:** Certificate Transparency logs / cert SAN lists
yield much of the same subdomain enumeration AXFR would, from a fully public channel.
For host discovery, CT is the safe workhorse; AXFR only adds never-certificated
internal names on top. A comprehensive inference layer should consume CT/SAN regardless
of whether AXFR is enabled.

### Beyond DNS — this wants to be a cross-source inference component

"Leave no signal out" spans sources, so the inference layer should be a **dedicated
component that aggregates per-company signals from all producers**, not a DNS-only
feature:
- **Web (CommonCrawl, already ingested):** response headers (`Server`, `X-Powered-By`,
  cookie names like `JSESSIONID`/`ASP.NET_SessionId`, WAF cookies `__cf_bm`), `<meta
  generator>`, script/link hosts (GTM, HubSpot, Segment, Intercom), framework and
  analytics/marketing-tag fingerprints, favicon hash.
- **TLS/CT:** issuer (CA vendor, corroborates CAA), SAN/CT (subdomains).
- **Ports/services (nmap in runner2):** banners → server software + versions (ground truth).

Each scanner stays a signal *producer*; the inference layer is the *consumer*, emitting
one fingerprint per company with per-signal confidence + provenance. This is the natural
home for the existing techdetect→Wappalyzer work, extended from web-only to DNS/TLS/port
signal types.

### Guardrails

- **No raw through the back door.** Do not store the evidence hostname/IP that triggered
  an inference in the product-facing record (that re-exposes the data we chose to drop).
  Keep triggering evidence internal/audit-only, or discard it.
- **Confidence tagging.** Internal-hostname inferences are strong-but-not-certain
  (naming conventions lie); tag them lower-confidence than record-based signals (MX/NS/TXT).

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

1. Land `ProbeAXFR` + tests behind `--axfr=false`. Extend the existing test DNS server
   harness (`internal/resolve/testserver_test.go`) to serve a canned zone, covering
   both the REFUSED path and a capped successful transfer, TDD-style.
2. Enable on a bounded `--limit` sample on the box; read real hit-rate and p99
   transfer time from stats.
3. Wire into the steady-state scan loop with tuned caps.

## Cost

- Refused case: 1 extra TCP round-trip per NS set (deduped) ≈ low-single-digit % over
  the current ~68 queries/domain.
- Open case: bounded by §2 caps, isolated by §3 lane — a handful of transfers, none
  able to stall the UDP workers.

## Risk — retention

Requesting AXFR is a standard, unauthenticated DNS query; if a server answers it chose
to serve the requester, and open-AXFR checks are standard in DNS posture tooling
(zonemaster — already integrated here — dnsrecon, nmap). The **request** is low-risk,
and the **misconfiguration finding** carries no exposure — it's pure defensive value.

The exposure lived in *retaining and productizing the raw zone* (internal hostnames,
dev/staging/VPN subdomains, internal IPs) — data the operator did not intend to publish.
Two axes: **authorization** (an open AXFR is a misconfiguration, not an intended public
channel — unlike a public web page — so the *access* is more contestable than
CommonCrawl/registry data regardless of jurisdiction), and **data protection** (GDPR
applies where names embed personal data, as it also does to public-web scraping). Note
much of the zone is independently discoverable (Certificate Transparency, passive DNS,
the company's own crawled pages) — which lowers the privacy delta *and* the unique
enrichment value, leaving the internal-only records as the small contestable slice.

**Resolution (this spec):** infer-and-discard (§5). The raw zone is never persisted to
the product store; only derived technology inferences and the finding are kept. This is
data minimization — it removes the retention exposure while keeping the useful signal,
which for the public-record inputs doesn't even require AXFR. Still recommended: an
allowlist/skip for domains outside authorized scope, and treating any internal-only
audit copy of triggering evidence (if kept at all) as restricted, not product data.
