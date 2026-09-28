# dns_detect slice 3 Implementation Plan: IP ranges with validity windows, and SPF below the apex

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:**
- Resolve apex and `www` A/AAAA records against provider-recon IP ranges. Each result's window is cut to the part of the record's window in which the range was valid; where ranges nest, the longest prefix wins.
- Resolve SPF `ip4:`/`ip6:` the same way.
- Route `v=spf1` records anywhere under the domain (e.g. `_spf.<domain>`) to the SPF analyzer.

**Architecture:**
- **IP index in the knowledge package.** Every range instance in the documents, including removed ones (kept indefinitely since slice 0), goes into a `gaissmai/bart` table of prefix → entries. Each entry carries its validity window.
- **Lookup.** `Knowledge.LookupIP(prefix)` returns every range whose prefix contains the given prefix, via `Supernets`.
- **Windowing.** A pure function `ipPieces(from, to, ranges)` splits the record's window at range boundaries and picks the longest prefix per piece. The `IP` analyzer and SPF `ip4`/`ip6` both use it.
- **Knowledge version** also hashes the ranges it uses (prefix, service, window), so a range change re-resolves results.

**Tech Stack:** Go 1.25 in `services/provider_recon`; `github.com/gaissmai/bart` (already a dependency); stdlib testing.

**Spec:** `docs/superpowers/specs/2026-09-28-dns-detect-service-design.md` (revision 2): "Output" (a window can be narrower for IP evidence), the `ip` row of "Routing and analyzers", and the SPF row. The owner ruled on 2026-09-28 that SPF below the apex is routed too, and that tests are ordinary tables.

## Rules

- **When a range is valid.**
  - **Start:** `first_seen`, except that ranges first seen on the timeline's first day (the earliest `first_seen` across all documents) are valid from the beginning of time (open start).
  - **End:** open while the range is `active`. For `missing` or `removed` ranges it is `last_seen`, the last day the feed listed it.
- **Pieces.** The record's window `[valid_from, valid_to]` (either side may be open) is split at every range boundary inside it. In each piece, the containing range valid for the whole piece with the longest prefix wins; ties go to the lower `rule_id`. Consecutive pieces with the same winning range are merged. A piece no range covers gives nothing.
- **A/AAAA rows.**
  - **Routing:** apex and `www` only. The value must parse as an IP; IPv4-mapped addresses are unmapped; anything else gives nothing.
  - **Rows:** one per service type of the winning range's service.
  - **Fields:** subject is the IP; `rule_id` is `<slug>/<service_key>/IP <cidr>`; confidence is the range's.
- **SPF `ip4:`/`ip6:` rows.**
  - **Parsing:** the value is a prefix; an address without a length becomes /32 or /128.
  - **Matching:** it matches ranges that contain the whole prefix.
  - **Rows:** service type `email_sending`, provider the range's provider. The service key is the range's service when that service has `email_sending`, otherwise empty.
  - **Window:** pieces as above.
- **SPF routing:** any TXT under the domain (apex or below) whose value is `v=spf1…` goes to `spf`. A `_name` TXT that isn't SPF still goes to `txt`.

---

### Task 1: SPF below the apex

- [ ] **Step 1: Failing test.** `_spf.example.se TXT "v=spf1 include:sendgrid.net -all"` routes to `spf` and gives an `email_sending sendgrid.net` row with record name `_spf.example.se`. `_spf.example.se TXT "hello"` still routes to `txt`.
- [ ] **Step 2: Implement** in `Route`.
- [ ] **Step 3: Checks.** `go vet`, gofmt and `go test -race ./...`.
- [ ] **Step 4: Commit:** `feat(provider_recon): SPF records anywhere under the domain are analysed`.

### Task 2: IP index in the knowledge package

- [ ] **Step 1: Failing tests.**
  - An active range has an open end.
  - A removed range ends at its `last_seen`.
  - A range from the first run has an open start; a later one starts at its `first_seen`.
  - Nested ranges: `LookupIP(/32)` returns both.
  - IPv6 works.
  - A prefix outside every range returns nothing.
  - An invalid CIDR refuses compilation.
  - The version changes when a range is added or when its status changes.
- [ ] **Step 2: Implement.**
  - `knowledge.IPRange{Prefix, ProviderSlug, ServiceKey, ServiceTypes, FeedTag, Confidence, From, To, RuleID}`.
  - `LookupIP(netip.Prefix) []IPRange` on the `Knowledge` interface.
  - The index is compiled from every service's `Evidence.IPRanges`, and the version includes the ranges.
- [ ] **Step 3: Checks.** The fake knowledge base in the resolve tests implements `LookupIP`.
- [ ] **Step 4: Commit:** `feat(provider_recon): detect/knowledge — IP index with range validity windows`.

### Task 3: Window pieces

- [ ] **Step 1: Failing tests** (table) for `ipPieces`:
  - one range covering the whole window;
  - nested ranges → the longer prefix;
  - a range removed mid-window → the piece ends on its `last_seen`, and the rest falls to the shorter prefix or to nothing;
  - a range starting after the window ends → none;
  - both sides open;
  - adjacent identical pieces merged.
- [ ] **Step 2: Implement** `ipPieces` in `resolve/ip.go`.
- [ ] **Step 3: Checks.**
- [ ] **Step 4: Commit:** `feat(provider_recon): IP evidence windows split at range changes`.

### Task 4: IP analyzer and SPF `ip4`/`ip6`

- [ ] **Step 1: Failing tests:**
  - apex A inside a CDN range → one row per service type, with the window cut;
  - `www` AAAA;
  - an A record at another subdomain → not routed;
  - a non-IP value → nothing;
  - an IPv4-mapped address;
  - SPF `ip4:198.51.100.0/24` inside a sending range → an `email_sending` row;
  - `ip4:` outside every range → nothing;
  - `ip6:` without a length.
- [ ] **Step 2: Implement** the `IP` analyzer, the routing, and the SPF `ip4`/`ip6` branch.
- [ ] **Step 3: Checks**, then update the README routing section and the spec's rows if the code differs.
- [ ] **Step 4: Commit:** `feat(provider_recon): IP analyzer for A/AAAA and SPF ip4/ip6 against provider ranges`.

### Task 5: Real-record check

- [ ] Resolve every record of spotify.com, volvo.com and loopia.se against the 37 live documents. Expected:
  - apex/`www` A/AAAA rows for addresses inside provider ranges (spotify.com's `35.186.224.24` is in Google Cloud space);
  - windows that are never wider than the record's;
  - no errors.

  Note the rows and anything surprising in the ledger.
