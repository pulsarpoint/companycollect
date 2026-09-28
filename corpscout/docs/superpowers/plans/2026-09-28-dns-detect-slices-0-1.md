# dns_detect slices 0–1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two things:
1. Provider-recon keeps removed items indefinitely (slice 0).
2. A new, isolated Go module `services/dns_detect` (slice 1) turns one domain's DNS records into `(service_type, provider)` services with evidence, as of a date.
   - It uses the NS, SOA, MX and CNAME analyzers, a knowledge index compiled from provider-recon documents, and a streaming CLI.

**Architecture:**
- **Packages:** `internal/model` (contract and `Snapshot`), `internal/hosts` (normalisation, public-suffix registrable domain), `internal/knowledge` (immutable `Index` behind a `Knowledge` interface), `internal/analyze` (one analyzer per record type plus the shared `LabelHost` fallback), `internal/engine` (snapshot, route, aggregate, deterministic order), and `cmd/dns-detect`.
- **Injected knowledge:** analyzers only see the `Knowledge` interface, so their tests inject a fake.
- **Verified before writing:** all code below was prototyped and passes `go test -race ./...`, `go vet` and gofmt. The CLI was also smoke-tested on the 37 production documents with real spotify.com, volvo.com and loopia.se records. Slice 0 was verified on a throwaway tree at 05fcb87d8+: 9/9 provider-recon packages pass.

**Tech Stack:** Go 1.26 (module directive `go 1.26.0`), `golang.org/x/net/publicsuffix` v0.59.0, stdlib testing.

**Spec:** `docs/superpowers/specs/2026-09-28-dns-detect-service-design.md` (owner-approved 2026-09-28, head 536f87417). Slices 2 (SPF/DKIM/DMARC/TXT and the new rule kinds) and 3 (the IP analyzer, `ChangePoints`, golden tests) are later plans.

## Global Constraints

- `dns_detect` never imports provider-recon packages. It decodes the published `provider-recon/v1` JSON with its own types.
- Knowledge is compiled once, immutable, and injected. No analyzer loads or reads anything else.
- Compilation refuses:
  - rule kinds outside the spec's list (NS/target, MX/target, CNAME/target, TXT/value, TXT/name, SPF/include, DKIM/selector, DKIM/target, DMARC/report);
  - unknown matchers;
  - invalid regexes;
  - other contract versions;
  - a provider key claimed by two providers.
- Same input, same result, same order.
- Empty lists serialise as `[]`, never `null`.
- `asOf` means the domain's latest observation at or before that date (spec, Time). `observed_at` is reported.
- Confidence without a rule: key match 0.8, self-hosted 0.8, unmapped 0.5. A rule's own confidence otherwise.
- Git:
  - commit by explicit path from the `corpscout` root;
  - Conventional Commits, each ending with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`;
  - work in a worktree under `companycollect/.worktrees/`.
- Checks per Go module: `go test -race ./...`, `go vet ./...`, and `test -z "$(gofmt -l .)"`.

## Review Focus

1. **A record instance with only `first_seen`**, open-ended and still present. Expected: it counts through `asOf` and sets `observed` to `asOf`. *(Task 1: `TestSnapshotUsesLatestObservationAtOrBeforeAsOf` covers dated and undated records. Add a first_seen-only row if the reviewer finds the rule unclear.)*
2. **Hosts that are IP literals, bare public suffixes or garbage in NS/MX/CNAME.** Expected: no detection, and no crash. *(Task 3: `192.0.2.53` NS row in `TestNSRuleKeyFallbackSelfHostedAndUnmapped`; Task 1: `TestRegistrable`.)*
3. **A provider document with a rule of a kind slice 1 doesn't analyse yet** (TXT/SPF…). Expected: it compiles (the kind is in the spec list), and analyzers simply never ask for it. The live documents contain TXT/value and TXT/name rules today. *(Task 2: `TestLoadDirCompilesRealDocuments` uses real documents.)*
4. **Rule precedence when two providers' rules match one host.** Expected: highest priority, then confidence, then rule id; deterministic. *(Task 2: `TestMatchPrefersPriorityThenConfidenceThenRuleID`.)*
5. **A large input stream in the CLI.** Expected: it streams one result per input without holding them all, and a malformed input stops with its position number. *(Task 5: `TestDetectRejectsBadInput`, `TestDetectStreamsOneResultPerInput`.)*

---

### Task 0: Provider-recon keeps removed items indefinitely

**Files:**
- Modify: `services/provider_recon/internal/assemble/assemble.go`, `services/provider_recon/internal/assemble/lifecycle_test.go`, `services/provider_recon/README.md`, `docs/superpowers/specs/2026-09-27-provider-recon-service-design.md`, `services/backoffice/app/routes/admin-provider-feeds-provider.tsx`

**Interfaces:** Produces `latest.json` documents that keep every removed item with its `removed_at` date. `Churn.Purged` stays in the contract and is always 0.

- [ ] **Step 1: Failing tests.** Apply to `internal/assemble/lifecycle_test.go`:

```diff
@@ -292,7 +292,7 @@ func TestCuratedRuleRemovedFromDefinition(t *testing.T) {
 	}
 }
 
-func TestServiceRemovedFromDefinitionThenPurged(t *testing.T) {
+func TestServiceRemovedFromDefinitionIsKept(t *testing.T) {
 	two := lcDef()
 	two.Services = append(two.Services, definitions.ServiceDef{Key: "aws.ec2", DisplayName: "EC2", ServiceTypes: []string{"iaas"}})
 	two.Feeds[0].TagMap["EC2"] = "aws.ec2"
@@ -317,31 +317,34 @@ func TestServiceRemovedFromDefinitionThenPurged(t *testing.T) {
 	if tags := d1.Collection.Collectors["aws_ip_ranges"].UnmappedTags; len(tags) != 1 || tags[0] != "EC2" {
 		t.Fatalf("unmapped = %v", tags)
 	}
-	d93 := run(t, lcDef(), out, &d1, 93)
-	for _, s := range d93.Services {
+	// Removed items are kept indefinitely (owner ruling 2026-09-28): the
+	// service and its removed range survive well past the old 90-day purge.
+	d500 := run(t, lcDef(), out, &d1, 500)
+	kept := false
+	for _, s := range d500.Services {
 		if s.Key == "aws.ec2" {
-			t.Fatal("service with only purged items should disappear")
+			kept = true
 		}
 	}
-	if len(ranges(d93, "3.5.140.0/22")) != 0 {
-		t.Fatal("removed range not purged after 90 days")
+	if !kept {
+		t.Fatal("service removed from the definition disappeared")
+	}
+	if got := ranges(d500, "3.5.140.0/22"); len(got) != 1 || got[0].RemovedAt != dstr(1) {
+		t.Fatalf("removed range after 500 days = %+v", got)
 	}
 }
 
-func TestRetentionPurgesRemovedAfter90Days(t *testing.T) {
+func TestRemovedItemsAreKeptIndefinitely(t *testing.T) {
 	d0 := run(t, lcDef(), cidrs(10), nil, 0)
 	d1 := run(t, lcDef(), cidrs(10, 3), &d0, 1)
 	d8 := run(t, lcDef(), cidrs(10, 3), &d1, 8)
-	d98 := run(t, lcDef(), cidrs(10, 3), &d8, 98)
-	if len(ranges(d98, "10.0.3.0/24")) != 1 {
-		t.Fatal("purged before the 90-day retention ended")
-	}
-	d99 := run(t, lcDef(), cidrs(10, 3), &d98, 99)
-	if len(ranges(d99, "10.0.3.0/24")) != 0 {
-		t.Fatal("not purged after 90 days")
+	d400 := run(t, lcDef(), cidrs(10, 3), &d8, 400)
+	got := ranges(d400, "10.0.3.0/24")
+	if len(got) != 1 || got[0].Status != model.StatusRemoved || got[0].RemovedAt == "" || got[0].FirstSeen != dstr(0) {
+		t.Fatalf("removed range after 400 days = %+v", got)
 	}
-	if c := d99.Collection.Collectors["aws_ip_ranges"].Churn; c.Purged != 1 {
-		t.Fatalf("churn = %+v", c)
+	if c := d400.Collection.Collectors["aws_ip_ranges"].Churn; c.Purged != 0 {
+		t.Fatalf("churn = %+v, want nothing purged", c)
 	}
 }
 
```

Run: `go test ./internal/assemble/` (in `services/provider_recon`)
Expected: FAIL. `service removed from the definition disappeared`, and `removed range after 400 days = []`.

- [ ] **Step 2: Remove the purge.** Apply to `internal/assemble/assemble.go`:

```diff
@@ -28,9 +28,6 @@ const (
 	curatedConfidence  = 1.0
 	officialConfidence = 1.0
 	bgpConfidence      = 0.8
-	// RemovedRetentionDays is how long a removed item stays in latest.json.
-	// History objects and the ClickHouse timeline keep it forever.
-	RemovedRetentionDays = 90
 )
 
 // bgpCollectors publish announcements, not operator-published ranges.
@@ -128,7 +125,6 @@ func Build(def definitions.Definition, outcomes map[string]FeedOutcome, prev *mo
 		appendRange(&doc, index, p)
 	}
 
-	purge(&doc, today)
 	model.Normalize(&doc)
 	hash, err := model.ContentHash(doc)
 	if err != nil {
@@ -551,46 +547,6 @@ func better(a, b model.IPRange) bool {
 	return a.Region < b.Region
 }
 
-// purge drops removed items older than the retention period, and services
-// dropped from the definition once they hold no items.
-func purge(doc *model.Document, today string) {
-	out := doc.Services[:0:0]
-	for _, s := range doc.Services {
-		e := &s.Evidence
-		e.IPRanges = keepFresh(e.IPRanges, func(x *model.IPRange) *model.Lifecycle { return &x.Lifecycle }, today, func(x model.IPRange) {
-			if st, ok := doc.Collection.Collectors[x.Collector]; ok {
-				st.Churn.Purged++
-				doc.Collection.Collectors[x.Collector] = st
-			}
-		})
-		e.ASNs = keepFresh(e.ASNs, func(x *model.ASN) *model.Lifecycle { return &x.Lifecycle }, today, nil)
-		e.DNSRules = keepFresh(e.DNSRules, func(x *model.DNSRule) *model.Lifecycle { return &x.Lifecycle }, today, nil)
-		e.HTTPRules = keepFresh(e.HTTPRules, func(x *model.HTTPRule) *model.Lifecycle { return &x.Lifecycle }, today, nil)
-		e.PTRRules = keepFresh(e.PTRRules, func(x *model.PTRRule) *model.Lifecycle { return &x.Lifecycle }, today, nil)
-		e.CertificateIdentities = keepFresh(e.CertificateIdentities, func(x *model.CertificateIdentity) *model.Lifecycle { return &x.Lifecycle }, today, nil)
-		if s.RemovedAt != "" && len(e.IPRanges)+len(e.ASNs)+len(e.DNSRules)+len(e.HTTPRules)+len(e.PTRRules)+len(e.CertificateIdentities) == 0 {
-			continue
-		}
-		out = append(out, s)
-	}
-	doc.Services = out
-}
-
-func keepFresh[T any](items []T, life func(*T) *model.Lifecycle, today string, onPurge func(T)) []T {
-	out := items[:0:0]
-	for _, it := range items {
-		l := life(&it)
-		if l.Status == model.StatusRemoved && daysBetween(l.RemovedAt, today) > RemovedRetentionDays {
-			if onPurge != nil {
-				onPurge(it)
-			}
-			continue
-		}
-		out = append(out, it)
-	}
-	return out
-}
-
 // upgrade gives items published before lifecycle tracking an active lifecycle
 // starting on the day of that run. It works on a copy.
 func upgrade(prev *model.Document) *model.Document {
```

- [ ] **Step 3: Docs and UI text.**
  - README: replace "Removed items stay in `latest.json` for 90 days. History objects keep them…" with "Removed items stay in `latest.json` indefinitely, with `removed_at` and `removal_action`, so the document is the complete timeline (owner ruling 2026-09-28)." and keep the rest of the paragraph accurate.
  - Provider-recon spec, **Retention** bullet: the same sentence, plus "(was 90 days until 2026-09-28)".
  - Backoffice provider page `CardDescription`: `Removed ranges stay here with their removal date; restore undoes a wrong removal.`

- [ ] **Step 4: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"` (provider_recon), then `npx vitest run tests/provider-feeds.test.tsx && npm run typecheck` (backoffice).
Expected: all pass.

- [ ] **Step 5: Commit.**

```bash
git add services/provider_recon/internal/assemble/assemble.go services/provider_recon/internal/assemble/lifecycle_test.go services/provider_recon/README.md docs/superpowers/specs/2026-09-27-provider-recon-service-design.md services/backoffice/app/routes/admin-provider-feeds-provider.tsx
git commit -m "feat(provider_recon): keep removed items indefinitely so latest.json is the full timeline

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Redeploy provider-recon** with its Ansible README Deploy block (S3 keys from the main checkout's `services/backoffice/.env`, run with `</dev/null` and output to a log). Then:

```bash
H=http://companycollect.taileb086.ts.net:8095; RUN=$(curl -s -X POST $H/v1/collect | jq -r .run_id); sleep 30; curl -s $H/v1/runs/$RUN | jq -c '{status, issues: (.issues|length)}'
```
Expected: `{"status":"succeeded","issues":0}`.

---

### Task 1: Module skeleton, model and hosts

**Files:**
- Create: `services/dns_detect/go.mod`, `go.sum`, `internal/model/model.go`, `internal/model/model_test.go`, `internal/hosts/hosts.go`, `internal/hosts/hosts_test.go`

**Interfaces:**
- Produces: `model.Record`, `model.Input`, `model.Evidence`, `model.Service`, `model.Finding` and `model.Result` (with `ObservedAt`); `model.Snapshot(records, asOf) ([]Record, string)`; `model.SelfHosted`.
- Produces: `hosts.Normalize`, `hosts.Registrable` and `hosts.Under`.

- [ ] **Step 1: Module.** From `corpscout/services`, run `mkdir dns_detect && cd dns_detect && go mod init dns_detect`, then set the directive to `go 1.26.0`.

- [ ] **Step 2: Failing tests.** `internal/model/model_test.go`:

```go
package model

import (
	"reflect"
	"testing"
)

func values(rs []Record) []string {
	out := []string{}
	for _, r := range rs {
		out = append(out, r.Value)
	}
	return out
}

// Point scans on 08-10, 09-19 and 09-26: each instance carries its scan's window.
var scans = []Record{
	{Value: "ns-a", FirstSeen: "2026-08-10", LastSeen: "2026-08-10"},
	{Value: "ns-a", FirstSeen: "2026-09-19", LastSeen: "2026-09-19 00:00:00.000"},
	{Value: "ns-b", FirstSeen: "2026-09-19", LastSeen: "2026-09-19"},
	{Value: "ns-a", FirstSeen: "2026-09-26", LastSeen: "2026-09-26"},
	{Value: "always"},
}

func TestSnapshotUsesLatestObservationAtOrBeforeAsOf(t *testing.T) {
	for _, c := range []struct {
		asOf, observed string
		want           []string
	}{
		{"2026-09-25", "2026-09-19", []string{"ns-a", "ns-b", "always"}},
		{"2026-09-19", "2026-09-19", []string{"ns-a", "ns-b", "always"}},
		{"2026-08-20", "2026-08-10", []string{"ns-a", "always"}},
		{"2026-12-31", "2026-09-26", []string{"ns-a", "always"}},
		{"2026-01-01", "", []string{"always"}},
	} {
		got, observed := Snapshot(scans, c.asOf)
		if !reflect.DeepEqual(values(got), c.want) || observed != c.observed {
			t.Errorf("Snapshot(%s) = %v @%s, want %v @%s", c.asOf, values(got), observed, c.want, c.observed)
		}
	}
}

func TestSnapshotCarriesTheLastObservationForward(t *testing.T) {
	dated := scans[:4]
	got, observed := Snapshot(dated, "2027-03-01")
	if !reflect.DeepEqual(values(got), []string{"ns-a"}) || observed != "2026-09-26" {
		t.Fatalf("after the last scan: %v @%s, want [ns-a] @2026-09-26", values(got), observed)
	}
	got, observed = Snapshot(dated, "2026-09-25")
	if !reflect.DeepEqual(values(got), []string{"ns-a", "ns-b"}) || observed != "2026-09-19" {
		t.Fatalf("between scans: %v @%s", values(got), observed)
	}
	if got, _ := Snapshot(dated, "2026-01-01"); len(got) != 0 {
		t.Fatalf("before the first scan: %v", values(got))
	}
	if got, observed := Snapshot(dated, ""); len(got) != 4 || observed != "" {
		t.Fatal("empty asOf must keep every record")
	}
}
```

`internal/hosts/hosts_test.go`:

```go
package hosts

import "testing"

func TestNormalize(t *testing.T) {
	for in, want := range map[string]string{
		"NS1.Binero.SE.":   "ns1.binero.se",
		" mx.example.com ": "mx.example.com",
		".":                "",
	} {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRegistrable(t *testing.T) {
	for in, want := range map[string]string{
		"ns1.binero.se.":         "binero.se",
		"mx1.example.co.uk":      "example.co.uk",
		"ns-12.awsdns-01.co.uk.": "awsdns-01.co.uk",
		"foo.bar.github.io":      "bar.github.io",
		"aspmx.l.google.com":     "google.com",
		"com":                    "",
		"192.0.2.1":              "",
		"":                       "",
	} {
		if got := Registrable(in); got != want {
			t.Errorf("Registrable(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestUnder(t *testing.T) {
	if !Under("mail.example.se.", "example.se") || !Under("example.se", "EXAMPLE.se") {
		t.Fatal("host under its domain not recognised")
	}
	if Under("badexample.se", "example.se") {
		t.Fatal("suffix without a dot boundary counted as under")
	}
}
```

Run: `go test ./...`
Expected: FAIL (undefined: `Snapshot`, `Record`, `Normalize`, …).

- [ ] **Step 3: Implement.** Run `go get golang.org/x/net@v0.59.0`.

`internal/model/model.go`:

```go
// Package model is the dns_detect JSON contract: the records a caller sends
// and the services and evidence it gets back.
package model

// Record is one DNS record as the DNS store keeps it: presentation type and
// RDATA, with the dates it was seen. Dates are YYYY-MM-DD; an empty
// FirstSeen/LastSeen leaves that side of the window open.
type Record struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Value     string `json:"value"`
	FirstSeen string `json:"first_seen,omitempty"`
	LastSeen  string `json:"last_seen,omitempty"`
}

func day(date string) string { return date[:min(len(date), 10)] }

// Snapshot returns the records describing the domain as of asOf
// (YYYY-MM-DD), and the date of that observation.
//
// DNS observations are point scans: a record instance is stamped with the
// scans that saw it, so a date between two scans lies inside no window. The
// snapshot is therefore the domain's latest observation at or before asOf:
// observed = max over records with first_seen <= asOf of min(last_seen, asOf),
// and the records kept are those with first_seen <= asOf and last_seen >=
// observed. After the last scan this carries the last known state forward.
// An empty asOf keeps every record; a record without dates is always kept
// and does not move the observation date.
func Snapshot(records []Record, asOf string) ([]Record, string) {
	if asOf == "" {
		return records, ""
	}
	observed := ""
	for _, r := range records {
		if r.FirstSeen == "" && r.LastSeen == "" {
			continue // undated: always present, says nothing about when the domain was observed
		}
		if r.FirstSeen != "" && day(r.FirstSeen) > asOf {
			continue
		}
		last := asOf
		if r.LastSeen != "" && day(r.LastSeen) < asOf {
			last = day(r.LastSeen)
		}
		observed = max(observed, last)
	}
	var out []Record
	for _, r := range records {
		if r.FirstSeen != "" && day(r.FirstSeen) > asOf {
			continue
		}
		if r.LastSeen != "" && day(r.LastSeen) < observed {
			continue
		}
		out = append(out, r)
	}
	return out, observed
}

// Input is one domain and all its records.
type Input struct {
	Domain  string   `json:"domain"`
	Records []Record `json:"records"`
}

// Evidence is one record (or part of one) that proves a service.
type Evidence struct {
	Analyzer   string  `json:"analyzer"`
	RecordName string  `json:"record_name"`
	RecordType string  `json:"record_type"`
	Value      string  `json:"value"`
	RuleID     string  `json:"rule_id,omitempty"`
	Confidence float64 `json:"confidence"`
}

// Service is one (service type, provider) the domain uses, with its evidence.
// ProviderKey is the provider-recon slug for a named provider, the
// registrable domain for an unmapped one, and "self-hosted".
type Service struct {
	ServiceType  string     `json:"service_type"`
	ProviderKey  string     `json:"provider_key"`
	ProviderSlug string     `json:"provider_slug"`
	ServiceKeys  []string   `json:"service_keys"`
	Confidence   float64    `json:"confidence"`
	Evidence     []Evidence `json:"evidence"`
}

// Finding is an observation that is not a service (e.g. two SPF records).
type Finding struct {
	Analyzer string `json:"analyzer"`
	Code     string `json:"code"`
	Detail   string `json:"detail,omitempty"`
}

// Result is the answer for one domain at one date.
type Result struct {
	Domain           string    `json:"domain"`
	AsOf             string    `json:"as_of,omitempty"`
	ObservedAt       string    `json:"observed_at,omitempty"`
	KnowledgeVersion string    `json:"knowledge_version"`
	Services         []Service `json:"services"`
	Findings         []Finding `json:"findings"`
}

// SelfHosted is the provider key of evidence that points at the domain itself.
const SelfHosted = "self-hosted"
```

`internal/hosts/hosts.go`:

```go
// Package hosts normalises DNS host names and derives provider keys.
package hosts

import (
	"strings"

	"golang.org/x/net/publicsuffix"
)

// Normalize lower-cases a host and drops surrounding space and the trailing dot.
func Normalize(host string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
}

// Registrable is the host's registrable domain (eTLD+1) from the public
// suffix list, e.g. ns1.binero.se → binero.se, x.github.io → x.github.io.
// It returns "" for a host that has none (a bare suffix, an IP, garbage).
func Registrable(host string) string {
	host = Normalize(host)
	if host == "" || strings.Trim(host, "0123456789.:") == "" {
		return ""
	}
	key, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return ""
	}
	return key
}

// Under reports whether host is domain itself or a name below it.
func Under(host, domain string) bool {
	host, domain = Normalize(host), Normalize(domain)
	return host == domain || strings.HasSuffix(host, "."+domain)
}
```

Run `go mod tidy`. `go.mod` must then read:

```
module dns_detect

go 1.26.0

require golang.org/x/net v0.59.0
```

- [ ] **Step 4: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS (model, hosts).

- [ ] **Step 5: Commit.**

```bash
git add services/dns_detect/go.mod services/dns_detect/go.sum services/dns_detect/internal/model services/dns_detect/internal/hosts
git commit -m "feat(dns_detect): module with the record/result contract, point-scan snapshots and host keys

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Knowledge index

**Files:**
- Create: `internal/knowledge/document.go`, `knowledge.go`, `load.go`, `knowledge_test.go`, and `testdata/providers/{beebyte,cloudflare,glesys,godaddy,loopia}.json`

**Interfaces:**
- Produces:
  - `knowledge.Kind`, with vars `NSTarget`, `MXTarget`, `CNAMETarget`, `TXTValue`, `TXTName`, `SPFInclude`, `DKIMSelector`, `DKIMTarget`, `DMARCReport`;
  - `knowledge.Match{ProviderSlug, ServiceKey, ServiceTypes, RuleID, Confidence}`;
  - `knowledge.Provider{Slug, Name, Country, Services []ProviderService}` with `FirstService(serviceType) string`, and `knowledge.ProviderService{Key, Types}`;
  - `knowledge.Knowledge` (`Match`, `ProviderForKey`, `Version`);
  - `knowledge.Compile([][]byte) (*Index, error)` and `knowledge.LoadDir(dir) (*Index, error)`.

- [ ] **Step 1: Fixtures**, real production documents, pretty-printed:

```bash
cd services/dns_detect && mkdir -p internal/knowledge/testdata/providers
for s in loopia glesys godaddy cloudflare beebyte; do
  ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT json FROM corpscout.provider_recon_documents_s3 WHERE JSONExtractString(json,'slug')='$s' FORMAT RawBLOB\"" \
  | python3 -c "import json,sys; json.dump(json.load(sys.stdin), open('internal/knowledge/testdata/providers/$s.json','w'), indent=2, sort_keys=True)"
done
```
Expected: 5 files, 1–15 KB each. GoDaddy's key is `domaincontrol.com`. Cloudflare has the `cloudflare.dns`, `cloudflare.edge` (cdn, ddos_protection, waf) and `cloudflare.email-routing` services. Loopia's NS and MX rules are suffix `loopia.se`.

- [ ] **Step 2: Failing tests.** `internal/knowledge/knowledge_test.go`:

```go
package knowledge

import (
	"encoding/json"
	"strings"
	"testing"
)

// rule and doc build provider-recon/v1 documents for tests.
func rule(recordType, field, matcher, pattern string, priority int, confidence float64) map[string]any {
	return map[string]any{"record_type": recordType, "match_field": field, "matcher_type": matcher,
		"pattern": pattern, "priority": priority, "confidence": confidence, "status": "active"}
}

func svc(key string, types []string, rules ...map[string]any) map[string]any {
	if rules == nil {
		rules = []map[string]any{}
	}
	return map[string]any{"service_key": key, "service_types": types, "evidence": map[string]any{"dns_rules": rules}}
}

func doc(t *testing.T, slug string, keys []string, services ...map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"version": "provider-recon/v1", "slug": slug, "display_name": strings.ToUpper(slug),
		"provider_keys": keys, "services": services})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestLoadDirCompilesRealDocuments(t *testing.T) {
	idx, err := LoadDir("testdata/providers")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		kind    Kind
		subject string
		service string
	}{
		{NSTarget, "ns1.loopia.se", "loopia.dns"},
		{NSTarget, "abby.ns.cloudflare.com", "cloudflare.dns"},
		{NSTarget, "ns51.domaincontrol.com", "godaddy.dns"},
		{MXTarget, "route1.mx.cloudflare.net", "cloudflare.email-routing"},
		{CNAMETarget, "example.com.cdn.cloudflare.net", "cloudflare.edge"},
	} {
		m, ok := idx.Match(c.kind, c.subject)
		if !ok || m.ServiceKey != c.service {
			t.Errorf("Match(%s, %q) = %+v %v, want %s", c.kind, c.subject, m, ok, c.service)
		}
	}
	if p, ok := idx.ProviderForKey("domaincontrol.com"); !ok || p.Slug != "godaddy" || p.FirstService("dns") != "godaddy.dns" {
		t.Fatalf("ProviderForKey(domaincontrol.com) = %+v %v", p, ok)
	}
	if !strings.HasPrefix(idx.Version(), "sha256:") {
		t.Fatalf("version = %q", idx.Version())
	}
	again, err := LoadDir("testdata/providers")
	if err != nil || again.Version() != idx.Version() {
		t.Fatalf("version not stable: %v %q vs %q", err, again.Version(), idx.Version())
	}
}

func TestMatchPrefersPriorityThenConfidenceThenRuleID(t *testing.T) {
	idx, err := Compile([][]byte{
		doc(t, "a", nil, svc("a.low", []string{"dns"}, rule("NS", "target", "suffix", "example.net", 0, 1))),
		doc(t, "b", nil, svc("b.high", []string{"dns"}, rule("NS", "target", "suffix", "ns.example.net", 10, 0.5))),
		doc(t, "c", nil, svc("c.tie", []string{"dns"}, rule("NS", "target", "suffix", "ns.example.net", 10, 0.5))),
	})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := idx.Match(NSTarget, "NS1.ns.example.net")
	if !ok || m.ServiceKey != "b.high" {
		t.Fatalf("got %+v; want b.high (priority beats confidence, rule id breaks the tie)", m)
	}
	if m, _ := idx.Match(NSTarget, "ns1.other.example.net"); m.ServiceKey != "a.low" {
		t.Fatalf("got %+v; want a.low", m)
	}
	if _, ok := idx.Match(NSTarget, "notexample.net"); ok {
		t.Fatal("suffix matched without a dot boundary")
	}
}

func TestMatcherTypes(t *testing.T) {
	idx, err := Compile([][]byte{doc(t, "p", nil,
		svc("p.exact", []string{"email"}, rule("MX", "target", "exact", "mx.p.com", 0, 1)),
		svc("p.prefix", []string{"saas_verification"}, rule("TXT", "value", "prefix", "p-verification=", 0, 1)),
		svc("p.contains", []string{"email_sending"}, rule("TXT", "value", "contains", "include:spf.p.com", 0, 1)),
		svc("p.regex", []string{"dns"}, rule("NS", "target", "regex", `^ns[0-9]+\.p\.com$`, 0, 1)),
	)})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		kind    Kind
		subject string
		want    string
	}{
		{MXTarget, "MX.P.COM", "p.exact"},
		{MXTarget, "a.mx.p.com", ""},
		{TXTValue, "p-verification=abc", "p.prefix"},
		{TXTValue, "v=spf1 include:spf.p.com ~all", "p.contains"},
		{NSTarget, "ns12.p.com", "p.regex"},
		{NSTarget, "xns12.p.com", ""},
	} {
		m, _ := idx.Match(c.kind, c.subject)
		if m.ServiceKey != c.want {
			t.Errorf("Match(%s, %q) = %q, want %q", c.kind, c.subject, m.ServiceKey, c.want)
		}
	}
}

func TestProviderKeysExactAndGlob(t *testing.T) {
	idx, err := Compile([][]byte{
		doc(t, "aws", []string{"amazonaws.com", "awsdns-*"}, svc("aws.route53", []string{"dns"}), svc("aws.ses", []string{"email_sending"})),
	})
	if err != nil {
		t.Fatal(err)
	}
	p, ok := idx.ProviderForKey("awsdns-01.co.uk")
	if !ok || p.Slug != "aws" || p.FirstService("dns") != "aws.route53" || p.FirstService("cdn") != "" {
		t.Fatalf("glob key: %+v %v", p, ok)
	}
	if _, ok := idx.ProviderForKey("example.com"); ok {
		t.Fatal("unknown key matched")
	}
}

func TestRemovedRulesAndServicesAreIgnored(t *testing.T) {
	removedRule := rule("NS", "target", "suffix", "old.example.net", 0, 1)
	removedRule["status"] = "removed"
	removedSvc := svc("p.gone", []string{"dns"}, rule("NS", "target", "suffix", "gone.example.net", 0, 1))
	removedSvc["removed_at"] = "2026-09-01"
	idx, err := Compile([][]byte{doc(t, "p", []string{"p.com"}, svc("p.dns", []string{"dns"}, removedRule), removedSvc)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.Match(NSTarget, "ns.old.example.net"); ok {
		t.Fatal("removed rule matched")
	}
	if _, ok := idx.Match(NSTarget, "ns.gone.example.net"); ok {
		t.Fatal("rule of a removed service matched")
	}
	if p, _ := idx.ProviderForKey("p.com"); p.FirstService("dns") != "p.dns" {
		t.Fatal("removed service offered as the provider's dns service")
	}
}

func TestCompileRefusesBadKnowledge(t *testing.T) {
	for name, raw := range map[string][]byte{
		"unknown kind":    doc(t, "p", nil, svc("p.a", []string{"cdn"}, rule("A", "value", "exact", "192.0.2.1", 0, 1))),
		"unknown matcher": doc(t, "p", nil, svc("p.a", []string{"dns"}, rule("NS", "target", "glob", "*.p.com", 0, 1))),
		"invalid regex":   doc(t, "p", nil, svc("p.a", []string{"dns"}, rule("NS", "target", "regex", "(", 0, 1))),
		"other contract":  []byte(`{"version":"provider-recon/v2","slug":"p","services":[]}`),
		"not json":        []byte(`{`),
	} {
		if _, err := Compile([][]byte{raw}); err == nil {
			t.Errorf("%s: compiled without error", name)
		}
	}
	dup := [][]byte{doc(t, "a", []string{"shared.com"}), doc(t, "b", []string{"shared.com"})}
	if _, err := Compile(dup); err == nil {
		t.Error("a provider key claimed by two providers compiled")
	}
}
```

Run: `go test ./internal/knowledge/`
Expected: FAIL (undefined: `LoadDir`, `Compile`, …).

- [ ] **Step 3: Implement.** `internal/knowledge/document.go`:

```go
package knowledge

// The subset of a provider-recon/v1 document (providers/<slug>/latest.json)
// the engine reads. dns_detect decodes the published JSON with its own types;
// it never imports provider-recon's packages.

const contractVersion = "provider-recon/v1"

type document struct {
	Version      string    `json:"version"`
	Slug         string    `json:"slug"`
	DisplayName  string    `json:"display_name"`
	Country      string    `json:"country"`
	ProviderKeys []string  `json:"provider_keys"`
	Services     []service `json:"services"`
}

type service struct {
	Key          string   `json:"service_key"`
	ServiceTypes []string `json:"service_types"`
	RemovedAt    string   `json:"removed_at"`
	Evidence     struct {
		DNSRules []dnsRule `json:"dns_rules"`
	} `json:"evidence"`
}

type dnsRule struct {
	RecordType    string  `json:"record_type"`
	MatchField    string  `json:"match_field"`
	MatcherType   string  `json:"matcher_type"`
	Pattern       string  `json:"pattern"`
	CaseSensitive bool    `json:"case_sensitive"`
	Confidence    float64 `json:"confidence"`
	Priority      int     `json:"priority"`
	Status        string  `json:"status"`
}
```

`internal/knowledge/knowledge.go`:

```go
// Package knowledge compiles provider-recon documents into the immutable index
// every analyzer queries. Analyzers get it injected (as the Knowledge
// interface) and never load anything themselves, so each can be tested with a
// small hand-built index.
package knowledge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
)

// Kind names what a rule is matched against: a record type plus a field.
type Kind struct {
	RecordType string
	MatchField string
}

func (k Kind) String() string { return k.RecordType + "/" + k.MatchField }

// Rule kinds (spec: "Rule kinds"). Compilation refuses any other kind, so a
// rule no analyzer handles is rejected at load time instead of ignored.
var (
	NSTarget     = Kind{"NS", "target"}
	MXTarget     = Kind{"MX", "target"}
	CNAMETarget  = Kind{"CNAME", "target"}
	TXTValue     = Kind{"TXT", "value"}
	TXTName      = Kind{"TXT", "name"}
	SPFInclude   = Kind{"SPF", "include"}
	DKIMSelector = Kind{"DKIM", "selector"}
	DKIMTarget   = Kind{"DKIM", "target"}
	DMARCReport  = Kind{"DMARC", "report"}
)

var knownKinds = []Kind{NSTarget, MXTarget, CNAMETarget, TXTValue, TXTName, SPFInclude, DKIMSelector, DKIMTarget, DMARCReport}

// Match is a rule that matched: the provider service it names.
type Match struct {
	ProviderSlug string
	ServiceKey   string
	ServiceTypes []string
	RuleID       string
	Confidence   float64
}

// Provider is a named provider-recon provider with its live services.
type Provider struct {
	Slug     string
	Name     string
	Country  string
	Services []ProviderService
}

// ProviderService is one live service of a provider, in document order.
type ProviderService struct {
	Key   string
	Types []string
}

// FirstService is the provider's first live service of serviceType, in
// document order, or "" when it has none.
func (p Provider) FirstService(serviceType string) string {
	for _, s := range p.Services {
		if slices.Contains(s.Types, serviceType) {
			return s.Key
		}
	}
	return ""
}

// Knowledge is what analyzers may ask.
type Knowledge interface {
	// Match returns the best rule of kind matching subject: highest priority,
	// then confidence, then rule id.
	Match(kind Kind, subject string) (Match, bool)
	// ProviderForKey returns the provider owning a registrable domain, by an
	// exact provider key or a glob key such as "awsdns-*".
	ProviderForKey(key string) (Provider, bool)
	// Version identifies the documents the index was built from.
	Version() string
}

type compiledRule struct {
	Match
	matches  func(string) bool
	priority int
	caseSens bool
}

// Index is the compiled Knowledge.
type Index struct {
	rules     map[Kind][]compiledRule
	exactKeys map[string]*Provider
	globKeys  []globKey
	version   string
}

type globKey struct {
	pattern  string
	provider *Provider
}

var _ Knowledge = (*Index)(nil)

// Compile builds the index from provider-recon documents (raw JSON, one per
// provider). It fails on a document of another contract version, a rule of an
// unknown kind or matcher, or an invalid regex.
func Compile(raw [][]byte) (*Index, error) {
	idx := &Index{rules: map[Kind][]compiledRule{}, exactKeys: map[string]*Provider{}}
	sums := make([]string, 0, len(raw))
	for _, b := range raw {
		sum := sha256.Sum256(b)
		sums = append(sums, hex.EncodeToString(sum[:]))
		var d document
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, fmt.Errorf("decode provider document: %w", err)
		}
		if d.Version != contractVersion {
			return nil, fmt.Errorf("provider %q: contract %q, want %q", d.Slug, d.Version, contractVersion)
		}
		if err := idx.add(d); err != nil {
			return nil, err
		}
	}
	for kind := range idx.rules {
		slices.SortFunc(idx.rules[kind], func(a, b compiledRule) int { return strings.Compare(a.RuleID, b.RuleID) })
	}
	slices.SortFunc(idx.globKeys, func(a, b globKey) int { return strings.Compare(a.pattern, b.pattern) })
	slices.Sort(sums)
	all := sha256.Sum256([]byte(strings.Join(sums, "\n")))
	idx.version = "sha256:" + hex.EncodeToString(all[:])
	return idx, nil
}

func (idx *Index) add(d document) error {
	p := &Provider{Slug: d.Slug, Name: d.DisplayName, Country: d.Country}
	for _, s := range d.Services {
		if s.RemovedAt != "" {
			continue
		}
		p.Services = append(p.Services, ProviderService{Key: s.Key, Types: s.ServiceTypes})
		for _, r := range s.Evidence.DNSRules {
			if r.Status == "removed" {
				continue
			}
			kind := Kind{strings.ToUpper(r.RecordType), strings.ToLower(r.MatchField)}
			if !slices.Contains(knownKinds, kind) {
				return fmt.Errorf("provider %q service %q: unsupported rule kind %s", d.Slug, s.Key, kind)
			}
			id := fmt.Sprintf("%s/%s/%s %s %s", d.Slug, s.Key, kind, r.MatcherType, r.Pattern)
			fn, err := matcher(r)
			if err != nil {
				return fmt.Errorf("rule %s: %w", id, err)
			}
			idx.rules[kind] = append(idx.rules[kind], compiledRule{
				Match:    Match{ProviderSlug: d.Slug, ServiceKey: s.Key, ServiceTypes: s.ServiceTypes, RuleID: id, Confidence: r.Confidence},
				matches:  fn,
				priority: r.Priority,
				caseSens: r.CaseSensitive,
			})
		}
	}
	for _, k := range d.ProviderKeys {
		k = strings.ToLower(k)
		if strings.Contains(k, "*") {
			if _, err := path.Match(k, ""); err != nil {
				return fmt.Errorf("provider %q: bad key pattern %q", d.Slug, k)
			}
			idx.globKeys = append(idx.globKeys, globKey{pattern: k, provider: p})
			continue
		}
		if other, dup := idx.exactKeys[k]; dup && other.Slug != d.Slug {
			return fmt.Errorf("provider key %q claimed by both %q and %q", k, other.Slug, d.Slug)
		}
		idx.exactKeys[k] = p
	}
	return nil
}

func matcher(r dnsRule) (func(string) bool, error) {
	pattern := r.Pattern
	if !r.CaseSensitive {
		pattern = strings.ToLower(pattern)
	}
	switch r.MatcherType {
	case "exact":
		return func(s string) bool { return s == pattern }, nil
	case "suffix":
		return func(s string) bool { return s == pattern || strings.HasSuffix(s, "."+pattern) }, nil
	case "prefix":
		return func(s string) bool { return strings.HasPrefix(s, pattern) }, nil
	case "contains":
		return func(s string) bool { return strings.Contains(s, pattern) }, nil
	case "exists":
		return func(s string) bool { return s != "" }, nil
	case "regex":
		expr := r.Pattern
		if !r.CaseSensitive {
			expr = "(?i)" + expr
		}
		re, err := regexp.Compile(expr)
		if err != nil {
			return nil, fmt.Errorf("invalid regex %q: %w", r.Pattern, err)
		}
		return re.MatchString, nil
	}
	return nil, fmt.Errorf("unsupported matcher %q", r.MatcherType)
}

// Match implements Knowledge.
func (idx *Index) Match(kind Kind, subject string) (Match, bool) {
	var best *compiledRule
	lower := strings.ToLower(subject)
	for i := range idx.rules[kind] {
		r := &idx.rules[kind][i]
		s := lower
		if r.caseSens {
			s = subject
		}
		if !r.matches(s) {
			continue
		}
		if best == nil || r.priority > best.priority || (r.priority == best.priority && r.Confidence > best.Confidence) {
			best = r
		}
	}
	if best == nil {
		return Match{}, false
	}
	return best.Match, true
}

// ProviderForKey implements Knowledge.
func (idx *Index) ProviderForKey(key string) (Provider, bool) {
	key = strings.ToLower(key)
	if p, ok := idx.exactKeys[key]; ok {
		return *p, true
	}
	for _, g := range idx.globKeys {
		if ok, _ := path.Match(g.pattern, key); ok {
			return *g.provider, true
		}
	}
	return Provider{}, false
}

// Version implements Knowledge.
func (idx *Index) Version() string { return idx.version }
```

`internal/knowledge/load.go`:

```go
package knowledge

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// LoadDir compiles every *.json provider document in dir (the layout of a
// local copy of the provider-recon bucket's providers/<slug>/latest.json
// files flattened to <slug>.json, and of the test fixtures).
func LoadDir(dir string) (*Index, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no provider documents in %s", dir)
	}
	sort.Strings(paths)
	raw := make([][]byte, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		raw = append(raw, b)
	}
	return Compile(raw)
}
```

- [ ] **Step 4: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS (knowledge: 6 tests).

- [ ] **Step 5: Commit.**

```bash
git add services/dns_detect/internal/knowledge
git commit -m "feat(dns_detect): knowledge index compiled from provider-recon documents

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Host analyzers (NS, SOA, MX, CNAME) and the shared labelling

**Files:**
- Create: `internal/analyze/analyze.go`, `label.go`, `host_analyzers.go`, `fake_test.go`, `host_analyzers_test.go`

**Interfaces:**
- Consumes: `knowledge.Knowledge`, `knowledge.Kind` vars, `model.*`, `hosts.*`.
- Produces:
  - `analyze.Scope{Domain, AsOf, Records}` with `Named(name, type)` and `Apex(type)`;
  - `analyze.Detection`, `analyze.Output{Detections, Findings}` and `analyze.Analyzer` (`Name`, `Analyze(Scope, Knowledge) Output`);
  - `analyze.LabelHost(...)`, with the constants `KeyMatchConfidence`, `SelfHostedConfidence` and `UnmappedConfidence`;
  - the analyzers `analyze.NS{}`, `SOA{}`, `MX{}` and `CNAME{}`.

- [ ] **Step 1: Failing tests.** `internal/analyze/fake_test.go`, a fake knowledge base injected into every analyzer test:

```go
package analyze

import (
	"reflect"
	"testing"

	"dns_detect/internal/knowledge"
	"dns_detect/internal/model"
)

// fakeKB is an injected knowledge base: exact subjects per kind, and
// provider keys. It stands in for the compiled index in analyzer tests.
type fakeKB struct {
	rules map[knowledge.Kind]map[string]knowledge.Match
	keys  map[string]knowledge.Provider
}

func (f fakeKB) Match(kind knowledge.Kind, subject string) (knowledge.Match, bool) {
	m, ok := f.rules[kind][subject]
	return m, ok
}

func (f fakeKB) ProviderForKey(key string) (knowledge.Provider, bool) {
	p, ok := f.keys[key]
	return p, ok
}

func (fakeKB) Version() string { return "fake" }

var kb = fakeKB{
	rules: map[knowledge.Kind]map[string]knowledge.Match{
		knowledge.NSTarget: {
			"abby.ns.cloudflare.com": {ProviderSlug: "cloudflare", ServiceKey: "cloudflare.dns", ServiceTypes: []string{"dns"}, RuleID: "cloudflare/ns", Confidence: 1},
		},
		knowledge.MXTarget: {
			"aspmx.l.google.com": {ProviderSlug: "google", ServiceKey: "google.workspace-mail", ServiceTypes: []string{"email"}, RuleID: "google/mx", Confidence: 1},
		},
		knowledge.CNAMETarget: {
			"atc.spotify.map.fastly.net": {ProviderSlug: "fastly", ServiceKey: "fastly.edge", ServiceTypes: []string{"cdn", "ddos_protection"}, RuleID: "fastly/cname", Confidence: 1},
		},
	},
	keys: map[string]knowledge.Provider{
		"binero.se": {Slug: "binero", Services: []knowledge.ProviderService{{Key: "binero.web", Types: []string{"hosting"}}, {Key: "binero.dns", Types: []string{"dns"}}}},
	},
}

func rec(name, typ, value string) model.Record {
	return model.Record{Name: name, Type: typ, Value: value}
}

func scope(domain string, recs ...model.Record) Scope { return Scope{Domain: domain, Records: recs} }

// short renders detections as "type provider_key slug service_key value confidence" for compact assertions.
func short(ds []Detection) [][]any {
	out := [][]any{}
	for _, d := range ds {
		out = append(out, []any{d.ServiceType, d.ProviderKey, d.ProviderSlug, d.ServiceKey, d.Evidence.Value, d.Evidence.Confidence})
	}
	return out
}

func assertDetections(t *testing.T, got []Detection, want [][]any) {
	t.Helper()
	if g := short(got); !reflect.DeepEqual(g, want) {
		t.Fatalf("detections\n got %v\nwant %v", g, want)
	}
}
```

`internal/analyze/host_analyzers_test.go`:

```go
package analyze

import (
	"testing"

	"dns_detect/internal/model"
)

func TestNSRuleKeyFallbackSelfHostedAndUnmapped(t *testing.T) {
	out := NS{}.Analyze(scope("example.se",
		rec("example.se", "NS", "ABBY.ns.cloudflare.com."),
		rec("example.se", "NS", "ns1.binero.se."),
		rec("example.se", "NS", "ns1.example.se."),
		rec("example.se", "NS", "dns1.p07.nsone.net."),
		rec("example.se", "NS", "192.0.2.53"),
		rec("sub.example.se", "NS", "ns.elsewhere.net."),
	), kb)
	assertDetections(t, out.Detections, [][]any{
		{"dns", "cloudflare", "cloudflare", "cloudflare.dns", "abby.ns.cloudflare.com", 1.0},
		{"dns", "binero", "binero", "binero.dns", "ns1.binero.se", KeyMatchConfidence},
		{"dns", model.SelfHosted, "", "", "ns1.example.se", SelfHostedConfidence},
		{"dns", "nsone.net", "", "", "dns1.p07.nsone.net", UnmappedConfidence},
	})
	if out.Detections[0].Evidence.RuleID != "cloudflare/ns" || out.Detections[0].Evidence.Analyzer != "ns" {
		t.Fatalf("evidence = %+v", out.Detections[0].Evidence)
	}
}

func TestSOAOnlyWithoutNS(t *testing.T) {
	soa := rec("example.se", "SOA", "ns1.binero.se. hostmaster.binero.se. 1 2 3 4 5")
	assertDetections(t, SOA{}.Analyze(scope("example.se", soa), kb).Detections, [][]any{
		{"dns", "binero", "binero", "binero.dns", "ns1.binero.se", KeyMatchConfidence},
	})
	withNS := scope("example.se", soa, rec("example.se", "NS", "abby.ns.cloudflare.com."))
	got := SOA{}.Analyze(withNS, kb).Detections
	if len(got) != 0 {
		t.Fatalf("SOA used although NS exists: %v", short(got))
	}
}

func TestMXHostsNullMXAndPlaceholders(t *testing.T) {
	out := MX{}.Analyze(scope("example.se",
		rec("example.se", "MX", "1 ASPMX.L.GOOGLE.COM."),
		rec("example.se", "MX", "10 mail.example.se."),
		rec("example.se", "MX", "0 ."),
		rec("example.se", "MX", "10 localhost."),
	), kb)
	assertDetections(t, out.Detections, [][]any{
		{"email", "google", "google", "google.workspace-mail", "aspmx.l.google.com", 1.0},
		{"email", model.SelfHosted, "", "", "mail.example.se", SelfHostedConfidence},
	})
	if len(out.Findings) != 1 || out.Findings[0].Code != "null_mx" {
		t.Fatalf("findings = %+v", out.Findings)
	}
}

func TestCNAMEApexAndWwwOnly(t *testing.T) {
	out := CNAME{}.Analyze(scope("spotify.com",
		rec("www.spotify.com", "CNAME", "atc.spotify.map.fastly.net."),
		rec("shop.spotify.com", "CNAME", "shops.myshopify.com."),
		rec("spotify.com", "CNAME", "spotify.github.io."),
	), kb)
	assertDetections(t, out.Detections, [][]any{
		{"hosting", "spotify.github.io", "", "", "spotify.github.io", UnmappedConfidence},
		{"cdn", "fastly", "fastly", "fastly.edge", "atc.spotify.map.fastly.net", 1.0},
		{"ddos_protection", "fastly", "fastly", "fastly.edge", "atc.spotify.map.fastly.net", 1.0},
	})
}

func TestKeyMatchWithoutServiceOfThatType(t *testing.T) {
	out := MX{}.Analyze(scope("example.se", rec("example.se", "MX", "10 mx.binero.se.")), kb)
	// Binero is known but has no email service: named provider, empty service key.
	assertDetections(t, out.Detections, [][]any{{"email", "binero", "binero", "", "mx.binero.se", KeyMatchConfidence}})
}
```

Run: `go test ./internal/analyze/`
Expected: FAIL (undefined: `Scope`, `Detection`, `NS`, …).

- [ ] **Step 2: Implement.** `internal/analyze/analyze.go`:

```go
// Package analyze holds one analyzer per record type or protocol. Every
// analyzer is a pure function of the domain's records (already narrowed to the
// evaluation date by the engine) and the injected knowledge.
package analyze

import (
	"strings"

	"dns_detect/internal/knowledge"
	"dns_detect/internal/model"
)

// Scope is what an analyzer sees: the domain and its records at AsOf, with
// names normalised (lower case, no trailing dot) and types upper-cased.
type Scope struct {
	Domain  string
	AsOf    string
	Records []model.Record
}

// Named returns the records of recordType at name.
func (s Scope) Named(name, recordType string) []model.Record {
	var out []model.Record
	for _, r := range s.Records {
		if r.Name == name && r.Type == recordType {
			out = append(out, r)
		}
	}
	return out
}

// Apex returns the records of recordType at the domain itself.
func (s Scope) Apex(recordType string) []model.Record { return s.Named(s.Domain, recordType) }

// Detection is one service an analyzer found, with the evidence for it.
type Detection struct {
	ServiceType  string
	ProviderKey  string
	ProviderSlug string
	ServiceKey   string
	Evidence     model.Evidence
}

// Output is an analyzer's answer.
type Output struct {
	Detections []Detection
	Findings   []model.Finding
}

// Analyzer is implemented by every record-type analyzer.
type Analyzer interface {
	Name() string
	Analyze(s Scope, kb knowledge.Knowledge) Output
}

func fields(value string) []string { return strings.Fields(value) }
```

`internal/analyze/label.go`:

```go
package analyze

import (
	"dns_detect/internal/hosts"
	"dns_detect/internal/knowledge"
	"dns_detect/internal/model"
)

// Confidence of a detection that no provider-recon rule produced.
const (
	KeyMatchConfidence   = 0.8
	SelfHostedConfidence = 0.8
	UnmappedConfidence   = 0.5
)

// LabelHost labels one host named by a record. The best rule of kind wins,
// giving one detection per service type of the matched service. Without a
// rule the host's registrable domain decides: the domain itself (or a host
// under it) is self-hosted, a provider key names that provider (with its first
// service of fallbackType), anything else is kept as an unmapped key. A host
// with no registrable domain (an IP, garbage) yields nothing.
func LabelHost(s Scope, kb knowledge.Knowledge, analyzer string, kind knowledge.Kind, fallbackType string, rec model.Record, host string) []Detection {
	host = hosts.Normalize(host)
	if host == "" {
		return nil
	}
	ev := model.Evidence{Analyzer: analyzer, RecordName: rec.Name, RecordType: rec.Type, Value: host}
	if m, ok := kb.Match(kind, host); ok {
		ev.RuleID, ev.Confidence = m.RuleID, m.Confidence
		out := make([]Detection, 0, len(m.ServiceTypes))
		for _, t := range m.ServiceTypes {
			out = append(out, Detection{ServiceType: t, ProviderKey: m.ProviderSlug, ProviderSlug: m.ProviderSlug, ServiceKey: m.ServiceKey, Evidence: ev})
		}
		return out
	}
	if hosts.Under(host, s.Domain) {
		ev.Confidence = SelfHostedConfidence
		return []Detection{{ServiceType: fallbackType, ProviderKey: model.SelfHosted, Evidence: ev}}
	}
	key := hosts.Registrable(host)
	if key == "" {
		return nil
	}
	if key == s.Domain {
		ev.Confidence = SelfHostedConfidence
		return []Detection{{ServiceType: fallbackType, ProviderKey: model.SelfHosted, Evidence: ev}}
	}
	if p, ok := kb.ProviderForKey(key); ok {
		ev.Confidence = KeyMatchConfidence
		return []Detection{{ServiceType: fallbackType, ProviderKey: p.Slug, ProviderSlug: p.Slug, ServiceKey: p.FirstService(fallbackType), Evidence: ev}}
	}
	ev.Confidence = UnmappedConfidence
	return []Detection{{ServiceType: fallbackType, ProviderKey: key, Evidence: ev}}
}
```

`internal/analyze/host_analyzers.go`:

```go
package analyze

import (
	"dns_detect/internal/knowledge"
	"dns_detect/internal/model"
)

// NS labels the apex nameservers: service type dns.
type NS struct{}

func (NS) Name() string { return "ns" }

func (a NS) Analyze(s Scope, kb knowledge.Knowledge) Output {
	var out Output
	for _, r := range s.Apex("NS") {
		out.Detections = append(out.Detections, LabelHost(s, kb, a.Name(), knowledge.NSTarget, "dns", r, r.Value)...)
	}
	return out
}

// SOA labels the SOA primary nameserver (MNAME), only for a domain without
// apex NS records: NS is the better evidence whenever it exists.
type SOA struct{}

func (SOA) Name() string { return "soa" }

func (a SOA) Analyze(s Scope, kb knowledge.Knowledge) Output {
	var out Output
	if len(s.Apex("NS")) > 0 {
		return out
	}
	for _, r := range s.Apex("SOA") {
		f := fields(r.Value)
		if len(f) == 0 {
			continue
		}
		out.Detections = append(out.Detections, LabelHost(s, kb, a.Name(), knowledge.NSTarget, "dns", r, f[0])...)
	}
	return out
}

// MX labels the apex mail exchangers: service type email. A null MX
// ("0 ." per RFC 7505) is a finding, not a provider.
type MX struct{}

func (MX) Name() string { return "mx" }

func (a MX) Analyze(s Scope, kb knowledge.Knowledge) Output {
	var out Output
	for _, r := range s.Apex("MX") {
		f := fields(r.Value)
		if len(f) == 0 {
			continue
		}
		host := f[len(f)-1]
		if host == "." || host == "" {
			out.Findings = append(out.Findings, model.Finding{Analyzer: a.Name(), Code: "null_mx", Detail: "the domain accepts no mail"})
			continue
		}
		if host == "localhost" || host == "localhost." || host == "~" {
			continue
		}
		out.Detections = append(out.Detections, LabelHost(s, kb, a.Name(), knowledge.MXTarget, "email", r, host)...)
	}
	return out
}

// CNAME labels apex and www CNAME targets: service type hosting unless a
// rule says otherwise (a CDN edge, a PaaS).
type CNAME struct{}

func (CNAME) Name() string { return "cname" }

func (a CNAME) Analyze(s Scope, kb knowledge.Knowledge) Output {
	var out Output
	for _, name := range []string{s.Domain, "www." + s.Domain} {
		for _, r := range s.Named(name, "CNAME") {
			out.Detections = append(out.Detections, LabelHost(s, kb, a.Name(), knowledge.CNAMETarget, "hosting", r, r.Value)...)
		}
	}
	return out
}
```

- [ ] **Step 3: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS (analyze: 5 tests).

- [ ] **Step 4: Commit.**

```bash
git add services/dns_detect/internal/analyze
git commit -m "feat(dns_detect): NS, SOA, MX and CNAME analyzers with rule, provider-key and self-hosted labelling

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Engine

**Files:**
- Create: `internal/engine/engine.go`, `internal/engine/engine_test.go`

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: `engine.New(kb, analyzers...)`, `engine.Default(kb)` and `(*Engine).Detect(model.Input, asOf string) model.Result`.

- [ ] **Step 1: Failing test.** `internal/engine/engine_test.go`:

```go
package engine

import (
	"encoding/json"
	"math/rand/v2"
	"reflect"
	"testing"

	"dns_detect/internal/knowledge"
	"dns_detect/internal/model"
)

func loadKB(t *testing.T) *knowledge.Index {
	t.Helper()
	kb, err := knowledge.LoadDir("../knowledge/testdata/providers")
	if err != nil {
		t.Fatal(err)
	}
	return kb
}

func r(name, typ, value, first, last string) model.Record {
	return model.Record{Name: name, Type: typ, Value: value, FirstSeen: first, LastSeen: last}
}

var example = model.Input{Domain: "Example.SE.", Records: []model.Record{
	r("example.se.", "NS", "ns1.loopia.se.", "2026-02-02", "2026-09-25"),
	r("example.se.", "NS", "ns2.loopia.se.", "2026-02-02", "2026-09-25"),
	r("example.se.", "NS", "ns51.domaincontrol.com.", "2026-01-01", "2026-02-01"),
	r("example.se.", "MX", "10 mailcluster.loopia.se.", "2026-01-01", "2026-09-25"),
	r("example.se.", "MX", "0 .", "2026-01-01", "2026-09-25"),
	r("www.example.se.", "CNAME", "example.se.cdn.cloudflare.net.", "2026-01-01", "2026-09-25"),
	r("example.se.", "SOA", "ns1.loopia.se. registry.loopia.se. 1 2 3 4 5", "2026-01-01", "2026-09-25"),
}}

func TestDetectAggregatesServicesWithEvidence(t *testing.T) {
	got := Default(loadKB(t)).Detect(example, "2026-09-20")
	type row struct {
		Type, Key, Slug string
		Keys            []string
		Evidence        int
	}
	var rows []row
	for _, s := range got.Services {
		rows = append(rows, row{s.ServiceType, s.ProviderKey, s.ProviderSlug, s.ServiceKeys, len(s.Evidence)})
	}
	want := []row{
		{"cdn", "cloudflare", "cloudflare", []string{"cloudflare.edge"}, 1},
		{"ddos_protection", "cloudflare", "cloudflare", []string{"cloudflare.edge"}, 1},
		{"dns", "loopia", "loopia", []string{"loopia.dns"}, 2},
		{"email", "loopia", "loopia", []string{"loopia.email"}, 1},
		{"waf", "cloudflare", "cloudflare", []string{"cloudflare.edge"}, 1},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("services\n got %+v\nwant %+v", rows, want)
	}
	if got.Domain != "example.se" || got.AsOf != "2026-09-20" || got.KnowledgeVersion == "" {
		t.Fatalf("header = %q %q %q", got.Domain, got.AsOf, got.KnowledgeVersion)
	}
	if len(got.Findings) != 1 || got.Findings[0].Code != "null_mx" {
		t.Fatalf("findings = %+v", got.Findings)
	}
}

func TestDetectAsOfSlidesThroughTime(t *testing.T) {
	e := Default(loadKB(t))
	dnsProviders := func(asOf string) []string {
		var out []string
		for _, s := range e.Detect(example, asOf).Services {
			if s.ServiceType == "dns" {
				out = append(out, s.ProviderKey)
			}
		}
		return out
	}
	if got := dnsProviders("2026-01-15"); !reflect.DeepEqual(got, []string{"godaddy"}) {
		t.Fatalf("January dns = %v, want [godaddy]", got)
	}
	if got := dnsProviders("2026-09-20"); !reflect.DeepEqual(got, []string{"loopia"}) {
		t.Fatalf("September dns = %v, want [loopia]", got)
	}
	// After the last observation the last known state carries forward.
	if got := dnsProviders("2027-01-01"); !reflect.DeepEqual(got, []string{"loopia"}) {
		t.Fatalf("after the last scan dns = %v, want [loopia]", got)
	}
	if got := e.Detect(example, "2027-01-01").ObservedAt; got != "2026-09-25" {
		t.Fatalf("observed_at = %q, want 2026-09-25", got)
	}
	if got := dnsProviders("2025-12-31"); got != nil {
		t.Fatalf("before the first scan dns = %v, want none", got)
	}
}

func TestDetectIsDeterministic(t *testing.T) {
	e := Default(loadKB(t))
	want, _ := json.Marshal(e.Detect(example, "2026-09-20"))
	for i := range 20 {
		shuffled := model.Input{Domain: example.Domain, Records: append([]model.Record(nil), example.Records...)}
		rand.New(rand.NewPCG(uint64(i), 7)).Shuffle(len(shuffled.Records), func(a, b int) {
			shuffled.Records[a], shuffled.Records[b] = shuffled.Records[b], shuffled.Records[a]
		})
		got, _ := json.Marshal(e.Detect(shuffled, "2026-09-20"))
		if string(got) != string(want) {
			t.Fatalf("shuffle %d changed the result:\n%s\n%s", i, got, want)
		}
	}
}

func TestDetectEmptyDomainGivesEmptyLists(t *testing.T) {
	got, _ := json.Marshal(Default(loadKB(t)).Detect(model.Input{Domain: "nothing.se"}, ""))
	var back map[string]any
	_ = json.Unmarshal(got, &back)
	if back["services"] == nil || back["findings"] == nil {
		t.Fatalf("empty result must serialise [] not null: %s", got)
	}
}
```

Run: `go test ./internal/engine/`
Expected: FAIL (undefined: `Default`).

- [ ] **Step 2: Implement.** `internal/engine/engine.go`:

```go
// Package engine runs the analyzers over one domain's records at one date and
// aggregates their detections into services with evidence. It is a pure
// function of (records, asOf, knowledge): the same inputs give the same
// result in the same order.
package engine

import (
	"cmp"
	"slices"
	"strings"

	"dns_detect/internal/analyze"
	"dns_detect/internal/hosts"
	"dns_detect/internal/knowledge"
	"dns_detect/internal/model"
)

// Engine holds the knowledge and the analyzers, both fixed at construction.
type Engine struct {
	kb        knowledge.Knowledge
	analyzers []analyze.Analyzer
}

// New builds an engine over kb running analyzers in the given order.
func New(kb knowledge.Knowledge, analyzers ...analyze.Analyzer) *Engine {
	return &Engine{kb: kb, analyzers: analyzers}
}

// Default is the engine with every analyzer this build has.
func Default(kb knowledge.Knowledge) *Engine {
	return New(kb, analyze.NS{}, analyze.SOA{}, analyze.MX{}, analyze.CNAME{})
}

// Detect evaluates in as of asOf (YYYY-MM-DD): the domain's latest
// observation at or before that date (model.Snapshot). "" uses every record.
func (e *Engine) Detect(in model.Input, asOf string) model.Result {
	s := analyze.Scope{Domain: hosts.Normalize(in.Domain), AsOf: asOf}
	records, observed := model.Snapshot(in.Records, asOf)
	for _, r := range records {
		r.Name = hosts.Normalize(r.Name)
		r.Type = strings.ToUpper(strings.TrimSpace(r.Type))
		s.Records = append(s.Records, r)
	}
	var detections []analyze.Detection
	var findings []model.Finding
	for _, a := range e.analyzers {
		out := a.Analyze(s, e.kb)
		detections = append(detections, out.Detections...)
		findings = append(findings, out.Findings...)
	}
	return model.Result{
		Domain:           s.Domain,
		AsOf:             asOf,
		ObservedAt:       observed,
		KnowledgeVersion: e.kb.Version(),
		Services:         aggregate(detections),
		Findings:         sortFindings(findings),
	}
}

func aggregate(ds []analyze.Detection) []model.Service {
	type key struct{ serviceType, providerKey string }
	byKey := map[key]*model.Service{}
	for _, d := range ds {
		k := key{d.ServiceType, d.ProviderKey}
		svc, ok := byKey[k]
		if !ok {
			svc = &model.Service{ServiceType: d.ServiceType, ProviderKey: d.ProviderKey, ServiceKeys: []string{}}
			byKey[k] = svc
		}
		if svc.ProviderSlug == "" {
			svc.ProviderSlug = d.ProviderSlug
		}
		if d.ServiceKey != "" && !slices.Contains(svc.ServiceKeys, d.ServiceKey) {
			svc.ServiceKeys = append(svc.ServiceKeys, d.ServiceKey)
		}
		if !slices.Contains(svc.Evidence, d.Evidence) {
			svc.Evidence = append(svc.Evidence, d.Evidence)
		}
		svc.Confidence = max(svc.Confidence, d.Evidence.Confidence)
	}
	out := make([]model.Service, 0, len(byKey))
	for _, svc := range byKey {
		slices.Sort(svc.ServiceKeys)
		slices.SortFunc(svc.Evidence, func(a, b model.Evidence) int {
			return cmp.Or(
				cmp.Compare(a.Analyzer, b.Analyzer), cmp.Compare(a.RecordName, b.RecordName),
				cmp.Compare(a.RecordType, b.RecordType), cmp.Compare(a.Value, b.Value), cmp.Compare(a.RuleID, b.RuleID),
			)
		})
		out = append(out, *svc)
	}
	slices.SortFunc(out, func(a, b model.Service) int {
		return cmp.Or(cmp.Compare(a.ServiceType, b.ServiceType), cmp.Compare(a.ProviderKey, b.ProviderKey))
	})
	return out
}

func sortFindings(fs []model.Finding) []model.Finding {
	out := []model.Finding{}
	for _, f := range fs {
		if !slices.Contains(out, f) {
			out = append(out, f)
		}
	}
	slices.SortFunc(out, func(a, b model.Finding) int {
		return cmp.Or(cmp.Compare(a.Analyzer, b.Analyzer), cmp.Compare(a.Code, b.Code), cmp.Compare(a.Detail, b.Detail))
	})
	return out
}
```

- [ ] **Step 3: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS (engine: 4 tests).

- [ ] **Step 4: Commit.**

```bash
git add services/dns_detect/internal/engine
git commit -m "feat(dns_detect): engine with point-scan snapshots, aggregation and deterministic output

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: CLI, README and real-data smoke

**Files:**
- Create: `cmd/dns-detect/main.go`, `cmd/dns-detect/main_test.go`, `services/dns_detect/README.md`

**Interfaces:**
- Produces: `dns-detect detect -knowledge DIR [-as-of YYYY-MM-DD]`. Input objects on stdin, one result per line on stdout. Exit 2 on bad arguments, 1 on bad input or bad knowledge.

- [ ] **Step 1: Failing test.** `cmd/dns-detect/main_test.go`:

```go
package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"dns_detect/internal/model"
)

const knowledgeDir = "../../internal/knowledge/testdata/providers"

func TestDetectStreamsOneResultPerInput(t *testing.T) {
	stdin := strings.NewReader(`{"domain":"a.se","records":[{"name":"a.se","type":"NS","value":"ns1.loopia.se."}]}
{"domain":"b.se","records":[{"name":"b.se","type":"MX","value":"10 mx.b.se."}]}`)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"detect", "-knowledge", knowledgeDir, "-as-of", "2026-09-28"}, stdin, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 result lines, got %q", stdout.String())
	}
	var a, b model.Result
	if err := json.Unmarshal([]byte(lines[0]), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &b); err != nil {
		t.Fatal(err)
	}
	if a.Domain != "a.se" || len(a.Services) != 1 || a.Services[0].ProviderKey != "loopia" || a.AsOf != "2026-09-28" {
		t.Fatalf("a = %+v", a)
	}
	if b.Domain != "b.se" || len(b.Services) != 1 || b.Services[0].ProviderKey != model.SelfHosted {
		t.Fatalf("b = %+v", b)
	}
}

func TestDetectRejectsBadArguments(t *testing.T) {
	for name, args := range map[string][]string{
		"no subcommand":  {},
		"no knowledge":   {"detect"},
		"bad date":       {"detect", "-knowledge", knowledgeDir, "-as-of", "28/09/2026"},
		"missing folder": {"detect", "-knowledge", t.TempDir()},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, strings.NewReader(""), &stdout, &stderr); code == 0 {
			t.Errorf("%s: exit 0", name)
		}
	}
}

func TestDetectRejectsBadInput(t *testing.T) {
	for name, stdin := range map[string]string{
		"not json":       `{"domain":`,
		"missing domain": `{"records":[]}`,
	} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"detect", "-knowledge", knowledgeDir}, strings.NewReader(stdin), &stdout, &stderr); code != 1 {
			t.Errorf("%s: exit %d, want 1 (%s)", name, code, stderr.String())
		}
	}
}
```

Run: `go test ./cmd/dns-detect/`
Expected: FAIL (undefined: `run`).

- [ ] **Step 2: Implement.** `cmd/dns-detect/main.go`:

```go
// Command dns-detect runs the detection engine over domains read from stdin.
//
//	dns-detect detect -knowledge DIR [-as-of YYYY-MM-DD] < inputs.json > results.ndjson
//
// stdin holds one or more input objects ({"domain": …, "records": [...]}),
// concatenated or one per line; stdout gets one result object per line, in
// input order. DIR holds provider-recon documents (<slug>.json).
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"dns_detect/internal/engine"
	"dns_detect/internal/knowledge"
	"dns_detect/internal/model"
)

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "detect" {
		fmt.Fprintln(stderr, "usage: dns-detect detect -knowledge DIR [-as-of YYYY-MM-DD] < inputs.json")
		return 2
	}
	fs := flag.NewFlagSet("detect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("knowledge", "", "directory of provider-recon documents (<slug>.json)")
	asOf := fs.String("as-of", "", "evaluation date YYYY-MM-DD (default: ignore seen windows)")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *dir == "" {
		fmt.Fprintln(stderr, "detect: -knowledge is required")
		return 2
	}
	if *asOf != "" {
		if _, err := time.Parse(time.DateOnly, *asOf); err != nil {
			fmt.Fprintf(stderr, "detect: -as-of %q is not YYYY-MM-DD\n", *asOf)
			return 2
		}
	}
	kb, err := knowledge.LoadDir(*dir)
	if err != nil {
		fmt.Fprintf(stderr, "detect: load knowledge: %v\n", err)
		return 1
	}
	e := engine.Default(kb)
	dec := json.NewDecoder(bufio.NewReader(stdin))
	out := bufio.NewWriter(stdout)
	defer out.Flush()
	enc := json.NewEncoder(out)
	for n := 1; ; n++ {
		var in model.Input
		if err := dec.Decode(&in); errors.Is(err, io.EOF) {
			return 0
		} else if err != nil {
			fmt.Fprintf(stderr, "detect: input %d: %v\n", n, err)
			return 1
		}
		if in.Domain == "" {
			fmt.Fprintf(stderr, "detect: input %d: domain is required\n", n)
			return 1
		}
		if err := enc.Encode(e.Detect(in, *asOf)); err != nil {
			fmt.Fprintf(stderr, "detect: write: %v\n", err)
			return 1
		}
	}
}
```

`services/dns_detect/README.md` covers:
- purpose: one domain's DNS records → services and evidence, as of a date;
- the spec link;
- package layout;
- the CLI usage line and one example input and output;
- the `asOf` point-scan rule;
- how to refresh the test fixtures (Task 2, Step 1);
- that SPF, DKIM, DMARC, TXT and IP come in slices 2–3.

- [ ] **Step 3: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS (all six packages).

- [ ] **Step 4: Real-data smoke** (not committed; output in the ledger):

```bash
D=$(mktemp -d); for s in $(ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT JSONExtractString(json,'slug') FROM corpscout.provider_recon_documents_s3 FORMAT TSV\""); do
  ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT json FROM corpscout.provider_recon_documents_s3 WHERE JSONExtractString(json,'slug')='$s' FORMAT RawBLOB\"" > $D/$s.json; done
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT root_domain AS domain, groupArray(map('name', name, 'type', toString(record_type), 'value', value, 'first_seen', toString(toDate(first_seen)), 'last_seen', toString(toDate(last_seen)))) AS records FROM corpscout.commoncrawl_domain_dns_records WHERE root_domain IN ('spotify.com','volvo.com','loopia.se') GROUP BY root_domain FORMAT JSONEachRow\"" \
 | go run ./cmd/dns-detect detect -knowledge $D -as-of 2026-09-25 | jq -c '{domain, observed_at, services: [.services[] | [.service_type, .provider_key]]}'
```
Expected (as prototyped on 2026-09-28):
- spotify.com, observed 2026-09-25: `cdn fastly`, `dns google`, `dns nsone.net`, `email google`;
- volvo.com, observed 2026-09-19 (between scans): `cdn akamai`, `dns self-hosted`, `dns volvo.se`, `email microsoft`;
- loopia.se, observed 2026-09-23: `dns loopia`, `email loopia`, `hosting self-hosted`.

The service lists can grow if provider definitions changed since; the observation dates must match.

- [ ] **Step 5: Commit.**

```bash
git add services/dns_detect/cmd services/dns_detect/README.md
git commit -m "feat(dns_detect): streaming detect CLI and README

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```
