# dns_detect slices 0–1 Implementation Plan (revision 2: per-record resolver)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two things:
1. Provider-recon keeps removed items indefinitely (slice 0).
2. Inside the provider-recon Go module, a pure per-record resolver (slice 1). It turns one DNS record instance into the `(service_type, provider)` services it proves, each carrying the record's window. It has NS, SOA (fallback), MX and CNAME analyzers, a knowledge index compiled from provider-recon documents through the shared `internal/matcher`, and a streaming `dns-detect resolve` CLI.

**Architecture:**
- **Packages** in `services/provider_recon`:
  - `internal/detect/hosts`: normalisation and registrable domain, via `golang.org/x/net/publicsuffix`.
  - `internal/detect/knowledge`: an immutable `Index` behind the `Knowledge` interface, compiled from `[]model.Document`, with patterns from `matcher.Compile`.
  - `internal/detect/resolve`: the `Record`/`Result`/`Finding`/`Output` types, `LabelHost`, the analyzers, `Route` and `Resolve`.
  - `cmd/dns-detect`: the CLI.
- **Resolving:** `Resolve(record, kb)` routes the record by type and name to one analyzer. The window is copied onto every result, and the output is sorted.
- **Verified before writing:** all code below was prototyped in the module and passes `go test -race ./...` (13 packages), `go vet` and gofmt. It was also run on the 134 real NS/SOA/MX/CNAME records of spotify.com, volvo.com and loopia.se, against the 37 production documents (Task 4, Step 4).

**Tech Stack:** Go 1.25 (the module directive stays `go 1.25.0`), `golang.org/x/net` v0.50.0 (v0.59 would force go 1.26), stdlib testing.

**Spec:** `docs/superpowers/specs/2026-09-28-dns-detect-service-design.md` (revision 2, owner-approved 2026-09-28). This plan covers "Owner decisions", "Input", "Output", "Knowledge", "Rule kinds" (compile-time refusal), "Routing and analyzers" (slice-1 rows) and "Testing". Slices 2–4 are later plans.

## Global Constraints

- **Location:** everything lives in the provider-recon module. It reuses `internal/model` (documents) and `internal/matcher` (pattern semantics), and never re-declares them.
- **Knowledge** is compiled once, immutable, and injected. Analyzers never load or read anything else.
- **Compilation refuses:**
  - rule kinds outside NS/target, MX/target, CNAME/target, TXT/value, TXT/name, SPF/include, DKIM/selector, DKIM/target, DMARC/report;
  - invalid patterns;
  - other contract versions;
  - a provider key claimed by two providers.
- **Compilation skips** removed rules and removed services.
- **`knowledge.Version()`** hashes each document's `model.ContentHash`, so it changes only with provider content.
- **Output:**
  - The same record and knowledge give byte-identical output.
  - Empty lists serialise as `[]`.
  - Every result carries the record's `record_id`, name, type and window (`valid_from`/`valid_to`, the date part of `first_seen`/`last_seen`).
- **Confidence** without a rule: key match 0.8, self-hosted 0.8, unmapped 0.5. A rule's own confidence otherwise.
- **SOA results** are `fallback: true`.
- **Git:**
  - commit by explicit path from the `corpscout` root;
  - Conventional Commits, each ending with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`;
  - work in a worktree under `companycollect/.worktrees/`.
- **Checks** (in `services/provider_recon`): `go test -race ./...`, `go vet ./...`, and `test -z "$(gofmt -l .)"`.

## Review Focus

1. **A record whose root_domain differs in case or trailing dot from its name** (`Example.SE.` vs `EXAMPLE.se.`). Expected: both are normalised before routing, so it routes as apex. *(Task 3: `TestResultsCarryTheRecordAndItsWindow`.)*
2. **Hosts that are IP literals, a lone `.`, or under the domain itself.** Expected: nothing for the first two, `self-hosted` for the third. *(Task 3: `TestNSRuleProviderKeySelfHostedUnmappedAndIP`.)*
3. **Real documents with rule kinds slice 1 doesn't analyse** (TXT/value, TXT/name exist in production). Expected: they compile, and no analyzer asks for them. *(Task 2: `TestLoadDirCompilesRealDocuments`.)*
4. **Two providers' rules match one host.** Expected: highest priority, then confidence, then rule id; deterministic. *(Task 2: `TestMatchPrefersPriorityThenConfidenceThenRuleID`.)*
5. **A malformed or incomplete record in a large CLI stream.** Expected: the stream stops with the record's position number and exit code 1; earlier lines are already written. *(Task 4: `TestResolveRejectsBadRecords`.)*

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

### Task 1: Host normalisation and provider keys

**Files:**
- Create: `internal/detect/hosts/hosts.go`, `internal/detect/hosts/hosts_test.go`
- Modify: `go.mod`, `go.sum`

**Interfaces:** Produces:
- `hosts.Normalize(host) string`: lower-cased, trimmed, no trailing dot;
- `hosts.Registrable(host) string`: eTLD+1 from the public suffix list, `""` for IPs, bare suffixes and garbage;
- `hosts.Under(host, domain) bool`.

- [ ] **Step 1: Failing test.** `internal/detect/hosts/hosts_test.go`:

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

Run: `go test ./internal/detect/hosts/`
Expected: FAIL (undefined: `Normalize`, `Registrable`, `Under`).

- [ ] **Step 2: Implement.** Run `go get golang.org/x/net@v0.50.0`, then create `internal/detect/hosts/hosts.go`:

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

Then run `go mod tidy`. The only `go.mod` change must be `golang.org/x/net v0.50.0` in the direct `require` block, with the directive still `go 1.25.0`.

- [ ] **Step 3: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add services/provider_recon/go.mod services/provider_recon/go.sum services/provider_recon/internal/detect/hosts
git commit -m "feat(provider_recon): detect/hosts — host normalisation and public-suffix provider keys

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Knowledge index

**Files:**
- Create: `internal/detect/knowledge/knowledge.go`, `load.go`, `knowledge_test.go`, and `testdata/providers/{beebyte,cloudflare,glesys,godaddy,loopia}.json`

**Interfaces:**
- Consumes: `model.Document`, `model.ContractVersion`, `model.ContentHash`, `model.StatusRemoved`, and `matcher.Compile`/`Pattern.Match`.
- Produces:
  - `knowledge.Kind`, with vars `NSTarget`, `MXTarget`, `CNAMETarget`, `TXTValue`, `TXTName`, `SPFInclude`, `DKIMSelector`, `DKIMTarget`, `DMARCReport`;
  - `knowledge.Match{ProviderSlug, ServiceKey, ServiceTypes, RuleID, Confidence}`;
  - `knowledge.Provider{Slug, Name, Country, Services []ProviderService}` with `FirstService(t) string`, and `knowledge.ProviderService{Key, Types}`;
  - the interface `knowledge.Knowledge` (`Match`, `ProviderForKey`, `Version`);
  - `knowledge.Compile([]model.Document) (*Index, error)` and `knowledge.LoadDir(dir) (*Index, error)`.

- [ ] **Step 1: Fixtures**, real production documents, pretty-printed:

```bash
cd services/provider_recon && mkdir -p internal/detect/knowledge/testdata/providers
for s in loopia glesys godaddy cloudflare beebyte; do
  ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT json FROM corpscout.provider_recon_documents_s3 WHERE JSONExtractString(json,'slug')='$s' FORMAT RawBLOB\"" \
  | python3 -c "import json,sys; json.dump(json.load(sys.stdin), open('internal/detect/knowledge/testdata/providers/$s.json','w'), indent=2, sort_keys=True)"
done
```
Expected: 5 files, 1–15 KB each. GoDaddy's key is `domaincontrol.com`. Cloudflare has the `cloudflare.dns`, `cloudflare.edge` (cdn, ddos_protection, waf) and `cloudflare.email-routing` services. Loopia's NS and MX rules are suffix `loopia.se`.

- [ ] **Step 2: Failing test.** `internal/detect/knowledge/knowledge_test.go`:

```go
package knowledge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"provider_recon/internal/model"
)

func rule(recordType, field, matcherType, pattern string, priority int, confidence float64) model.DNSRule {
	return model.DNSRule{RecordType: recordType, MatchField: field, MatcherType: matcherType, Pattern: pattern,
		Priority: priority, Confidence: confidence, Lifecycle: model.Lifecycle{Status: model.StatusActive}}
}

func svc(key string, types []string, rules ...model.DNSRule) model.Service {
	return model.Service{Key: key, ServiceTypes: types, Evidence: model.Evidence{DNSRules: rules}}
}

func doc(slug string, keys []string, services ...model.Service) model.Document {
	return model.Document{Version: model.ContractVersion, Slug: slug, DisplayName: strings.ToUpper(slug), ProviderKeys: keys, Services: services}
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
		{NSTarget, "ns1.loopia.se.", "loopia.dns"},
		{NSTarget, "abby.ns.cloudflare.com", "cloudflare.dns"},
		{NSTarget, "NS51.DOMAINCONTROL.COM.", "godaddy.dns"},
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

func TestLoadDirRefusesBrokenFiles(t *testing.T) {
	if _, err := LoadDir(t.TempDir()); err == nil {
		t.Error("empty directory loaded")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDir(dir); err == nil {
		t.Error("broken JSON loaded")
	}
}

func TestMatchPrefersPriorityThenConfidenceThenRuleID(t *testing.T) {
	idx, err := Compile([]model.Document{
		doc("a", nil, svc("a.low", []string{"dns"}, rule("NS", "target", "suffix", "example.net", 0, 1))),
		doc("b", nil, svc("b.high", []string{"dns"}, rule("NS", "target", "suffix", "ns.example.net", 10, 0.5))),
		doc("c", nil, svc("c.tie", []string{"dns"}, rule("NS", "target", "suffix", "ns.example.net", 10, 0.5))),
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
		t.Fatal("suffix matched without a label boundary")
	}
}

func TestMatcherTypesComeFromTheSharedMatcher(t *testing.T) {
	idx, err := Compile([]model.Document{doc("p", nil,
		svc("p.exact", []string{"email"}, rule("MX", "target", "exact", "mx.p.com", 0, 1)),
		svc("p.prefix", []string{"saas_verification"}, rule("TXT", "value", "prefix", "p-verification=", 0, 1)),
		svc("p.contains", []string{"email_sending"}, rule("TXT", "value", "contains", "include:spf.p.com", 0, 1)),
		svc("p.regex", []string{"dns"}, rule("NS", "target", "regex", `^ns[0-9]+\.p\.com$`, 0, 1)),
		svc("p.glob", []string{"hosting"}, rule("CNAME", "target", "glob", "*.edge-*.p.net", 0, 1)),
	)})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		kind    Kind
		subject string
		want    string
	}{
		{MXTarget, "MX.P.COM.", "p.exact"},
		{MXTarget, "a.mx.p.com", ""},
		{TXTValue, "p-verification=abc", "p.prefix"},
		{TXTValue, "v=spf1 include:spf.p.com ~all", "p.contains"},
		{NSTarget, "ns12.p.com.", "p.regex"},
		{NSTarget, "xns12.p.com", ""},
		{CNAMETarget, "site.edge-eu.p.net", "p.glob"},
	} {
		m, _ := idx.Match(c.kind, c.subject)
		if m.ServiceKey != c.want {
			t.Errorf("Match(%s, %q) = %q, want %q", c.kind, c.subject, m.ServiceKey, c.want)
		}
	}
}

func TestProviderKeysExactAndGlob(t *testing.T) {
	idx, err := Compile([]model.Document{
		doc("aws", []string{"amazonaws.com", "awsdns-*"}, svc("aws.route53", []string{"dns"}), svc("aws.ses", []string{"email_sending"})),
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

func TestRemovedRulesAndServicesAreSkipped(t *testing.T) {
	removedRule := rule("NS", "target", "suffix", "old.example.net", 0, 1)
	removedRule.Status = model.StatusRemoved
	removedSvc := svc("p.gone", []string{"dns"}, rule("NS", "target", "suffix", "gone.example.net", 0, 1))
	removedSvc.RemovedAt = "2026-09-01"
	idx, err := Compile([]model.Document{doc("p", []string{"p.com"}, svc("p.dns", []string{"dns"}, removedRule), removedSvc)})
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
	other := doc("p", nil)
	other.Version = "provider-recon/v2"
	for name, d := range map[string]model.Document{
		"unknown kind":    doc("p", nil, svc("p.a", []string{"cdn"}, rule("A", "value", "exact", "192.0.2.1", 0, 1))),
		"unknown matcher": doc("p", nil, svc("p.a", []string{"dns"}, rule("NS", "target", "wildcard", "*.p.com", 0, 1))),
		"invalid regex":   doc("p", nil, svc("p.a", []string{"dns"}, rule("NS", "target", "regex", "(", 0, 1))),
		"other contract":  other,
	} {
		if _, err := Compile([]model.Document{d}); err == nil {
			t.Errorf("%s: compiled without error", name)
		}
	}
	if _, err := Compile([]model.Document{doc("a", []string{"shared.com"}), doc("b", []string{"shared.com"})}); err == nil {
		t.Error("a provider key claimed by two providers compiled")
	}
}
```

Run: `go test ./internal/detect/knowledge/`
Expected: FAIL (undefined: `LoadDir`, `Compile`, …).

- [ ] **Step 3: Implement.** `internal/detect/knowledge/knowledge.go`:

```go
// Package knowledge compiles provider-recon documents into the immutable index
// the resolver's analyzers query. Analyzers get it injected (as the Knowledge
// interface) and never load anything themselves, so each can be tested with a
// fake. Patterns go through provider-recon's matcher, so the definitions
// validator and the resolver share one implementation of rule semantics.
package knowledge

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"provider_recon/internal/matcher"
	"provider_recon/internal/model"
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

// FirstService is the provider's first live service of serviceType, or "".
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
	// Version identifies the provider content the index was built from.
	Version() string
}

type compiledRule struct {
	Match
	pattern  matcher.Pattern
	priority int
}

type globKey struct {
	pattern  matcher.Pattern
	raw      string
	provider *Provider
}

// Index is the compiled Knowledge.
type Index struct {
	rules     map[Kind][]compiledRule
	exactKeys map[string]*Provider
	globKeys  []globKey
	version   string
}

var _ Knowledge = (*Index)(nil)

// Compile builds the index. It fails on a document of another contract
// version, a rule of an unknown kind, an invalid pattern, or a provider key
// claimed by two providers. Removed rules and services are skipped.
//
// The version hashes each document's content hash (which excludes collection
// timestamps), so it changes only when provider content changes.
func Compile(docs []model.Document) (*Index, error) {
	idx := &Index{rules: map[Kind][]compiledRule{}, exactKeys: map[string]*Provider{}}
	hashes := make([]string, 0, len(docs))
	for _, d := range docs {
		if d.Version != model.ContractVersion {
			return nil, fmt.Errorf("provider %q: contract %q, want %q", d.Slug, d.Version, model.ContractVersion)
		}
		h, err := model.ContentHash(d)
		if err != nil {
			return nil, fmt.Errorf("provider %q: %w", d.Slug, err)
		}
		hashes = append(hashes, d.Slug+"="+h)
		if err := idx.add(d); err != nil {
			return nil, err
		}
	}
	for kind := range idx.rules {
		slices.SortFunc(idx.rules[kind], func(a, b compiledRule) int { return strings.Compare(a.RuleID, b.RuleID) })
	}
	slices.SortFunc(idx.globKeys, func(a, b globKey) int { return strings.Compare(a.raw, b.raw) })
	slices.Sort(hashes)
	sum := sha256.Sum256([]byte(strings.Join(hashes, "\n")))
	idx.version = "sha256:" + hex.EncodeToString(sum[:])
	return idx, nil
}

func (idx *Index) add(d model.Document) error {
	p := &Provider{Slug: d.Slug, Name: d.DisplayName, Country: d.Country}
	for _, s := range d.Services {
		if s.RemovedAt != "" {
			continue
		}
		p.Services = append(p.Services, ProviderService{Key: s.Key, Types: s.ServiceTypes})
		for _, r := range s.Evidence.DNSRules {
			if r.Status == model.StatusRemoved {
				continue
			}
			kind := Kind{strings.ToUpper(r.RecordType), strings.ToLower(r.MatchField)}
			if !slices.Contains(knownKinds, kind) {
				return fmt.Errorf("provider %q service %q: unsupported rule kind %s", d.Slug, s.Key, kind)
			}
			id := fmt.Sprintf("%s/%s/%s %s %s", d.Slug, s.Key, kind, r.MatcherType, r.Pattern)
			pat, err := matcher.Compile(r.MatcherType, r.Pattern, r.CaseSensitive)
			if err != nil {
				return fmt.Errorf("rule %s: %w", id, err)
			}
			idx.rules[kind] = append(idx.rules[kind], compiledRule{
				Match:    Match{ProviderSlug: d.Slug, ServiceKey: s.Key, ServiceTypes: s.ServiceTypes, RuleID: id, Confidence: r.Confidence},
				pattern:  pat,
				priority: r.Priority,
			})
		}
	}
	for _, k := range d.ProviderKeys {
		k = strings.ToLower(strings.TrimSpace(k))
		if strings.Contains(k, "*") {
			pat, err := matcher.Compile("glob", k, false)
			if err != nil {
				return fmt.Errorf("provider %q: key %q: %w", d.Slug, k, err)
			}
			idx.globKeys = append(idx.globKeys, globKey{pattern: pat, raw: k, provider: p})
			continue
		}
		if other, dup := idx.exactKeys[k]; dup && other.Slug != d.Slug {
			return fmt.Errorf("provider key %q claimed by both %q and %q", k, other.Slug, d.Slug)
		}
		idx.exactKeys[k] = p
	}
	return nil
}

// Match implements Knowledge.
func (idx *Index) Match(kind Kind, subject string) (Match, bool) {
	var best *compiledRule
	for i := range idx.rules[kind] {
		r := &idx.rules[kind][i]
		if !r.pattern.Match(subject) {
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
		if g.pattern.Match(key) {
			return *g.provider, true
		}
	}
	return Provider{}, false
}

// Version implements Knowledge.
func (idx *Index) Version() string { return idx.version }
```

`internal/detect/knowledge/load.go`:

```go
package knowledge

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"provider_recon/internal/model"
)

// LoadDir compiles every *.json provider document in dir: a local copy of the
// bucket's providers/<slug>/latest.json files flattened to <slug>.json, and
// the layout of the test fixtures.
func LoadDir(dir string) (*Index, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no provider documents in %s", dir)
	}
	sort.Strings(paths)
	docs := make([]model.Document, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var d model.Document
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(p), err)
		}
		docs = append(docs, d)
	}
	return Compile(docs)
}
```

- [ ] **Step 4: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS (knowledge: 7 tests).

- [ ] **Step 5: Commit.**

```bash
git add services/provider_recon/internal/detect/knowledge
git commit -m "feat(provider_recon): detect/knowledge — index compiled from provider documents over the shared matcher

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Per-record resolver (NS, SOA, MX, CNAME)

**Files:**
- Create: `internal/detect/resolve/types.go`, `label.go`, `analyzers.go`, `resolve.go`, `fake_test.go`, `resolve_test.go`

**Interfaces:**
- Consumes: `knowledge.Knowledge`, the `knowledge.Kind` vars, and `hosts.*`.
- Produces:
  - the types `resolve.Record`, `Result`, `Finding` and `Output{RecordID, Results, Findings}`;
  - constants `resolve.SelfHosted`, `KeyMatchConfidence`, `SelfHostedConfidence` and `UnmappedConfidence`;
  - `resolve.LabelHost(kb, base, kind, fallbackType, host) []Result`;
  - `resolve.Analyzer` and the analyzers `NS{}`, `SOA{}`, `MX{}` and `CNAME{}`;
  - `resolve.Route(Record) Analyzer`;
  - `resolve.Resolve(Record, Knowledge) Output`.

- [ ] **Step 1: Failing tests.** `internal/detect/resolve/fake_test.go`, the injected fake knowledge base:

```go
package resolve

import (
	"reflect"
	"testing"

	"provider_recon/internal/detect/knowledge"
)

// fakeKB is injected knowledge: exact subjects per kind, and provider keys.
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
			"example.se.cdn.cloudflare.net": {ProviderSlug: "cloudflare", ServiceKey: "cloudflare.edge", ServiceTypes: []string{"cdn", "ddos_protection", "waf"}, RuleID: "cloudflare/cname", Confidence: 1},
		},
	},
	keys: map[string]knowledge.Provider{
		"binero.se": {Slug: "binero", Services: []knowledge.ProviderService{{Key: "binero.web", Types: []string{"hosting"}}, {Key: "binero.dns", Types: []string{"dns"}}}},
	},
}

func rec(name, typ, value string) Record {
	return Record{RecordID: "r1", RootDomain: "example.se", Name: name, Type: typ, Value: value, FirstSeen: "2026-08-10", LastSeen: "2026-09-19"}
}

// short renders results as [service_type provider_key provider_slug service_key subject confidence fallback].
func short(rs []Result) [][]any {
	out := [][]any{}
	for _, r := range rs {
		out = append(out, []any{r.ServiceType, r.ProviderKey, r.ProviderSlug, r.ServiceKey, r.Subject, r.Confidence, r.Fallback})
	}
	return out
}

func assertResults(t *testing.T, got []Result, want [][]any) {
	t.Helper()
	if g := short(got); !reflect.DeepEqual(g, want) {
		t.Fatalf("results\n got %v\nwant %v", g, want)
	}
}
```

`internal/detect/resolve/resolve_test.go`:

```go
package resolve

import (
	"encoding/json"
	"testing"
)

func TestNSRuleProviderKeySelfHostedUnmappedAndIP(t *testing.T) {
	for _, c := range []struct {
		value string
		want  [][]any
	}{
		{"ABBY.ns.cloudflare.com.", [][]any{{"dns", "cloudflare", "cloudflare", "cloudflare.dns", "abby.ns.cloudflare.com", 1.0, false}}},
		{"ns1.binero.se.", [][]any{{"dns", "binero", "binero", "binero.dns", "ns1.binero.se", KeyMatchConfidence, false}}},
		{"ns1.example.se.", [][]any{{"dns", SelfHosted, "", "", "ns1.example.se", SelfHostedConfidence, false}}},
		{"dns1.p07.nsone.net.", [][]any{{"dns", "nsone.net", "", "", "dns1.p07.nsone.net", UnmappedConfidence, false}}},
		{"192.0.2.53", [][]any{}},
		{".", [][]any{}},
	} {
		t.Run(c.value, func(t *testing.T) {
			assertResults(t, Resolve(rec("example.se", "NS", c.value), kb).Results, c.want)
		})
	}
}

func TestSOAIsFallbackEvidence(t *testing.T) {
	out := Resolve(rec("example.se.", "SOA", "ns1.binero.se. hostmaster.binero.se. 1 2 3 4 5"), kb)
	assertResults(t, out.Results, [][]any{{"dns", "binero", "binero", "binero.dns", "ns1.binero.se", KeyMatchConfidence, true}})
	if out.Results[0].Analyzer != "soa" {
		t.Fatalf("analyzer = %q", out.Results[0].Analyzer)
	}
}

func TestMXHostNullMXAndPlaceholders(t *testing.T) {
	assertResults(t, Resolve(rec("example.se", "MX", "1 ASPMX.L.GOOGLE.COM."), kb).Results,
		[][]any{{"email", "google", "google", "google.workspace-mail", "aspmx.l.google.com", 1.0, false}})
	assertResults(t, Resolve(rec("example.se", "MX", "10 mail.example.se."), kb).Results,
		[][]any{{"email", SelfHosted, "", "", "mail.example.se", SelfHostedConfidence, false}})
	// Binero is known but has no email service: named provider, empty service key.
	assertResults(t, Resolve(rec("example.se", "MX", "10 mx.binero.se."), kb).Results,
		[][]any{{"email", "binero", "binero", "", "mx.binero.se", KeyMatchConfidence, false}})
	null := Resolve(rec("example.se", "MX", "0 ."), kb)
	if len(null.Results) != 0 || len(null.Findings) != 1 || null.Findings[0].Code != "null_mx" || null.Findings[0].RecordID != "r1" {
		t.Fatalf("null MX = %+v", null)
	}
	if out := Resolve(rec("example.se", "MX", "10 localhost."), kb); len(out.Results)+len(out.Findings) != 0 {
		t.Fatalf("localhost MX = %+v", out)
	}
}

func TestCNAMEEdgeGivesOneResultPerServiceType(t *testing.T) {
	assertResults(t, Resolve(rec("www.example.se", "CNAME", "example.se.cdn.cloudflare.net."), kb).Results, [][]any{
		{"cdn", "cloudflare", "cloudflare", "cloudflare.edge", "example.se.cdn.cloudflare.net", 1.0, false},
		{"ddos_protection", "cloudflare", "cloudflare", "cloudflare.edge", "example.se.cdn.cloudflare.net", 1.0, false},
		{"waf", "cloudflare", "cloudflare", "cloudflare.edge", "example.se.cdn.cloudflare.net", 1.0, false},
	})
	assertResults(t, Resolve(rec("example.se", "CNAME", "example.github.io."), kb).Results,
		[][]any{{"hosting", "example.github.io", "", "", "example.github.io", UnmappedConfidence, false}})
}

func TestRoutingIgnoresRecordsNoAnalyzerHandles(t *testing.T) {
	for _, r := range []Record{
		rec("sub.example.se", "NS", "ns.elsewhere.net."),
		rec("shop.example.se", "CNAME", "shops.myshopify.com."),
		rec("www.example.se", "MX", "10 mx.elsewhere.net."),
		rec("example.se", "TXT", `"v=spf1 include:_spf.google.com ~all"`),
		rec("example.se", "A", "192.0.2.1"),
	} {
		if out := Resolve(r, kb); len(out.Results)+len(out.Findings) != 0 {
			t.Errorf("%s %s routed: %+v", r.Name, r.Type, out)
		}
	}
}

func TestResultsCarryTheRecordAndItsWindow(t *testing.T) {
	r := Record{RecordID: "abc", RootDomain: "Example.SE.", Name: "EXAMPLE.se.", Type: "ns", Value: "ns1.binero.se.",
		FirstSeen: "2026-08-10 00:00:00.000", LastSeen: "2026-09-19"}
	got := Resolve(r, kb).Results
	if len(got) != 1 {
		t.Fatalf("results = %+v", got)
	}
	g := got[0]
	if g.RecordID != "abc" || g.RootDomain != "example.se" || g.RecordName != "example.se" || g.RecordType != "NS" ||
		g.Analyzer != "ns" || g.ValidFrom != "2026-08-10" || g.ValidTo != "2026-09-19" {
		t.Fatalf("result = %+v", g)
	}
}

func TestResolveIsDeterministicAndSerialisesEmptyLists(t *testing.T) {
	r := rec("www.example.se", "CNAME", "example.se.cdn.cloudflare.net.")
	first, _ := json.Marshal(Resolve(r, kb))
	for range 20 {
		again, _ := json.Marshal(Resolve(r, kb))
		if string(again) != string(first) {
			t.Fatalf("output changed:\n%s\n%s", again, first)
		}
	}
	empty, _ := json.Marshal(Resolve(rec("example.se", "A", "192.0.2.1"), kb))
	if string(empty) != `{"record_id":"r1","results":[],"findings":[]}` {
		t.Fatalf("empty output = %s", empty)
	}
}
```

Run: `go test ./internal/detect/resolve/`
Expected: FAIL (undefined: `Record`, `Resolve`, …).

- [ ] **Step 2: Implement.** `internal/detect/resolve/types.go`:

```go
// Package resolve turns one DNS record into the services it proves. Every
// record is resolved on its own, against injected knowledge: a pure function
// of (record, knowledge). A domain's history is a view over the per-record
// results (spec: docs/superpowers/specs/2026-09-28-dns-detect-service-design.md).
package resolve

// Record is one DNS record instance as the DNS store keeps it: presentation
// type and RDATA, and the window it was seen in (YYYY-MM-DD; empty = open).
type Record struct {
	RecordID   string `json:"record_id"`
	RootDomain string `json:"root_domain"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	Value      string `json:"value"`
	FirstSeen  string `json:"first_seen,omitempty"`
	LastSeen   string `json:"last_seen,omitempty"`
}

// Result is one service a record proves. ProviderKey is the provider-recon
// slug for a named provider, the registrable domain for an unmapped one, and
// SelfHosted when the evidence points at the domain itself. Fallback results
// count only where no better evidence of the same service type covers the
// same time (SOA MNAME versus NS); the history view applies that.
type Result struct {
	RecordID     string  `json:"record_id"`
	RootDomain   string  `json:"root_domain"`
	RecordName   string  `json:"record_name"`
	RecordType   string  `json:"record_type"`
	Analyzer     string  `json:"analyzer"`
	Subject      string  `json:"subject"`
	ServiceType  string  `json:"service_type"`
	ProviderKey  string  `json:"provider_key"`
	ProviderSlug string  `json:"provider_slug"`
	ServiceKey   string  `json:"service_key"`
	RuleID       string  `json:"rule_id"`
	Confidence   float64 `json:"confidence"`
	Fallback     bool    `json:"fallback"`
	ValidFrom    string  `json:"valid_from"`
	ValidTo      string  `json:"valid_to"`
}

// Finding is an observation about a record that is not a service.
type Finding struct {
	RecordID string `json:"record_id"`
	Analyzer string `json:"analyzer"`
	Code     string `json:"code"`
	Detail   string `json:"detail,omitempty"`
}

// Output is everything one record resolves to.
type Output struct {
	RecordID string    `json:"record_id"`
	Results  []Result  `json:"results"`
	Findings []Finding `json:"findings"`
}

// SelfHosted is the provider key of evidence that points at the domain itself.
const SelfHosted = "self-hosted"

// Confidence of a result that no provider-recon rule produced.
const (
	KeyMatchConfidence   = 0.8
	SelfHostedConfidence = 0.8
	UnmappedConfidence   = 0.5
)
```

`internal/detect/resolve/label.go`:

```go
package resolve

import (
	"provider_recon/internal/detect/hosts"
	"provider_recon/internal/detect/knowledge"
)

// LabelHost labels one host a record names. The best rule of kind wins,
// giving one result per service type of the matched service. Without a rule
// the host's registrable domain decides: the domain itself (or a host under
// it) is self-hosted; a provider key names that provider, with its first
// service of fallbackType; anything else is kept as an unmapped key. A host
// with no registrable domain (an IP literal, garbage) yields nothing.
//
// base carries the record fields and window; LabelHost fills in the rest.
func LabelHost(kb knowledge.Knowledge, base Result, kind knowledge.Kind, fallbackType, host string) []Result {
	host = hosts.Normalize(host)
	if host == "" {
		return nil
	}
	base.Subject = host
	if m, ok := kb.Match(kind, host); ok {
		out := make([]Result, 0, len(m.ServiceTypes))
		for _, t := range m.ServiceTypes {
			r := base
			r.ServiceType, r.ProviderKey, r.ProviderSlug, r.ServiceKey = t, m.ProviderSlug, m.ProviderSlug, m.ServiceKey
			r.RuleID, r.Confidence = m.RuleID, m.Confidence
			out = append(out, r)
		}
		return out
	}
	r := base
	r.ServiceType = fallbackType
	key := hosts.Registrable(host)
	switch {
	case hosts.Under(host, base.RootDomain) || (key != "" && key == base.RootDomain):
		r.ProviderKey, r.Confidence = SelfHosted, SelfHostedConfidence
	case key == "":
		return nil
	default:
		if p, ok := kb.ProviderForKey(key); ok {
			r.ProviderKey, r.ProviderSlug, r.ServiceKey, r.Confidence = p.Slug, p.Slug, p.FirstService(fallbackType), KeyMatchConfidence
		} else {
			r.ProviderKey, r.Confidence = key, UnmappedConfidence
		}
	}
	return []Result{r}
}
```

`internal/detect/resolve/analyzers.go`:

```go
package resolve

import (
	"strings"

	"provider_recon/internal/detect/knowledge"
)

// Analyzer resolves the records routed to it. base is the result template for
// the record (record fields and window already set).
type Analyzer interface {
	Name() string
	Analyze(rec Record, base Result, kb knowledge.Knowledge) Output
}

// NS labels an apex nameserver: service type dns.
type NS struct{}

func (NS) Name() string { return "ns" }

func (NS) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	return Output{Results: LabelHost(kb, base, knowledge.NSTarget, "dns", rec.Value)}
}

// SOA labels the SOA primary nameserver (MNAME) with NS rules. Its results
// are fallback: NS is the better evidence wherever it covers the same time.
type SOA struct{}

func (SOA) Name() string { return "soa" }

func (SOA) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	f := strings.Fields(rec.Value)
	if len(f) == 0 {
		return Output{}
	}
	base.Fallback = true
	return Output{Results: LabelHost(kb, base, knowledge.NSTarget, "dns", f[0])}
}

// MX labels an apex mail exchanger: service type email. A null MX ("0 .",
// RFC 7505) is a finding, not a provider; localhost placeholders are ignored.
type MX struct{}

func (MX) Name() string { return "mx" }

func (a MX) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	f := strings.Fields(rec.Value)
	if len(f) == 0 {
		return Output{}
	}
	host := strings.ToLower(f[len(f)-1])
	switch host {
	case ".":
		return Output{Findings: []Finding{{RecordID: rec.RecordID, Analyzer: a.Name(), Code: "null_mx", Detail: "the domain accepts no mail"}}}
	case "localhost", "localhost.", "~":
		return Output{}
	}
	return Output{Results: LabelHost(kb, base, knowledge.MXTarget, "email", host)}
}

// CNAME labels an apex or www CNAME target: service type hosting unless a
// rule says otherwise (a CDN edge, a PaaS).
type CNAME struct{}

func (CNAME) Name() string { return "cname" }

func (CNAME) Analyze(rec Record, base Result, kb knowledge.Knowledge) Output {
	return Output{Results: LabelHost(kb, base, knowledge.CNAMETarget, "hosting", rec.Value)}
}
```

`internal/detect/resolve/resolve.go`:

```go
package resolve

import (
	"cmp"
	"slices"
	"strings"

	"provider_recon/internal/detect/hosts"
	"provider_recon/internal/detect/knowledge"
)

// Route picks the analyzer for a normalised record, or nil when no analyzer
// handles it (spec: "Routing and analyzers").
func Route(rec Record) Analyzer {
	apex := rec.Name == rec.RootDomain
	www := rec.Name == "www."+rec.RootDomain
	switch {
	case rec.Type == "NS" && apex:
		return NS{}
	case rec.Type == "SOA" && apex:
		return SOA{}
	case rec.Type == "MX" && apex:
		return MX{}
	case rec.Type == "CNAME" && (apex || www):
		return CNAME{}
	}
	return nil
}

// Resolve turns one record into the services it proves. It normalises names
// and type, routes the record, copies its window onto every result, and sorts
// the output so the same record always gives identical output.
func Resolve(rec Record, kb knowledge.Knowledge) Output {
	rec.RootDomain = hosts.Normalize(rec.RootDomain)
	rec.Name = hosts.Normalize(rec.Name)
	rec.Type = strings.ToUpper(strings.TrimSpace(rec.Type))
	out := Output{RecordID: rec.RecordID, Results: []Result{}, Findings: []Finding{}}
	a := Route(rec)
	if a == nil {
		return out
	}
	base := Result{
		RecordID: rec.RecordID, RootDomain: rec.RootDomain, RecordName: rec.Name, RecordType: rec.Type,
		Analyzer: a.Name(), ValidFrom: day(rec.FirstSeen), ValidTo: day(rec.LastSeen),
	}
	got := a.Analyze(rec, base, kb)
	out.Results = append(out.Results, got.Results...)
	out.Findings = append(out.Findings, got.Findings...)
	slices.SortFunc(out.Results, func(a, b Result) int {
		return cmp.Or(cmp.Compare(a.ServiceType, b.ServiceType), cmp.Compare(a.ProviderKey, b.ProviderKey),
			cmp.Compare(a.Subject, b.Subject), cmp.Compare(a.RuleID, b.RuleID))
	})
	return out
}

func day(date string) string { return date[:min(len(date), 10)] }
```

- [ ] **Step 3: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS (resolve: 7 tests, including the sub-tests).

- [ ] **Step 4: Commit.**

```bash
git add services/provider_recon/internal/detect/resolve
git commit -m "feat(provider_recon): detect/resolve — per-record NS, SOA, MX and CNAME resolution with rule and provider-key labelling

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `dns-detect resolve` CLI, README and real-data smoke

**Files:**
- Create: `cmd/dns-detect/main.go`, `cmd/dns-detect/main_test.go`
- Modify: `services/provider_recon/README.md` (new section "dns-detect")

**Interfaces:**
- Produces: `dns-detect resolve -knowledge DIR`. Record objects on stdin; one line per record on stdout, `{"knowledge_version", "record_id", "results", "findings"}`, in input order. Exit 2 on bad arguments; 1 on a bad record or bad knowledge.

- [ ] **Step 1: Failing test.** `cmd/dns-detect/main_test.go`:

```go
package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

const knowledgeDir = "../../internal/detect/knowledge/testdata/providers"

type outLine struct {
	KnowledgeVersion string `json:"knowledge_version"`
	RecordID         string `json:"record_id"`
	Results          []struct {
		ServiceType string `json:"service_type"`
		ProviderKey string `json:"provider_key"`
		ValidFrom   string `json:"valid_from"`
	} `json:"results"`
	Findings []struct {
		Code string `json:"code"`
	} `json:"findings"`
}

func TestResolveStreamsOneLinePerRecord(t *testing.T) {
	stdin := strings.NewReader(`{"record_id":"a","root_domain":"a.se","name":"a.se.","type":"NS","value":"ns1.loopia.se.","first_seen":"2026-09-01","last_seen":"2026-09-23"}
{"record_id":"b","root_domain":"b.se","name":"b.se.","type":"MX","value":"0 ."}
{"record_id":"c","root_domain":"c.se","name":"c.se.","type":"A","value":"192.0.2.1"}`)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"resolve", "-knowledge", knowledgeDir}, stdin, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 lines, got %q", stdout.String())
	}
	var got []outLine
	for _, l := range lines {
		var o outLine
		if err := json.Unmarshal([]byte(l), &o); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(o.KnowledgeVersion, "sha256:") {
			t.Fatalf("knowledge version = %q", o.KnowledgeVersion)
		}
		got = append(got, o)
	}
	if got[0].RecordID != "a" || len(got[0].Results) != 1 || got[0].Results[0].ProviderKey != "loopia" || got[0].Results[0].ValidFrom != "2026-09-01" {
		t.Fatalf("a = %+v", got[0])
	}
	if got[1].RecordID != "b" || len(got[1].Results) != 0 || len(got[1].Findings) != 1 || got[1].Findings[0].Code != "null_mx" {
		t.Fatalf("b = %+v", got[1])
	}
	if got[2].RecordID != "c" || len(got[2].Results)+len(got[2].Findings) != 0 {
		t.Fatalf("c = %+v", got[2])
	}
}

func TestResolveRejectsBadArguments(t *testing.T) {
	for name, args := range map[string][]string{
		"no subcommand":  {},
		"no knowledge":   {"resolve"},
		"missing folder": {"resolve", "-knowledge", t.TempDir()},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, strings.NewReader(""), &stdout, &stderr); code == 0 {
			t.Errorf("%s: exit 0", name)
		}
	}
}

func TestResolveRejectsBadRecords(t *testing.T) {
	for name, stdin := range map[string]string{
		"not json":       `{"record_id":`,
		"missing fields": `{"record_id":"x","value":"ns1.loopia.se."}`,
	} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"resolve", "-knowledge", knowledgeDir}, strings.NewReader(stdin), &stdout, &stderr); code != 1 {
			t.Errorf("%s: exit %d, want 1 (%s)", name, code, stderr.String())
		}
	}
}
```

Run: `go test ./cmd/dns-detect/`
Expected: FAIL (undefined: `run`).

- [ ] **Step 2: Implement.** `cmd/dns-detect/main.go`:

```go
// Command dns-detect resolves DNS records into the services they prove.
//
//	dns-detect resolve -knowledge DIR < records.ndjson > results.ndjson
//
// stdin holds record objects ({"record_id", "root_domain", "name", "type",
// "value", "first_seen", "last_seen"}), concatenated or one per line. stdout
// gets one line per record, in input order: its results and findings plus the
// knowledge version that produced them. DIR holds provider-recon documents
// (<slug>.json). The ClickHouse worker (spec slice 4) wraps the same resolver.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"provider_recon/internal/detect/knowledge"
	"provider_recon/internal/detect/resolve"
)

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

// line is one output line: a record's output and the knowledge version.
type line struct {
	KnowledgeVersion string `json:"knowledge_version"`
	resolve.Output
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "resolve" {
		fmt.Fprintln(stderr, "usage: dns-detect resolve -knowledge DIR < records.ndjson")
		return 2
	}
	fs := flag.NewFlagSet("resolve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("knowledge", "", "directory of provider-recon documents (<slug>.json)")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *dir == "" {
		fmt.Fprintln(stderr, "resolve: -knowledge is required")
		return 2
	}
	kb, err := knowledge.LoadDir(*dir)
	if err != nil {
		fmt.Fprintf(stderr, "resolve: load knowledge: %v\n", err)
		return 1
	}
	dec := json.NewDecoder(bufio.NewReader(stdin))
	out := bufio.NewWriter(stdout)
	defer out.Flush()
	enc := json.NewEncoder(out)
	for n := 1; ; n++ {
		var rec resolve.Record
		if err := dec.Decode(&rec); errors.Is(err, io.EOF) {
			return 0
		} else if err != nil {
			fmt.Fprintf(stderr, "resolve: record %d: %v\n", n, err)
			return 1
		}
		if rec.RootDomain == "" || rec.Name == "" || rec.Type == "" {
			fmt.Fprintf(stderr, "resolve: record %d: root_domain, name and type are required\n", n)
			return 1
		}
		if err := enc.Encode(line{KnowledgeVersion: kb.Version(), Output: resolve.Resolve(rec, kb)}); err != nil {
			fmt.Fprintf(stderr, "resolve: write: %v\n", err)
			return 1
		}
	}
}
```

The README section "dns-detect" covers:
- purpose: one DNS record → the services it proves, with the record's window;
- the spec link;
- the package layout (`internal/detect/...`);
- the CLI usage line with one example record and its output line;
- the routing table for slice 1;
- that fallback rows are applied by the history view (slice 4);
- how to refresh the fixtures (Task 2, Step 1).

- [ ] **Step 3: Run.**

Run: `go test -race ./... && go vet ./... && test -z "$(gofmt -l .)"`
Expected: PASS (13 packages).

- [ ] **Step 4: Real-data smoke** (not committed; summary in the ledger):

```bash
D=$(mktemp -d); for s in $(ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT JSONExtractString(json,'slug') FROM corpscout.provider_recon_documents_s3 FORMAT TSV\""); do
  ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT json FROM corpscout.provider_recon_documents_s3 WHERE JSONExtractString(json,'slug')='$s' FORMAT RawBLOB\"" > $D/$s.json; done
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT lower(hex(record_id)) AS record_id, root_domain, name, toString(record_type) AS type, value, toString(toDate(first_seen)) AS first_seen, toString(toDate(last_seen)) AS last_seen FROM corpscout.commoncrawl_domain_dns_records WHERE root_domain IN ('spotify.com','volvo.com','loopia.se') AND record_type IN ('NS','SOA','MX','CNAME') FORMAT JSONEachRow\"" \
 | go run ./cmd/dns-detect resolve -knowledge $D \
 | jq -r '.results[] | [.root_domain, .service_type, .provider_key, (if .fallback then "fallback" else "" end), .valid_from, .valid_to] | @tsv' \
 | sort | awk -F'\t' '{k=$1" "$2" "$3" "$4; if(!(k in f)||$5<f[k])f[k]=$5; if($6>l[k])l[k]=$6; n[k]++} END{for(k in n) print k, f[k], "->", l[k], n[k]" rows"}' | sort
```
Expected (as prototyped on 2026-09-28, from 134 records, 25 of which have no result because they are non-apex):
```
loopia.se dns loopia  2026-07-14 -> 2026-09-23 4 rows
loopia.se dns loopia fallback 2026-07-14 -> 2026-09-23 14 rows
loopia.se email loopia  2026-07-14 -> 2026-09-23 2 rows
loopia.se hosting self-hosted  2026-07-09 -> 2026-09-23 3 rows
spotify.com cdn fastly  2026-07-10 -> 2026-09-25 4 rows
spotify.com dns google  2026-07-15 -> 2026-09-25 12 rows
spotify.com dns google fallback 2026-07-10 -> 2026-09-25 4 rows
spotify.com dns nsone.net  2026-07-15 -> 2026-09-25 3 rows
spotify.com dns nsone.net fallback 2026-07-23 -> 2026-08-14 1 rows
spotify.com email google  2026-07-15 -> 2026-09-25 21 rows
volvo.com cdn akamai  2026-07-10 -> 2026-09-26 5 rows
volvo.com dns self-hosted  2026-07-15 -> 2026-09-26 8 rows
volvo.com dns volvo.se  2026-07-15 -> 2026-09-26 8 rows
volvo.com dns volvo.se fallback 2026-07-10 -> 2026-09-26 16 rows
volvo.com email microsoft  2026-07-15 -> 2026-09-26 4 rows
```
New scans or definitions can extend the dates, the row counts or the providers. The service and provider pairs above must still be present.

- [ ] **Step 5: Commit.**

```bash
git add services/provider_recon/cmd/dns-detect services/provider_recon/README.md
git commit -m "feat(provider_recon): dns-detect resolve CLI streaming per-record results

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```
