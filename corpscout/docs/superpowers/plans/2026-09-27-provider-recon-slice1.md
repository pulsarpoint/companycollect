# provider_recon slice 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Go CLI, `provider-recon`, that reads hand-written provider definitions (YAML), collects official IP-range feeds and BGP announcements, and publishes one deterministic `provider-recon/v1` JSON document per provider to S3, plus history copies and a per-run change manifest.

**Architecture:** New module `corpscout/services/provider_recon`, with these packages:
- `model`: the document, deterministic marshal, content hash.
- `matcher`: pattern and IP prefix matching, ported from runner3.
- `definitions`: strict YAML load and validation, ported from pulsarprotectbackoffice-v2.
- `feeds`: one collector per official feed.
- `build`: definitions + feed results → document, keeping stale ranges when a feed fails.
- `publish`: diff, store (filesystem/S3), latest/history/changes.
- `cmd/provider-recon`: the `validate`, `schema` and `collect` subcommands.

ClickHouse tables and the Dagster job are out of scope; they are stage 3.

**Tech Stack:**
- Go 1.25
- `go.yaml.in/yaml/v3` (strict decode)
- `github.com/invopop/jsonschema` (editor schema)
- `github.com/gaissmai/bart` (longest-prefix IP trie)
- `github.com/aws/aws-sdk-go-v2` (S3, path-style for RustFS)
- stdlib `testing` + `net/http/httptest`. No testify, matching `cc-dns-scan`.

**Spec:** `docs/superpowers/specs/2026-09-27-provider-recon-service-design.md`. The background model is in `services/dagster_v3/docs/superpowers/specs/2026-09-27-domain-services-and-technology-domains-design.md`.

## Global Constraints

- Module path `provider_recon`, directory `corpscout/services/provider_recon`, `go 1.25.0` in `go.mod` (same as `services/cc-dns-scan/go.mod`). Binary name `provider-recon`.
- **No imports from PulsarProtect** (`runner3`, `pulsarprotectbackoffice-v2`, `pulsarprotectcontracts`). Ported logic is copied and adapted, never referenced.
- Contract version string is exactly `provider-recon/v1`.
- Service types: closed list, exactly `cdn, ddos_protection, dns, email, email_security, email_sending, hosting, iaas, paas, saas_verification, waf`.
- Traits: closed list, exactly `cloud_provider_ip, origin_obscured, rate_limit_sensitive, shared_infrastructure`.
- One provider per file: `definitions/<slug>.yaml`, strict decode (unknown fields are errors), one YAML document per file.
- **Output is S3 only.** Default bucket `provider-recon` (env `PROVIDER_RECON_BUCKET` overrides). Credentials come from `CORPSCOUT_S3_ENDPOINT`, `CORPSCOUT_S3_ACCESS_KEY` and `CORPSCOUT_S3_SECRET_KEY`. Object keys:
  - `providers/<slug>/latest.json`: rewritten every run.
  - `providers/<slug>/history/<YYYYMMDDTHHMMSSZ>.json`: written only when the content hash changed.
  - `changes/<YYYYMMDDTHHMMSSZ>.json`: the change manifest, one per run.
- The content hash covers identity + evidence only. It excludes the `collection` block and per-item `source_version`, so unchanged feed content means an unchanged hash.
- **Never publish an empty provider.** A failing or empty feed keeps the previous run's ranges (status `stale`). With no previous data the status is `failed`, and the curated evidence is still published.
- Tests: stdlib `testing` only, table-driven where there are several cases. Network tests only behind the `live` build tag; S3 tests only when `PROVIDER_RECON_S3_IT=1`.
- Git: work on branch `provider-recon-slice1` in a worktree (superpowers:using-git-worktrees). **Commit by explicit path only, never `git add -A`**, because the main tree carries unrelated in-progress work. Conventional Commits. End every message with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Use `rg`, not `grep` (repo CLAUDE.md).
- Run `go`/`make` commands from `corpscout/services/provider_recon`, and `git` commands from the `corpscout` root (the commit paths below are relative to it).

## Review Focus

1. **A feed answers 200 with an HTML error page, XML, or a changed layout** (Azure download page, Bunny without the JSON Accept header). Expected: the collector returns an error, and the builder keeps the previous ranges as `stale`. The provider is never emptied. *(Tests: Task 5 Bunny XML, Task 6 Azure link missing, Task 8 stale carry-forward.)*
2. **Re-run with unchanged feed content but a new syncToken/changeNumber.** Expected: no history object and no "changed" entry. *(Tests: Task 8 hash stable across runs, Task 9 second publish unchanged.)*
3. **Malformed feed entries.** Host bits set (`3.5.140.1/22`), garbage lines, bare IPs. Expected: prefixes are masked, bare IPs become /32 or /128, bad lines are counted in `skipped_lines`, and the feed is never aborted. *(Tests: Task 4 AWS, Task 5 Bunny, Task 7 geofeed.)*
4. **A new feed tag appears** (AWS launches a service). Expected: it is listed in the collector's `unmapped_tags` and not attached to any service, unless the definition declares a `"*"` catch-all. *(Test: Task 8.)*
5. **Definition mistakes.** Unknown YAML field, misspelled service type, `tag_map` pointing at a missing service, provider key claimed twice, curated CIDR with host bits. Expected: `validate` fails naming the file and field path, and `collect` refuses to run and writes nothing. *(Tests: Task 3, Task 10.)*

---

## File Structure

```
services/provider_recon/
  go.mod, go.sum, Makefile, .gitignore, README.md
  cmd/provider-recon/main.go             CLI: validate | schema | collect
  cmd/provider-recon/main_test.go
  internal/model/document.go             v1 types, Normalize, Marshal, ContentHash
  internal/model/document_test.go
  internal/matcher/pattern.go            exact/prefix/suffix/contains/glob/regex/exists (port of runner3 match.go)
  internal/matcher/pattern_test.go
  internal/matcher/iptrie.go             longest-prefix lookup over documents
  internal/matcher/iptrie_test.go
  internal/definitions/types.go          YAML definition types, FeedRef.ID, FeedRef.ResolveTag
  internal/definitions/load.go           strict loader
  internal/definitions/validate.go       normalisation + validation (port of backoffice validation)
  internal/definitions/schema.go         JSON Schema generation
  internal/definitions/*_test.go, testdata/
  internal/feeds/fetch.go                HTTP fetch with retry + size cap
  internal/feeds/feeds.go                Collector interface, Result, Registry, helpers
  internal/feeds/{aws,google,cloudflare,fastly,bunny,azure,oracle,github,geofeed,ripestat}.go
  internal/feeds/*_test.go, testdata/, live_test.go (build tag live)
  internal/build/build.go                definition + feed outcomes (+ previous doc) → document
  internal/build/build_test.go
  internal/publish/diff.go               per-provider change computation
  internal/publish/store.go              Store interface + FSStore
  internal/publish/s3.go                 S3Store (aws-sdk-go-v2, path style)
  internal/publish/publish.go            latest/history/changes writer
  internal/publish/*_test.go
  definitions/schema.json                generated, committed
  definitions/<slug>.yaml                first-slice providers
```

---

### Task 1: Module scaffold and the v1 document model

**Files:**
- Create: `services/provider_recon/go.mod`, `Makefile`, `.gitignore`
- Create: `services/provider_recon/internal/model/document.go`
- Test: `services/provider_recon/internal/model/document_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces (package `provider_recon/internal/model`):
  - `const ContractVersion = "provider-recon/v1"`; `var ServiceTypes, Traits []string`
  - `type Source string`, with constants `SourceCurated`, `SourceOfficialFeed`, `SourceBGP`
  - Types `Provenance{Source, Collector, SourceURL, SourceVersion}`
  - Evidence item types, each with a `Key() string` method:
    - `IPRange{CIDR, Region, FeedTag string; Confidence float64; Note string; Provenance}`
    - `ASN{ASN uint32; Confidence; Note; Provenance}`
    - `DNSRule{RecordType, MatchField, MatcherType, Pattern string; CaseSensitive bool; Confidence float64; Priority int; Note; Provenance}`
    - `HTTPRule{HTTPPart, HeaderName, MatcherType, Pattern, PathScope string; CaseSensitive bool; Confidence; Priority; Note; Provenance}`
    - `PTRRule{MatcherType, Pattern; Confidence; Note; Provenance}`
    - `CertificateIdentity{IdentityType, IdentityValue; Confidence; Note; Provenance}`
    - `Candidate{Kind, ServiceKey string; Payload json.RawMessage; Rationale string; SourceURLs []string; Provenance}`
  - Containers:
    - `Evidence{IPRanges, ASNs, DNSRules, HTTPRules, PTRRules, CertificateIdentities}`
    - `Service{Key, DisplayName string; ServiceTypes, Traits []string; Evidence Evidence}`
    - `CollectorStatus{Status, SourceURL, SourceVersion string; Items int; FetchedAt time.Time; LastSuccessAt *time.Time; Error string; UnmappedTags []string; SkippedLines int}`
    - `Collection{CollectedAt time.Time; Collectors map[string]CollectorStatus; ContentHash string}`
    - `Document{Version, Slug, DisplayName, Category, Website, Country string; Aliases, ProviderKeys []string; Services []Service; Collection Collection; Candidates []Candidate}`
  - Functions:
    - `func Normalize(doc *Document)`
    - `func Marshal(doc Document) ([]byte, error)`
    - `func ContentHash(doc Document) (string, error)`
    - `func Contains(list []string, v string) bool`

- [ ] **Step 1: Create the module skeleton**

```bash
mkdir -p services/provider_recon/internal/model && cd services/provider_recon
go mod init provider_recon && go mod edit -go=1.25.0
```

`Makefile`:
```make
.PHONY: build test vet fmt live clean

build:
	mkdir -p bin
	go build -o bin/provider-recon ./cmd/provider-recon

test:
	go test ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

live:
	go test -tags live ./internal/feeds/ -run TestLive -v

clean:
	rm -rf bin
```

`.gitignore`:
```
/bin/
/out/
```

- [ ] **Step 2: Write the failing tests** — `internal/model/document_test.go`

```go
package model

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func sampleDoc() Document {
	return Document{
		Version:      ContractVersion,
		Slug:         "aws",
		DisplayName:  "Amazon Web Services",
		Category:     "cloud",
		Aliases:      []string{"Amazon", "AWS"},
		ProviderKeys: []string{"cloudfront.net", "amazonaws.com"},
		Services: []Service{
			{Key: "aws.ec2", DisplayName: "EC2", ServiceTypes: []string{"iaas"}},
			{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"},
				Evidence: Evidence{IPRanges: []IPRange{
					{CIDR: "52.84.0.0/15", FeedTag: "CLOUDFRONT", Confidence: 1,
						Provenance: Provenance{Source: SourceOfficialFeed, Collector: "aws_ip_ranges", SourceVersion: "syncToken=1"}},
					{CIDR: "13.32.0.0/15", FeedTag: "CLOUDFRONT", Confidence: 1,
						Provenance: Provenance{Source: SourceOfficialFeed, Collector: "aws_ip_ranges", SourceVersion: "syncToken=1"}},
				}}},
		},
		Collection: Collection{
			CollectedAt: time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC),
			Collectors: map[string]CollectorStatus{
				"aws_ip_ranges": {Status: "ok", SourceVersion: "syncToken=1", Items: 2,
					FetchedAt: time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)},
			},
		},
	}
}

func TestMarshalIsDeterministicRegardlessOfInputOrder(t *testing.T) {
	a := sampleDoc()
	b := sampleDoc()
	b.Aliases = []string{"AWS", "Amazon"}
	b.Services[0], b.Services[1] = b.Services[1], b.Services[0]
	r := b.Services[0].Evidence.IPRanges
	r[0], r[1] = r[1], r[0]

	ja, err := Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	jb, err := Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ja, jb) {
		t.Fatalf("marshal differs:\n%s\n---\n%s", ja, jb)
	}
}

func TestMarshalEmitsEmptyArraysNotNull(t *testing.T) {
	out, err := Marshal(Document{Version: ContractVersion, Slug: "x", Services: []Service{{Key: "x.a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "null") {
		t.Fatalf("marshalled document contains null:\n%s", out)
	}
}

func TestMarshalDoesNotReorderCallerSlices(t *testing.T) {
	doc := sampleDoc()
	if _, err := Marshal(doc); err != nil {
		t.Fatal(err)
	}
	if doc.Services[0].Key != "aws.ec2" {
		t.Fatalf("Marshal reordered the caller's services: %q first", doc.Services[0].Key)
	}
}

func TestContentHashIgnoresRunMetadataAndSourceVersions(t *testing.T) {
	base, err := ContentHash(sampleDoc())
	if err != nil {
		t.Fatal(err)
	}

	rerun := sampleDoc()
	rerun.Collection.CollectedAt = rerun.Collection.CollectedAt.Add(24 * time.Hour)
	rerun.Collection.Collectors["aws_ip_ranges"] = CollectorStatus{Status: "stale", SourceVersion: "syncToken=2"}
	for i := range rerun.Services[1].Evidence.IPRanges {
		rerun.Services[1].Evidence.IPRanges[i].SourceVersion = "syncToken=2"
	}
	got, err := ContentHash(rerun)
	if err != nil {
		t.Fatal(err)
	}
	if got != base {
		t.Fatalf("hash changed on metadata-only change: %s vs %s", got, base)
	}

	changed := sampleDoc()
	changed.Services[1].Evidence.IPRanges[0].CIDR = "52.86.0.0/15"
	got, err = ContentHash(changed)
	if err != nil {
		t.Fatal(err)
	}
	if got == base {
		t.Fatal("hash did not change when a CIDR changed")
	}
	if !strings.HasPrefix(base, "sha256:") {
		t.Fatalf("hash %q lacks sha256: prefix", base)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd services/provider_recon && go test ./internal/model/`
Expected: FAIL, build errors (`undefined: Document`, …).

- [ ] **Step 4: Implement** — `internal/model/document.go`

```go
// Package model defines the provider-recon/v1 per-provider document: one
// provider, its services and the evidence that identifies each service.
package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"
)

// ContractVersion is written into every document's version field.
const ContractVersion = "provider-recon/v1"

// ServiceTypes is the closed list of service types a provider service can carry.
var ServiceTypes = []string{
	"cdn", "ddos_protection", "dns", "email", "email_security", "email_sending",
	"hosting", "iaas", "paas", "saas_verification", "waf",
}

// Traits is the closed list of service traits (vocabulary ported from runner3).
var Traits = []string{"cloud_provider_ip", "origin_obscured", "rate_limit_sensitive", "shared_infrastructure"}

// Contains reports whether v is in list.
func Contains(list []string, v string) bool { return slices.Contains(list, v) }

// Source says how an evidence item was obtained.
type Source string

// Evidence sources.
const (
	SourceCurated      Source = "curated"
	SourceOfficialFeed Source = "official_feed"
	SourceBGP          Source = "bgp"
)

// Provenance records where an evidence item came from.
type Provenance struct {
	Source        Source `json:"source"`
	Collector     string `json:"collector,omitempty"`
	SourceURL     string `json:"source_url,omitempty"`
	SourceVersion string `json:"source_version,omitempty"`
}

// IPRange is an address block operated for a service.
type IPRange struct {
	CIDR       string  `json:"cidr"`
	Region     string  `json:"region,omitempty"`
	FeedTag    string  `json:"feed_tag,omitempty"`
	Confidence float64 `json:"confidence"`
	Note       string  `json:"note,omitempty"`
	Provenance
}

// Key identifies the item for sorting and diffs.
func (r IPRange) Key() string { return r.CIDR }

// ASN is an autonomous system operated for a service.
type ASN struct {
	ASN        uint32  `json:"asn"`
	Confidence float64 `json:"confidence"`
	Note       string  `json:"note,omitempty"`
	Provenance
}

// Key identifies the item for sorting and diffs.
func (a ASN) Key() string { return fmt.Sprintf("AS%d", a.ASN) }

// DNSRule matches one field of a DNS record.
type DNSRule struct {
	RecordType    string  `json:"record_type"`
	MatchField    string  `json:"match_field"`
	MatcherType   string  `json:"matcher_type"`
	Pattern       string  `json:"pattern"`
	CaseSensitive bool    `json:"case_sensitive,omitempty"`
	Confidence    float64 `json:"confidence"`
	Priority      int     `json:"priority"`
	Note          string  `json:"note,omitempty"`
	Provenance
}

// Key identifies the item for sorting and diffs.
func (r DNSRule) Key() string {
	return r.RecordType + " " + r.MatchField + " " + r.MatcherType + " " + r.Pattern
}

// HTTPRule matches one part of an HTTP response.
type HTTPRule struct {
	HTTPPart      string  `json:"http_part"`
	HeaderName    string  `json:"header_name,omitempty"`
	MatcherType   string  `json:"matcher_type"`
	Pattern       string  `json:"pattern"`
	PathScope     string  `json:"path_scope,omitempty"`
	CaseSensitive bool    `json:"case_sensitive,omitempty"`
	Confidence    float64 `json:"confidence"`
	Priority      int     `json:"priority"`
	Note          string  `json:"note,omitempty"`
	Provenance
}

// Key identifies the item for sorting and diffs.
func (r HTTPRule) Key() string {
	return r.HTTPPart + " " + r.HeaderName + " " + r.MatcherType + " " + r.Pattern
}

// PTRRule matches a reverse-DNS hostname.
type PTRRule struct {
	MatcherType string  `json:"matcher_type"`
	Pattern     string  `json:"pattern"`
	Confidence  float64 `json:"confidence"`
	Note        string  `json:"note,omitempty"`
	Provenance
}

// Key identifies the item for sorting and diffs.
func (r PTRRule) Key() string { return r.MatcherType + " " + r.Pattern }

// CertificateIdentity matches a TLS certificate field.
type CertificateIdentity struct {
	IdentityType  string  `json:"identity_type"`
	IdentityValue string  `json:"identity_value"`
	Confidence    float64 `json:"confidence"`
	Note          string  `json:"note,omitempty"`
	Provenance
}

// Key identifies the item for sorting and diffs.
func (c CertificateIdentity) Key() string { return c.IdentityType + " " + c.IdentityValue }

// Candidate is unreviewed evidence; detection never uses it.
type Candidate struct {
	Kind       string          `json:"kind"`
	ServiceKey string          `json:"service_key,omitempty"`
	Payload    json.RawMessage `json:"payload"`
	Rationale  string          `json:"rationale,omitempty"`
	SourceURLs []string        `json:"source_urls,omitempty"`
	Provenance
}

// Key identifies the item for sorting and diffs.
func (c Candidate) Key() string { return c.Kind + " " + c.ServiceKey + " " + string(c.Payload) }

// Evidence groups a service's identifying evidence by kind.
type Evidence struct {
	IPRanges              []IPRange             `json:"ip_ranges"`
	ASNs                  []ASN                 `json:"asns"`
	DNSRules              []DNSRule             `json:"dns_rules"`
	HTTPRules             []HTTPRule            `json:"http_rules"`
	PTRRules              []PTRRule             `json:"ptr_rules"`
	CertificateIdentities []CertificateIdentity `json:"certificate_identities"`
}

// Service is one thing a provider offers, typed by our closed service list.
type Service struct {
	Key          string   `json:"service_key"`
	DisplayName  string   `json:"display_name"`
	ServiceTypes []string `json:"service_types"`
	Traits       []string `json:"traits"`
	Evidence     Evidence `json:"evidence"`
}

// CollectorStatus reports one feed's outcome for this provider in this run.
type CollectorStatus struct {
	Status        string     `json:"status"` // ok | stale | failed
	SourceURL     string     `json:"source_url,omitempty"`
	SourceVersion string     `json:"source_version,omitempty"`
	Items         int        `json:"items"`
	FetchedAt     time.Time  `json:"fetched_at"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	Error         string     `json:"error,omitempty"`
	UnmappedTags  []string   `json:"unmapped_tags,omitempty"`
	SkippedLines  int        `json:"skipped_lines,omitempty"`
}

// Collection is run metadata. It is excluded from the content hash.
type Collection struct {
	CollectedAt time.Time                  `json:"collected_at"`
	Collectors  map[string]CollectorStatus `json:"collectors"`
	ContentHash string                     `json:"content_hash"`
}

// Document is the provider-recon/v1 object for one provider.
type Document struct {
	Version      string      `json:"version"`
	Slug         string      `json:"slug"`
	DisplayName  string      `json:"display_name"`
	Category     string      `json:"category"`
	Website      string      `json:"website,omitempty"`
	Country      string      `json:"country,omitempty"`
	Aliases      []string    `json:"aliases"`
	ProviderKeys []string    `json:"provider_keys"`
	Services     []Service   `json:"services"`
	Collection   Collection  `json:"collection"`
	Candidates   []Candidate `json:"candidates"`
}

// Normalize sorts and de-duplicates every list and replaces nil slices and maps
// with empty ones, so equal content always marshals to identical bytes. It
// clones slices before sorting, so a caller's slices are never reordered.
func Normalize(doc *Document) {
	doc.Aliases = sortedUnique(doc.Aliases)
	doc.ProviderKeys = sortedUnique(doc.ProviderKeys)
	doc.Services = slices.Clone(doc.Services)
	if doc.Services == nil {
		doc.Services = []Service{}
	}
	sort.SliceStable(doc.Services, func(i, j int) bool { return doc.Services[i].Key < doc.Services[j].Key })
	for i := range doc.Services {
		s := &doc.Services[i]
		s.ServiceTypes = sortedUnique(s.ServiceTypes)
		s.Traits = sortedUnique(s.Traits)
		e := &s.Evidence
		e.IPRanges = sortByKey(e.IPRanges, IPRange.Key)
		e.ASNs = sortByKey(e.ASNs, ASN.Key)
		e.DNSRules = sortByKey(e.DNSRules, DNSRule.Key)
		e.HTTPRules = sortByKey(e.HTTPRules, HTTPRule.Key)
		e.PTRRules = sortByKey(e.PTRRules, PTRRule.Key)
		e.CertificateIdentities = sortByKey(e.CertificateIdentities, CertificateIdentity.Key)
	}
	doc.Candidates = sortByKey(doc.Candidates, Candidate.Key)
	if doc.Collection.Collectors == nil {
		doc.Collection.Collectors = map[string]CollectorStatus{}
	}
}

// Marshal renders the normalized document as indented JSON with a trailing newline.
func Marshal(doc Document) ([]byte, error) {
	Normalize(&doc)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ContentHash hashes identity and evidence only. The collection block and
// per-item source versions are excluded, so re-running collectors against
// unchanged feed content yields the same hash even when a feed's sync token
// moved.
func ContentHash(doc Document) (string, error) {
	raw, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	var view Document
	if err := json.Unmarshal(raw, &view); err != nil {
		return "", err
	}
	view.Collection = Collection{}
	for i := range view.Services {
		e := &view.Services[i].Evidence
		for j := range e.IPRanges {
			e.IPRanges[j].SourceVersion = ""
		}
		for j := range e.ASNs {
			e.ASNs[j].SourceVersion = ""
		}
		for j := range e.DNSRules {
			e.DNSRules[j].SourceVersion = ""
		}
		for j := range e.HTTPRules {
			e.HTTPRules[j].SourceVersion = ""
		}
		for j := range e.PTRRules {
			e.PTRRules[j].SourceVersion = ""
		}
		for j := range e.CertificateIdentities {
			e.CertificateIdentities[j].SourceVersion = ""
		}
	}
	for j := range view.Candidates {
		view.Candidates[j].SourceVersion = ""
	}
	Normalize(&view)
	b, err := json.Marshal(view)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func sortedUnique(values []string) []string {
	out := slices.Clone(values)
	if out == nil {
		return []string{}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// sortByKey returns a sorted clone; ties on Key break on the JSON encoding so
// the order is total and deterministic.
func sortByKey[T any](items []T, key func(T) string) []T {
	out := slices.Clone(items)
	if out == nil {
		return []T{}
	}
	sort.SliceStable(out, func(i, j int) bool {
		ki, kj := key(out[i]), key(out[j])
		if ki != kj {
			return ki < kj
		}
		bi, _ := json.Marshal(out[i])
		bj, _ := json.Marshal(out[j])
		return bytes.Compare(bi, bj) < 0
	})
	return out
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/model/ && go vet ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add services/provider_recon/go.mod services/provider_recon/Makefile services/provider_recon/.gitignore services/provider_recon/internal/model
git commit -m "feat(provider_recon): module scaffold and provider-recon/v1 document model"
```

---

### Task 2: Pattern matcher and IP prefix table

**Files:**
- Create: `internal/matcher/pattern.go`, `internal/matcher/iptrie.go`
- Test: `internal/matcher/pattern_test.go`, `internal/matcher/iptrie_test.go`

**Interfaces:**
- Consumes: `model.Document`, `model.IPRange`, `model.Source` (Task 1).
- Produces (package `provider_recon/internal/matcher`):
  - `var MatcherTypes []string`
  - `type Pattern struct{…}`
  - `func Compile(matcherType, pattern string, caseSensitive bool) (Pattern, error)`
  - `func (p Pattern) Match(value string) bool`
  - `type IPEntry struct{ ProviderSlug, ServiceKey, FeedTag string; Source model.Source; Confidence float64 }`
  - `type IPTable`
  - `func NewIPTable(docs []model.Document) (*IPTable, error)`
  - `func (t *IPTable) Lookup(addr netip.Addr) (netip.Prefix, []IPEntry, bool)`

- [ ] **Step 1: Write the failing tests** — `internal/matcher/pattern_test.go`

```go
package matcher

import (
	"strings"
	"testing"
)

func TestPatternMatch(t *testing.T) {
	cases := []struct {
		kind, pattern string
		cs            bool
		value         string
		want          bool
	}{
		{"suffix", "ns.cloudflare.com", false, "Adam.NS.cloudflare.com.", true},
		{"suffix", "ns.cloudflare.com", false, "ns.cloudflare.com", true},
		{"suffix", "ns.cloudflare.com", false, "evilns.cloudflare.com", false}, // label-aware
		{"suffix", ".one.com", false, "mx1.one.com", true},
		{"prefix", "google-site-verification=", false, "google-site-verification=abc", true},
		{"prefix", "ms=", false, "MS=ms123456", true},
		{"contains", "include:amazonses.com", false, "v=spf1 include:amazonses.com ~all", true},
		{"exact", "smtp.google.com", false, "SMTP.google.com.", true},
		{"glob", "awsdns-*", true, "awsdns-45.co.uk", true},
		{"glob", "AzureCloud*", true, "AzureCloud.westeurope", true},
		{"glob", "AzureCloud*", true, "AppService", false},
		{"glob", "*", true, "ANYTHING", true},
		{"regex", `^ns-[0-9]+\.awsdns-[0-9]+\.(com|net|org|co\.uk)$`, false, "NS-12.awsdns-45.co.uk", true},
		{"regex", `^ns-[0-9]+\.awsdns`, true, "NS-12.awsdns-45.com", false},
		{"exists", "", false, "", true},
		{"suffix", "one.com", false, "", false},
	}
	for _, c := range cases {
		p, err := Compile(c.kind, c.pattern, c.cs)
		if err != nil {
			t.Fatalf("Compile(%q,%q): %v", c.kind, c.pattern, err)
		}
		if got := p.Match(c.value); got != c.want {
			t.Errorf("%s %q vs %q = %v, want %v", c.kind, c.pattern, c.value, got, c.want)
		}
	}
}

func TestCompileRejects(t *testing.T) {
	cases := []struct{ kind, pattern, wantErr string }{
		{"regex", "(", "invalid regex"},
		{"suffix", "  ", "pattern is required"},
		{"exists", "x", "exists takes an empty pattern"},
		{"fuzzy", "x", "unsupported matcher_type"},
	}
	for _, c := range cases {
		_, err := Compile(c.kind, c.pattern, false)
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("Compile(%q,%q) error = %v, want %q", c.kind, c.pattern, err, c.wantErr)
		}
	}
}
```

`internal/matcher/iptrie_test.go`:
```go
package matcher

import (
	"net/netip"
	"testing"

	"provider_recon/internal/model"
)

func rng(cidr, tag string) model.IPRange {
	return model.IPRange{CIDR: cidr, FeedTag: tag, Confidence: 1, Provenance: model.Provenance{Source: model.SourceOfficialFeed}}
}

func TestIPTableLongestPrefixWins(t *testing.T) {
	docs := []model.Document{
		{Slug: "aws", Services: []model.Service{
			{Key: "aws.other", Evidence: model.Evidence{IPRanges: []model.IPRange{rng("52.0.0.0/10", "AMAZON")}}},
			{Key: "aws.cloudfront", Evidence: model.Evidence{IPRanges: []model.IPRange{rng("52.84.0.0/15", "CLOUDFRONT"), rng("2600:9000::/28", "CLOUDFRONT")}}},
		}},
		{Slug: "other", Services: []model.Service{
			{Key: "other.x", Evidence: model.Evidence{IPRanges: []model.IPRange{rng("52.84.0.0/15", "X")}}},
		}},
	}
	table, err := NewIPTable(docs)
	if err != nil {
		t.Fatal(err)
	}

	p, entries, ok := table.Lookup(netip.MustParseAddr("52.84.1.1"))
	if !ok || p.String() != "52.84.0.0/15" || len(entries) != 2 {
		t.Fatalf("52.84.1.1 → %v %v %v", p, entries, ok)
	}
	if entries[0].ServiceKey != "aws.cloudfront" || entries[1].ProviderSlug != "other" {
		t.Fatalf("entries in unexpected order: %+v", entries)
	}

	_, entries, ok = table.Lookup(netip.MustParseAddr("52.1.1.1"))
	if !ok || entries[0].ServiceKey != "aws.other" {
		t.Fatalf("52.1.1.1 → %+v %v", entries, ok)
	}

	_, entries, ok = table.Lookup(netip.MustParseAddr("2600:9000::1"))
	if !ok || entries[0].ServiceKey != "aws.cloudfront" {
		t.Fatalf("v6 lookup → %+v %v", entries, ok)
	}

	if _, _, ok := table.Lookup(netip.MustParseAddr("10.0.0.1")); ok {
		t.Fatal("10.0.0.1 unexpectedly matched")
	}
}

func TestIPTableRejectsBadCIDR(t *testing.T) {
	_, err := NewIPTable([]model.Document{{Slug: "x", Services: []model.Service{
		{Key: "x.a", Evidence: model.Evidence{IPRanges: []model.IPRange{rng("nope", "")}}},
	}}})
	if err == nil {
		t.Fatal("expected error for invalid CIDR")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/matcher/`
Expected: FAIL (`undefined: Compile`).

- [ ] **Step 3: Implement** — `internal/matcher/pattern.go`

This is ported from runner3 `internal/providerintel/match.go`, with regex and exists added (the backoffice-v2 matcher set).

```go
// Package matcher evaluates provider evidence patterns and IP ranges.
package matcher

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// MatcherTypes lists the supported matcher types.
var MatcherTypes = []string{"contains", "exact", "exists", "glob", "prefix", "regex", "suffix"}

// Pattern is a compiled matcher.
type Pattern struct {
	kind          string
	pattern       string
	caseSensitive bool
	re            *regexp.Regexp
}

// Compile validates and prepares a matcher. Non-regex patterns are compared
// after trimming spaces and a trailing dot, lowercased unless caseSensitive.
// suffix is label-aware: "one.com" matches "mx1.one.com" but not "bone.com".
func Compile(matcherType, pattern string, caseSensitive bool) (Pattern, error) {
	kind := strings.ToLower(strings.TrimSpace(matcherType))
	p := Pattern{kind: kind, caseSensitive: caseSensitive, pattern: normalize(pattern, caseSensitive)}
	switch kind {
	case "exists":
		if strings.TrimSpace(pattern) != "" {
			return Pattern{}, errors.New("exists takes an empty pattern")
		}
		return p, nil
	case "regex":
		expr := strings.TrimSpace(pattern)
		if expr == "" {
			return Pattern{}, errors.New("pattern is required")
		}
		if !caseSensitive {
			expr = "(?i)" + expr
		}
		re, err := regexp.Compile(expr)
		if err != nil {
			return Pattern{}, fmt.Errorf("invalid regex: %w", err)
		}
		p.re = re
		return p, nil
	case "exact", "prefix", "suffix", "contains", "glob":
		if p.pattern == "" {
			return Pattern{}, errors.New("pattern is required")
		}
		return p, nil
	default:
		return Pattern{}, fmt.Errorf("unsupported matcher_type %q", matcherType)
	}
}

// Match reports whether value satisfies the pattern.
func (p Pattern) Match(value string) bool {
	switch p.kind {
	case "exists":
		return true
	case "regex":
		return p.re.MatchString(strings.TrimSuffix(strings.TrimSpace(value), "."))
	}
	v := normalize(value, p.caseSensitive)
	if v == "" {
		return false
	}
	switch p.kind {
	case "exact":
		return v == p.pattern
	case "prefix":
		return strings.HasPrefix(v, p.pattern)
	case "suffix":
		s := strings.TrimPrefix(p.pattern, ".")
		return v == s || strings.HasSuffix(v, "."+s)
	case "contains":
		return strings.Contains(v, p.pattern)
	case "glob":
		return globMatch(p.pattern, v)
	}
	return false
}

func normalize(value string, caseSensitive bool) string {
	value = strings.TrimSpace(value)
	if !caseSensitive {
		value = strings.ToLower(value)
	}
	return strings.TrimSuffix(value, ".")
}

// globMatch supports '*' only (ported from runner3).
func globMatch(pattern, value string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return value == pattern
	}
	if !strings.HasPrefix(value, parts[0]) {
		return false
	}
	cursor := len(parts[0])
	for _, part := range parts[1 : len(parts)-1] {
		idx := strings.Index(value[cursor:], part)
		if idx < 0 {
			return false
		}
		cursor += idx + len(part)
	}
	last := parts[len(parts)-1]
	return len(value)-len(last) >= cursor && strings.HasSuffix(value, last)
}
```

- [ ] **Step 4: Add bart and implement** — `internal/matcher/iptrie.go`

Run: `go get github.com/gaissmai/bart@latest`, then `go doc github.com/gaissmai/bart Table.Lookup`. Confirm the signature is `Lookup(ip netip.Addr) (val V, ok bool)`. If the current version names it differently, adapt the single call below. The table stores the matched prefix as its value, so no other bart API is needed.

```go
package matcher

import (
	"fmt"
	"net/netip"

	"github.com/gaissmai/bart"

	"provider_recon/internal/model"
)

// IPEntry is one service registered on a prefix.
type IPEntry struct {
	ProviderSlug string
	ServiceKey   string
	FeedTag      string
	Source       model.Source
	Confidence   float64
}

// IPTable answers longest-prefix lookups over provider documents.
type IPTable struct {
	trie    bart.Table[netip.Prefix]
	entries map[netip.Prefix][]IPEntry
}

// NewIPTable indexes every ip_ranges item of docs. Several services may share
// a prefix; Lookup returns all of them in document order.
func NewIPTable(docs []model.Document) (*IPTable, error) {
	t := &IPTable{entries: map[netip.Prefix][]IPEntry{}}
	for _, doc := range docs {
		for _, svc := range doc.Services {
			for _, r := range svc.Evidence.IPRanges {
				p, err := netip.ParsePrefix(r.CIDR)
				if err != nil {
					return nil, fmt.Errorf("%s %s: %w", doc.Slug, svc.Key, err)
				}
				p = p.Masked()
				if _, seen := t.entries[p]; !seen {
					t.trie.Insert(p, p)
				}
				t.entries[p] = append(t.entries[p], IPEntry{
					ProviderSlug: doc.Slug, ServiceKey: svc.Key, FeedTag: r.FeedTag,
					Source: r.Source, Confidence: r.Confidence,
				})
			}
		}
	}
	return t, nil
}

// Lookup returns the most specific prefix containing addr and its entries.
func (t *IPTable) Lookup(addr netip.Addr) (netip.Prefix, []IPEntry, bool) {
	p, ok := t.trie.Lookup(addr)
	if !ok {
		return netip.Prefix{}, nil, false
	}
	return p, t.entries[p], true
}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/matcher/ && go vet ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add services/provider_recon/go.mod services/provider_recon/go.sum services/provider_recon/internal/matcher
git commit -m "feat(provider_recon): pattern matcher and longest-prefix IP table"
```

---

### Task 3: Definitions — strict YAML load, validation, JSON Schema

**Files:**
- Create: `internal/definitions/types.go`, `load.go`, `validate.go`, `schema.go`
- Create: `internal/definitions/testdata/valid/example.yaml`
- Create: `definitions/schema.json` (generated in Step 6)
- Test: `internal/definitions/definitions_test.go`

**Interfaces:**
- Consumes:
  - `matcher.Compile` (Task 2)
  - `model.ServiceTypes`, `model.Traits`, `model.Contains` (Task 1)
- Produces (package `provider_recon/internal/definitions`):
  - Types: `Definition`, `ServiceDef`, `IPRangeDef`, `ASNDef`, `DNSRuleDef`, `HTTPRuleDef`, `PTRRuleDef`, `CertIdentityDef`, `FeedRef` (fields below)
  - `func (f FeedRef) ID() string`
  - `func (f FeedRef) ResolveTag(tag string) (serviceKey string, ok bool)`
  - `func (f FeedRef) IsGeneric(tag string) bool`
  - `func ConfidenceOr(p *float64, def float64) float64`
  - `type ParamValidator interface{ ValidateParams(map[string]string) error }`
  - `func LoadDir(dir string) ([]Definition, error)`
  - `func LoadFile(path string) (Definition, error)`
  - `func Validate(defs []Definition, collectors map[string]ParamValidator) error`
  - `func Schema() ([]byte, error)`

- [ ] **Step 1: Add dependencies**

```bash
go get go.yaml.in/yaml/v3@latest github.com/invopop/jsonschema@latest
```

- [ ] **Step 2: Write the test fixture** — `internal/definitions/testdata/valid/example.yaml`

```yaml
slug: example
display_name: Example Cloud
category: CLOUD
country: se
aliases: ["Example", "Example AB", "example"]
provider_keys: ["Example.COM.", "example-dns-*"]
services:
  - key: example.cdn
    display_name: Example CDN
    service_types: [cdn, ddos_protection]
    traits: [shared_infrastructure]
    ip_ranges:
      - {cidr: "192.0.2.0/24", note: "documentation range"}
    asns:
      - {asn: 64500}
    dns_rules:
      - {record_type: cname, match_field: TARGET, matcher_type: Suffix, pattern: "CDN.Example.com."}
    http_rules:
      - {http_part: header, header_name: X-Example-Id, matcher_type: exists}
    ptr_rules:
      - {matcher_type: suffix, pattern: "edge.example.com", confidence: 0.9}
  - key: example.dns
    display_name: Example DNS
    service_types: [dns]
    dns_rules:
      - {record_type: NS, match_field: target, matcher_type: regex, pattern: '^ns[0-9]+\.Example\.com$'}
feeds:
  - collector: fake_feed
    tag_map:
      EDGE: example.cdn
      "EDGE_*": example.cdn
      HEALTH: ""
      ALL: example.cdn
    generic_tags: [ALL]
```

- [ ] **Step 3: Write the failing tests** — `internal/definitions/definitions_test.go`

```go
package definitions

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite definitions/schema.json")

type fakeFeed struct{}

func (fakeFeed) ValidateParams(p map[string]string) error {
	if len(p) > 0 {
		return errors.New("takes no params")
	}
	return nil
}

func collectors() map[string]ParamValidator { return map[string]ParamValidator{"fake_feed": fakeFeed{}} }

func writeYAML(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestValidExampleLoadsAndNormalizes(t *testing.T) {
	defs, err := LoadDir("testdata/valid")
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(defs, collectors()); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	d := defs[0]
	if d.Category != "cloud" || d.Country != "SE" {
		t.Errorf("category/country not normalized: %q %q", d.Category, d.Country)
	}
	if strings.Join(d.ProviderKeys, ",") != "example-dns-*,example.com" {
		t.Errorf("provider keys = %v", d.ProviderKeys)
	}
	if strings.Join(d.Aliases, ",") != "Example,Example AB" {
		t.Errorf("aliases = %v (case-insensitive dedupe, sorted)", d.Aliases)
	}
	r := d.Services[0].DNSRules[0]
	if r.RecordType != "CNAME" || r.MatchField != "target" || r.MatcherType != "suffix" || r.Pattern != "cdn.example.com" || r.Priority != 100 {
		t.Errorf("dns rule not normalized: %+v", r)
	}
	if got := d.Services[1].DNSRules[0].Pattern; got != `^ns[0-9]+\.Example\.com$` {
		t.Errorf("regex pattern must be kept verbatim, got %q", got)
	}
	if h := d.Services[0].HTTPRules[0].HeaderName; h != "x-example-id" {
		t.Errorf("header name = %q", h)
	}
}

func TestLoadFileRejectsUnknownField(t *testing.T) {
	p := writeYAML(t, t.TempDir(), "x.yaml", "slug: x\ndisplay_name: X\ncategory: cloud\nservces: []\n")
	_, err := LoadFile(p)
	if err == nil || !strings.Contains(err.Error(), "servces") {
		t.Fatalf("err = %v, want unknown field servces", err)
	}
}

func TestLoadFileRejectsSecondDocument(t *testing.T) {
	p := writeYAML(t, t.TempDir(), "x.yaml", "slug: x\n---\nslug: y\n")
	_, err := LoadFile(p)
	if err == nil || !strings.Contains(err.Error(), "one definition per file") {
		t.Fatalf("err = %v", err)
	}
}

func base() Definition {
	return Definition{
		Slug: "acme", DisplayName: "Acme", Category: "hosting", File: "defs/acme.yaml",
		Services: []ServiceDef{{Key: "acme.web", DisplayName: "Web", ServiceTypes: []string{"hosting"}}},
	}
}

func TestValidateReportsProblems(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Definition)
		want   string
	}{
		{"service type typo", func(d *Definition) { d.Services[0].ServiceTypes = []string{"hostng"} }, `services[0].service_types: unknown service type "hostng"`},
		{"host bits", func(d *Definition) { d.Services[0].IPRanges = []IPRangeDef{{CIDR: "10.0.0.1/8"}} }, "did you mean 10.0.0.0/8"},
		{"bad cidr", func(d *Definition) { d.Services[0].IPRanges = []IPRangeDef{{CIDR: "10.0.0/8"}} }, "services[0].ip_ranges[0]"},
		{"service key prefix", func(d *Definition) { d.Services[0].Key = "web" }, `must start with "acme."`},
		{"duplicate service", func(d *Definition) { d.Services = append(d.Services, d.Services[0]) }, `duplicate service key "acme.web"`},
		{"bad regex", func(d *Definition) {
			d.Services[0].DNSRules = []DNSRuleDef{{RecordType: "NS", MatchField: "target", MatcherType: "regex", Pattern: "("}}
		}, "invalid regex"},
		{"bad record type", func(d *Definition) {
			d.Services[0].DNSRules = []DNSRuleDef{{RecordType: "SOA", MatchField: "target", MatcherType: "suffix", Pattern: "x"}}
		}, `unsupported record_type "SOA"`},
		{"header without name", func(d *Definition) {
			d.Services[0].HTTPRules = []HTTPRuleDef{{HTTPPart: "header", MatcherType: "exists"}}
		}, "header_name is required"},
		{"unknown collector", func(d *Definition) {
			d.Feeds = []FeedRef{{Collector: "nope", TagMap: map[string]string{"X": "acme.web"}}}
		}, `unknown collector "nope"`},
		{"tag to missing service", func(d *Definition) {
			d.Feeds = []FeedRef{{Collector: "fake_feed", TagMap: map[string]string{"X": "acme.cdn"}}}
		}, `tag_map["X"] points to unknown service "acme.cdn"`},
		{"collector params", func(d *Definition) {
			d.Feeds = []FeedRef{{Collector: "fake_feed", Params: map[string]string{"a": "b"}, TagMap: map[string]string{"X": "acme.web"}}}
		}, "takes no params"},
		{"file name", func(d *Definition) { d.File = "defs/other.yaml" }, "file must be named acme.yaml"},
		{"confidence range", func(d *Definition) {
			c := 1.5
			d.Services[0].ASNs = []ASNDef{{ASN: 1, Confidence: &c}}
		}, "confidence must be between 0 and 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := base()
			c.mutate(&d)
			err := Validate([]Definition{d}, collectors())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want substring %q", err, c.want)
			}
			if !strings.Contains(err.Error(), "defs/") {
				t.Fatalf("error does not name the file: %v", err)
			}
		})
	}
}

func TestValidateRejectsProviderKeyClaimedTwice(t *testing.T) {
	a, b := base(), base()
	b.Slug, b.File, b.Services[0].Key = "other", "defs/other.yaml", "other.web"
	a.ProviderKeys = []string{"shared.net"}
	b.ProviderKeys = []string{"shared.net"}
	err := Validate([]Definition{a, b}, collectors())
	if err == nil || !strings.Contains(err.Error(), `key "shared.net" also claimed by acme`) {
		t.Fatalf("err = %v", err)
	}
}

func TestResolveTag(t *testing.T) {
	f := FeedRef{TagMap: map[string]string{
		"EC2": "aws.ec2", "AzureCloud*": "az.cloud", "AzureCloud.west*": "az.west", "*": "aws.other", "HEALTH": "",
	}}
	cases := map[string]struct {
		svc string
		ok  bool
	}{
		"EC2":                   {"aws.ec2", true},
		"AzureCloud.westeurope": {"az.west", true}, // longest glob wins
		"AzureCloud":            {"az.cloud", true},
		"NEW_SERVICE":           {"aws.other", true},
		"HEALTH":                {"", true}, // explicitly ignored
	}
	for tag, want := range cases {
		svc, ok := f.ResolveTag(tag)
		if svc != want.svc || ok != want.ok {
			t.Errorf("ResolveTag(%q) = %q,%v want %q,%v", tag, svc, ok, want.svc, want.ok)
		}
	}
	if _, ok := (FeedRef{TagMap: map[string]string{"EC2": "x"}}).ResolveTag("S3"); ok {
		t.Error("unmapped tag resolved")
	}
}

func TestFeedRefID(t *testing.T) {
	if got := (FeedRef{Collector: "aws_ip_ranges"}).ID(); got != "aws_ip_ranges" {
		t.Errorf("ID = %q", got)
	}
	f := FeedRef{Collector: "ripestat_announced", Params: map[string]string{"asn": "24940"}}
	if got := f.ID(); got != "ripestat_announced:asn=24940" {
		t.Errorf("ID = %q", got)
	}
}

func TestSchemaIsCurrent(t *testing.T) {
	got, err := Schema()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "definitions", "schema.json")
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run: go test ./internal/definitions -run TestSchemaIsCurrent -update)", err)
	}
	if string(got) != string(want) {
		t.Fatal("definitions/schema.json is stale; run: go test ./internal/definitions -run TestSchemaIsCurrent -update")
	}
}
```

- [ ] **Step 4: Run to verify failure**

Run: `go test ./internal/definitions/`
Expected: FAIL (`undefined: LoadDir` …).

- [ ] **Step 5: Implement**

`internal/definitions/types.go`:
```go
// Package definitions loads and validates the hand-written provider files in
// services/provider_recon/definitions (one provider per <slug>.yaml).
package definitions

import (
	"sort"
	"strings"

	"provider_recon/internal/matcher"
)

// Definition is one provider's curated definition.
type Definition struct {
	Slug         string       `yaml:"slug"`
	DisplayName  string       `yaml:"display_name"`
	Category     string       `yaml:"category"`
	Website      string       `yaml:"website,omitempty"`
	Country      string       `yaml:"country,omitempty"`
	Aliases      []string     `yaml:"aliases,omitempty"`
	ProviderKeys []string     `yaml:"provider_keys,omitempty"`
	Services     []ServiceDef `yaml:"services"`
	Feeds        []FeedRef    `yaml:"feeds,omitempty"`

	// File is the path the definition was loaded from; used in error messages.
	File string `yaml:"-"`
}

// ServiceDef is one service and its curated evidence.
type ServiceDef struct {
	Key                   string            `yaml:"key"`
	DisplayName           string            `yaml:"display_name"`
	ServiceTypes          []string          `yaml:"service_types"`
	Traits                []string          `yaml:"traits,omitempty"`
	Note                  string            `yaml:"note,omitempty"`
	IPRanges              []IPRangeDef      `yaml:"ip_ranges,omitempty"`
	ASNs                  []ASNDef          `yaml:"asns,omitempty"`
	DNSRules              []DNSRuleDef      `yaml:"dns_rules,omitempty"`
	HTTPRules             []HTTPRuleDef     `yaml:"http_rules,omitempty"`
	PTRRules              []PTRRuleDef      `yaml:"ptr_rules,omitempty"`
	CertificateIdentities []CertIdentityDef `yaml:"certificate_identities,omitempty"`
}

// IPRangeDef is a curated address block (for providers without a feed).
type IPRangeDef struct {
	CIDR       string   `yaml:"cidr"`
	Region     string   `yaml:"region,omitempty"`
	Confidence *float64 `yaml:"confidence,omitempty"`
	SourceURL  string   `yaml:"source_url,omitempty"`
	Note       string   `yaml:"note,omitempty"`
}

// ASNDef is a curated autonomous system number.
type ASNDef struct {
	ASN        uint32   `yaml:"asn"`
	Confidence *float64 `yaml:"confidence,omitempty"`
	SourceURL  string   `yaml:"source_url,omitempty"`
	Note       string   `yaml:"note,omitempty"`
}

// DNSRuleDef matches one field of a DNS record.
type DNSRuleDef struct {
	RecordType    string   `yaml:"record_type"`
	MatchField    string   `yaml:"match_field"`
	MatcherType   string   `yaml:"matcher_type"`
	Pattern       string   `yaml:"pattern,omitempty"`
	CaseSensitive bool     `yaml:"case_sensitive,omitempty"`
	Confidence    *float64 `yaml:"confidence,omitempty"`
	Priority      int      `yaml:"priority,omitempty"`
	SourceURL     string   `yaml:"source_url,omitempty"`
	Note          string   `yaml:"note,omitempty"`
}

// HTTPRuleDef matches one part of an HTTP response.
type HTTPRuleDef struct {
	HTTPPart      string   `yaml:"http_part"`
	HeaderName    string   `yaml:"header_name,omitempty"`
	MatcherType   string   `yaml:"matcher_type"`
	Pattern       string   `yaml:"pattern,omitempty"`
	PathScope     string   `yaml:"path_scope,omitempty"`
	CaseSensitive bool     `yaml:"case_sensitive,omitempty"`
	Confidence    *float64 `yaml:"confidence,omitempty"`
	Priority      int      `yaml:"priority,omitempty"`
	SourceURL     string   `yaml:"source_url,omitempty"`
	Note          string   `yaml:"note,omitempty"`
}

// PTRRuleDef matches a reverse-DNS hostname.
type PTRRuleDef struct {
	MatcherType string   `yaml:"matcher_type"`
	Pattern     string   `yaml:"pattern"`
	Confidence  *float64 `yaml:"confidence,omitempty"`
	SourceURL   string   `yaml:"source_url,omitempty"`
	Note        string   `yaml:"note,omitempty"`
}

// CertIdentityDef matches a TLS certificate field.
type CertIdentityDef struct {
	IdentityType  string   `yaml:"identity_type"`
	IdentityValue string   `yaml:"identity_value"`
	Confidence    *float64 `yaml:"confidence,omitempty"`
	SourceURL     string   `yaml:"source_url,omitempty"`
	Note          string   `yaml:"note,omitempty"`
}

// FeedRef attaches a collector's output to this provider's services.
type FeedRef struct {
	Collector string            `yaml:"collector"`
	Params    map[string]string `yaml:"params,omitempty"`
	// TagMap maps feed tags (exact, or globs with '*') to service keys. An
	// empty service key deliberately ignores the tag.
	TagMap map[string]string `yaml:"tag_map"`
	// GenericTags (exact or glob) name umbrella tags whose ranges are dropped
	// when the same CIDR also appears under a specific tag (AWS AMAZON).
	GenericTags []string `yaml:"generic_tags,omitempty"`
}

// ID is the collector name plus sorted params: the key under which the
// collector's status is reported.
func (f FeedRef) ID() string {
	if len(f.Params) == 0 {
		return f.Collector
	}
	keys := make([]string, 0, len(f.Params))
	for k := range f.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + f.Params[k]
	}
	return f.Collector + ":" + strings.Join(parts, ",")
}

// ResolveTag maps a feed tag to a service key. Exact keys win over globs, and
// the longest glob wins among globs. ok is false when nothing matches; an
// empty service key with ok=true means the tag is deliberately ignored.
func (f FeedRef) ResolveTag(tag string) (string, bool) {
	if svc, ok := f.TagMap[tag]; ok {
		return svc, true
	}
	best, bestSvc, found := "", "", false
	for pattern, svc := range f.TagMap {
		if !strings.Contains(pattern, "*") || !globMatches(pattern, tag) {
			continue
		}
		if !found || len(pattern) > len(best) || (len(pattern) == len(best) && pattern < best) {
			best, bestSvc, found = pattern, svc, true
		}
	}
	return bestSvc, found
}

// IsGeneric reports whether tag is one of the feed's umbrella tags.
func (f FeedRef) IsGeneric(tag string) bool {
	for _, g := range f.GenericTags {
		if g == tag || (strings.Contains(g, "*") && globMatches(g, tag)) {
			return true
		}
	}
	return false
}

// ConfidenceOr returns *p, or def when p is nil.
func ConfidenceOr(p *float64, def float64) float64 {
	if p == nil {
		return def
	}
	return *p
}

func globMatches(pattern, value string) bool {
	p, err := matcher.Compile("glob", pattern, true)
	return err == nil && p.Match(value)
}
```

`internal/definitions/load.go`:
```go
package definitions

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"go.yaml.in/yaml/v3"
)

// LoadDir loads every *.yaml in dir, sorted by file name.
func LoadDir(dir string) ([]Definition, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no *.yaml definitions in %s", dir)
	}
	sort.Strings(paths)
	defs := make([]Definition, 0, len(paths))
	for _, p := range paths {
		d, err := LoadFile(p)
		if err != nil {
			return nil, err
		}
		defs = append(defs, d)
	}
	return defs, nil
}

// LoadFile decodes one definition strictly: unknown fields and extra YAML
// documents are errors.
func LoadFile(path string) (Definition, error) {
	f, err := os.Open(path)
	if err != nil {
		return Definition{}, err
	}
	defer f.Close()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	var d Definition
	if err := dec.Decode(&d); err != nil {
		return Definition{}, fmt.Errorf("%s: %w", path, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return Definition{}, fmt.Errorf("%s: one definition per file", path)
	}
	d.File = path
	return d, nil
}
```

`internal/definitions/validate.go`: ported from backoffice-v2 `provider_intelligence_validation.go`, with record types HTTPS and CAA added.
```go
package definitions

import (
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"provider_recon/internal/matcher"
	"provider_recon/internal/model"
)

// ParamValidator checks a collector's params; feeds.Collector satisfies it.
type ParamValidator interface {
	ValidateParams(params map[string]string) error
}

var (
	categories     = []string{"cdn", "cloud", "dns", "email", "hosting", "other", "paas", "saas", "security"}
	dnsRecordTypes = []string{"A", "AAAA", "CAA", "CNAME", "HTTPS", "MX", "NS", "SRV", "TXT"}
	dnsMatchFields = []string{"all", "name", "priority", "target", "value"}
	httpParts      = []string{"body", "cookie", "header", "redirect_location", "status", "title", "tls_alpn"}
	identityTypes  = []string{"issuer_cn", "issuer_org", "san_suffix", "subject_cn"}
	slugRE         = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`)
	serviceNameRE  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	providerKeyRE  = regexp.MustCompile(`^[a-z0-9*][a-z0-9.*-]*[a-z0-9*]$`)
	countryRE      = regexp.MustCompile(`^[A-Z]{2}$`)
)

type problems struct{ errs []error }

func (p *problems) add(d *Definition, path, format string, args ...any) {
	p.errs = append(p.errs, fmt.Errorf("%s: %s: %s", d.File, path, fmt.Sprintf(format, args...)))
}

// Validate normalizes defs in place and returns every problem found, joined.
func Validate(defs []Definition, collectors map[string]ParamValidator) error {
	var p problems
	slugs := map[string]string{}
	keyOwner := map[string]string{}
	for i := range defs {
		d := &defs[i]
		validateProvider(d, &p, collectors)
		if other, dup := slugs[d.Slug]; dup {
			p.add(d, "slug", "duplicate slug %q (also in %s)", d.Slug, other)
		}
		slugs[d.Slug] = d.File
		for _, k := range d.ProviderKeys {
			if owner, taken := keyOwner[k]; taken && owner != d.Slug {
				p.add(d, "provider_keys", "key %q also claimed by %s", k, owner)
				continue
			}
			keyOwner[k] = d.Slug
		}
	}
	return errors.Join(p.errs...)
}

func validateProvider(d *Definition, p *problems, collectors map[string]ParamValidator) {
	d.Slug = strings.TrimSpace(d.Slug)
	d.DisplayName = strings.TrimSpace(d.DisplayName)
	d.Category = strings.ToLower(strings.TrimSpace(d.Category))
	d.Country = strings.ToUpper(strings.TrimSpace(d.Country))
	d.Website = strings.TrimSpace(d.Website)

	if !slugRE.MatchString(d.Slug) {
		p.add(d, "slug", "must match %s", slugRE)
	}
	if d.File != "" && filepath.Base(d.File) != d.Slug+".yaml" {
		p.add(d, "slug", "file must be named %s.yaml", d.Slug)
	}
	if d.DisplayName == "" {
		p.add(d, "display_name", "is required")
	}
	if !slices.Contains(categories, d.Category) {
		p.add(d, "category", "unknown category %q (one of %s)", d.Category, strings.Join(categories, ", "))
	}
	if d.Country != "" && !countryRE.MatchString(d.Country) {
		p.add(d, "country", "must be an ISO 3166-1 alpha-2 code")
	}
	d.Aliases = uniqueFold(d.Aliases)

	keys := make([]string, 0, len(d.ProviderKeys))
	for i, k := range d.ProviderKeys {
		k = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(k)), ".")
		if !providerKeyRE.MatchString(k) {
			p.add(d, fmt.Sprintf("provider_keys[%d]", i), "invalid provider key %q", k)
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)
	d.ProviderKeys = slices.Compact(keys)

	if len(d.Services) == 0 {
		p.add(d, "services", "at least one service is required")
	}
	serviceKeys := map[string]bool{}
	for i := range d.Services {
		s := &d.Services[i]
		path := fmt.Sprintf("services[%d]", i)
		s.Key = strings.ToLower(strings.TrimSpace(s.Key))
		prefix := d.Slug + "."
		if !strings.HasPrefix(s.Key, prefix) || !serviceNameRE.MatchString(strings.TrimPrefix(s.Key, prefix)) {
			p.add(d, path+".key", "service key %q must start with %q followed by [a-z0-9-]", s.Key, prefix)
		}
		if serviceKeys[s.Key] {
			p.add(d, path+".key", "duplicate service key %q", s.Key)
		}
		serviceKeys[s.Key] = true
		if strings.TrimSpace(s.DisplayName) == "" {
			p.add(d, path+".display_name", "is required")
		}
		validateService(d, s, path, p)
	}

	for i := range d.Feeds {
		validateFeed(d, &d.Feeds[i], fmt.Sprintf("feeds[%d]", i), serviceKeys, collectors, p)
	}
}

func validateService(d *Definition, s *ServiceDef, path string, p *problems) {
	if len(s.ServiceTypes) == 0 {
		p.add(d, path+".service_types", "at least one service type is required")
	}
	for i, t := range s.ServiceTypes {
		s.ServiceTypes[i] = strings.ToLower(strings.TrimSpace(t))
		if !model.Contains(model.ServiceTypes, s.ServiceTypes[i]) {
			p.add(d, path+".service_types", "unknown service type %q", s.ServiceTypes[i])
		}
	}
	for i, t := range s.Traits {
		s.Traits[i] = strings.ToLower(strings.TrimSpace(t))
		if !model.Contains(model.Traits, s.Traits[i]) {
			p.add(d, path+".traits", "unknown trait %q", s.Traits[i])
		}
	}
	for i := range s.IPRanges {
		r := &s.IPRanges[i]
		rp := fmt.Sprintf("%s.ip_ranges[%d]", path, i)
		pfx, err := netip.ParsePrefix(strings.TrimSpace(r.CIDR))
		switch {
		case err != nil:
			p.add(d, rp, "invalid cidr %q: %v", r.CIDR, err)
		case pfx != pfx.Masked():
			p.add(d, rp, "cidr %q has host bits set; did you mean %s", r.CIDR, pfx.Masked())
		default:
			r.CIDR = pfx.String()
		}
		checkConfidence(d, rp, r.Confidence, p)
	}
	for i := range s.ASNs {
		rp := fmt.Sprintf("%s.asns[%d]", path, i)
		if s.ASNs[i].ASN == 0 {
			p.add(d, rp, "asn must be > 0")
		}
		checkConfidence(d, rp, s.ASNs[i].Confidence, p)
	}
	for i := range s.DNSRules {
		r := &s.DNSRules[i]
		rp := fmt.Sprintf("%s.dns_rules[%d]", path, i)
		r.RecordType = strings.ToUpper(strings.TrimSpace(r.RecordType))
		r.MatchField = strings.ToLower(strings.TrimSpace(r.MatchField))
		if !slices.Contains(dnsRecordTypes, r.RecordType) {
			p.add(d, rp, "unsupported record_type %q", r.RecordType)
		}
		if !slices.Contains(dnsMatchFields, r.MatchField) {
			p.add(d, rp, "unsupported match_field %q", r.MatchField)
		}
		r.MatcherType, r.Pattern = checkMatcher(d, rp, r.MatcherType, r.Pattern, r.CaseSensitive, p)
		checkConfidence(d, rp, r.Confidence, p)
		if r.Priority == 0 {
			r.Priority = 100
		}
	}
	for i := range s.HTTPRules {
		r := &s.HTTPRules[i]
		rp := fmt.Sprintf("%s.http_rules[%d]", path, i)
		r.HTTPPart = strings.ToLower(strings.TrimSpace(r.HTTPPart))
		r.HeaderName = strings.ToLower(strings.TrimSpace(r.HeaderName))
		r.PathScope = strings.TrimSpace(r.PathScope)
		if !slices.Contains(httpParts, r.HTTPPart) {
			p.add(d, rp, "unsupported http_part %q", r.HTTPPart)
		}
		if r.HTTPPart == "header" && r.HeaderName == "" {
			p.add(d, rp, "header_name is required for header rules")
		}
		if r.HTTPPart != "header" && r.HeaderName != "" {
			p.add(d, rp, "header_name only applies to http_part header")
		}
		r.MatcherType, r.Pattern = checkMatcher(d, rp, r.MatcherType, r.Pattern, r.CaseSensitive, p)
		checkConfidence(d, rp, r.Confidence, p)
		if r.Priority == 0 {
			r.Priority = 100
		}
	}
	for i := range s.PTRRules {
		r := &s.PTRRules[i]
		rp := fmt.Sprintf("%s.ptr_rules[%d]", path, i)
		if strings.EqualFold(strings.TrimSpace(r.MatcherType), "exists") {
			p.add(d, rp, "exists is not meaningful for ptr rules")
		}
		r.MatcherType, r.Pattern = checkMatcher(d, rp, r.MatcherType, r.Pattern, false, p)
		checkConfidence(d, rp, r.Confidence, p)
	}
	for i := range s.CertificateIdentities {
		c := &s.CertificateIdentities[i]
		rp := fmt.Sprintf("%s.certificate_identities[%d]", path, i)
		c.IdentityType = strings.ToLower(strings.TrimSpace(c.IdentityType))
		c.IdentityValue = strings.TrimSpace(c.IdentityValue)
		if !slices.Contains(identityTypes, c.IdentityType) {
			p.add(d, rp, "unsupported identity_type %q", c.IdentityType)
		}
		if c.IdentityValue == "" {
			p.add(d, rp, "identity_value is required")
		}
		checkConfidence(d, rp, c.Confidence, p)
	}
}

// checkMatcher validates via matcher.Compile and returns the normalized
// matcher type and pattern. Regex patterns are kept verbatim; other patterns
// are trimmed, trailing-dot stripped and lowercased unless case-sensitive.
func checkMatcher(d *Definition, path, kind, pattern string, caseSensitive bool, p *problems) (string, string) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if _, err := matcher.Compile(kind, pattern, caseSensitive); err != nil {
		p.add(d, path, "%v", err)
		return kind, pattern
	}
	if kind == "regex" {
		return kind, strings.TrimSpace(pattern)
	}
	pattern = strings.TrimSuffix(strings.TrimSpace(pattern), ".")
	if !caseSensitive {
		pattern = strings.ToLower(pattern)
	}
	return kind, pattern
}

func checkConfidence(d *Definition, path string, c *float64, p *problems) {
	if c != nil && (*c < 0 || *c > 1) {
		p.add(d, path, "confidence must be between 0 and 1")
	}
}

func validateFeed(d *Definition, f *FeedRef, path string, services map[string]bool, collectors map[string]ParamValidator, p *problems) {
	f.Collector = strings.TrimSpace(f.Collector)
	c, known := collectors[f.Collector]
	if !known {
		p.add(d, path+".collector", "unknown collector %q", f.Collector)
	} else if err := c.ValidateParams(f.Params); err != nil {
		p.add(d, path+".params", "%v", err)
	}
	if len(f.TagMap) == 0 {
		p.add(d, path+".tag_map", "at least one tag mapping is required")
	}
	tags := make([]string, 0, len(f.TagMap))
	for tag := range f.TagMap {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	for _, tag := range tags {
		svc := strings.ToLower(strings.TrimSpace(f.TagMap[tag]))
		f.TagMap[tag] = svc
		if svc != "" && !services[svc] {
			p.add(d, path, "tag_map[%q] points to unknown service %q", tag, svc)
		}
	}
	for _, g := range f.GenericTags {
		if strings.Contains(g, "*") {
			continue
		}
		if _, ok := f.ResolveTag(g); !ok {
			p.add(d, path+".generic_tags", "generic tag %q is not mapped by tag_map", g)
		}
	}
}

// uniqueFold trims, drops empties, de-duplicates case-insensitively (first
// spelling wins) and sorts.
func uniqueFold(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		k := strings.ToLower(v)
		if v == "" || seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
```

`internal/definitions/schema.go`:
```go
package definitions

import (
	"encoding/json"

	"github.com/invopop/jsonschema"
)

// Schema returns the JSON Schema for definition files. It is committed as
// definitions/schema.json for editor completion; Validate is the authority.
func Schema() ([]byte, error) {
	r := &jsonschema.Reflector{FieldNameTag: "yaml", ExpandedStruct: true}
	s := r.Reflect(&Definition{})
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
```

- [ ] **Step 6: Generate the schema, then run all tests**

```bash
go test ./internal/definitions -run TestSchemaIsCurrent -update
go test ./internal/definitions/ && go vet ./...
```
Expected: PASS. `definitions/schema.json` exists, and `rg '"additionalProperties": false' definitions/schema.json` finds matches.

- [ ] **Step 7: Commit**

```bash
git add services/provider_recon/go.mod services/provider_recon/go.sum services/provider_recon/internal/definitions services/provider_recon/definitions/schema.json
git commit -m "feat(provider_recon): strict YAML definitions with validation and JSON schema"
```

---

### Task 4: Feed framework and the AWS collector

**Files:**
- Create: `internal/feeds/fetch.go`, `internal/feeds/feeds.go`, `internal/feeds/aws.go`
- Create: `internal/feeds/testdata/aws.json`
- Test: `internal/feeds/fetch_test.go`, `internal/feeds/aws_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks. `feeds` must not import `definitions`, `build` or `publish`.
- Produces (package `provider_recon/internal/feeds`):
  - Fetching:
    - `type Fetcher struct{ Client *http.Client; Retries int; Backoff time.Duration }`
    - `func NewFetcher() *Fetcher`
    - `func (f *Fetcher) Get(ctx, url, accept string) (Response, error)`
    - `type Response struct{ Body []byte; ETag string; URL string }`
  - Results and collectors:
    - `type Range struct{ Prefix netip.Prefix; Tag, Region string }`
    - `type Result struct{ SourceURL, SourceVersion string; Ranges []Range; Skipped int }`
    - `type Collector interface{ Name() string; ValidateParams(map[string]string) error; Collect(ctx context.Context, f *Fetcher, params map[string]string) (Result, error) }`
    - `var ErrEmptyFeed`
    - `func Registry() map[string]Collector`
  - AWS: `type AWS struct{ URL string }`, `func NewAWS() *AWS`, collector name `aws_ip_ranges`
  - Package helpers used by later tasks: `parsePrefix`, `bodyVersion`, `rangeBuilder{res Result}` with `add(raw, tag, region string)`, `finish(Result) (Result, error)`, `noParams`

- [ ] **Step 1: Write the fixture** — `internal/feeds/testdata/aws.json`

```json
{
  "syncToken": "1727400000",
  "createDate": "2026-09-27-06-00-00",
  "prefixes": [
    {"ip_prefix": "52.84.0.0/15", "region": "GLOBAL", "service": "AMAZON", "network_border_group": "GLOBAL"},
    {"ip_prefix": "52.84.0.0/15", "region": "GLOBAL", "service": "CLOUDFRONT", "network_border_group": "GLOBAL"},
    {"ip_prefix": "3.5.140.1/22", "region": "ap-northeast-2", "service": "EC2", "network_border_group": "ap-northeast-2"},
    {"ip_prefix": "not-a-cidr", "region": "eu-north-1", "service": "EC2", "network_border_group": "eu-north-1"}
  ],
  "ipv6_prefixes": [
    {"ipv6_prefix": "2600:9000::/28", "region": "GLOBAL", "service": "CLOUDFRONT", "network_border_group": "GLOBAL"}
  ]
}
```

- [ ] **Step 2: Write the failing tests**

`internal/feeds/fetch_test.go`:
```go
package feeds

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testFetcher(srv *httptest.Server) *Fetcher {
	return &Fetcher{Client: srv.Client(), Retries: 2, Backoff: time.Millisecond}
}

func TestFetcherRetriesServerErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != userAgent {
			t.Errorf("user agent = %q", r.Header.Get("User-Agent"))
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("ETag", `"abc"`)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	resp, err := testFetcher(srv).Get(context.Background(), srv.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != "ok" || resp.ETag != "abc" || calls.Load() != 3 {
		t.Fatalf("body=%q etag=%q calls=%d", resp.Body, resp.ETag, calls.Load())
	}
}

func TestFetcherDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	if _, err := testFetcher(srv).Get(context.Background(), srv.URL, ""); err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}
```

`internal/feeds/aws_test.go`:
```go
package feeds

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func serveFile(t *testing.T, path string) *httptest.Server {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
}

func serveBody(body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
}

func TestAWSCollect(t *testing.T) {
	srv := serveFile(t, "testdata/aws.json")
	defer srv.Close()

	res, err := (&AWS{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.SourceVersion != "syncToken=1727400000" || res.SourceURL != srv.URL {
		t.Fatalf("version=%q url=%q", res.SourceVersion, res.SourceURL)
	}
	if len(res.Ranges) != 4 || res.Skipped != 1 {
		t.Fatalf("ranges=%d skipped=%d, want 4 and 1", len(res.Ranges), res.Skipped)
	}
	ec2 := res.Ranges[2]
	if ec2.Prefix.String() != "3.5.140.0/22" || ec2.Tag != "EC2" || ec2.Region != "ap-northeast-2" {
		t.Fatalf("host bits not masked or fields lost: %+v", ec2)
	}
	if res.Ranges[3].Prefix.String() != "2600:9000::/28" {
		t.Fatalf("ipv6 range = %v", res.Ranges[3].Prefix)
	}
}

func TestAWSEmptyFeedIsAnError(t *testing.T) {
	srv := serveBody(`{"syncToken":"1","prefixes":[],"ipv6_prefixes":[]}`)
	defer srv.Close()
	_, err := (&AWS{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if !errors.Is(err, ErrEmptyFeed) {
		t.Fatalf("err = %v, want ErrEmptyFeed", err)
	}
}

func TestAWSRejectsHTML(t *testing.T) {
	srv := serveBody(`<html>maintenance</html>`)
	defer srv.Close()
	if _, err := (&AWS{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestRegistryNamesMatchCollectors(t *testing.T) {
	for name, c := range Registry() {
		if c.Name() != name {
			t.Errorf("registry key %q holds collector %q", name, c.Name())
		}
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/feeds/`
Expected: FAIL (undefined identifiers).

- [ ] **Step 4: Implement**

`internal/feeds/fetch.go`:
```go
package feeds

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	userAgent = "corpscout-provider-recon/1"
	maxBody   = 64 << 20
)

// Response is a successful fetch.
type Response struct {
	Body []byte
	ETag string
	URL  string
}

// Fetcher performs GETs with retry on network errors, 429 and 5xx.
type Fetcher struct {
	Client  *http.Client
	Retries int
	Backoff time.Duration
}

// NewFetcher returns the production fetcher.
func NewFetcher() *Fetcher {
	return &Fetcher{Client: &http.Client{Timeout: 2 * time.Minute}, Retries: 3, Backoff: 2 * time.Second}
}

// Get fetches url; accept sets the Accept header when non-empty.
func (f *Fetcher) Get(ctx context.Context, url, accept string) (Response, error) {
	var lastErr error
	for attempt := 0; attempt <= f.Retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return Response{}, ctx.Err()
			case <-time.After(f.Backoff * time.Duration(attempt)):
			}
		}
		resp, retry, err := f.once(ctx, url, accept)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !retry {
			break
		}
	}
	return Response{}, fmt.Errorf("GET %s: %w", url, lastErr)
}

func (f *Fetcher) once(ctx context.Context, url, accept string) (Response, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Response{}, false, err
	}
	req.Header.Set("User-Agent", userAgent)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	res, err := f.Client.Do(req)
	if err != nil {
		return Response{}, true, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err != nil {
		return Response{}, true, err
	}
	if len(body) > maxBody {
		return Response{}, false, fmt.Errorf("body exceeds %d bytes", maxBody)
	}
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500 {
		return Response{}, true, fmt.Errorf("status %d", res.StatusCode)
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return Response{}, false, fmt.Errorf("status %d", res.StatusCode)
	}
	return Response{Body: body, ETag: strings.Trim(res.Header.Get("ETag"), `"`), URL: url}, false, nil
}
```

`internal/feeds/feeds.go`:
```go
// Package feeds fetches official provider IP-range publications. Each
// collector turns one publication into tagged prefixes; mapping tags to
// services is the definitions' job, not the collector's.
package feeds

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/netip"
	"strings"
)

// Range is one published prefix with the publisher's tag and region.
type Range struct {
	Prefix netip.Prefix
	Tag    string
	Region string
}

// Result is one collector run.
type Result struct {
	SourceURL     string
	SourceVersion string
	Ranges        []Range
	Skipped       int
}

// Collector fetches one publication.
type Collector interface {
	Name() string
	ValidateParams(params map[string]string) error
	Collect(ctx context.Context, f *Fetcher, params map[string]string) (Result, error)
}

// ErrEmptyFeed means a publication parsed but yielded no usable ranges.
var ErrEmptyFeed = errors.New("feed returned no usable ranges")

// Registry returns every production collector keyed by name.
func Registry() map[string]Collector {
	all := []Collector{
		NewAWS(),
	}
	out := make(map[string]Collector, len(all))
	for _, c := range all {
		out[c.Name()] = c
	}
	return out
}

type noParams struct{}

func (noParams) ValidateParams(params map[string]string) error {
	if len(params) > 0 {
		return errors.New("collector takes no params")
	}
	return nil
}

// parsePrefix accepts CIDRs (host bits are masked) and bare addresses
// (become /32 or /128).
func parsePrefix(s string) (netip.Prefix, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return netip.Prefix{}, false
	}
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return netip.Prefix{}, false
		}
		return p.Masked(), true
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, false
	}
	return netip.PrefixFrom(a, a.BitLen()), true
}

// bodyVersion is a short content hash for publications without a version field.
func bodyVersion(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:8])
}

type rangeBuilder struct{ res Result }

func (b *rangeBuilder) add(raw, tag, region string) {
	p, ok := parsePrefix(raw)
	if !ok {
		b.res.Skipped++
		return
	}
	b.res.Ranges = append(b.res.Ranges, Range{Prefix: p, Tag: tag, Region: region})
}

func finish(res Result) (Result, error) {
	if len(res.Ranges) == 0 {
		return res, ErrEmptyFeed
	}
	return res, nil
}
```

`internal/feeds/aws.go`:
```go
package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

const awsURL = "https://ip-ranges.amazonaws.com/ip-ranges.json"

// AWS collects ip-ranges.json; tags are AWS service names (EC2, CLOUDFRONT, AMAZON…).
type AWS struct {
	noParams
	URL string
}

// NewAWS returns the collector for the official feed.
func NewAWS() *AWS { return &AWS{URL: awsURL} }

// Name implements Collector.
func (*AWS) Name() string { return "aws_ip_ranges" }

// Collect implements Collector.
func (c *AWS) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		SyncToken string `json:"syncToken"`
		Prefixes  []struct {
			IPPrefix string `json:"ip_prefix"`
			Region   string `json:"region"`
			Service  string `json:"service"`
		} `json:"prefixes"`
		IPv6Prefixes []struct {
			IPv6Prefix string `json:"ipv6_prefix"`
			Region     string `json:"region"`
			Service    string `json:"service"`
		} `json:"ipv6_prefixes"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("aws: decode: %w", err)
	}
	b := rangeBuilder{res: Result{SourceURL: c.URL, SourceVersion: "syncToken=" + feed.SyncToken}}
	for _, p := range feed.Prefixes {
		b.add(p.IPPrefix, p.Service, p.Region)
	}
	for _, p := range feed.IPv6Prefixes {
		b.add(p.IPv6Prefix, p.Service, p.Region)
	}
	return finish(b.res)
}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/feeds/ && go vet ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add services/provider_recon/internal/feeds
git commit -m "feat(provider_recon): feed fetcher, collector interface and AWS ip-ranges collector"
```

---

### Task 5: Google, Cloudflare, Fastly and Bunny collectors

**Files:**
- Create: `internal/feeds/google.go`, `cloudflare.go`, `fastly.go`, `bunny.go`
- Modify: `internal/feeds/feeds.go` (the `Registry` list)
- Test: `internal/feeds/cdn_test.go`

**Interfaces:**
- Consumes: `Fetcher`, `Result`, `rangeBuilder`, `finish`, `bodyVersion`, `noParams` (Task 4).
- Produces collectors, each with a `URL` field (Bunny has two), for httptest overrides:
  - `NewGoogleCloud()` → `google_cloud`. Tag = the feed's `service` value, e.g. `"Google Cloud"`; region = `scope`.
  - `NewGoogleGoog()` → `google_goog`. Tag `GOOG`.
  - `NewCloudflare()` → `cloudflare_ips`. Tag `CLOUDFLARE`.
  - `NewFastly()` → `fastly_public_ips`. Tag `FASTLY`.
  - `NewBunny()` → `bunny_edge_servers`. Tag `EDGE`; fields `URLv4`, `URLv6`.

- [ ] **Step 1: Write the failing tests** — `internal/feeds/cdn_test.go`

```go
package feeds

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGoogleCloudCollect(t *testing.T) {
	srv := serveBody(`{"syncToken":"1727","creationTime":"x","prefixes":[
		{"ipv4Prefix":"34.1.208.0/20","service":"Google Cloud","scope":"africa-south1"},
		{"ipv6Prefix":"2600:1900:8000::/44","service":"Google Cloud","scope":"us-east4"}]}`)
	defer srv.Close()
	res, err := (&GoogleCloud{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[0].Tag != "Google Cloud" || res.Ranges[0].Region != "africa-south1" || res.SourceVersion != "syncToken=1727" {
		t.Fatalf("%+v", res)
	}
}

func TestGoogleGoogCollect(t *testing.T) {
	srv := serveBody(`{"syncToken":"9","prefixes":[{"ipv4Prefix":"8.8.4.0/24"},{"ipv6Prefix":"2001:4860::/32"}]}`)
	defer srv.Close()
	res, err := (&GoogleGoog{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[1].Tag != "GOOG" {
		t.Fatalf("%+v", res)
	}
}

func TestCloudflareCollect(t *testing.T) {
	srv := serveBody(`{"success":true,"result":{"ipv4_cidrs":["173.245.48.0/20"],"ipv6_cidrs":["2400:cb00::/32"],"etag":"38f79d050aa027e3be3865e495dcc9bc"}}`)
	defer srv.Close()
	res, err := (&Cloudflare{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[0].Tag != "CLOUDFLARE" || res.SourceVersion != "etag=38f79d050aa027e3be3865e495dcc9bc" {
		t.Fatalf("%+v", res)
	}
}

func TestCloudflareUnsuccessfulResponseIsAnError(t *testing.T) {
	srv := serveBody(`{"success":false,"errors":[{"message":"rate limited"}],"result":null}`)
	defer srv.Close()
	if _, err := (&Cloudflare{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestFastlyCollect(t *testing.T) {
	srv := serveBody(`{"addresses":["23.235.32.0/20"],"ipv6_addresses":["2a04:4e40::/32"]}`)
	defer srv.Close()
	res, err := (&Fastly{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[0].Tag != "FASTLY" || !strings.HasPrefix(res.SourceVersion, "sha256:") {
		t.Fatalf("%+v", res)
	}
}

func TestBunnyCollectsBareIPs(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/json" {
			w.Write([]byte(`<?xml version="1.0"?><ArrayOfString/>`))
			return
		}
		w.Write([]byte(`["89.187.188.227","bogus"]`))
	})
	mux.HandleFunc("/v6", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`["2400:52e0:1a00::1"]`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	res, err := (&Bunny{URLv4: srv.URL + "/v4", URLv6: srv.URL + "/v6"}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Skipped != 1 {
		t.Fatalf("ranges=%d skipped=%d", len(res.Ranges), res.Skipped)
	}
	if res.Ranges[0].Prefix.String() != "89.187.188.227/32" || res.Ranges[1].Prefix.String() != "2400:52e0:1a00::1/128" {
		t.Fatalf("%+v", res.Ranges)
	}
}

func TestBunnyXMLIsAnError(t *testing.T) {
	srv := serveBody(`<?xml version="1.0"?><ArrayOfString><string>1.2.3.4</string></ArrayOfString>`)
	defer srv.Close()
	_, err := (&Bunny{URLv4: srv.URL, URLv6: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err == nil || !strings.Contains(err.Error(), "bunny") {
		t.Fatalf("err = %v", err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/feeds/`
Expected: FAIL (`undefined: GoogleCloud` …).

- [ ] **Step 3: Implement**

`internal/feeds/google.go`:
```go
package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

type googleFeed struct {
	SyncToken string `json:"syncToken"`
	Prefixes  []struct {
		IPv4Prefix string `json:"ipv4Prefix"`
		IPv6Prefix string `json:"ipv6Prefix"`
		Service    string `json:"service"`
		Scope      string `json:"scope"`
	} `json:"prefixes"`
}

func collectGoogle(ctx context.Context, f *Fetcher, url, fixedTag string) (Result, error) {
	resp, err := f.Get(ctx, url, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed googleFeed
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("google: decode: %w", err)
	}
	b := rangeBuilder{res: Result{SourceURL: url, SourceVersion: "syncToken=" + feed.SyncToken}}
	for _, p := range feed.Prefixes {
		tag := fixedTag
		if tag == "" {
			tag = p.Service
		}
		raw := p.IPv4Prefix
		if raw == "" {
			raw = p.IPv6Prefix
		}
		b.add(raw, tag, p.Scope)
	}
	return finish(b.res)
}

// GoogleCloud collects cloud.json: ranges customers can use in Google Cloud.
type GoogleCloud struct {
	noParams
	URL string
}

// NewGoogleCloud returns the collector for the official feed.
func NewGoogleCloud() *GoogleCloud { return &GoogleCloud{URL: "https://www.gstatic.com/ipranges/cloud.json"} }

// Name implements Collector.
func (*GoogleCloud) Name() string { return "google_cloud" }

// Collect implements Collector.
func (c *GoogleCloud) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	return collectGoogle(ctx, f, c.URL, "")
}

// GoogleGoog collects goog.json: all Google-owned ranges (a superset of
// cloud.json). Longest-prefix lookup lets the more specific cloud ranges win.
type GoogleGoog struct {
	noParams
	URL string
}

// NewGoogleGoog returns the collector for the official feed.
func NewGoogleGoog() *GoogleGoog { return &GoogleGoog{URL: "https://www.gstatic.com/ipranges/goog.json"} }

// Name implements Collector.
func (*GoogleGoog) Name() string { return "google_goog" }

// Collect implements Collector.
func (c *GoogleGoog) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	return collectGoogle(ctx, f, c.URL, "GOOG")
}
```

`internal/feeds/cloudflare.go`:
```go
package feeds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Cloudflare collects the proxy network ranges from the public API.
type Cloudflare struct {
	noParams
	URL string
}

// NewCloudflare returns the collector for the official feed.
func NewCloudflare() *Cloudflare { return &Cloudflare{URL: "https://api.cloudflare.com/client/v4/ips"} }

// Name implements Collector.
func (*Cloudflare) Name() string { return "cloudflare_ips" }

// Collect implements Collector.
func (c *Cloudflare) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		Success bool `json:"success"`
		Result  *struct {
			IPv4 []string `json:"ipv4_cidrs"`
			IPv6 []string `json:"ipv6_cidrs"`
			ETag string   `json:"etag"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("cloudflare: decode: %w", err)
	}
	if !feed.Success || feed.Result == nil {
		return Result{}, errors.New("cloudflare: API reported success=false")
	}
	b := rangeBuilder{res: Result{SourceURL: c.URL, SourceVersion: "etag=" + feed.Result.ETag}}
	for _, s := range append(feed.Result.IPv4, feed.Result.IPv6...) {
		b.add(s, "CLOUDFLARE", "")
	}
	return finish(b.res)
}
```

`internal/feeds/fastly.go`:
```go
package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

// Fastly collects the edge network ranges.
type Fastly struct {
	noParams
	URL string
}

// NewFastly returns the collector for the official feed.
func NewFastly() *Fastly { return &Fastly{URL: "https://api.fastly.com/public-ip-list"} }

// Name implements Collector.
func (*Fastly) Name() string { return "fastly_public_ips" }

// Collect implements Collector.
func (c *Fastly) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		Addresses []string `json:"addresses"`
		IPv6      []string `json:"ipv6_addresses"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("fastly: decode: %w", err)
	}
	b := rangeBuilder{res: Result{SourceURL: c.URL, SourceVersion: bodyVersion(resp.Body)}}
	for _, s := range append(feed.Addresses, feed.IPv6...) {
		b.add(s, "FASTLY", "")
	}
	return finish(b.res)
}
```

`internal/feeds/bunny.go`:
```go
package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

// Bunny collects edge server addresses (bare IPs → /32, /128). The API
// answers XML unless asked for JSON.
type Bunny struct {
	noParams
	URLv4 string
	URLv6 string
}

// NewBunny returns the collector for the official lists.
func NewBunny() *Bunny {
	return &Bunny{
		URLv4: "https://bunnycdn.com/api/system/edgeserverlist",
		URLv6: "https://bunnycdn.com/api/system/edgeserverlist/ipv6",
	}
}

// Name implements Collector.
func (*Bunny) Name() string { return "bunny_edge_servers" }

// Collect implements Collector.
func (c *Bunny) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	b := rangeBuilder{res: Result{SourceURL: c.URLv4}}
	var bodies []byte
	for _, url := range []string{c.URLv4, c.URLv6} {
		resp, err := f.Get(ctx, url, "application/json")
		if err != nil {
			return Result{}, err
		}
		var ips []string
		if err := json.Unmarshal(resp.Body, &ips); err != nil {
			return Result{}, fmt.Errorf("bunny: %s is not a JSON list: %w", url, err)
		}
		for _, ip := range ips {
			b.add(ip, "EDGE", "")
		}
		bodies = append(bodies, resp.Body...)
	}
	b.res.SourceVersion = bodyVersion(bodies)
	return finish(b.res)
}
```

Modify `internal/feeds/feeds.go` so the registry list is:
```go
	all := []Collector{
		NewAWS(),
		NewGoogleCloud(),
		NewGoogleGoog(),
		NewCloudflare(),
		NewFastly(),
		NewBunny(),
	}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/feeds/ && go vet ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/feeds
git commit -m "feat(provider_recon): Google, Cloudflare, Fastly and Bunny collectors"
```

---

### Task 6: Azure, Oracle and GitHub collectors

**Files:**
- Create: `internal/feeds/azure.go`, `oracle.go`, `github.go`
- Modify: `internal/feeds/feeds.go` (the `Registry` list)
- Test: `internal/feeds/cloud_test.go`

**Interfaces:**
- Consumes: the Task 4 helpers.
- Produces:
  - `NewAzure()` → `azure_service_tags`.
    - Fields: `PageURL string`, `LinkPattern *regexp.Regexp`.
    - Tag = the service tag name (e.g. `AppService.WestEurope`); region = `properties.region`; version `changeNumber=N`.
  - `NewOracle()` → `oracle_public_ip_ranges`. One range per CIDR tag (`OCI`, `OSN`, `OBJECT_STORAGE`); region = the OCI region.
  - `NewGitHub()` → `github_meta`. Tag = the meta key (`pages`, `hooks`, …), taken only from keys holding CIDR lists.

- [ ] **Step 1: Write the failing tests** — `internal/feeds/cloud_test.go`

```go
package feeds

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

func TestAzureCollectFollowsDownloadLink(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<html><a href="%s/download/7/1/d/x/ServiceTags_Public_20260921.json">Download</a></html>`, srv.URL)
	})
	mux.HandleFunc("/download/7/1/d/x/ServiceTags_Public_20260921.json", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"changeNumber":412,"cloud":"Public","values":[
			{"name":"AzureFrontDoor.Frontend","properties":{"region":"","addressPrefixes":["13.107.246.0/24","2620:1ec:bdf::/48"]}},
			{"name":"AppService.WestEurope","properties":{"region":"westeurope","addressPrefixes":["20.50.2.0/24"]}}]}`))
	})

	c := &Azure{
		PageURL:     srv.URL + "/page",
		LinkPattern: regexp.MustCompile(regexp.QuoteMeta(srv.URL) + `/download/[^"'\s<>]+/ServiceTags_Public_\d{8}\.json`),
	}
	res, err := c.Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.SourceVersion != "changeNumber=412" || !strings.HasSuffix(res.SourceURL, "ServiceTags_Public_20260921.json") {
		t.Fatalf("version=%q url=%q", res.SourceVersion, res.SourceURL)
	}
	if len(res.Ranges) != 3 || res.Ranges[2].Tag != "AppService.WestEurope" || res.Ranges[2].Region != "westeurope" {
		t.Fatalf("%+v", res.Ranges)
	}
}

func TestAzureMissingLinkIsAnError(t *testing.T) {
	srv := serveBody(`<html>we moved things around</html>`)
	defer srv.Close()
	_, err := (&Azure{PageURL: srv.URL, LinkPattern: azureLinkRE}).Collect(context.Background(), testFetcher(srv), nil)
	if err == nil || !strings.Contains(err.Error(), "download link not found") {
		t.Fatalf("err = %v", err)
	}
}

func TestOracleCollectOneRangePerTag(t *testing.T) {
	srv := serveBody(`{"last_updated_timestamp":"2026-09-20T12:00:00.000000","regions":[
		{"region":"eu-stockholm-1","cidrs":[{"cidr":"138.2.0.0/16","tags":["OCI"]},{"cidr":"134.70.96.0/22","tags":["OSN","OBJECT_STORAGE"]}]}]}`)
	defer srv.Close()
	res, err := (&Oracle{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 3 || res.Ranges[2].Tag != "OBJECT_STORAGE" || res.Ranges[0].Region != "eu-stockholm-1" {
		t.Fatalf("%+v", res.Ranges)
	}
	if res.SourceVersion != "last_updated=2026-09-20T12:00:00.000000" {
		t.Fatalf("version = %q", res.SourceVersion)
	}
}

func TestGitHubCollectUsesOnlyCIDRLists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `W/"e1"`)
		w.Write([]byte(`{"verifiable_password_authentication":false,
			"ssh_keys":["ssh-ed25519 AAAAC3Nza"],
			"pages":["185.199.108.0/22","2606:50c0:8000::/64"],
			"hooks":["192.30.252.0/22","bad"],
			"domains":{"website":["*.github.com"]}}`))
	}))
	defer srv.Close()
	res, err := (&GitHub{URL: srv.URL}).Collect(context.Background(), testFetcher(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	tags := map[string]int{}
	for _, r := range res.Ranges {
		tags[r.Tag]++
	}
	if tags["pages"] != 2 || tags["hooks"] != 1 || len(tags) != 2 || res.Skipped != 1 {
		t.Fatalf("tags=%v skipped=%d", tags, res.Skipped)
	}
	if res.SourceVersion != `etag=W/"e1` {
		t.Fatalf("version = %q", res.SourceVersion)
	}
}
```

(The Fetcher trims surrounding `"` from the ETag, so `W/"e1"` arrives as `W/"e1`. The test pins that exact value.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/feeds/`
Expected: FAIL (`undefined: Azure` …).

- [ ] **Step 3: Implement**

`internal/feeds/azure.go`:
```go
package feeds

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
)

const azurePage = "https://www.microsoft.com/en-us/download/details.aspx?id=56519"

var azureLinkRE = regexp.MustCompile(`https://download\.microsoft\.com/download/[^"'\s<>]+/ServiceTags_Public_\d{8}\.json`)

// Azure collects the weekly Service Tags file. Its URL changes every week, so
// the collector reads the download page first and follows the JSON link.
type Azure struct {
	noParams
	PageURL     string
	LinkPattern *regexp.Regexp
}

// NewAzure returns the collector for the official publication.
func NewAzure() *Azure { return &Azure{PageURL: azurePage, LinkPattern: azureLinkRE} }

// Name implements Collector.
func (*Azure) Name() string { return "azure_service_tags" }

// Collect implements Collector.
func (c *Azure) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	page, err := f.Get(ctx, c.PageURL, "text/html")
	if err != nil {
		return Result{}, err
	}
	link := c.LinkPattern.Find(page.Body)
	if link == nil {
		return Result{}, fmt.Errorf("azure: ServiceTags download link not found on %s (page layout changed?)", c.PageURL)
	}
	url := string(link)
	resp, err := f.Get(ctx, url, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		ChangeNumber int `json:"changeNumber"`
		Values       []struct {
			Name       string `json:"name"`
			Properties struct {
				Region          string   `json:"region"`
				AddressPrefixes []string `json:"addressPrefixes"`
			} `json:"properties"`
		} `json:"values"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("azure: decode: %w", err)
	}
	b := rangeBuilder{res: Result{SourceURL: url, SourceVersion: fmt.Sprintf("changeNumber=%d", feed.ChangeNumber)}}
	for _, v := range feed.Values {
		for _, p := range v.Properties.AddressPrefixes {
			b.add(p, v.Name, v.Properties.Region)
		}
	}
	return finish(b.res)
}
```

`internal/feeds/oracle.go`:
```go
package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

// Oracle collects OCI public ranges; each CIDR carries one or more tags.
type Oracle struct {
	noParams
	URL string
}

// NewOracle returns the collector for the official feed.
func NewOracle() *Oracle {
	return &Oracle{URL: "https://docs.oracle.com/en-us/iaas/tools/public_ip_ranges.json"}
}

// Name implements Collector.
func (*Oracle) Name() string { return "oracle_public_ip_ranges" }

// Collect implements Collector.
func (c *Oracle) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		LastUpdated string `json:"last_updated_timestamp"`
		Regions     []struct {
			Region string `json:"region"`
			CIDRs  []struct {
				CIDR string   `json:"cidr"`
				Tags []string `json:"tags"`
			} `json:"cidrs"`
		} `json:"regions"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("oracle: decode: %w", err)
	}
	b := rangeBuilder{res: Result{SourceURL: c.URL, SourceVersion: "last_updated=" + feed.LastUpdated}}
	for _, r := range feed.Regions {
		for _, cidr := range r.CIDRs {
			for _, tag := range cidr.Tags {
				b.add(cidr.CIDR, tag, r.Region)
			}
		}
	}
	return finish(b.res)
}
```

`internal/feeds/github.go`:
```go
package feeds

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// GitHub collects /meta. Only keys whose value is a list containing at least
// one CIDR become tags; ssh_keys, domains and flags are ignored.
type GitHub struct {
	noParams
	URL string
}

// NewGitHub returns the collector for the official endpoint.
func NewGitHub() *GitHub { return &GitHub{URL: "https://api.github.com/meta"} }

// Name implements Collector.
func (*GitHub) Name() string { return "github_meta" }

// Collect implements Collector.
func (c *GitHub) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/vnd.github+json")
	if err != nil {
		return Result{}, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(resp.Body, &raw); err != nil {
		return Result{}, fmt.Errorf("github: decode: %w", err)
	}
	version := bodyVersion(resp.Body)
	if resp.ETag != "" {
		version = "etag=" + resp.ETag
	}
	res := Result{SourceURL: c.URL, SourceVersion: version}
	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		var list []string
		if json.Unmarshal(raw[key], &list) != nil {
			continue
		}
		var ranges []Range
		bad := 0
		for _, s := range list {
			if p, ok := parsePrefix(s); ok {
				ranges = append(ranges, Range{Prefix: p, Tag: key})
			} else {
				bad++
			}
		}
		if len(ranges) == 0 {
			continue // not a CIDR list (ssh_keys and friends)
		}
		res.Ranges = append(res.Ranges, ranges...)
		res.Skipped += bad
	}
	return finish(res)
}
```

Modify the `Registry` list in `feeds.go` to add `NewAzure(), NewOracle(), NewGitHub(),` after `NewBunny(),`.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/feeds/ && go vet ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/feeds
git commit -m "feat(provider_recon): Azure service tags, Oracle and GitHub meta collectors"
```

---

### Task 7: Geofeed and RIPEstat collectors, plus live feed tests

**Files:**
- Create: `internal/feeds/geofeed.go`, `ripestat.go`, `live_test.go`
- Modify: `internal/feeds/feeds.go` (the `Registry` list)
- Test: `internal/feeds/params_test.go`

**Interfaces:**
- Consumes: the Task 4 helpers.
- Produces:
  - `NewGeofeed()` → `geofeed`.
    - Param `url` (https only).
    - RFC 8805 CSV; tag `GEOFEED`; region = ISO 3166-2 region, or the country when that's empty.
  - `NewRIPEstat()` → `ripestat_announced`.
    - Param `asn` (digits).
    - Field `URLFormat string` (one `%s` for the ASN).
    - Tag `BGP`; version = hash of the sorted prefix list.

- [ ] **Step 1: Write the failing tests** — `internal/feeds/params_test.go`

```go
package feeds

import (
	"context"
	"strings"
	"testing"
)

func TestGeofeedCollect(t *testing.T) {
	srv := serveBody("# DigitalOcean geofeed\n5.101.96.0/21,NL,NL-NH,Amsterdam,\n\n2a03:b0c0::/32,NL,,Amsterdam,\nnonsense,SE,,,\n")
	defer srv.Close()
	res, err := NewGeofeed().Collect(context.Background(), testFetcher(srv), map[string]string{"url": srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Skipped != 1 {
		t.Fatalf("ranges=%d skipped=%d", len(res.Ranges), res.Skipped)
	}
	if res.Ranges[0].Region != "NL-NH" || res.Ranges[1].Region != "NL" || res.Ranges[0].Tag != "GEOFEED" {
		t.Fatalf("%+v", res.Ranges)
	}
	if res.SourceURL != srv.URL {
		t.Fatalf("source url = %q", res.SourceURL)
	}
}

func TestGeofeedParams(t *testing.T) {
	g := NewGeofeed()
	if err := g.ValidateParams(map[string]string{"url": "https://digitalocean.com/geo/google.csv"}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []map[string]string{nil, {"url": "http://x"}, {"url": "https://x", "extra": "1"}} {
		if err := g.ValidateParams(bad); err == nil {
			t.Errorf("params %v accepted", bad)
		}
	}
}

func TestRIPEstatCollect(t *testing.T) {
	srv := serveBody(`{"status":"ok","data":{"prefixes":[{"prefix":"88.198.0.0/16"},{"prefix":"2a01:4f8::/32"}]}}`)
	defer srv.Close()
	c := &RIPEstat{URLFormat: srv.URL + "/?resource=AS%s"}
	res, err := c.Collect(context.Background(), testFetcher(srv), map[string]string{"asn": "24940"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ranges) != 2 || res.Ranges[0].Tag != "BGP" || !strings.HasPrefix(res.SourceVersion, "sha256:") {
		t.Fatalf("%+v", res)
	}
	if !strings.Contains(res.SourceURL, "AS24940") {
		t.Fatalf("source url = %q", res.SourceURL)
	}
}

func TestRIPEstatStatusNotOK(t *testing.T) {
	srv := serveBody(`{"status":"error","messages":[["error","bad resource"]],"data":{}}`)
	defer srv.Close()
	c := &RIPEstat{URLFormat: srv.URL + "/?resource=AS%s"}
	if _, err := c.Collect(context.Background(), testFetcher(srv), map[string]string{"asn": "1"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestRIPEstatParams(t *testing.T) {
	r := NewRIPEstat()
	if err := r.ValidateParams(map[string]string{"asn": "24940"}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []map[string]string{nil, {"asn": "AS24940"}, {"asn": "1", "x": "y"}} {
		if err := r.ValidateParams(bad); err == nil {
			t.Errorf("params %v accepted", bad)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/feeds/`
Expected: FAIL (`undefined: NewGeofeed` …).

- [ ] **Step 3: Implement**

`internal/feeds/geofeed.go`:
```go
package feeds

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"strings"
)

// Geofeed collects an RFC 8805 self-published geofeed (DigitalOcean, Linode…).
type Geofeed struct{}

// NewGeofeed returns the generic geofeed collector; the URL is a param.
func NewGeofeed() *Geofeed { return &Geofeed{} }

// Name implements Collector.
func (*Geofeed) Name() string { return "geofeed" }

// ValidateParams requires exactly one https url.
func (*Geofeed) ValidateParams(params map[string]string) error {
	if len(params) != 1 || !strings.HasPrefix(params["url"], "https://") {
		return errors.New("geofeed takes exactly one param: url (https)")
	}
	return nil
}

// Collect implements Collector.
func (*Geofeed) Collect(ctx context.Context, f *Fetcher, params map[string]string) (Result, error) {
	url := params["url"]
	resp, err := f.Get(ctx, url, "text/csv")
	if err != nil {
		return Result{}, err
	}
	r := csv.NewReader(bytes.NewReader(resp.Body))
	r.FieldsPerRecord = -1
	r.Comment = '#'
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return Result{}, fmt.Errorf("geofeed: parse %s: %w", url, err)
	}
	b := rangeBuilder{res: Result{SourceURL: url, SourceVersion: bodyVersion(resp.Body)}}
	for _, rec := range records {
		if len(rec) == 0 || strings.TrimSpace(rec[0]) == "" {
			continue
		}
		region := ""
		if len(rec) > 2 && strings.TrimSpace(rec[2]) != "" {
			region = strings.TrimSpace(rec[2])
		} else if len(rec) > 1 {
			region = strings.TrimSpace(rec[1])
		}
		b.add(rec[0], "GEOFEED", region)
	}
	return finish(b.res)
}
```

`internal/feeds/ripestat.go`:
```go
package feeds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var asnRE = regexp.MustCompile(`^[0-9]+$`)

// RIPEstat collects the prefixes an ASN currently announces. Used for
// providers without an official range feed (Hetzner, OVH, Akamai…); the
// ranges are provider-level evidence with source "bgp".
type RIPEstat struct {
	URLFormat string
}

// NewRIPEstat returns the collector for the public RIPEstat API.
func NewRIPEstat() *RIPEstat {
	return &RIPEstat{URLFormat: "https://stat.ripe.net/data/announced-prefixes/data.json?resource=AS%s&sourceapp=corpscout-provider-recon"}
}

// Name implements Collector.
func (*RIPEstat) Name() string { return "ripestat_announced" }

// ValidateParams requires exactly one numeric asn.
func (*RIPEstat) ValidateParams(params map[string]string) error {
	if len(params) != 1 || !asnRE.MatchString(params["asn"]) {
		return errors.New(`ripestat_announced takes exactly one param: asn (digits, no "AS" prefix)`)
	}
	return nil
}

// Collect implements Collector.
func (c *RIPEstat) Collect(ctx context.Context, f *Fetcher, params map[string]string) (Result, error) {
	url := fmt.Sprintf(c.URLFormat, params["asn"])
	resp, err := f.Get(ctx, url, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		Status string `json:"status"`
		Data   struct {
			Prefixes []struct {
				Prefix string `json:"prefix"`
			} `json:"prefixes"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("ripestat: decode: %w", err)
	}
	if feed.Status != "ok" {
		return Result{}, fmt.Errorf("ripestat: status %q for AS%s", feed.Status, params["asn"])
	}
	prefixes := make([]string, 0, len(feed.Data.Prefixes))
	b := rangeBuilder{res: Result{SourceURL: url}}
	for _, p := range feed.Data.Prefixes {
		b.add(p.Prefix, "BGP", "")
		prefixes = append(prefixes, p.Prefix)
	}
	// The response embeds query timestamps; version on the prefix set only.
	sort.Strings(prefixes)
	b.res.SourceVersion = bodyVersion([]byte(strings.Join(prefixes, "\n")))
	return finish(b.res)
}
```

Modify the `Registry` list in `feeds.go` to add `NewGeofeed(), NewRIPEstat(),` last.

`internal/feeds/live_test.go`:
```go
//go:build live

package feeds

import (
	"context"
	"testing"
	"time"
)

// TestLive hits every real publication. Run with `make live` when a feed
// format may have changed; never in CI.
func TestLive(t *testing.T) {
	params := map[string]map[string]string{
		"geofeed":            {"url": "https://digitalocean.com/geo/google.csv"},
		"ripestat_announced": {"asn": "24940"},
	}
	minimum := map[string]int{
		"aws_ip_ranges": 1000, "azure_service_tags": 1000, "google_cloud": 100, "google_goog": 50,
		"cloudflare_ips": 10, "fastly_public_ips": 10, "bunny_edge_servers": 50,
		"oracle_public_ip_ranges": 100, "github_meta": 10, "geofeed": 100, "ripestat_announced": 10,
	}
	f := NewFetcher()
	for name, c := range Registry() {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			res, err := c.Collect(ctx, f, params[name])
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Ranges) < minimum[name] {
				t.Fatalf("%d ranges, want >= %d", len(res.Ranges), minimum[name])
			}
			t.Logf("%s: %d ranges, %d skipped, version %s", name, len(res.Ranges), res.Skipped, res.SourceVersion)
		})
	}
}
```

- [ ] **Step 4: Run unit tests, then the live tests once**

Run: `go test ./internal/feeds/ && go vet ./... && make live`
Expected: unit tests PASS; every live subtest PASS. If a live test fails, the real format differs from the fixture: fix the collector **and** update the fixture to match the real format before continuing. Don't loosen the thresholds.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/feeds
git commit -m "feat(provider_recon): geofeed and RIPEstat collectors plus live feed tests"
```

---

### Task 8: Build a document from a definition and feed outcomes

**Files:**
- Create: `internal/build/build.go`
- Test: `internal/build/build_test.go`

**Interfaces:**
- Consumes:
  - `definitions.Definition`, `FeedRef.ID`, `FeedRef.ResolveTag`, `FeedRef.IsGeneric`, `ConfidenceOr` (Task 3)
  - `feeds.Result`, `feeds.Range` (Task 4)
  - `model.*`, `model.Normalize`, `model.ContentHash` (Task 1)
- Produces (package `provider_recon/internal/build`):
  - `type FeedOutcome struct{ Result feeds.Result; Err error }`
  - `func Build(def definitions.Definition, outcomes map[string]FeedOutcome, prev *model.Document, now time.Time) (model.Document, error)`

  `outcomes` is keyed by `FeedRef.ID()`. `prev` is the provider's previous `latest.json`, or nil.

- [ ] **Step 1: Write the failing tests** — `internal/build/build_test.go`

```go
package build

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
)

var t0 = time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)

func awsDef() definitions.Definition {
	return definitions.Definition{
		Slug: "aws", DisplayName: "Amazon Web Services", Category: "cloud",
		ProviderKeys: []string{"amazonaws.com"},
		Services: []definitions.ServiceDef{
			{Key: "aws.cloudfront", DisplayName: "CloudFront", ServiceTypes: []string{"cdn"},
				DNSRules: []definitions.DNSRuleDef{{RecordType: "CNAME", MatchField: "target", MatcherType: "suffix", Pattern: "cloudfront.net", Priority: 100}}},
			{Key: "aws.ec2", DisplayName: "EC2", ServiceTypes: []string{"iaas"}},
			{Key: "aws.other", DisplayName: "Other", ServiceTypes: []string{"iaas"}},
		},
		Feeds: []definitions.FeedRef{{
			Collector:   "aws_ip_ranges",
			TagMap:      map[string]string{"CLOUDFRONT": "aws.cloudfront", "EC2": "aws.ec2", "AMAZON": "aws.other", "ROUTE53_HEALTHCHECKS": ""},
			GenericTags: []string{"AMAZON"},
		}},
	}
}

func r(cidr, tag, region string) feeds.Range {
	return feeds.Range{Prefix: netip.MustParsePrefix(cidr), Tag: tag, Region: region}
}

func okOutcome(version string, ranges ...feeds.Range) map[string]FeedOutcome {
	return map[string]FeedOutcome{"aws_ip_ranges": {Result: feeds.Result{
		SourceURL: "https://ip-ranges.amazonaws.com/ip-ranges.json", SourceVersion: version, Ranges: ranges,
	}}}
}

func service(t *testing.T, doc model.Document, key string) model.Service {
	t.Helper()
	for _, s := range doc.Services {
		if s.Key == key {
			return s
		}
	}
	t.Fatalf("service %s missing", key)
	return model.Service{}
}

func TestBuildMapsTagsAndDropsGenericDuplicates(t *testing.T) {
	doc, err := Build(awsDef(), okOutcome("syncToken=1",
		r("52.84.0.0/15", "AMAZON", "GLOBAL"),
		r("52.84.0.0/15", "CLOUDFRONT", "GLOBAL"),
		r("3.0.0.0/9", "AMAZON", "GLOBAL"),
		r("15.177.0.0/18", "ROUTE53_HEALTHCHECKS", "GLOBAL"),
	), nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	cf := service(t, doc, "aws.cloudfront").Evidence.IPRanges
	other := service(t, doc, "aws.other").Evidence.IPRanges
	if len(cf) != 1 || cf[0].CIDR != "52.84.0.0/15" || cf[0].Source != model.SourceOfficialFeed || cf[0].Collector != "aws_ip_ranges" {
		t.Fatalf("cloudfront ranges = %+v", cf)
	}
	if len(other) != 1 || other[0].CIDR != "3.0.0.0/9" {
		t.Fatalf("AMAZON duplicate of a specific range was kept, or unique AMAZON dropped: %+v", other)
	}
	st := doc.Collection.Collectors["aws_ip_ranges"]
	if st.Status != "ok" || st.Items != 2 || len(st.UnmappedTags) != 0 || st.LastSuccessAt == nil {
		t.Fatalf("status = %+v", st)
	}
	if rule := service(t, doc, "aws.cloudfront").Evidence.DNSRules; len(rule) != 1 || rule[0].Source != model.SourceCurated || rule[0].Confidence != 1 {
		t.Fatalf("curated dns rule = %+v", rule)
	}
	if doc.Version != model.ContractVersion || doc.Collection.ContentHash == "" {
		t.Fatalf("version=%q hash=%q", doc.Version, doc.Collection.ContentHash)
	}
}

func TestBuildReportsUnmappedTags(t *testing.T) {
	doc, err := Build(awsDef(), okOutcome("syncToken=1",
		r("52.84.0.0/15", "CLOUDFRONT", ""),
		r("18.0.0.0/16", "NEW_SERVICE", ""),
		r("18.1.0.0/16", "NEW_SERVICE", ""),
	), nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	st := doc.Collection.Collectors["aws_ip_ranges"]
	if len(st.UnmappedTags) != 1 || st.UnmappedTags[0] != "NEW_SERVICE" {
		t.Fatalf("unmapped = %v", st.UnmappedTags)
	}
	for _, s := range doc.Services {
		for _, ip := range s.Evidence.IPRanges {
			if ip.FeedTag == "NEW_SERVICE" {
				t.Fatalf("unmapped tag attached to %s", s.Key)
			}
		}
	}
}

func TestBuildKeepsPreviousRangesWhenFeedFails(t *testing.T) {
	first, err := Build(awsDef(), okOutcome("syncToken=1", r("52.84.0.0/15", "CLOUDFRONT", "")), nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	failed := map[string]FeedOutcome{"aws_ip_ranges": {Err: errors.New("status 503")}}
	second, err := Build(awsDef(), failed, &first, t0.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	st := second.Collection.Collectors["aws_ip_ranges"]
	if st.Status != "stale" || st.Error != "status 503" || st.LastSuccessAt == nil || !st.LastSuccessAt.Equal(t0) || st.Items != 1 {
		t.Fatalf("status = %+v", st)
	}
	if len(service(t, second, "aws.cloudfront").Evidence.IPRanges) != 1 {
		t.Fatal("previous ranges not carried forward")
	}
	if second.Collection.ContentHash != first.Collection.ContentHash {
		t.Fatal("a stale carry-forward must not change the content hash")
	}
}

func TestBuildFailsWithoutHistoryButKeepsCuratedEvidence(t *testing.T) {
	doc, err := Build(awsDef(), map[string]FeedOutcome{"aws_ip_ranges": {Err: feeds.ErrEmptyFeed}}, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	if st := doc.Collection.Collectors["aws_ip_ranges"]; st.Status != "failed" || st.LastSuccessAt != nil {
		t.Fatalf("status = %+v", st)
	}
	if len(service(t, doc, "aws.cloudfront").Evidence.DNSRules) != 1 {
		t.Fatal("curated evidence lost when the feed failed")
	}
}

func TestBuildMissingOutcomeCountsAsFailure(t *testing.T) {
	doc, err := Build(awsDef(), map[string]FeedOutcome{}, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	if st := doc.Collection.Collectors["aws_ip_ranges"]; st.Status != "failed" || st.Error == "" {
		t.Fatalf("status = %+v", st)
	}
}

func TestBuildHashStableAcrossRunsWithNewSyncToken(t *testing.T) {
	a, err := Build(awsDef(), okOutcome("syncToken=1", r("52.84.0.0/15", "CLOUDFRONT", "")), nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build(awsDef(), okOutcome("syncToken=2", r("52.84.0.0/15", "CLOUDFRONT", "")), &a, t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if a.Collection.ContentHash != b.Collection.ContentHash {
		t.Fatal("hash changed although only the sync token moved")
	}
}

func TestBuildDedupesRegionalDuplicatesKeepingRegion(t *testing.T) {
	def := definitions.Definition{
		Slug: "microsoft", DisplayName: "Microsoft", Category: "cloud",
		Services: []definitions.ServiceDef{{Key: "microsoft.azure-cloud", DisplayName: "Azure", ServiceTypes: []string{"iaas"}}},
		Feeds: []definitions.FeedRef{{Collector: "azure_service_tags", TagMap: map[string]string{"AzureCloud*": "microsoft.azure-cloud"}}},
	}
	outcomes := map[string]FeedOutcome{"azure_service_tags": {Result: feeds.Result{SourceVersion: "changeNumber=1", Ranges: []feeds.Range{
		r("20.50.0.0/16", "AzureCloud", ""),
		r("20.50.0.0/16", "AzureCloud.westeurope", "westeurope"),
	}}}}
	doc, err := Build(def, outcomes, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	got := doc.Services[0].Evidence.IPRanges
	if len(got) != 1 || got[0].Region != "westeurope" {
		t.Fatalf("ranges = %+v", got)
	}
}

func TestBuildBGPSourceAndConfidence(t *testing.T) {
	def := definitions.Definition{
		Slug: "hetzner", DisplayName: "Hetzner", Category: "hosting",
		Services: []definitions.ServiceDef{{Key: "hetzner.cloud", DisplayName: "Hetzner", ServiceTypes: []string{"hosting"}}},
		Feeds: []definitions.FeedRef{{Collector: "ripestat_announced", Params: map[string]string{"asn": "24940"}, TagMap: map[string]string{"BGP": "hetzner.cloud"}}},
	}
	outcomes := map[string]FeedOutcome{"ripestat_announced:asn=24940": {Result: feeds.Result{Ranges: []feeds.Range{r("88.198.0.0/16", "BGP", "")}}}}
	doc, err := Build(def, outcomes, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	ip := doc.Services[0].Evidence.IPRanges[0]
	if ip.Source != model.SourceBGP || ip.Confidence != 0.8 || ip.Collector != "ripestat_announced:asn=24940" {
		t.Fatalf("%+v", ip)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/build/`
Expected: FAIL (`undefined: Build`).

- [ ] **Step 3: Implement** — `internal/build/build.go`

```go
// Package build turns a curated definition plus this run's feed outcomes into
// a provider-recon/v1 document.
package build

import (
	"errors"
	"net/netip"
	"sort"
	"time"

	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
)

// FeedOutcome is one collector run: a result or an error.
type FeedOutcome struct {
	Result feeds.Result
	Err    error
}

const (
	curatedConfidence  = 1.0
	officialConfidence = 1.0
	bgpConfidence      = 0.8
)

// bgpCollectors publish announcements, not operator-published ranges.
var bgpCollectors = map[string]bool{"ripestat_announced": true}

// Build assembles the document. A failed feed keeps prev's ranges for that
// feed (status stale); with no previous success its status is failed and only
// curated evidence remains. It never returns a document that lost ranges
// because a feed broke.
func Build(def definitions.Definition, outcomes map[string]FeedOutcome, prev *model.Document, now time.Time) (model.Document, error) {
	doc := model.Document{
		Version: model.ContractVersion, Slug: def.Slug, DisplayName: def.DisplayName,
		Category: def.Category, Website: def.Website, Country: def.Country,
		Aliases: def.Aliases, ProviderKeys: def.ProviderKeys,
		Collection: model.Collection{CollectedAt: now, Collectors: map[string]model.CollectorStatus{}},
	}
	index := map[string]int{}
	for _, s := range def.Services {
		index[s.Key] = len(doc.Services)
		doc.Services = append(doc.Services, curatedService(s))
	}
	for _, ref := range def.Feeds {
		id := ref.ID()
		outcome, ran := outcomes[id]
		if !ran {
			outcome = FeedOutcome{Err: errors.New("collector did not run")}
		}
		if outcome.Err == nil {
			doc.Collection.Collectors[id] = applyFeed(&doc, index, ref, outcome.Result, now)
		} else {
			doc.Collection.Collectors[id] = carryForward(&doc, index, id, outcome.Err, prev, now)
		}
	}
	for i := range doc.Services {
		doc.Services[i].Evidence.IPRanges = dedupeRanges(doc.Services[i].Evidence.IPRanges)
	}
	model.Normalize(&doc)
	hash, err := model.ContentHash(doc)
	if err != nil {
		return model.Document{}, err
	}
	doc.Collection.ContentHash = hash
	return doc, nil
}

func curated(url string) model.Provenance {
	return model.Provenance{Source: model.SourceCurated, SourceURL: url}
}

func curatedService(s definitions.ServiceDef) model.Service {
	out := model.Service{Key: s.Key, DisplayName: s.DisplayName, ServiceTypes: s.ServiceTypes, Traits: s.Traits}
	e := &out.Evidence
	for _, r := range s.IPRanges {
		e.IPRanges = append(e.IPRanges, model.IPRange{CIDR: r.CIDR, Region: r.Region,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Note: r.Note, Provenance: curated(r.SourceURL)})
	}
	for _, a := range s.ASNs {
		e.ASNs = append(e.ASNs, model.ASN{ASN: a.ASN,
			Confidence: definitions.ConfidenceOr(a.Confidence, curatedConfidence), Note: a.Note, Provenance: curated(a.SourceURL)})
	}
	for _, r := range s.DNSRules {
		e.DNSRules = append(e.DNSRules, model.DNSRule{RecordType: r.RecordType, MatchField: r.MatchField,
			MatcherType: r.MatcherType, Pattern: r.Pattern, CaseSensitive: r.CaseSensitive,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Priority: r.Priority,
			Note: r.Note, Provenance: curated(r.SourceURL)})
	}
	for _, r := range s.HTTPRules {
		e.HTTPRules = append(e.HTTPRules, model.HTTPRule{HTTPPart: r.HTTPPart, HeaderName: r.HeaderName,
			MatcherType: r.MatcherType, Pattern: r.Pattern, PathScope: r.PathScope, CaseSensitive: r.CaseSensitive,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Priority: r.Priority,
			Note: r.Note, Provenance: curated(r.SourceURL)})
	}
	for _, r := range s.PTRRules {
		e.PTRRules = append(e.PTRRules, model.PTRRule{MatcherType: r.MatcherType, Pattern: r.Pattern,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Note: r.Note, Provenance: curated(r.SourceURL)})
	}
	for _, c := range s.CertificateIdentities {
		e.CertificateIdentities = append(e.CertificateIdentities, model.CertificateIdentity{IdentityType: c.IdentityType,
			IdentityValue: c.IdentityValue, Confidence: definitions.ConfidenceOr(c.Confidence, curatedConfidence),
			Note: c.Note, Provenance: curated(c.SourceURL)})
	}
	return out
}

func applyFeed(doc *model.Document, index map[string]int, ref definitions.FeedRef, res feeds.Result, now time.Time) model.CollectorStatus {
	id := ref.ID()
	source, confidence := model.SourceOfficialFeed, officialConfidence
	if bgpCollectors[ref.Collector] {
		source, confidence = model.SourceBGP, bgpConfidence
	}
	specific := map[netip.Prefix]bool{}
	for _, r := range res.Ranges {
		if !ref.IsGeneric(r.Tag) {
			specific[r.Prefix] = true
		}
	}
	unmapped := map[string]bool{}
	items := 0
	for _, r := range res.Ranges {
		if ref.IsGeneric(r.Tag) && specific[r.Prefix] {
			continue
		}
		svcKey, ok := ref.ResolveTag(r.Tag)
		if !ok {
			unmapped[r.Tag] = true
			continue
		}
		if svcKey == "" {
			continue
		}
		i, ok := index[svcKey]
		if !ok {
			unmapped[r.Tag] = true
			continue
		}
		doc.Services[i].Evidence.IPRanges = append(doc.Services[i].Evidence.IPRanges, model.IPRange{
			CIDR: r.Prefix.String(), Region: r.Region, FeedTag: r.Tag, Confidence: confidence,
			Provenance: model.Provenance{Source: source, Collector: id, SourceURL: res.SourceURL, SourceVersion: res.SourceVersion},
		})
		items++
	}
	success := now
	return model.CollectorStatus{
		Status: "ok", SourceURL: res.SourceURL, SourceVersion: res.SourceVersion, Items: items,
		FetchedAt: now, LastSuccessAt: &success, SkippedLines: res.Skipped, UnmappedTags: sortedKeys(unmapped),
	}
}

func carryForward(doc *model.Document, index map[string]int, id string, cause error, prev *model.Document, now time.Time) model.CollectorStatus {
	status := model.CollectorStatus{Status: "failed", FetchedAt: now, Error: cause.Error()}
	if prev == nil {
		return status
	}
	prevStatus, ok := prev.Collection.Collectors[id]
	if !ok || prevStatus.LastSuccessAt == nil {
		return status
	}
	items := 0
	for _, svc := range prev.Services {
		i, ok := index[svc.Key]
		if !ok {
			continue
		}
		for _, r := range svc.Evidence.IPRanges {
			if r.Collector != id {
				continue
			}
			doc.Services[i].Evidence.IPRanges = append(doc.Services[i].Evidence.IPRanges, r)
			items++
		}
	}
	status.Status = "stale"
	status.SourceURL = prevStatus.SourceURL
	status.SourceVersion = prevStatus.SourceVersion
	status.Items = items
	status.LastSuccessAt = prevStatus.LastSuccessAt
	return status
}

var sourceRank = map[model.Source]int{model.SourceOfficialFeed: 0, model.SourceCurated: 1, model.SourceBGP: 2}

// dedupeRanges keeps one item per CIDR: the one with a region, then the most
// authoritative source, then a stable tie-break on collector and tag.
func dedupeRanges(in []model.IPRange) []model.IPRange {
	groups := map[string][]model.IPRange{}
	for _, r := range in {
		groups[r.CIDR] = append(groups[r.CIDR], r)
	}
	out := make([]model.IPRange, 0, len(groups))
	for _, g := range groups {
		sort.SliceStable(g, func(i, j int) bool {
			a, b := g[i], g[j]
			if (a.Region == "") != (b.Region == "") {
				return a.Region != ""
			}
			if sourceRank[a.Source] != sourceRank[b.Source] {
				return sourceRank[a.Source] < sourceRank[b.Source]
			}
			if a.Collector != b.Collector {
				return a.Collector < b.Collector
			}
			if a.FeedTag != b.FeedTag {
				return a.FeedTag < b.FeedTag
			}
			return a.Region < b.Region
		})
		out = append(out, g[0])
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/build/ && go vet ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/internal/build
git commit -m "feat(provider_recon): build provider documents with tag mapping and stale carry-forward"
```

---

### Task 9: Publish — diff, stores, latest/history/changes

**Files:**
- Create: `internal/publish/diff.go`, `store.go`, `s3.go`, `publish.go`
- Test: `internal/publish/publish_test.go`, `internal/publish/s3_test.go`

**Interfaces:**
- Consumes: `model.Document`, `model.Marshal`, and the `Key()` methods (Task 1).
- Produces (package `provider_recon/internal/publish`):
  - Diff types and function:
    - `type KindDiff struct{ Added, Removed []string; AddedCount, RemovedCount int; Truncated bool }`
    - `type VersionChange struct{ Old, New string }`
    - `type ProviderChange struct{ Slug string; Created bool; OldHash, NewHash string; Evidence map[string]KindDiff; FeedVersions map[string]VersionChange }`
    - `func Diff(old *model.Document, cur model.Document) ProviderChange`
  - Stores:
    - `type Store interface{ Get(ctx, key string) ([]byte, bool, error); Put(ctx, key string, body []byte, contentType string) error }`
    - `type FSStore struct{ Root string }`
    - `type S3Config struct{ Endpoint, AccessKey, SecretKey, Bucket, Region string }`
    - `func S3ConfigFromEnv() (S3Config, error)`
    - `func NewS3Store(ctx, S3Config) (*S3Store, error)`
    - `(*S3Store).EnsureBucket(ctx) error`, `(*S3Store).Delete(ctx, key) error`
  - Publishing:
    - `const RunIDLayout = "20060102T150405Z"`
    - `func LatestKey(slug string) string`, `func HistoryKey(slug string, at time.Time) string`, `func ChangesKey(runID string) string`
    - `func LoadLatest(ctx, store Store, slug string) (*model.Document, error)`
    - `type CollectorIssue struct{ Slug, Collector, Status, Error string }`
    - `type Manifest struct{ RunID string; PublishedAt time.Time; Changed []ProviderChange; Unchanged []string; CollectorIssues []CollectorIssue }`
    - `func Publish(ctx, store Store, docs []model.Document, now time.Time) (Manifest, error)`

- [ ] **Step 1: Add the S3 SDK**

```bash
go get github.com/aws/aws-sdk-go-v2/config@latest github.com/aws/aws-sdk-go-v2/credentials@latest github.com/aws/aws-sdk-go-v2/service/s3@latest github.com/aws/smithy-go@latest
```

- [ ] **Step 2: Write the failing tests** — `internal/publish/publish_test.go`

```go
package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"provider_recon/internal/model"
)

var t0 = time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)

func doc(t *testing.T, cidrs ...string) model.Document {
	t.Helper()
	d := model.Document{Version: model.ContractVersion, Slug: "aws", DisplayName: "AWS", Category: "cloud",
		Services: []model.Service{{Key: "aws.cloudfront", ServiceTypes: []string{"cdn"}}},
		Collection: model.Collection{Collectors: map[string]model.CollectorStatus{
			"aws_ip_ranges": {Status: "ok", SourceVersion: "syncToken=1"},
		}}}
	for _, c := range cidrs {
		d.Services[0].Evidence.IPRanges = append(d.Services[0].Evidence.IPRanges, model.IPRange{CIDR: c, Confidence: 1,
			Provenance: model.Provenance{Source: model.SourceOfficialFeed, Collector: "aws_ip_ranges"}})
	}
	h, err := model.ContentHash(d)
	if err != nil {
		t.Fatal(err)
	}
	d.Collection.ContentHash = h
	return d
}

func historyCount(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "providers", "aws", "history"))
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func TestPublishLifecycle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := FSStore{Root: root}

	m1, err := Publish(ctx, store, []model.Document{doc(t, "52.84.0.0/15")}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(m1.Changed) != 1 || !m1.Changed[0].Created || m1.Changed[0].Evidence["ip_ranges"].AddedCount != 1 {
		t.Fatalf("first run manifest = %+v", m1)
	}
	if len(m1.Changed[0].Evidence["ip_ranges"].Added) != 0 {
		t.Fatal("created providers must list counts only, not every item")
	}
	if _, ok, _ := store.Get(ctx, ChangesKey(m1.RunID)); !ok {
		t.Fatal("changes manifest not written")
	}

	second := doc(t, "52.84.0.0/15")
	second.Collection.CollectedAt = t0.Add(24 * time.Hour)
	m2, err := Publish(ctx, store, []model.Document{second}, t0.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(m2.Changed) != 0 || len(m2.Unchanged) != 1 || historyCount(t, root) != 1 {
		t.Fatalf("unchanged run wrote history or reported change: %+v", m2)
	}
	latest, err := LoadLatest(ctx, store, "aws")
	if err != nil || latest == nil || !latest.Collection.CollectedAt.Equal(t0.Add(24*time.Hour)) {
		t.Fatalf("latest not rewritten on unchanged run: %+v %v", latest, err)
	}

	m3, err := Publish(ctx, store, []model.Document{doc(t, "52.84.0.0/15", "13.32.0.0/15")}, t0.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	d := m3.Changed[0].Evidence["ip_ranges"]
	if len(m3.Changed) != 1 || len(d.Added) != 1 || d.Added[0] != "aws.cloudfront 13.32.0.0/15" || historyCount(t, root) != 2 {
		t.Fatalf("third run manifest = %+v", m3)
	}
}

func TestPublishReportsCollectorIssues(t *testing.T) {
	d := doc(t, "52.84.0.0/15")
	d.Collection.Collectors["aws_ip_ranges"] = model.CollectorStatus{Status: "stale", Error: "status 503"}
	m, err := Publish(context.Background(), FSStore{Root: t.TempDir()}, []model.Document{d}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.CollectorIssues) != 1 || m.CollectorIssues[0].Status != "stale" || m.CollectorIssues[0].Slug != "aws" {
		t.Fatalf("issues = %+v", m.CollectorIssues)
	}
}

func TestDiffTruncatesLongLists(t *testing.T) {
	old := doc(t)
	cidrs := make([]string, 1500)
	for i := range cidrs {
		cidrs[i] = fmt.Sprintf("10.%d.%d.0/24", i/256, i%256)
	}
	ch := Diff(&old, doc(t, cidrs...))
	d := ch.Evidence["ip_ranges"]
	if d.AddedCount != 1500 || len(d.Added) != maxListed || !d.Truncated {
		t.Fatalf("count=%d listed=%d truncated=%v", d.AddedCount, len(d.Added), d.Truncated)
	}
}

func TestDiffFeedVersions(t *testing.T) {
	old := doc(t, "52.84.0.0/15")
	cur := doc(t, "52.84.0.0/15")
	cur.Collection.Collectors["aws_ip_ranges"] = model.CollectorStatus{Status: "ok", SourceVersion: "syncToken=2"}
	ch := Diff(&old, cur)
	if v := ch.FeedVersions["aws_ip_ranges"]; v.Old != "syncToken=1" || v.New != "syncToken=2" {
		t.Fatalf("feed versions = %+v", ch.FeedVersions)
	}
	b, _ := json.Marshal(ch)
	if len(b) == 0 {
		t.Fatal("change does not marshal")
	}
}
```

`internal/publish/s3_test.go`:
```go
package publish

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestS3StoreRoundTrip runs against the real object store only when
// PROVIDER_RECON_S3_IT=1 and CORPSCOUT_S3_* are set. It uses bucket
// provider-recon-it and removes what it writes.
func TestS3StoreRoundTrip(t *testing.T) {
	if os.Getenv("PROVIDER_RECON_S3_IT") != "1" {
		t.Skip("set PROVIDER_RECON_S3_IT=1 to run against the object store")
	}
	ctx := context.Background()
	cfg, err := S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bucket = "provider-recon-it"
	s, err := NewS3Store(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket not idempotent: %v", err)
	}
	key := fmt.Sprintf("it/%d/x.json", time.Now().UnixNano())
	defer s.Delete(ctx, key)

	if _, ok, err := s.Get(ctx, key); err != nil || ok {
		t.Fatalf("missing key: ok=%v err=%v", ok, err)
	}
	if err := s.Put(ctx, key, []byte(`{"a":1}`), "application/json"); err != nil {
		t.Fatal(err)
	}
	b, ok, err := s.Get(ctx, key)
	if err != nil || !ok || string(b) != `{"a":1}` {
		t.Fatalf("get: %q %v %v", b, ok, err)
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/publish/`
Expected: FAIL (`undefined: Publish` …).

- [ ] **Step 4: Implement**

`internal/publish/diff.go`:
```go
package publish

import (
	"sort"

	"provider_recon/internal/model"
)

// maxListed caps the item lists in a change; counts stay exact.
const maxListed = 1000

// KindDiff is the change of one evidence kind.
type KindDiff struct {
	Added        []string `json:"added,omitempty"`
	Removed      []string `json:"removed,omitempty"`
	AddedCount   int      `json:"added_count"`
	RemovedCount int      `json:"removed_count"`
	Truncated    bool     `json:"truncated,omitempty"`
}

// VersionChange is a feed version before and after the run.
type VersionChange struct {
	Old string `json:"old"`
	New string `json:"new"`
}

// ProviderChange describes what changed for one provider in a run.
type ProviderChange struct {
	Slug         string                   `json:"slug"`
	Created      bool                     `json:"created,omitempty"`
	OldHash      string                   `json:"old_hash,omitempty"`
	NewHash      string                   `json:"new_hash"`
	Evidence     map[string]KindDiff      `json:"evidence,omitempty"`
	FeedVersions map[string]VersionChange `json:"feed_versions,omitempty"`
}

var kinds = []string{"aliases", "asns", "certificate_identities", "dns_rules", "http_rules", "ip_ranges", "provider_keys", "ptr_rules", "services"}

// Diff compares cur with the previous document (nil for a new provider). A
// new provider reports counts only; listing every item of a first run is noise.
func Diff(old *model.Document, cur model.Document) ProviderChange {
	ch := ProviderChange{Slug: cur.Slug, NewHash: cur.Collection.ContentHash, Created: old == nil,
		Evidence: map[string]KindDiff{}, FeedVersions: map[string]VersionChange{}}
	if old != nil {
		ch.OldHash = old.Collection.ContentHash
	}
	before, after := itemSets(old), itemSets(&cur)
	for _, kind := range kinds {
		d := diffSets(before[kind], after[kind], !ch.Created)
		if d.AddedCount+d.RemovedCount > 0 {
			ch.Evidence[kind] = d
		}
	}
	for id, st := range cur.Collection.Collectors {
		prev := ""
		if old != nil {
			prev = old.Collection.Collectors[id].SourceVersion
		}
		if prev != st.SourceVersion {
			ch.FeedVersions[id] = VersionChange{Old: prev, New: st.SourceVersion}
		}
	}
	return ch
}

func itemSets(doc *model.Document) map[string]map[string]bool {
	sets := map[string]map[string]bool{}
	for _, k := range kinds {
		sets[k] = map[string]bool{}
	}
	if doc == nil {
		return sets
	}
	for _, a := range doc.Aliases {
		sets["aliases"][a] = true
	}
	for _, k := range doc.ProviderKeys {
		sets["provider_keys"][k] = true
	}
	for _, s := range doc.Services {
		sets["services"][s.Key] = true
		e := s.Evidence
		for _, x := range e.IPRanges {
			sets["ip_ranges"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.ASNs {
			sets["asns"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.DNSRules {
			sets["dns_rules"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.HTTPRules {
			sets["http_rules"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.PTRRules {
			sets["ptr_rules"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.CertificateIdentities {
			sets["certificate_identities"][s.Key+" "+x.Key()] = true
		}
	}
	return sets
}

func diffSets(before, after map[string]bool, list bool) KindDiff {
	var d KindDiff
	var added, removed []string
	for k := range after {
		if !before[k] {
			added = append(added, k)
		}
	}
	for k := range before {
		if !after[k] {
			removed = append(removed, k)
		}
	}
	d.AddedCount, d.RemovedCount = len(added), len(removed)
	if !list {
		return d
	}
	sort.Strings(added)
	sort.Strings(removed)
	if len(added) > maxListed {
		added, d.Truncated = added[:maxListed], true
	}
	if len(removed) > maxListed {
		removed, d.Truncated = removed[:maxListed], true
	}
	d.Added, d.Removed = added, removed
	return d
}
```

`internal/publish/store.go`:
```go
// Package publish writes provider documents, their history and per-run change
// manifests to an object store.
package publish

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Store is the minimal object store publish needs.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Put(ctx context.Context, key string, body []byte, contentType string) error
}

// FSStore stores objects as files under Root (tests and local dry runs).
type FSStore struct{ Root string }

// Get implements Store.
func (s FSStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	b, err := os.ReadFile(filepath.Join(s.Root, filepath.FromSlash(key)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// Put implements Store.
func (s FSStore) Put(_ context.Context, key string, body []byte, _ string) error {
	p := filepath.Join(s.Root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, body, 0o644)
}
```

`internal/publish/s3.go`:
```go
package publish

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// DefaultBucket holds provider-recon output unless PROVIDER_RECON_BUCKET overrides it.
const DefaultBucket = "provider-recon"

// S3Config addresses the corpscout object store (RustFS, S3 API).
type S3Config struct {
	Endpoint, AccessKey, SecretKey, Bucket, Region string
}

// S3ConfigFromEnv reads the shared CORPSCOUT_S3_* variables.
func S3ConfigFromEnv() (S3Config, error) {
	c := S3Config{
		Endpoint:  os.Getenv("CORPSCOUT_S3_ENDPOINT"),
		AccessKey: os.Getenv("CORPSCOUT_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("CORPSCOUT_S3_SECRET_KEY"),
		Bucket:    os.Getenv("PROVIDER_RECON_BUCKET"),
		Region:    "us-east-1",
	}
	if c.Bucket == "" {
		c.Bucket = DefaultBucket
	}
	if c.Endpoint == "" || c.AccessKey == "" || c.SecretKey == "" {
		return c, errors.New("CORPSCOUT_S3_ENDPOINT, CORPSCOUT_S3_ACCESS_KEY and CORPSCOUT_S3_SECRET_KEY must be set")
	}
	return c, nil
}

// S3Store implements Store on one bucket.
type S3Store struct {
	client *s3.Client
	bucket string
}

// NewS3Store builds a path-style client for the S3-compatible endpoint.
func NewS3Store(ctx context.Context, c S3Config) (*S3Store, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(c.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")),
	)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(c.Endpoint)
		o.UsePathStyle = true
		// S3-compatible servers can reject the SDK's default trailing checksums.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &S3Store{client: client, bucket: c.Bucket}, nil
}

// EnsureBucket creates the bucket when missing.
func (s *S3Store) EnsureBucket(ctx context.Context) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(s.bucket)})
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" || apiErr.ErrorCode() == "BucketAlreadyExists") {
		return nil
	}
	return fmt.Errorf("create bucket %s: %w", s.bucket, err)
}

// Get implements Store.
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, bool, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get s3://%s/%s: %w", s.bucket, key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// Put implements Store.
func (s *S3Store) Put(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("put s3://%s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// Delete removes one object (used by the integration test).
func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	return err
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == 404
}
```

`internal/publish/publish.go`:
```go
package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"provider_recon/internal/model"
)

// RunIDLayout formats run ids and history object names (UTC).
const RunIDLayout = "20060102T150405Z"

// LatestKey is the current document of a provider.
func LatestKey(slug string) string { return "providers/" + slug + "/latest.json" }

// HistoryKey is the copy written when a provider's content hash changes.
func HistoryKey(slug string, at time.Time) string {
	return "providers/" + slug + "/history/" + at.UTC().Format(RunIDLayout) + ".json"
}

// ChangesKey is the per-run change manifest.
func ChangesKey(runID string) string { return "changes/" + runID + ".json" }

// CollectorIssue is a collector that was not ok for a provider.
type CollectorIssue struct {
	Slug      string `json:"slug"`
	Collector string `json:"collector"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// Manifest is written to changes/<run_id>.json on every run.
type Manifest struct {
	RunID           string           `json:"run_id"`
	PublishedAt     time.Time        `json:"published_at"`
	Changed         []ProviderChange `json:"changed"`
	Unchanged       []string         `json:"unchanged"`
	CollectorIssues []CollectorIssue `json:"collector_issues"`
}

// LoadLatest returns the provider's latest document, or nil when none exists.
func LoadLatest(ctx context.Context, store Store, slug string) (*model.Document, error) {
	b, ok, err := store.Get(ctx, LatestKey(slug))
	if err != nil || !ok {
		return nil, err
	}
	var d model.Document
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("decode %s: %w", LatestKey(slug), err)
	}
	return &d, nil
}

// Publish writes history (only on a content-hash change) and then latest
// (always) for every document, then the run's change manifest. History is
// written before latest, so an interrupted run is re-detected as a change next
// time rather than lost.
func Publish(ctx context.Context, store Store, docs []model.Document, now time.Time) (Manifest, error) {
	m := Manifest{RunID: now.UTC().Format(RunIDLayout), PublishedAt: now.UTC(),
		Changed: []ProviderChange{}, Unchanged: []string{}, CollectorIssues: []CollectorIssue{}}
	sorted := append([]model.Document(nil), docs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Slug < sorted[j].Slug })
	for _, doc := range sorted {
		prev, err := LoadLatest(ctx, store, doc.Slug)
		if err != nil {
			return m, err
		}
		body, err := model.Marshal(doc)
		if err != nil {
			return m, err
		}
		if prev == nil || prev.Collection.ContentHash != doc.Collection.ContentHash {
			m.Changed = append(m.Changed, Diff(prev, doc))
			if err := store.Put(ctx, HistoryKey(doc.Slug, now), body, "application/json"); err != nil {
				return m, err
			}
		} else {
			m.Unchanged = append(m.Unchanged, doc.Slug)
		}
		if err := store.Put(ctx, LatestKey(doc.Slug), body, "application/json"); err != nil {
			return m, err
		}
		ids := make([]string, 0, len(doc.Collection.Collectors))
		for id := range doc.Collection.Collectors {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if st := doc.Collection.Collectors[id]; st.Status != "ok" {
				m.CollectorIssues = append(m.CollectorIssues, CollectorIssue{Slug: doc.Slug, Collector: id, Status: st.Status, Error: st.Error})
			}
		}
	}
	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return m, err
	}
	return m, store.Put(ctx, ChangesKey(m.RunID), append(mb, '\n'), "application/json")
}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/publish/ && go vet ./...`
Expected: PASS. The S3 test is SKIPPED here; it runs in Task 11.

- [ ] **Step 6: Commit**

```bash
git add services/provider_recon/go.mod services/provider_recon/go.sum services/provider_recon/internal/publish
git commit -m "feat(provider_recon): publish latest, history and change manifests to fs or S3"
```

---

### Task 10: The `provider-recon` CLI

**Files:**
- Create: `cmd/provider-recon/main.go`
- Test: `cmd/provider-recon/main_test.go`

**Interfaces:**
- Consumes: `definitions.LoadDir`, `definitions.Validate`, `definitions.Schema`, `feeds.Registry`, `feeds.NewFetcher`, `build.Build`, `publish.*` (Tasks 3–9).
- Produces commands:
  - `provider-recon validate [-definitions dir]`
  - `provider-recon schema [-o path]`
  - `provider-recon collect [-definitions dir] [-provider slug] [-out dir] [-concurrency n]`

  Exit codes:
  - 0: ok
  - 1: error (invalid definitions, store failure)
  - 2: published, but at least one collector was not ok
  - 64: usage

  The package variable `registry = feeds.Registry` can be overridden in tests.

- [ ] **Step 1: Write the failing tests** — `cmd/provider-recon/main_test.go`

```go
package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"provider_recon/internal/feeds"
)

const awsYAML = `slug: aws
display_name: Amazon Web Services
category: cloud
services:
  - key: aws.cloudfront
    display_name: CloudFront
    service_types: [cdn]
feeds:
  - collector: aws_ip_ranges
    tag_map: {CLOUDFRONT: aws.cloudfront}
`

func withAWSServer(t *testing.T, body string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
	t.Cleanup(srv.Close)
	orig := registry
	registry = func() map[string]feeds.Collector {
		return map[string]feeds.Collector{"aws_ip_ranges": &feeds.AWS{URL: srv.URL}}
	}
	t.Cleanup(func() { registry = orig })
}

func defsDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestCollectTwiceIsIdempotent(t *testing.T) {
	withAWSServer(t, `{"syncToken":"1","prefixes":[{"ip_prefix":"52.84.0.0/15","region":"GLOBAL","service":"CLOUDFRONT"}],"ipv6_prefixes":[]}`)
	defs := defsDir(t, map[string]string{"aws.yaml": awsYAML})
	out := t.TempDir()

	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("first collect exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "1 changed, 0 unchanged") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	stdout.Reset()
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("second collect exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "0 changed, 1 unchanged") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	if _, err := os.Stat(filepath.Join(out, "providers", "aws", "latest.json")); err != nil {
		t.Fatal(err)
	}
}

func TestCollectExitsTwoWhenACollectorFails(t *testing.T) {
	withAWSServer(t, `<html>down</html>`)
	defs := defsDir(t, map[string]string{"aws.yaml": awsYAML})
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", t.TempDir()}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stdout.String(), "issue aws aws_ip_ranges failed") {
		t.Fatalf("exit %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

func TestCollectRefusesInvalidDefinitionsAndWritesNothing(t *testing.T) {
	withAWSServer(t, `{}`)
	defs := defsDir(t, map[string]string{"aws.yaml": strings.Replace(awsYAML, "[cdn]", "[cnd]", 1)})
	out := t.TempDir()
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-out", out}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(stderr.String(), `unknown service type "cnd"`) {
		t.Fatalf("stderr = %s", stderr.String())
	}
	if entries, _ := os.ReadDir(out); len(entries) != 0 {
		t.Fatalf("collect wrote %d entries despite invalid definitions", len(entries))
	}
}

func TestCollectUnknownProvider(t *testing.T) {
	withAWSServer(t, `{}`)
	defs := defsDir(t, map[string]string{"aws.yaml": awsYAML})
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"collect", "-definitions", defs, "-provider", "nope", "-out", t.TempDir()}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d", code)
	}
}

func TestUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), nil, &stdout, &stderr); code != 64 {
		t.Fatalf("exit %d", code)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/provider-recon/`
Expected: FAIL (`undefined: run`).

- [ ] **Step 3: Implement** — `cmd/provider-recon/main.go`

```go
// Command provider-recon collects provider evidence (official IP-range feeds,
// BGP announcements, curated rules) into provider-recon/v1 documents.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"provider_recon/internal/build"
	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
	"provider_recon/internal/publish"
)

// registry is overridden in tests.
var registry = feeds.Registry

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 64
	}
	switch args[0] {
	case "validate":
		return cmdValidate(args[1:], stdout, stderr)
	case "schema":
		return cmdSchema(args[1:], stderr)
	case "collect":
		return cmdCollect(ctx, args[1:], stdout, stderr)
	default:
		usage(stderr)
		return 64
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: provider-recon validate|schema|collect [flags]")
}

func loadDefinitions(dir string) ([]definitions.Definition, error) {
	defs, err := definitions.LoadDir(dir)
	if err != nil {
		return nil, err
	}
	validators := map[string]definitions.ParamValidator{}
	for name, c := range registry() {
		validators[name] = c
	}
	if err := definitions.Validate(defs, validators); err != nil {
		return nil, err
	}
	return defs, nil
}

func cmdValidate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("definitions", "definitions", "definitions directory")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	defs, err := loadDefinitions(*dir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "%d definitions valid\n", len(defs))
	return 0
}

func cmdSchema(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("schema", flag.ContinueOnError)
	fs.SetOutput(stderr)
	out := fs.String("o", "definitions/schema.json", "output path")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	b, err := definitions.Schema()
	if err == nil {
		err = os.WriteFile(*out, b, 0o644)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func cmdCollect(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("collect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("definitions", "definitions", "definitions directory")
	only := fs.String("provider", "", "collect a single provider slug")
	outDir := fs.String("out", "", "write to this local directory instead of S3")
	concurrency := fs.Int("concurrency", 4, "collectors run in parallel")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	defs, err := loadDefinitions(*dir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if *only != "" {
		var picked []definitions.Definition
		for _, d := range defs {
			if d.Slug == *only {
				picked = append(picked, d)
			}
		}
		if len(picked) == 0 {
			fmt.Fprintf(stderr, "no definition with slug %q\n", *only)
			return 1
		}
		defs = picked
	}

	store, err := openStore(ctx, *outDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	now := time.Now().UTC().Truncate(time.Second)
	outcomes := collectAll(ctx, uniqueFeeds(defs), registry(), feeds.NewFetcher(), *concurrency, logger)

	docs := make([]model.Document, 0, len(defs))
	for _, d := range defs {
		prev, err := publish.LoadLatest(ctx, store, d.Slug)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		doc, err := build.Build(d, outcomes, prev, now)
		if err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", d.Slug, err)
			return 1
		}
		docs = append(docs, doc)
	}

	m, err := publish.Publish(ctx, store, docs, now)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "run %s: %d changed, %d unchanged, %d collector issues\n",
		m.RunID, len(m.Changed), len(m.Unchanged), len(m.CollectorIssues))
	for _, c := range m.Changed {
		fmt.Fprintf(stdout, "changed %s %s\n", c.Slug, c.NewHash)
	}
	for _, i := range m.CollectorIssues {
		fmt.Fprintf(stdout, "issue %s %s %s: %s\n", i.Slug, i.Collector, i.Status, i.Error)
	}
	if len(m.CollectorIssues) > 0 {
		return 2
	}
	return 0
}

func openStore(ctx context.Context, outDir string) (publish.Store, error) {
	if outDir != "" {
		return publish.FSStore{Root: outDir}, nil
	}
	cfg, err := publish.S3ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	s, err := publish.NewS3Store(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return s, s.EnsureBucket(ctx)
}

// uniqueFeeds returns each feed reference once (by ID), sorted by ID.
func uniqueFeeds(defs []definitions.Definition) []definitions.FeedRef {
	seen := map[string]definitions.FeedRef{}
	for _, d := range defs {
		for _, f := range d.Feeds {
			seen[f.ID()] = f
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]definitions.FeedRef, len(ids))
	for i, id := range ids {
		out[i] = seen[id]
	}
	return out
}

func collectAll(ctx context.Context, refs []definitions.FeedRef, collectors map[string]feeds.Collector, f *feeds.Fetcher, limit int, logger *slog.Logger) map[string]build.FeedOutcome {
	out := make(map[string]build.FeedOutcome, len(refs))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(limit, 1))
	for _, ref := range refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			start := time.Now()
			res, err := collectors[ref.Collector].Collect(ctx, f, ref.Params)
			if err != nil {
				logger.Warn("collector failed", "collector", ref.ID(), "err", err)
			} else {
				logger.Info("collected", "collector", ref.ID(), "ranges", len(res.Ranges), "skipped", res.Skipped,
					"version", res.SourceVersion, "took", time.Since(start).Round(time.Millisecond))
			}
			mu.Lock()
			out[ref.ID()] = build.FeedOutcome{Result: res, Err: err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./... && go vet ./... && make build`
Expected: PASS; `bin/provider-recon` exists.

- [ ] **Step 5: Commit**

```bash
git add services/provider_recon/cmd
git commit -m "feat(provider_recon): provider-recon CLI with validate, schema and collect"
```

---

### Task 11: First-slice definitions, a real run, and the README

**Files:**
- Create: `services/provider_recon/definitions/*.yaml` (the list below)
- Create: `services/provider_recon/README.md`
- Modify: `cmd/provider-recon/main_test.go` (add `TestRepoDefinitionsValidate`)

**Interfaces:**
- Consumes: the whole CLI (Task 10).
- Produces: committed definitions for the first slice, and the `provider-recon` bucket populated on the corpscout object store.

Every YAML file starts with `# yaml-language-server: $schema=./schema.json`. NS/MX patterns for Nordic hosters come from a 1/16 sample of `.se` apex records in `corpscout.commoncrawl_domain_dns_records` (2026-09-27): NS and MX registrable domains ranked by domain count. **ASNs for the Nordic hosters are deliberately left out** until verified; that's the miner's job in a later slice.

- [ ] **Step 1: Write the failing test**

Append to `cmd/provider-recon/main_test.go`:
```go
func TestRepoDefinitionsValidate(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"validate", "-definitions", "../../definitions"}, &stdout, &stderr); code != 0 {
		t.Fatalf("repo definitions invalid:\n%s", stderr.String())
	}
}
```
Run: `go test ./cmd/provider-recon/ -run TestRepoDefinitionsValidate`
Expected: FAIL (`no *.yaml definitions`).

- [ ] **Step 2: Write the cloud and CDN definitions**

`definitions/aws.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: aws
display_name: Amazon Web Services
category: cloud
website: https://aws.amazon.com
country: US
aliases: ["AWS", "Amazon Web Services", "Amazon.com, Inc."]
provider_keys: ["amazonaws.com", "amazonses.com", "cloudfront.net", "awsdns-*", "awsglobalaccelerator.com", "elasticbeanstalk.com"]
services:
  - key: aws.cloudfront
    display_name: Amazon CloudFront
    service_types: [cdn]
    traits: [shared_infrastructure, origin_obscured]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "cloudfront.net"}
    http_rules:
      - {http_part: header, header_name: x-amz-cf-id, matcher_type: exists}
    ptr_rules:
      - {matcher_type: suffix, pattern: "cloudfront.net", confidence: 0.9}
  - key: aws.global-accelerator
    display_name: AWS Global Accelerator
    service_types: [cdn]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "awsglobalaccelerator.com"}
  - key: aws.ec2
    display_name: Amazon EC2
    service_types: [iaas]
    traits: [cloud_provider_ip]
    ptr_rules:
      - {matcher_type: suffix, pattern: "compute.amazonaws.com", confidence: 0.9}
  - key: aws.s3
    display_name: Amazon S3
    service_types: [hosting]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: regex, pattern: '(^|\.)s3([.-][a-z0-9-]+)*\.amazonaws\.com$'}
  - key: aws.api-gateway
    display_name: Amazon API Gateway
    service_types: [paas]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "execute-api.amazonaws.com"}
  - key: aws.elastic-beanstalk
    display_name: AWS Elastic Beanstalk
    service_types: [paas]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "elasticbeanstalk.com"}
  - key: aws.route53
    display_name: Amazon Route 53
    service_types: [dns]
    dns_rules:
      # NS hosts look like ns-123.awsdns-45.co.uk
      - {record_type: NS, match_field: target, matcher_type: regex, pattern: '^ns-[0-9]+\.awsdns-[0-9]+\.(com|net|org|co\.uk)$'}
  - key: aws.ses
    display_name: Amazon SES
    service_types: [email_sending]
    dns_rules:
      - {record_type: TXT, match_field: value, matcher_type: contains, pattern: "include:amazonses.com"}
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "dkim.amazonses.com"}
      - {record_type: TXT, match_field: name, matcher_type: prefix, pattern: "_amazonses.", confidence: 0.9}
  - key: aws.other
    display_name: Other AWS address space
    service_types: [iaas]
    traits: [cloud_provider_ip]
feeds:
  - collector: aws_ip_ranges
    tag_map:
      CLOUDFRONT: aws.cloudfront
      GLOBALACCELERATOR: aws.global-accelerator
      EC2: aws.ec2
      S3: aws.s3
      API_GATEWAY: aws.api-gateway
      ROUTE53: aws.route53
      # Health checkers and origin-facing CloudFront are AWS→customer traffic, not customer-facing endpoints.
      ROUTE53_HEALTHCHECKS: ""
      ROUTE53_HEALTHCHECKS_PUBLISHING: ""
      CLOUDFRONT_ORIGIN_FACING: ""
      AMAZON: aws.other
      "*": aws.other
    generic_tags: [AMAZON]
```

`definitions/microsoft.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: microsoft
display_name: Microsoft
category: cloud
website: https://www.microsoft.com
country: US
aliases: ["Microsoft", "Microsoft Corporation", "Azure", "Microsoft 365"]
provider_keys: ["outlook.com", "onmicrosoft.com", "microsoftonline.com", "azure-dns.com", "azure-dns.net", "azure-dns.org", "azure-dns.info", "azurewebsites.net", "azurefd.net", "azureedge.net", "azure.com", "windows.net"]
services:
  - key: microsoft.365-mail
    display_name: Microsoft 365 (Exchange Online)
    service_types: [email]
    dns_rules:
      - {record_type: MX, match_field: target, matcher_type: suffix, pattern: "mail.protection.outlook.com"}
  - key: microsoft.365-sending
    display_name: Microsoft 365 outbound mail
    service_types: [email_sending]
    dns_rules:
      - {record_type: TXT, match_field: value, matcher_type: contains, pattern: "include:spf.protection.outlook.com"}
      # DKIM: selector1._domainkey CNAME selector1-<domain>._domainkey.<tenant>.onmicrosoft.com
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "onmicrosoft.com"}
  - key: microsoft.365-verification
    display_name: Microsoft 365 domain verification
    service_types: [saas_verification]
    dns_rules:
      - {record_type: TXT, match_field: value, matcher_type: prefix, pattern: "ms=", confidence: 0.9}
  - key: microsoft.365-dns
    display_name: Microsoft 365 managed DNS
    service_types: [dns]
    dns_rules:
      - {record_type: NS, match_field: target, matcher_type: suffix, pattern: "microsoftonline.com"}
  - key: microsoft.azure-dns
    display_name: Azure DNS
    service_types: [dns]
    dns_rules:
      - {record_type: NS, match_field: target, matcher_type: regex, pattern: '^ns[1-4]-[0-9]+\.azure-dns\.(com|net|org|info)$'}
  - key: microsoft.azure-front-door
    display_name: Azure Front Door / Azure CDN
    service_types: [cdn, waf]
    traits: [shared_infrastructure, origin_obscured]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "azurefd.net"}
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "azureedge.net"}
  - key: microsoft.azure-app-service
    display_name: Azure App Service
    service_types: [paas]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "azurewebsites.net"}
  - key: microsoft.azure-storage
    display_name: Azure Storage static websites
    service_types: [hosting]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "web.core.windows.net"}
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "blob.core.windows.net"}
  - key: microsoft.azure-cloud
    display_name: Azure compute
    service_types: [iaas]
    traits: [cloud_provider_ip]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "cloudapp.azure.com"}
feeds:
  - collector: azure_service_tags
    tag_map:
      "AzureFrontDoor.Frontend": microsoft.azure-front-door
      # Backend/FirstParty are Microsoft→origin traffic, not customer-facing.
      "AzureFrontDoor.*": ""
      "AppService*": microsoft.azure-app-service
      "Storage*": microsoft.azure-storage
      "AzureCloud*": microsoft.azure-cloud
      # ~2,800 other service tags (monitoring, management planes…) are not
      # customer-facing hosting; ignored rather than reported as unmapped.
      "*": ""
    generic_tags: ["AzureCloud*"]
```

`definitions/google.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: google
display_name: Google
category: cloud
website: https://cloud.google.com
country: US
aliases: ["Google", "Google LLC", "Google Cloud", "Google Workspace"]
provider_keys: ["google.com", "googlemail.com", "googledomains.com", "googlehosted.com", "appspot.com", "googleusercontent.com"]
services:
  - key: google.workspace-mail
    display_name: Google Workspace (Gmail)
    service_types: [email]
    dns_rules:
      # aspmx.l.google.com, smtp.google.com, alt*.aspmx.l.google.com
      - {record_type: MX, match_field: target, matcher_type: suffix, pattern: "google.com", confidence: 0.95}
      - {record_type: MX, match_field: target, matcher_type: suffix, pattern: "googlemail.com", confidence: 0.95}
  - key: google.workspace-sending
    display_name: Google Workspace outbound mail
    service_types: [email_sending]
    dns_rules:
      - {record_type: TXT, match_field: value, matcher_type: contains, pattern: "include:_spf.google.com"}
  - key: google.site-verification
    display_name: Google site verification
    service_types: [saas_verification]
    dns_rules:
      - {record_type: TXT, match_field: value, matcher_type: prefix, pattern: "google-site-verification=", confidence: 0.95}
  - key: google.cloud-dns
    display_name: Google Cloud DNS
    service_types: [dns]
    dns_rules:
      - {record_type: NS, match_field: target, matcher_type: regex, pattern: '^ns-cloud-[a-e][1-4]\.googledomains\.com$'}
  - key: google.app-engine
    display_name: Google App Engine / Sites custom domains
    service_types: [paas]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "ghs.googlehosted.com"}
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "appspot.com"}
  - key: google.cloud
    display_name: Google Cloud
    service_types: [iaas]
    traits: [cloud_provider_ip]
    ptr_rules:
      - {matcher_type: suffix, pattern: "bc.googleusercontent.com", confidence: 0.9}
  - key: google.frontend
    display_name: Google front ends (Google-owned address space)
    service_types: [hosting]
    note: "goog.json is a superset of cloud.json; longest-prefix lookup lets cloud ranges win."
feeds:
  - collector: google_cloud
    tag_map: {"Google Cloud": google.cloud}
  - collector: google_goog
    tag_map: {GOOG: google.frontend}
```

`definitions/cloudflare.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: cloudflare
display_name: Cloudflare
category: cdn
website: https://www.cloudflare.com
country: US
aliases: ["Cloudflare", "Cloudflare, Inc."]
provider_keys: ["cloudflare.com", "cloudflare.net"]
services:
  - key: cloudflare.dns
    display_name: Cloudflare DNS
    service_types: [dns]
    dns_rules:
      # <name>.ns.cloudflare.com — top-3 NS key on .se (1,751 domains in a 1/16 sample, 2026-09-27)
      - {record_type: NS, match_field: target, matcher_type: suffix, pattern: "ns.cloudflare.com"}
  - key: cloudflare.edge
    display_name: Cloudflare proxy network
    service_types: [cdn, ddos_protection, waf]
    traits: [shared_infrastructure, origin_obscured]
    asns:
      - {asn: 13335}
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "cdn.cloudflare.net"}
    http_rules:
      - {http_part: header, header_name: cf-ray, matcher_type: exists}
  - key: cloudflare.email-routing
    display_name: Cloudflare Email Routing
    service_types: [email]
    dns_rules:
      - {record_type: MX, match_field: target, matcher_type: suffix, pattern: "mx.cloudflare.net"}
feeds:
  - collector: cloudflare_ips
    tag_map: {CLOUDFLARE: cloudflare.edge}
```

`definitions/fastly.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: fastly
display_name: Fastly
category: cdn
website: https://www.fastly.com
country: US
aliases: ["Fastly", "Fastly, Inc."]
provider_keys: ["fastly.net", "fastlylb.net"]
services:
  - key: fastly.edge
    display_name: Fastly edge cloud
    service_types: [cdn]
    traits: [shared_infrastructure, origin_obscured]
    asns:
      - {asn: 54113}
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "fastly.net"}
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "fastlylb.net"}
    http_rules:
      - {http_part: header, header_name: x-served-by, matcher_type: prefix, pattern: "cache-", confidence: 0.8}
feeds:
  - collector: fastly_public_ips
    tag_map: {FASTLY: fastly.edge}
```

`definitions/akamai.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: akamai
display_name: Akamai
category: cdn
website: https://www.akamai.com
country: US
aliases: ["Akamai", "Akamai Technologies"]
provider_keys: ["akamai.net", "akamaiedge.net", "edgekey.net", "edgesuite.net", "akamaized.net", "akamaihd.net"]
services:
  - key: akamai.edge
    display_name: Akamai edge
    service_types: [cdn]
    traits: [shared_infrastructure, origin_obscured]
    note: "No official range feed. BGP coverage is partial: many edge servers sit inside ISP networks."
    asns:
      - {asn: 20940}
      - {asn: 16625}
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "edgekey.net"}
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "edgesuite.net"}
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "akamaiedge.net"}
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "akamaized.net"}
feeds:
  - collector: ripestat_announced
    params: {asn: "20940"}
    tag_map: {BGP: akamai.edge}
  - collector: ripestat_announced
    params: {asn: "16625"}
    tag_map: {BGP: akamai.edge}
```

`definitions/bunny.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: bunny
display_name: bunny.net
category: cdn
website: https://bunny.net
country: SI
aliases: ["bunny.net", "BunnyCDN", "BunnyWay"]
provider_keys: ["b-cdn.net", "bunny.net", "bunnycdn.com"]
services:
  - key: bunny.edge
    display_name: bunny.net CDN
    service_types: [cdn]
    traits: [shared_infrastructure, origin_obscured]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "b-cdn.net"}
    http_rules:
      - {http_part: header, header_name: server, matcher_type: prefix, pattern: "bunnycdn", confidence: 0.95}
feeds:
  - collector: bunny_edge_servers
    tag_map: {EDGE: bunny.edge}
```

`definitions/oracle.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: oracle
display_name: Oracle Cloud
category: cloud
website: https://www.oracle.com/cloud
country: US
aliases: ["Oracle", "Oracle Cloud Infrastructure", "OCI"]
provider_keys: ["oraclecloud.com"]
services:
  - key: oracle.oci
    display_name: OCI compute
    service_types: [iaas]
    traits: [cloud_provider_ip]
  - key: oracle.object-storage
    display_name: OCI Object Storage
    service_types: [hosting]
  - key: oracle.services-network
    display_name: Oracle Services Network
    service_types: [iaas]
feeds:
  - collector: oracle_public_ip_ranges
    tag_map: {OCI: oracle.oci, OBJECT_STORAGE: oracle.object-storage, OSN: oracle.services-network}
```

`definitions/github.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: github
display_name: GitHub
category: paas
website: https://github.com
country: US
aliases: ["GitHub", "GitHub, Inc."]
provider_keys: ["github.io"]
services:
  - key: github.pages
    display_name: GitHub Pages
    service_types: [hosting]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "github.io"}
feeds:
  - collector: github_meta
    tag_map:
      pages: github.pages
      # hooks, web, api, actions… are GitHub's own egress/infrastructure, not customer hosting.
      "*": ""
```

`definitions/digitalocean.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: digitalocean
display_name: DigitalOcean
category: cloud
website: https://www.digitalocean.com
country: US
aliases: ["DigitalOcean", "DigitalOcean, LLC"]
provider_keys: ["digitalocean.com", "ondigitalocean.app"]
services:
  - key: digitalocean.droplets
    display_name: DigitalOcean Droplets
    service_types: [iaas]
    traits: [cloud_provider_ip]
  - key: digitalocean.dns
    display_name: DigitalOcean DNS
    service_types: [dns]
    dns_rules:
      - {record_type: NS, match_field: target, matcher_type: suffix, pattern: "digitalocean.com"}
  - key: digitalocean.app-platform
    display_name: DigitalOcean App Platform
    service_types: [paas]
    dns_rules:
      - {record_type: CNAME, match_field: target, matcher_type: suffix, pattern: "ondigitalocean.app"}
feeds:
  - collector: geofeed
    params: {url: "https://digitalocean.com/geo/google.csv"}
    tag_map: {GEOFEED: digitalocean.droplets}
```

`definitions/linode.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: linode
display_name: Akamai Cloud (Linode)
category: cloud
website: https://www.linode.com
country: US
aliases: ["Linode", "Akamai Connected Cloud"]
provider_keys: ["linode.com"]
services:
  - key: linode.compute
    display_name: Linode compute
    service_types: [iaas]
    traits: [cloud_provider_ip]
  - key: linode.dns
    display_name: Linode DNS
    service_types: [dns]
    dns_rules:
      - {record_type: NS, match_field: target, matcher_type: suffix, pattern: "linode.com"}
feeds:
  - collector: geofeed
    params: {url: "https://geoip.linode.com/"}
    tag_map: {GEOFEED: linode.compute}
```

`definitions/hetzner.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: hetzner
display_name: Hetzner
category: hosting
website: https://www.hetzner.com
country: DE
aliases: ["Hetzner", "Hetzner Online GmbH"]
provider_keys: ["hetzner.com", "hetzner.de", "your-server.de"]
services:
  - key: hetzner.cloud
    display_name: Hetzner servers and cloud
    service_types: [hosting, iaas]
    traits: [cloud_provider_ip]
    asns:
      - {asn: 24940}
    ptr_rules:
      - {matcher_type: suffix, pattern: "your-server.de", confidence: 0.9}
  - key: hetzner.dns
    display_name: Hetzner DNS
    service_types: [dns]
    dns_rules:
      - {record_type: NS, match_field: target, matcher_type: suffix, pattern: "ns.hetzner.com"}
      - {record_type: NS, match_field: target, matcher_type: suffix, pattern: "ns.hetzner.de"}
      - {record_type: NS, match_field: target, matcher_type: suffix, pattern: "your-server.de"}
feeds:
  - collector: ripestat_announced
    params: {asn: "24940"}
    tag_map: {BGP: hetzner.cloud}
```

`definitions/ovh.yaml`:
```yaml
# yaml-language-server: $schema=./schema.json
slug: ovh
display_name: OVHcloud
category: hosting
website: https://www.ovhcloud.com
country: FR
aliases: ["OVH", "OVHcloud", "OVH SAS"]
provider_keys: ["ovh.net", "ovh.com"]
services:
  - key: ovh.hosting
    display_name: OVHcloud servers and hosting
    service_types: [hosting, iaas]
    traits: [cloud_provider_ip]
    asns:
      - {asn: 16276}
  - key: ovh.dns
    display_name: OVHcloud DNS
    service_types: [dns]
    dns_rules:
      - {record_type: NS, match_field: target, matcher_type: suffix, pattern: "ovh.net"}
  - key: ovh.email
    display_name: OVHcloud email
    service_types: [email]
    dns_rules:
      - {record_type: MX, match_field: target, matcher_type: suffix, pattern: "mail.ovh.net"}
feeds:
  - collector: ripestat_announced
    params: {asn: "16276"}
    tag_map: {BGP: ovh.hosting}
```

- [ ] **Step 3: Write the Nordic and registrar DNS/email definitions**

Each file follows this exact template. `<dns rules>` and `<mx rules>` come from the table's NS/MX columns; omit a service when its column is `—`.

```yaml
# yaml-language-server: $schema=./schema.json
slug: <slug>
display_name: <display name>
category: hosting
country: <country>
aliases: ["<display name>"]
provider_keys: [<keys>]
services:
  - key: <slug>.dns
    display_name: <display name> DNS
    service_types: [dns]
    dns_rules:
      - {record_type: NS, match_field: target, matcher_type: suffix, pattern: "<ns suffix>"}   # one line per NS suffix
  - key: <slug>.email
    display_name: <display name> email
    service_types: [email]
    dns_rules:
      - {record_type: MX, match_field: target, matcher_type: suffix, pattern: "<mx suffix>"}   # one line per MX suffix
```

| file / slug | display name | country | provider_keys | NS suffixes | MX suffixes | .se sample (NS / MX domains) |
|---|---|---|---|---|---|---|
| `one-com` | One.com | DK | `one.com` | `one.com` | `one.com` | 4,967 / 4,200 |
| `loopia` | Loopia | SE | `loopia.se`, `loopiagroup.com` | `loopia.se`, `loopiagroup.com` | `loopia.se` | 4,743+1,353 / 2,203 |
| `oderland` | Oderland | SE | `oderland.com` | `oderland.com` | `oderland.com` | 948 / 723 |
| `simply` | Simply.com | DK | `simply.com` | `simply.com` | `simply.com` | 783 / 507 |
| `glesys` | GleSYS | SE | `glesys.se` | — | `glesys.se` | — / 362 |
| `websupport` | Websupport | SE | `websupport.se` | — | `websupport.se` | — / 277 |
| `inleed` | Inleed | SE | `inleed.net` | `inleed.net` | — | 418 / — |
| `ilait` | iLait | SE | `ilait.se` | `ilait.se` | `ilait.se` | 222 / 132 |
| `svenska-domaner` | Svenska Domäner | SE | `svenskadomaner.se` | `svenskadomaner.se` | `svenskadomaner.se` | 128 / 108 |
| `beebyte` | Beebyte | SE | `beebyte.net`, `beebyte.se` | `beebyte.net` | `beebyte.se` | 107 / 54 |
| `miss-hosting` | Miss Hosting | SE | `misshosting.com` | — | `misshosting.com` | — / 105 |
| `wix` | Wix | IL | `wixdns.net` | `wixdns.net` | — | 458 / — |
| `godaddy` | GoDaddy | US | `domaincontrol.com` | `domaincontrol.com` | — | 297 / — |
| `hostinger` | Hostinger | LT | `hostinger.com`, `dns-parking.com` | `dns-parking.com` | `hostinger.com` | 143 / 73 |

For `wix` and `godaddy`, set `category: dns`.

Left unmapped on purpose, because their operators aren't identified yet: `misspark.com`, `hyp.net`, `namesystem.se`, `ports.se`, `ports.net`, `dipcon.com`, `dnshost.net`, `nameshift.com`, `manufrog.com`, `rzone.de`, `register.it`. They surface in the unmapped-provider review list of the domain-services work.

- [ ] **Step 4: Write the verification-token providers** (seed patterns ported from runner3 `devsnapshot`)

Template:
```yaml
# yaml-language-server: $schema=./schema.json
slug: <slug>
display_name: <display name>
category: <category>
country: US
services:
  - key: <slug>.domain-verification
    display_name: <display name> domain verification
    service_types: [saas_verification]
    dns_rules:
      - {record_type: TXT, match_field: value, matcher_type: prefix, pattern: "<prefix>", confidence: <confidence>}
```

| slug | display name | category | TXT prefix | confidence |
|---|---|---|---|---|
| `meta` | Meta | saas | `facebook-domain-verification=` | 0.9 |
| `apple` | Apple | saas | `apple-domain-verification=` | 0.9 |
| `atlassian` | Atlassian | saas | `atlassian-domain-verification=` | 0.9 |
| `adobe` | Adobe | saas | `adobe-idp-site-verification=` | 0.9 |
| `dropbox` | Dropbox | saas | `dropbox-domain-verification=` | 0.9 |
| `mongodb` | MongoDB | saas | `mongodb-site-verification=` | 0.9 |
| `stripe` | Stripe | saas | `stripe-verification=` | 0.85 |
| `docker` | Docker | saas | `docker-verification=` | 0.85 |
| `globalsign` | GlobalSign | security | `globalsign-domain-verification=` | 0.85 |
| `have-i-been-pwned` | Have I Been Pwned | security | `have-i-been-pwned-verification=` | 0.85 |

For `globalsign`, set `country: BE`. For `have-i-been-pwned`, set `country: AU`.

- [ ] **Step 5: Validate**

Run: `go test ./cmd/provider-recon/ -run TestRepoDefinitionsValidate && go run ./cmd/provider-recon validate`
Expected: PASS, and `37 definitions valid` (13 cloud/CDN/host files from Step 2 + 14 from Step 3 + 10 from Step 4).

- [ ] **Step 6: Real run to a local directory, twice**

```bash
OUT=$(mktemp -d)
go run ./cmd/provider-recon collect -out "$OUT"; echo "exit=$?"
go run ./cmd/provider-recon collect -out "$OUT"; echo "exit=$?"
ls "$OUT"/providers/aws/history | wc -l
jq '.collection.collectors | map_values({status, items, unmapped_tags})' "$OUT"/providers/aws/latest.json
jq '[.services[] | {k: .service_key, n: (.evidence.ip_ranges | length)}]' "$OUT"/providers/microsoft/latest.json
```
Expected:
- First run: exit 0, all providers `changed`.
- Second run: `0 changed` (unless a feed truly changed in between), and one history file for aws.
- Every collector status is `ok`.
- AWS `unmapped_tags` is empty or null, because of the `"*"` catch-all.
- Microsoft services carry ranges.
- `aws.cloudfront` has ~200+ ranges; `microsoft.azure-cloud` has thousands.

If a collector is not `ok`, fix it before continuing.

- [ ] **Step 7: Real run to S3, plus the S3 integration test**

```bash
set -a; source ../backoffice/.env; set +a
PROVIDER_RECON_S3_IT=1 go test ./internal/publish/ -run TestS3StoreRoundTrip -v
go run ./cmd/provider-recon collect; echo "exit=$?"
go run ./cmd/provider-recon collect; echo "exit=$?"
```
Expected:
- The integration test passes.
- First collect reports every provider changed; second collect reports `0 changed`.
- Both exit 0.

If the round trip fails with a checksum/`XAmzContentSHA256Mismatch`-style error, the RustFS build needs the checksum options set in `NewS3Store`: confirm they are there. Report the exact error instead of working around it.

- [ ] **Step 8: README** — `services/provider_recon/README.md`

````markdown
# provider_recon

Collects service-provider evidence and publishes one `provider-recon/v1` JSON
document per provider. Sources:
- official IP-range feeds
- BGP announcements via RIPEstat
- hand-written DNS/HTTP/PTR rules

Design: `docs/superpowers/specs/2026-09-27-provider-recon-service-design.md`.

## Commands

```bash
make build
bin/provider-recon validate                      # check definitions/*.yaml
bin/provider-recon collect -out /tmp/recon       # dry run to a local directory
bin/provider-recon collect                       # publish to S3
bin/provider-recon collect -provider aws         # one provider
bin/provider-recon schema                        # regenerate definitions/schema.json
make live                                        # hit every real feed (format-drift check)
```

`collect` exit codes:
- 0: ok
- 1: error (nothing published when the definitions are invalid)
- 2: published, but some collector was stale or failed

## Output (bucket `provider-recon`)

- `providers/<slug>/latest.json`: the current document, rewritten every run.
- `providers/<slug>/history/<YYYYMMDDTHHMMSSZ>.json`: written only when the content hash changes.
- `changes/<run_id>.json`: what changed per provider (added/removed evidence, feed versions) and collector issues.

The content hash covers identity + evidence and ignores sync tokens. A feed
that republishes identical ranges therefore creates no history. A failing
feed keeps the previous ranges (`stale`); the provider is never emptied.

## Environment

`CORPSCOUT_S3_ENDPOINT`, `CORPSCOUT_S3_ACCESS_KEY`, `CORPSCOUT_S3_SECRET_KEY`;
optional `PROVIDER_RECON_BUCKET` (default `provider-recon`).

## Adding a provider

1. Create `definitions/<slug>.yaml`. The editor schema is `definitions/schema.json`.
2. Service keys are `<slug>.<name>`, and service types come from the closed list in `internal/model`.
3. Attach feeds with `tag_map`:
   - exact tags or `*` globs
   - `""` ignores a tag
   - `generic_tags` drops umbrella ranges that repeat a specific tag's CIDR
4. Run `bin/provider-recon validate`, then a local `collect -provider <slug> -out /tmp/x`.
````

- [ ] **Step 9: Commit**

```bash
git add services/provider_recon/definitions services/provider_recon/README.md services/provider_recon/cmd/provider-recon/main_test.go
git commit -m "feat(provider_recon): first-slice provider definitions and README"
```

---

## Deferred (not in this plan)

- **PeeringDB** org → ASN discovery. The first slice uses curated ASNs.
- **Miners** over corpscout data: unmapped NS/MX/CNAME/PTR keys → candidates.
- **AI augmentation**: porting the DSPy agents, with web verification.
- **ClickHouse tables**, the **Dagster** schedule and the auto-update pipeline (stage 3), and wiring into `domain_services` detection.
- ASNs and IP evidence for the Nordic hosters.
