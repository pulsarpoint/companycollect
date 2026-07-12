# Extraction Libraries Adoption — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adopt battle-tested libraries for charset transcoding, main-content text extraction, microdata org profiles, and phone normalization in cc-enrich-worker, per the approved spec `docs/superpowers/specs/2026-07-13-extraction-libraries-design.md`.

**Architecture:** Four independent additions behind small package-level functions (`parse.DecodeHTML`, `parse.MainText`, `extract.ExtractProfileMicrodata`, `extract.NormalizePhone`), wired into the two fetch paths in `internal/worker/worker.go`. Every new path degrades to current behavior on failure and never fails a page. No output schema changes.

**Tech Stack:** Go 1.26; `golang.org/x/net/html/charset` (+ `x/text/transform`), `github.com/markusmobius/go-trafilatura` v1.12.2, `github.com/iand/microdata` v0.0.28, `github.com/nyaruka/phonenumbers` v1.8.0.

## Global Constraints

- Work in `corpscout/commoncrawl/cc-enrich-worker` (module `cc-enrich-worker`); git root is `companycollect/`.
- TDD: write the failing test, watch it fail, implement, watch it pass. One commit per task, Conventional Commits style, ending with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Raw (undecoded) body ALWAYS feeds `tech.DetectTech` — upstream wappalyzergo parity.
- All existing tests must stay green: `go test -race ./...`, `gofmt -l .` empty, `go vet ./...` clean.
- No changes to output row structs / parquet schemas / ClickHouse mapping.

---

### Task 1: Charset transcoding (`parse.DecodeHTML`)

**Files:**
- Create: `internal/parse/decode.go`, `internal/parse/decode_test.go`
- Modify: `internal/worker/worker.go` (`processPage`, industry-stream fetch worker, new `contentTypeOf` helper)
- Modify test: `internal/worker/worker_test.go` (integration test)

**Interfaces:**
- Produces: `parse.DecodeHTML(body []byte, contentType string) ([]byte, string)` — returns UTF-8 body (input unchanged when already UTF-8) and the decided encoding name. Later tasks feed the decoded body to `parse.MainText` and `extract.ExtractProfileMicrodata`.
- Produces: `contentTypeOf(headers map[string][]string) string` (unexported, worker package).

- [ ] **Step 1: Write the failing tests** — `internal/parse/decode_test.go`:

```go
package parse

import (
	"strings"
	"testing"
)

func TestDecodeHTMLMetaHTTPEquivLatin1(t *testing.T) {
	body := []byte("<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=iso-8859-1\"></head><body>M\xfcller GmbH</body></html>")
	decoded, name := DecodeHTML(body, "")
	if !strings.Contains(string(decoded), "Müller GmbH") {
		t.Fatalf("decoded = %q, want to contain %q (name=%s)", decoded, "Müller GmbH", name)
	}
}

func TestDecodeHTMLHeaderCharset(t *testing.T) {
	decoded, _ := DecodeHTML([]byte("caf\xe9"), "text/html; charset=iso-8859-1")
	if string(decoded) != "café" {
		t.Fatalf("decoded = %q, want café", decoded)
	}
}

func TestDecodeHTMLUTF8Passthrough(t *testing.T) {
	body := []byte(`<html><head><meta charset="utf-8"></head><body>Müller</body></html>`)
	decoded, name := DecodeHTML(body, "")
	if name != "utf-8" || &decoded[0] != &body[0] {
		t.Fatalf("declared UTF-8 must pass through unchanged (name=%s)", name)
	}
}

func TestDecodeHTMLUndeclaredUTF8NotMangled(t *testing.T) {
	// No declaration anywhere: DetermineEncoding defaults to windows-1252, which would
	// turn every multi-byte rune into mojibake. Valid UTF-8 must always pass through.
	body := []byte("<html><body>Müller — København</body></html>")
	decoded, _ := DecodeHTML(body, "")
	if !strings.Contains(string(decoded), "Müller — København") {
		t.Fatalf("undeclared UTF-8 was mangled: %q", decoded)
	}
}
```

- [ ] **Step 2: Run to verify failure** — `go test ./internal/parse/ -run TestDecodeHTML` → FAIL: `undefined: DecodeHTML`.

- [ ] **Step 3: Implement** — `internal/parse/decode.go`:

```go
package parse

import (
	"unicode/utf8"

	"golang.org/x/net/html/charset"
	"golang.org/x/text/transform"
)

// DecodeHTML transcodes an HTML body to UTF-8 using the encoding declared in the Content-Type
// header, BOM, <meta charset> or <meta http-equiv> form (charset.DetermineEncoding sniffs all).
// Already-UTF-8 bodies are returned unchanged (no copy). Undeclared encodings default to
// windows-1252 per the HTML spec, EXCEPT when the body is valid UTF-8 — reinterpreting real
// UTF-8 as 1252 would corrupt it, so valid UTF-8 always passes through. Returns the body
// (decoded or original) and the encoding name that was decided.
func DecodeHTML(body []byte, contentType string) ([]byte, string) {
	enc, name, certain := charset.DetermineEncoding(body, contentType)
	if name == "utf-8" {
		return body, name
	}
	if !certain && utf8.Valid(body) {
		return body, "utf-8"
	}
	decoded, _, err := transform.Bytes(enc.NewDecoder(), body)
	if err != nil || len(decoded) == 0 {
		return body, name
	}
	return decoded, name
}
```

Run `go mod tidy` if `golang.org/x/text` moves from indirect to direct.

- [ ] **Step 4: Run to verify pass** — `go test ./internal/parse/` → PASS.

- [ ] **Step 5: Failing worker integration test** — append to `internal/worker/worker_test.go`:

```go
func TestProcessShardDecodesLatin1(t *testing.T) {
	latin1 := "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=iso-8859-1\">" +
		"<script type=\"application/ld+json\">{\"@type\":\"Organization\",\"name\":\"M\xfcller GmbH\"}</script>" +
		"</head><body>M\xfcller GmbH</body></html>"
	page := gzWarc("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=iso-8859-1\r\n\r\n" + latin1)
	getter := multiGetter{"f.warc.gz:0": page}
	items := []model.WorklistItem{
		{RootDomain: "mueller.de", URL: "https://mueller.de/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(page)), Primary: true},
	}
	cfg := ShardConfig{CrawlID: "C", ResolvedAt: time.Unix(1700000000, 0).UTC(), Concurrency: 1, Mode: "tech"}
	res, err := ProcessShard(context.Background(), items, getter, nil, nil, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Metadata) != 1 || res.Metadata[0].Name != "Müller GmbH" {
		t.Fatalf("latin-1 JSON-LD name mangled: %+v", res.Metadata)
	}
	if len(res.PageMeta) != 1 || res.PageMeta[0].Charset == "" {
		t.Fatalf("detected charset not backfilled: %+v", res.PageMeta)
	}
}
```

NOTE: today the latin-1 name reaches `json.Unmarshal` as invalid UTF-8 and the 0xFC byte is replaced with U+FFFD, so `Metadata[0].Name` comes out as `M�ller GmbH` → the test FAILS before the wiring (verify with `go test ./internal/worker/ -run TestProcessShardDecodesLatin1`).

- [ ] **Step 6: Wire into worker** — `internal/worker/worker.go`:

Add helper (near `subdomainOf`):

```go
// contentTypeOf returns the response Content-Type header (any casing), or "".
func contentTypeOf(headers map[string][]string) string {
	for k, v := range headers {
		if strings.EqualFold(k, "Content-Type") && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}
```

In `processPage`, right after the fetch succeeds, decode once, then swap every extraction input from `body` to `decoded` — tech detection keeps RAW `body`:

```go
	decoded, encName := parse.DecodeHTML(body, contentTypeOf(headers))
	r := pageResult{source: it, sub: subdomainOf(it.URL, it.RootDomain), primary: it.Primary}
	if runTech {
		techBody := body // RAW bytes: upstream wappalyzer parity
		if limit := cfg.TechMaxBytes; limit > 0 && len(techBody) > limit {
			techBody = techBody[:limit]
		}
		t2 := time.Now()
		r.tech = tech.DetectTech(headers, techBody)
		atomic.AddInt64(&stats.techNs, time.Since(t2).Nanoseconds())
		r.emails = parse.Emails(string(decoded))
		prof, structIDs := extract.ExtractProfile(decoded)
		r.profile = prof
		r.ids = append(r.ids, structIDs...)
		r.ids = append(r.ids, extract.ExtractLEIs(decoded)...)
		r.ids = append(r.ids, extract.ExtractVATs(decoded)...)
		r.ids = append(r.ids, extract.Trackers(decoded)...)
		if it.Primary {
			r.security = security.HeaderMap(headers)
			r.meta = parse.ParseHeadMeta(string(decoded))
			if r.meta.Charset == "" {
				r.meta.Charset = encName // head declared nothing — record what we detected
			}
			r.jsonldTypes = extract.JSONLDTypes(decoded)
		}
	}
	if runEmbed && it.Primary {
		t1 := time.Now()
		text, _, _ := parse.ParseHTML(string(decoded))
		atomic.AddInt64(&stats.parseNs, time.Since(t1).Nanoseconds())
		r.text = text
	}
```

In `ProcessIndustryStream`'s fetch worker, stop discarding headers and decode:

```go
	headers, body, err := fetch.FetchRecord(fctx, getter, ccBucket, wit.WarcFilename, wit.Offset, wit.Length)
	...
	decoded, _ := parse.DecodeHTML(body, contentTypeOf(headers))
	t1 := time.Now()
	text, _, _ := parse.ParseHTML(string(decoded))
```

- [ ] **Step 7: Verify** — `go test ./internal/worker/ ./internal/parse/` → all PASS (incl. Step 5's test).

- [ ] **Step 8: Commit**

```bash
git add corpscout/commoncrawl/cc-enrich-worker
git commit -m "feat(cc-enrich-worker): transcode non-UTF-8 pages before extraction"
```

---

### Task 2: Main-content embed text (`parse.MainText`, go-trafilatura)

**Files:**
- Create: `internal/parse/maintext.go`, `internal/parse/maintext_test.go` (tests + benchmark)
- Modify: `internal/worker/worker.go` (two `parse.ParseHTML(string(decoded))` embed-text call sites from Task 1)

**Interfaces:**
- Consumes: `parse.DecodeHTML` (Task 1) — MainText input is already UTF-8.
- Produces: `parse.MainText(body []byte, pageURL string) string`.

- [ ] **Step 1: Add dependency** — `go get github.com/markusmobius/go-trafilatura@v1.12.2 && go mod tidy`.

- [ ] **Step 2: Write the failing tests** — `internal/parse/maintext_test.go`:

```go
package parse

import (
	"strings"
	"testing"
)

const boilerplatePage = `<html><head><title>Acme</title></head><body>
<nav><ul><li>Home</li><li>Products</li><li>Cookie settings</li></ul></nav>
<main><article>
<h1>Acme Industrial Pumps</h1>
<p>Acme designs and manufactures industrial pumps for the chemical, food and marine sectors.
Our engineering team has delivered custom pumping solutions since 1968, serving more than two
thousand customers across Europe. The quarterly results reflect continued growth in demand for
corrosion-resistant magnetic drive pumps and after-sales service contracts.</p>
<p>Beyond manufacturing, Acme operates a service network with certified technicians in twelve
countries, offering preventive maintenance programs, spare part logistics and on-site repairs
around the clock for critical process equipment.</p>
</article></main>
<footer><a href="/imprint">Imprint</a> <a href="/privacy">Privacy policy</a> © 2026 Acme</footer>
</body></html>`

func TestMainTextDropsBoilerplate(t *testing.T) {
	got := MainText([]byte(boilerplatePage), "https://acme.example/")
	if !strings.Contains(got, "quarterly results") {
		t.Fatalf("main content missing: %q", got)
	}
	if strings.Contains(got, "Cookie settings") || strings.Contains(got, "Privacy policy") {
		t.Fatalf("boilerplate leaked into embed text: %q", got)
	}
}

func TestMainTextFallsBackToWalk(t *testing.T) {
	// Too little content for trafilatura -> must fall back to the walk, never return less than today.
	tiny := `<html><body>Acme software company</body></html>`
	if got := MainText([]byte(tiny), "https://acme.example/"); !strings.Contains(got, "Acme software company") {
		t.Fatalf("fallback lost text: %q", got)
	}
}

func BenchmarkMainText(b *testing.B) {
	body := []byte(strings.Replace(boilerplatePage, "</article>", strings.Repeat("<p>Filler paragraph about pump engineering and service operations in industrial plants.</p>\n", 400)+"</article>", 1))
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MainText(body, "https://acme.example/")
	}
}

func BenchmarkWalkText(b *testing.B) {
	body := strings.Replace(boilerplatePage, "</article>", strings.Repeat("<p>Filler paragraph about pump engineering and service operations in industrial plants.</p>\n", 400)+"</article>", 1)
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseHTML(body)
	}
}
```

- [ ] **Step 3: Run to verify failure** — `go test ./internal/parse/ -run TestMainText` → FAIL: `undefined: MainText`.

- [ ] **Step 4: Implement** — `internal/parse/maintext.go`:

```go
package parse

import (
	"bytes"
	nurl "net/url"
	"strings"

	"github.com/markusmobius/go-trafilatura"
)

// MainText extracts the main readable content of a page for embedding — navigation, cookie
// banners and footer boilerplate excluded — so the vector represents what the company says,
// not the page chrome. Falls back to the whole-document text walk whenever trafilatura fails
// or finds nothing, so no page ever yields less text than the previous extractor.
func MainText(body []byte, pageURL string) string {
	opts := trafilatura.Options{ExcludeComments: true}
	if u, err := nurl.Parse(pageURL); err == nil {
		opts.OriginalURL = u
	}
	if res, err := trafilatura.Extract(bytes.NewReader(body), opts); err == nil && res != nil {
		if t := strings.TrimSpace(res.ContentText); t != "" {
			return t
		}
	}
	text, _, _ := ParseHTML(string(body))
	return text
}
```

- [ ] **Step 5: Run to verify pass** — `go test ./internal/parse/` → PASS. Also run `go test ./internal/parse/ -bench BenchmarkMainText -bench BenchmarkWalkText -benchtime 20x -run XXX` and record ns/op in the commit message (informational; expected single-digit ms/page).

- [ ] **Step 6: Wire into worker** — in `internal/worker/worker.go` replace both embed-text call sites from Task 1:

`processPage`:
```go
	if runEmbed && it.Primary {
		t1 := time.Now()
		r.text = parse.MainText(decoded, it.URL)
		atomic.AddInt64(&stats.parseNs, time.Since(t1).Nanoseconds())
	}
```

`ProcessIndustryStream` fetch worker:
```go
	decoded, _ := parse.DecodeHTML(body, contentTypeOf(headers))
	t1 := time.Now()
	text := parse.MainText(decoded, wit.URL)
	atomic.AddInt64(&parseNs, time.Since(t1).Nanoseconds())
```

- [ ] **Step 7: Verify** — `go test -race ./internal/worker/ ./internal/parse/` → PASS (existing tiny-page tests survive via the fallback).

- [ ] **Step 8: Commit**

```bash
git add corpscout/commoncrawl/cc-enrich-worker
git commit -m "feat(cc-enrich-worker): trafilatura main-content extraction for embed text"
```

---

### Task 3: Microdata org profiles (`extract.ExtractProfileMicrodata`)

**Files:**
- Create: `internal/extract/microdata.go`, `internal/extract/microdata_test.go`
- Modify: `internal/worker/worker.go` (`pageResult` + `domainAgg` gain `profileSource string`; `processPage` merge; `mergePageResults`; `Finalize` MetadataRow `Source`)
- Modify test: `internal/worker/worker_test.go`

**Interfaces:**
- Consumes: `isOrgType(string) bool`, `identValid`, `parseYear`, `ldNumber` (existing, package `extract`).
- Produces: `extract.ExtractProfileMicrodata(body []byte, pageURL string) (model.CompanyProfile, []model.Identifier)`.

- [ ] **Step 1: Add dependency** — `go get github.com/iand/microdata@v0.0.28 && go mod tidy`.

- [ ] **Step 2: Write the failing test** — `internal/extract/microdata_test.go`:

```go
package extract

import (
	"testing"
)

func TestExtractProfileMicrodata(t *testing.T) {
	body := []byte(`<html><body>
<div itemscope itemtype="http://schema.org/Dentist">
  <span itemprop="name">Smile Dental</span>
  <span itemprop="description">Family dentistry in Berlin.</span>
  <a itemprop="email" href="mailto:info@smile-dental.de">mail us</a>
  <span itemprop="telephone">+49 30 1234567</span>
  <span itemprop="vatID">DE136695976</span>
  <div itemprop="address" itemscope itemtype="http://schema.org/PostalAddress">
    <span itemprop="addressCountry">de</span>
  </div>
</div></body></html>`)
	prof, ids := ExtractProfileMicrodata(body, "https://smile-dental.de/")
	if prof.Name != "Smile Dental" || prof.Description != "Family dentistry in Berlin." {
		t.Fatalf("profile wrong: %+v", prof)
	}
	if prof.Email != "info@smile-dental.de" {
		t.Fatalf("mailto: prefix not stripped: %q", prof.Email)
	}
	if prof.Phone != "+49 30 1234567" || prof.Country != "DE" {
		t.Fatalf("phone/country wrong: %+v", prof)
	}
	if len(ids) != 1 || ids[0].Type != "vat" || ids[0].Value != "DE136695976" || ids[0].Source != "microdata" {
		t.Fatalf("vat identifier wrong: %+v", ids)
	}
}

func TestExtractProfileMicrodataSkipsPagesWithout(t *testing.T) {
	prof, ids := ExtractProfileMicrodata([]byte(`<html><body>plain page</body></html>`), "https://x.de/")
	if !prof.Empty() || len(ids) != 0 {
		t.Fatalf("expected empty result, got %+v %+v", prof, ids)
	}
}
```

- [ ] **Step 3: Run to verify failure** — `go test ./internal/extract/ -run TestExtractProfileMicrodata` → FAIL: `undefined: ExtractProfileMicrodata`.

- [ ] **Step 4: Implement** — `internal/extract/microdata.go`:

```go
package extract

import (
	"bytes"
	"net/url"
	"strings"

	"github.com/iand/microdata"

	"cc-enrich-worker/internal/model"
)

// ExtractProfileMicrodata parses schema.org microdata (itemscope/itemprop) org items — the
// pre-JSON-LD markup older sites still carry — into the same profile/identifier shapes as the
// JSON-LD path. Identifiers get Source "microdata". Cheap on the ~95% of pages without
// microdata: it bails before any DOM parse unless "itemtype" occurs in the body.
func ExtractProfileMicrodata(body []byte, pageURL string) (model.CompanyProfile, []model.Identifier) {
	var prof model.CompanyProfile
	var ids []model.Identifier
	if !bytes.Contains(body, []byte("itemtype")) {
		return prof, ids
	}
	base, err := url.Parse(pageURL)
	if err != nil {
		base = &url.URL{}
	}
	data, err := microdata.NewParser(bytes.NewReader(body), base).Parse()
	if err != nil || data == nil {
		return prof, ids
	}
	seen := map[string]bool{}
	for _, it := range data.Items {
		mdVisit(it, 0, &prof, &ids, seen)
	}
	return prof, ids
}

// mdVisit fills from org-typed items and recurses into nested items (depth-capped).
func mdVisit(it *microdata.Item, depth int, prof *model.CompanyProfile, ids *[]model.Identifier, seen map[string]bool) {
	if it == nil || depth > 4 {
		return
	}
	if mdIsOrg(it) {
		mdFill(it, prof, ids, seen)
	}
	for _, vals := range it.Properties {
		for _, v := range vals {
			if child, ok := v.(*microdata.Item); ok {
				mdVisit(child, depth+1, prof, ids, seen)
			}
		}
	}
}

func mdIsOrg(it *microdata.Item) bool {
	for _, t := range it.Types {
		if isOrgType(t) { // itemtype is an IRI; isOrgType strips the last path segment
			return true
		}
	}
	return false
}

func mdString(it *microdata.Item, key string) string {
	for _, v := range it.Properties[key] {
		if s, ok := v.(string); ok {
			if s = strings.TrimSpace(s); s != "" {
				return s
			}
		}
	}
	return ""
}

func mdFill(it *microdata.Item, prof *model.CompanyProfile, ids *[]model.Identifier, seen map[string]bool) {
	for _, pair := range []struct{ key, typ string }{
		{"leiCode", "lei"}, {"vatID", "vat"}, {"taxID", "tax"}, {"duns", "duns"}, {"naics", "naics"},
	} {
		val := strings.ToUpper(mdString(it, pair.key))
		if val == "" || seen[pair.typ+":"+val] {
			continue
		}
		seen[pair.typ+":"+val] = true
		*ids = append(*ids, model.Identifier{Type: pair.typ, Value: val, Valid: identValid(pair.typ, val), Source: "microdata"})
	}
	if prof.Name == "" {
		if prof.Name = mdString(it, "name"); prof.Name == "" {
			prof.Name = mdString(it, "legalName")
		}
	}
	if prof.Description == "" {
		prof.Description = mdString(it, "description")
	}
	if prof.Logo == "" {
		prof.Logo = mdString(it, "logo")
	}
	if prof.Email == "" {
		prof.Email = strings.TrimPrefix(mdString(it, "email"), "mailto:") // <a itemprop=email href=mailto:…>
	}
	if prof.Phone == "" {
		prof.Phone = mdString(it, "telephone")
	}
	if prof.FoundingYear == 0 {
		prof.FoundingYear = parseYear(mdString(it, "foundingDate"))
	}
	if prof.EmployeeCount == 0 {
		if n := mdString(it, "numberOfEmployees"); n != "" {
			prof.EmployeeCount = ldNumber(n)
		}
	}
	if prof.Country == "" {
		for _, v := range it.Properties["address"] {
			if addr, ok := v.(*microdata.Item); ok {
				if c := mdString(addr, "addressCountry"); c != "" {
					if len(c) == 2 {
						c = strings.ToUpper(c)
					}
					prof.Country = c
					break
				}
			}
		}
	}
	if len(prof.SameAs) == 0 {
		for _, v := range it.Properties["sameAs"] {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				prof.SameAs = append(prof.SameAs, strings.TrimSpace(s))
			}
		}
	}
}
```

- [ ] **Step 5: Run to verify pass** — `go test ./internal/extract/` → PASS.

- [ ] **Step 6: Failing worker integration test** — append to `internal/worker/worker_test.go`:

```go
func TestProcessShardMicrodataProfileFallback(t *testing.T) {
	page := gzWarc("HTTP/1.1 200 OK\r\n\r\n<html><body>" +
		`<div itemscope itemtype="http://schema.org/Dentist"><span itemprop="name">Smile Dental</span></div>` +
		"</body></html>")
	getter := multiGetter{"f.warc.gz:0": page}
	items := []model.WorklistItem{
		{RootDomain: "smile.de", URL: "https://smile.de/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(page)), Primary: true},
	}
	cfg := ShardConfig{CrawlID: "C", ResolvedAt: time.Unix(1700000000, 0).UTC(), Concurrency: 1, Mode: "tech"}
	res, err := ProcessShard(context.Background(), items, getter, nil, nil, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Metadata) != 1 || res.Metadata[0].Name != "Smile Dental" || res.Metadata[0].Source != "microdata" {
		t.Fatalf("microdata profile not used: %+v", res.Metadata)
	}
}
```

Run `go test ./internal/worker/ -run TestProcessShardMicrodataProfileFallback` → FAIL (no metadata row today).

- [ ] **Step 7: Wire into worker** — `internal/worker/worker.go`:

Add `profileSource string` to `pageResult` (after `profile`) and to `domainAgg` (after `profile`).

`processPage` (`runTech` branch, replacing the two profile lines from Task 1):

```go
		prof, structIDs := extract.ExtractProfile(decoded)
		r.profileSource = "jsonld"
		if prof.Empty() { // JSON-LD wins; microdata fills the gap on older sites
			if mdProf, mdIDs := extract.ExtractProfileMicrodata(decoded, it.URL); !mdProf.Empty() || len(mdIDs) > 0 {
				if !mdProf.Empty() {
					prof = mdProf
					r.profileSource = "microdata"
				}
				structIDs = append(structIDs, mdIDs...)
			}
		}
		r.profile = prof
```

`mergePageResults` (where the first non-empty profile is kept):

```go
		if agg.profile.Empty() && !r.profile.Empty() {
			agg.profile = r.profile
			agg.profileSource = r.profileSource
		}
```

`Finalize`: the per-page MetadataRow uses the page's source, and the profile-derived contacts use the aggregate's:

```go
			if profile := page.profile; hasMetadataEvidence(profile) {
				src := page.profileSource
				if src == "" {
					src = "jsonld"
				}
				metaRows = append(metaRows, output.MetadataRow{
					... // unchanged fields
					EmployeeCount: profile.EmployeeCount, Source: src,
					...
				})
			}
```

and in the contacts block replace the literal `"jsonld"` source for email/phone/social rows with:

```go
			profSrc := a.profileSource
			if profSrc == "" {
				profSrc = "jsonld"
			}
			// then contactRow(..., "email", a.profile.Email, profSrc) etc. for the three profile-derived rows
			// (the regex-email rows keep Source "regex")
```

- [ ] **Step 8: Verify** — `go test -race ./internal/worker/ ./internal/extract/` → all PASS.

- [ ] **Step 9: Commit**

```bash
git add corpscout/commoncrawl/cc-enrich-worker
git commit -m "feat(cc-enrich-worker): microdata org profiles as JSON-LD fallback"
```

---

### Task 4: Phone normalization (`extract.NormalizePhone`)

**Files:**
- Create: `internal/extract/phone.go`, `internal/extract/phone_test.go`
- Modify: `internal/worker/worker.go` (`Finalize` phone contact row)

**Interfaces:**
- Produces: `extract.NormalizePhone(raw, rootDomain string) string`.

- [ ] **Step 1: Add dependency** — `go get github.com/nyaruka/phonenumbers@v1.8.0 && go mod tidy`.

- [ ] **Step 2: Write the failing test** — `internal/extract/phone_test.go`:

```go
package extract

import "testing"

func TestNormalizePhone(t *testing.T) {
	tests := []struct {
		raw, domain, want string
	}{
		{"+1 650-253-0000", "acme.com", "+16502530000"},   // international: normalized regardless of TLD
		{"020 7946 0958", "acme.uk", "+442079460958"},     // national format + ccTLD hint (.uk -> GB)
		{" +49 (0)30 901820 ", "acme.de", "+4930901820"},  // messy formatting collapses to E.164
		{"call us", "acme.com", "call us"},                // unparseable: kept raw (trimmed)
		{"12345", "acme.com", "12345"},                    // generic TLD, no region hint: kept raw
		{"", "acme.de", ""},
	}
	for _, tt := range tests {
		if got := NormalizePhone(tt.raw, tt.domain); got != tt.want {
			t.Errorf("NormalizePhone(%q, %q) = %q, want %q", tt.raw, tt.domain, got, tt.want)
		}
	}
}
```

NOTE: if `IsValidNumber` rejects a fixture (metadata changes over versions), swap that case for another known-valid number rather than weakening the assertion.

- [ ] **Step 3: Run to verify failure** — `go test ./internal/extract/ -run TestNormalizePhone` → FAIL: `undefined: NormalizePhone`.

- [ ] **Step 4: Implement** — `internal/extract/phone.go`:

```go
package extract

import (
	"strings"

	"github.com/nyaruka/phonenumbers"
)

// ccTLDRegionOverrides maps ccTLDs whose ISO 3166-1 region code differs from the TLD itself.
var ccTLDRegionOverrides = map[string]string{"uk": "GB"}

// NormalizePhone returns the E.164 form of a phone found on a page when it parses as a valid
// number, using the domain's ccTLD as the region hint (so national formats on .de sites parse
// as DE; generic TLDs get no hint, so only +-prefixed numbers parse). E.164 dedupes the many
// formatting variants of one number across pages. Anything unparseable or invalid is returned
// trimmed, never dropped.
func NormalizePhone(raw, rootDomain string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return trimmed
	}
	region := ""
	if i := strings.LastIndexByte(rootDomain, '.'); i >= 0 {
		if tld := strings.ToLower(rootDomain[i+1:]); len(tld) == 2 {
			if r, ok := ccTLDRegionOverrides[tld]; ok {
				region = r
			} else {
				region = strings.ToUpper(tld)
			}
		}
	}
	num, err := phonenumbers.Parse(trimmed, region)
	if err != nil || !phonenumbers.IsValidNumber(num) {
		return trimmed
	}
	return phonenumbers.Format(num, phonenumbers.E164)
}
```

- [ ] **Step 5: Run to verify pass** — `go test ./internal/extract/` → PASS.

- [ ] **Step 6: Wire into worker** — in `Finalize`'s contacts block:

```go
			if a.profile.Phone != "" {
				contacts = append(contacts, contactRow(cfg, a.rootDomain, a.primaryURL, "phone",
					extract.NormalizePhone(a.profile.Phone, a.rootDomain), profSrc))
			}
```

- [ ] **Step 7: Verify** — `go test -race ./internal/worker/ ./internal/extract/` → PASS.

- [ ] **Step 8: Commit**

```bash
git add corpscout/commoncrawl/cc-enrich-worker
git commit -m "feat(cc-enrich-worker): normalize phone contacts to E.164 via libphonenumber"
```

---

### Task 5: Full verification

- [ ] **Step 1:** `gofmt -l .` → empty; `go vet ./...` → clean; `go test -race ./...` → all PASS.
- [ ] **Step 2:** `go build ./...` and `go mod tidy` → no diff.
- [ ] **Step 3:** Run benchmarks once more, note MainText ns/op vs WalkText in the final summary (no commit unless files changed).
