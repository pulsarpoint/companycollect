# CommonCrawl Enrichment Go Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. This is a **Go** module — tests use `go test`, not pytest.

**Goal:** A single Go binary that processes the index-driven worklist (top-K pages per domain) and writes per-domain industry + per-page technologies + contacts to Parquet, embedding the primary page on the DGX while Wappalyzer-scanning all K pages on the CPU in the GPU's shadow.

**Architecture:** Goroutine pipeline — fetch+parse+wappalyzer across all cores feeds a batched embed client (the DGX bottleneck); classify with a ported cosine-matmul + gates; reference matrices loaded from ClickHouse; output Parquet → S3. See `docs/superpowers/specs/2026-06-23-cc-enrich-go-worker-design.md`.

**Tech Stack:** Go 1.26; `clickhouse-go/v2`, `aws-sdk-go-v2/{config,service/s3}`, `projectdiscovery/wappalyzergo`, `golang.org/x/net/html`, `parquet-go/parquet-go`. Matmul is a manual `float32` dot loop (no gonum needed). Build context `corpscout/cc-enrich-worker/`.

---

## File Structure (`corpscout/cc-enrich-worker/`)
- `go.mod` / `go.sum`
- `types.go` — `Technology`, `Reference`, `Prototypes`, `DomainResult`, `PageFetch`
- `reference.go` — load NACE matrix + page-type prototypes from ClickHouse
- `classify.go` — cosine top-3 + margin + division-consensus + page-type + keyword signals
- `signals.go` — the ~28 page-type keyword regexes (port of `page_types.PAGE_TYPE_SIGNALS`)
- `embed.go` — batched `/v1/embeddings` client, L2-normalized vectors
- `fetch.go` — S3 byte-range GET → single WARC record → `(headers, body)`
- `parse.go` — html → text, emails, socials
- `tech.go` — wappalyzergo wrapper
- `output.go` — Parquet writers (domains + technologies) + S3 upload
- `worker.go` — the goroutine pipeline
- `main.go` — config + run one shard
- `Dockerfile`
- `*_test.go` per unit

All `go` commands run from `corpscout/cc-enrich-worker/`.

---

## Task 1: Module + types + classify constants

**Files:** Create `go.mod`, `types.go`, `consts.go`

- [ ] **Step 1: Init the module + deps**
```bash
cd corpscout/cc-enrich-worker
go mod init cc-enrich-worker
go get github.com/ClickHouse/clickhouse-go/v2@latest \
       github.com/aws/aws-sdk-go-v2/config@latest \
       github.com/aws/aws-sdk-go-v2/service/s3@latest \
       github.com/projectdiscovery/wappalyzergo@v0.2.57 \
       golang.org/x/net/html@latest \
       github.com/parquet-go/parquet-go@latest
```

- [ ] **Step 2: Types + thresholds (defaults match the validated Python values)**

`types.go`:
```go
package main

type Technology struct {
	Name, Category, Version string
}

// Reference: NACE matrix, rows co-ordered with Codes/Labels/Divisions, L2-normalized.
type Reference struct {
	Codes, Labels, Divisions []string
	M                        [][]float32 // len = N x dim
}

// Prototypes: page-type vectors, Labels[i] is the class of row i, L2-normalized.
type Prototypes struct {
	Labels []string
	P      [][]float32
}

type PageFetch struct {
	URL, RootDomain, Subdomain string
	Primary                    bool
	Text                       string
	Emails, Socials            []string
	Tech                       []Technology
}

type DomainResult struct {
	CrawlID, RootDomain, URL, Subdomain string
	Emails                              []string
	PageType                            string
	PageTypeScore                       float32
	NaceCode, NaceLabel, NaceDivision   string
	NaceConfident                       bool
	NaceMargin, NaceScore               float32
	Top3Codes, Top3Labels               []string
	Top3Scores                          []float32
	Tech                                []Technology // unioned across the domain's K pages
}
```

`consts.go`:
```go
package main

const (
	embedMaxChars      = 2000  // COMMONCRAWL_EMBED_MAX_CHARS default
	marginThreshold    = 0.03  // confident if top1-top2 >= this
	pageTypeThreshold  = 0.55  // page-type detected if max prototype sim >= this
	embedBatch         = 128
	ccBucket           = "commoncrawl"
)
```

- [ ] **Step 3: Build**
```bash
go build ./...
```
Expected: compiles (empty main is fine for now; add a stub `main.go` with `func main(){}`).

- [ ] **Step 4: Commit**
```bash
git add corpscout/cc-enrich-worker/go.mod corpscout/cc-enrich-worker/go.sum \
        corpscout/cc-enrich-worker/types.go corpscout/cc-enrich-worker/consts.go \
        corpscout/cc-enrich-worker/main.go
git commit -m "feat(go-worker): module init + types + thresholds"
```

---

## Task 2: Classify (cosine top-3 + gates) — the ported core

**Files:** Create `classify.go`, `classify_test.go`

- [ ] **Step 1: Write the failing test**

`classify_test.go` (uses the package-level `norm` defined in `classify.go`):
```go
package main

import "testing"

func TestClassifyConfidentByMargin(t *testing.T) {
	ref := &Reference{Codes: []string{"62.01", "47.11"}, Labels: []string{"Prog", "Retail"},
		Divisions: []string{"62", "47"}, M: [][]float32{norm([]float32{1, 0, 0}), norm([]float32{0, 1, 0})}}
	protos := &Prototypes{}
	r := Classify(norm([]float32{1, 0, 0}), ref, protos)
	if r.NaceCode != "62.01" || !r.NaceConfident {
		t.Fatalf("want 62.01 confident, got %+v", r)
	}
}

func TestPageTypeDetected(t *testing.T) {
	ref := &Reference{Codes: []string{"62.01"}, Labels: []string{"x"}, Divisions: []string{"62"},
		M: [][]float32{norm([]float32{1, 0, 0, 0})}}
	protos := &Prototypes{Labels: []string{"parked"}, P: [][]float32{norm([]float32{0, 0, 1, 0})}}
	r := Classify(norm([]float32{0, 0, 1, 0}), ref, protos)
	if r.PageType != "parked" || r.NaceConfident {
		t.Fatalf("want parked, not confident; got %+v", r)
	}
}
```

- [ ] **Step 2: Run to verify it fails**
```bash
go test ./... -run TestClassify -v
```
Expected: FAIL (undefined `Classify`, `sqrt32`).

- [ ] **Step 3: Implement `classify.go`**
```go
package main

import (
	"math"
	"sort"
	"strings"
)

func sqrt32(x float32) float32 { return float32(math.Sqrt(float64(x))) }

func dot(a, b []float32) float32 {
	var s float32
	for i := range a { s += a[i] * b[i] }
	return s
}

// norm L2-normalizes a vector (used here and by the embed client).
func norm(v []float32) []float32 {
	s := float32(1.0) / (sqrt32(dot(v, v)) + 1e-9)
	out := make([]float32, len(v))
	for i, x := range v { out[i] = x * s }
	return out
}

// division: 2-digit NACE division from a possibly-messy code.
func division(code string) string {
	for i := 0; i+1 < len(code); i++ {
		if code[i] >= '0' && code[i] <= '9' && code[i+1] >= '0' && code[i+1] <= '9' {
			return code[i : i+2]
		}
	}
	if code != "" { return strings.ToUpper(code[:1]) }
	return ""
}

// Classify a normalized page vector against the NACE matrix + page-type prototypes.
func Classify(page []float32, ref *Reference, protos *Prototypes) DomainResult {
	type sc struct { i int; s float32 }
	sims := make([]sc, len(ref.M))
	for i, row := range ref.M { sims[i] = sc{i, dot(page, row)} }
	sort.Slice(sims, func(a, b int) bool { return sims[a].s > sims[b].s })

	k := 3
	if len(sims) < k { k = len(sims) }
	var codes, labels []string
	var scores []float32
	divs := map[string]bool{}
	for n := 0; n < k; n++ {
		idx := sims[n].i
		codes = append(codes, ref.Codes[idx]); labels = append(labels, ref.Labels[idx])
		scores = append(scores, sims[n].s); divs[ref.Divisions[idx]] = true
	}
	margin := scores[0]
	if len(scores) > 1 { margin = scores[0] - scores[1] }
	consensus := len(divs) == 1

	// page-type prototypes: max similarity + its label
	var ptScore float32
	ptLabel := ""
	for i, row := range protos.P {
		if s := dot(page, row); s > ptScore { ptScore, ptLabel = s, protos.Labels[i] }
	}
	isPageType := ptScore >= pageTypeThreshold

	r := DomainResult{
		PageTypeScore: ptScore,
		NaceCode:      codes[0], NaceLabel: labels[0], NaceDivision: ref.Divisions[sims[0].i],
		NaceMargin: margin, NaceScore: scores[0],
		Top3Codes: codes, Top3Labels: labels, Top3Scores: scores,
		NaceConfident: (margin >= marginThreshold || consensus) && !isPageType,
	}
	if isPageType { r.PageType = ptLabel }
	return r
}
```

- [ ] **Step 4: Run tests**
```bash
go test ./... -run TestClassify -v
```
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add corpscout/cc-enrich-worker/classify.go corpscout/cc-enrich-worker/classify_test.go
git commit -m "feat(go-worker): classify — cosine top-3 + margin/consensus/page-type gates"
```

---

## Task 3: Keyword page-type signals (port of `page_types.PAGE_TYPE_SIGNALS`)

**Files:** Create `signals.go`, `signals_test.go`

- [ ] **Step 1: Write the failing test**

`signals_test.go`:
```go
package main

import "testing"

func TestMatchSignal(t *testing.T) {
	cases := map[string]string{
		"This domain is for sale":                              "for_sale",
		"Welcome to nginx! If you see this page":               "default_server",
		"This site is under construction":                     "under_construction",
		"Index of /pub\nParent Directory\nLast modified":      "directory_listing",
		"We are a law firm, courtesy of our partners":         "", // must NOT fire
		"Original artwork for sale by the artist":             "", // generic 'for sale'
	}
	for text, want := range cases {
		if got := MatchSignal(text); got != want {
			t.Errorf("MatchSignal(%q)=%q want %q", text, got, want)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails**
```bash
go test ./... -run TestMatchSignal -v
```
Expected: FAIL (undefined `MatchSignal`).

- [ ] **Step 3: Implement `signals.go`** — port the high-precision patterns + labels verbatim from `commoncrawl_enrich/page_types.py` `PAGE_TYPE_SIGNALS` (case-insensitive, DOTALL), scanning only the first 2000 chars.
```go
package main

import "regexp"

type sig struct { label string; re *regexp.Regexp }

var signals = func() []sig {
	pats := []struct{ label, p string }{
		{"for_sale", `(?is)\bbuy this domain\b`},
		{"for_sale", `(?is)\bpurchase this domain\b`},
		{"for_sale", `(?is)this domain (?:is|may be|could be) for sale`},
		{"for_sale", `(?is)\bdomain(?: name)? is for sale\b`},
		{"for_sale", `(?is)the domain .{0,60}? is for sale`},
		{"for_sale", `(?is)\bthis domain has expired\b`},
		{"for_sale", `(?is)\binterested in this domain\b`},
		{"parking_provider", `(?is)\bsedoparking\b`},
		{"parking_provider", `(?is)\bparkingcrew\b`},
		{"parking_provider", `(?is)\bafternic\b`},
		{"parking_provider", `(?is)\bhugedomains\b`},
		{"parking_provider", `(?is)\bcashparking\b`},
		{"parking_provider", `(?is)this (?:web ?)?page is parked`},
		{"parking_provider", `(?is)\bthis domain (?:is )?parked\b`},
		{"parking_provider", `(?is)\bparked free\b`},
		{"parking_provider", `(?is)\bdomain parking\b`},
		{"parking_provider", `(?is)\bparked by (?:the )?(?:domain|registrant|owner)\b`},
		{"under_construction", `(?is)\bunder construction\b`},
		{"under_construction", `(?is)\bwebsite coming soon\b`},
		{"default_server", `(?is)\bwelcome to nginx\b`},
		{"default_server", `(?is)apache2?.{0,30}default page`},
		{"default_server", `(?is)\btest page for the (?:apache|nginx)\b`},
		{"default_server", `(?is)\bapache http server test page\b`},
		{"default_server", `(?is)\b(?:iis windows server|iisstart|internet information services)\b`},
		{"default_server", `(?is)\bsite not configured\b`},
		{"default_server", `(?is)\bno (?:web)?site (?:is )?configured\b`},
		{"default_server", `(?is)\byour new website is ready\b`},
		{"default_server", `(?is)\bweb server'?s default (?:web ?)?page\b`},
		{"directory_listing", `(?is)\bindex of /\S*.{0,120}(?:parent directory|last modified)`},
	}
	out := make([]sig, len(pats))
	for i, p := range pats { out[i] = sig{p.label, regexp.MustCompile(p.p)} }
	return out
}()

func MatchSignal(text string) string {
	head := text
	if len(head) > 2000 { head = head[:2000] }
	for _, s := range signals {
		if s.re.MatchString(head) { return s.label }
	}
	return ""
}
```

- [ ] **Step 4: Run tests**
```bash
go test ./... -run TestMatchSignal -v
```
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add corpscout/cc-enrich-worker/signals.go corpscout/cc-enrich-worker/signals_test.go
git commit -m "feat(go-worker): keyword page-type signals (port of page_types)"
```

---

## Task 4: Batched embedding client

**Files:** Create `embed.go`, `embed_test.go`

- [ ] **Step 1: Write the failing test** — `httptest` server returning fixed vectors; assert batching (≤embedBatch per request) + normalization + the query instruction prefix.

`embed_test.go`:
```go
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEmbedBatchesAndNormalizes(t *testing.T) {
	var reqCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		var body struct{ Input []string `json:"input"` }
		json.NewDecoder(r.Body).Decode(&body)
		out := struct{ Data []struct{ Embedding []float32 `json:"embedding"` } `json:"data"` }{}
		for range body.Input {
			out.Data = append(out.Data, struct{ Embedding []float32 `json:"embedding"` }{[]float32{3, 4}})
		}
		json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()
	c := NewEmbedClient(srv.URL, "m", 2) // batch size 2
	vecs, err := c.Embed([]string{"a", "b", "c"}, "Classify")
	if err != nil { t.Fatal(err) }
	if reqCount != 2 { t.Fatalf("want 2 batched requests, got %d", reqCount) }
	if d := dot(vecs[0], vecs[0]); d < 0.99 || d > 1.01 { t.Fatalf("not normalized: %v", d) }
}
```

- [ ] **Step 2: Run to verify it fails**
```bash
go test ./... -run TestEmbed -v
```
Expected: FAIL (undefined `NewEmbedClient`).

- [ ] **Step 3: Implement `embed.go`** — POST `{model, input:[...]}` to `<baseURL>/embeddings`, chunk by batch, prefix queries `Instruct: <instr>\nQuery: <text>`, truncate to `embedMaxChars`, L2-normalize each returned vector.
```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type EmbedClient struct {
	url, model string
	batch      int
	http       *http.Client
}

func NewEmbedClient(baseURL, model string, batch int) *EmbedClient {
	return &EmbedClient{url: baseURL + "/embeddings", model: model, batch: batch, http: &http.Client{}}
}

func (c *EmbedClient) Embed(texts []string, instruction string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for i := 0; i < len(texts); i += c.batch {
		end := i + c.batch
		if end > len(texts) { end = len(texts) }
		chunk := make([]string, 0, end-i)
		for _, t := range texts[i:end] {
			if len(t) > embedMaxChars { t = t[:embedMaxChars] }
			if instruction != "" { t = "Instruct: " + instruction + "\nQuery: " + t }
			chunk = append(chunk, t)
		}
		body, _ := json.Marshal(map[string]any{"model": c.model, "input": chunk})
		resp, err := c.http.Post(c.url, "application/json", bytes.NewReader(body))
		if err != nil { return nil, err }
		var parsed struct {
			Data []struct{ Embedding []float32 `json:"embedding"` } `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil { resp.Body.Close(); return nil, err }
		resp.Body.Close()
		if len(parsed.Data) != len(chunk) {
			return nil, fmt.Errorf("embed: got %d vectors for %d inputs", len(parsed.Data), len(chunk))
		}
		for _, d := range parsed.Data { out = append(out, norm(d.Embedding)) }
	}
	return out, nil
}
```
(`norm` is the package-level helper added to `classify.go` in Task 2.)

- [ ] **Step 4: Run tests**
```bash
go test ./... -run TestEmbed -v
```
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add corpscout/cc-enrich-worker/embed.go corpscout/cc-enrich-worker/embed_test.go
git commit -m "feat(go-worker): batched /v1/embeddings client (normalized)"
```

---

## Task 5: Parse + tech (HTML → text/emails/socials; wappalyzergo)

**Files:** Create `parse.go`, `tech.go`, `parse_test.go`

- [ ] **Step 1: Write the failing test**

`parse_test.go`:
```go
package main

import "testing"

func TestParseAndTech(t *testing.T) {
	html := `<html><head><link href="wp-content/x.css"></head><body>ACME software info@acme.com <a href="https://facebook.com/acme">fb</a></body></html>`
	text, emails, socials := ParseHTML(html)
	if !contains(emails, "info@acme.com") { t.Fatalf("emails=%v", emails) }
	if !contains(socials, "facebook") { t.Fatalf("socials=%v", socials) }
	if len(text) == 0 { t.Fatal("no text") }
	techs := DetectTech(map[string][]string{"Server": {"nginx"}}, []byte(html))
	if !hasTech(techs, "Nginx") || !hasTech(techs, "WordPress") { t.Fatalf("techs=%v", techs) }
}
```
(Add small `contains`/`hasTech` helpers in the test.)

- [ ] **Step 2: Run to verify it fails**
```bash
go test ./... -run TestParseAndTech -v
```
Expected: FAIL (undefined `ParseHTML`/`DetectTech`).

- [ ] **Step 3: Implement** `parse.go` (walk `x/net/html` for text nodes; `regexp` for emails `[\w.+-]+@[\w-]+\.[\w.-]+`; map social hosts → platform) and `tech.go`:
```go
// tech.go
package main

import wappalyzer "github.com/projectdiscovery/wappalyzergo"

var wapp, _ = wappalyzer.New()

func DetectTech(headers map[string][]string, body []byte) []Technology {
	out := []Technology{}
	for key, app := range wapp.FingerprintWithInfo(headers, body) {
		name, version := key, ""
		for i := 0; i < len(key); i++ {
			if key[i] == ':' { name, version = key[:i], key[i+1:]; break }
		}
		cat := ""
		if len(app.Categories) > 0 { cat = app.Categories[0] }
		out = append(out, Technology{Name: name, Category: cat, Version: version})
	}
	return out
}
```

- [ ] **Step 4: Run tests**
```bash
go test ./... -run TestParseAndTech -v
```
Expected: PASS (Nginx from header, WordPress from `wp-content`).

- [ ] **Step 5: Commit**
```bash
git add corpscout/cc-enrich-worker/parse.go corpscout/cc-enrich-worker/tech.go corpscout/cc-enrich-worker/parse_test.go
git commit -m "feat(go-worker): HTML parse (text/emails/socials) + wappalyzergo tech"
```

---

## Task 6: Fetch one WARC record by byte range

**Files:** Create `fetch.go`, `fetch_test.go`

- [ ] **Step 1: Write the failing test** — a fake getter returning a gzipped single WARC `response` record; assert it parses to headers+body. Use an interface so S3 is swappable:
```go
type rangeGetter interface {
	GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
}
```
`fetch_test.go` builds a record with a gzip of `WARC/1.0\r\n…\r\n\r\nHTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n<html>wp-content</html>` and asserts `FetchRecord` returns `Server: nginx` + body containing `wp-content`.

- [ ] **Step 2: Run to verify it fails** — `go test ./... -run TestFetch -v` → FAIL.

- [ ] **Step 3: Implement `fetch.go`** — `GetRange` → `gzip.NewReader` → read the WARC record: split WARC headers (until blank line), then the HTTP response (status line + headers until blank line) → `(map[string][]string, []byte)`. Provide an S3-backed `rangeGetter` using `aws-sdk-go-v2` (`GetObject` with `Range: bytes=start-end`).

- [ ] **Step 4: Run tests** — `go test ./... -run TestFetch -v` → PASS.

- [ ] **Step 5: Commit**
```bash
git add corpscout/cc-enrich-worker/fetch.go corpscout/cc-enrich-worker/fetch_test.go
git commit -m "feat(go-worker): S3 byte-range fetch -> WARC record -> headers+body"
```

---

## Task 7: Reference loader (ClickHouse → matrices)

**Files:** Create `reference.go`, `reference_test.go`

- [ ] **Step 1: Write the failing test** — feed synthetic rows through the builder (not a live CH): `BuildReference(rows)` / `BuildPrototypes(rows)` where a row is `{code,label,division,embedding}`; assert co-ordering + normalization. Keep the CH query behind a thin `queryRows` func so the matrix-building is unit-tested without a DB.

- [ ] **Step 2: Run to verify it fails** — FAIL (undefined `BuildReference`).

- [ ] **Step 3: Implement `reference.go`** — `LoadReference(ctx, conn)` runs
  `SELECT code,label,division,embedding FROM corpscout.nace_category_embeddings FINAL ORDER BY code`
  and `SELECT page_type,root_domain,embedding FROM corpscout.page_type_exemplars FINAL` via
  `clickhouse-go/v2` (embedding scans into `[]float32`), then `BuildReference`/`BuildPrototypes`
  normalize each row into the matrices.

- [ ] **Step 4: Run tests** — PASS.

- [ ] **Step 5: Commit**
```bash
git add corpscout/cc-enrich-worker/reference.go corpscout/cc-enrich-worker/reference_test.go
git commit -m "feat(go-worker): load NACE matrix + page-type prototypes from ClickHouse"
```

---

## Task 8: Output (Parquet writers) + worker pipeline

**Files:** Create `output.go`, `worker.go`, `worker_test.go`

- [ ] **Step 1: Write the failing test** — `worker_test.go`: a fake `rangeGetter` serving 2 pages for one domain (K=2) + a fake embedder (returns a vector that classifies to a known NACE) → `ProcessShard` produces one `DomainResult` with the expected NACE + the union of both pages' tech, and `WriteDomains`/`WriteTech` round-trip through Parquet with the migration-046/047 column order.

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement**
  - `output.go`: `parquet-go` structs whose field order/tags == `commoncrawl_domains` (046) and `commoncrawl_technologies` (047); `WriteDomains(path, rows)` / `WriteTech(path, rows)`; `UploadToS3`.
  - `worker.go`: the pipeline —
    1. read the worklist shard (Parquet: `root_domain,url,warc_filename,offset,length,rn`), group by `root_domain`;
    2. a bounded goroutine pool fetches+parses+`DetectTech` each page (all K), collecting per-domain primary text + emails/socials + unioned tech;
    3. batch the primary texts → `EmbedClient.Embed(..., "Classify the business into its industry category")`;
    4. for each domain: `MatchSignal(text)` → keyword page-type; else `Classify(vec, ref, protos)`; build `DomainResult` (+ crawl/source/resolved fields);
    5. `WriteDomains` + `WriteTech` → upload.

- [ ] **Step 4: Run tests** — `go test ./...` → PASS (whole module).

- [ ] **Step 5: Commit**
```bash
git add corpscout/cc-enrich-worker/output.go corpscout/cc-enrich-worker/worker.go corpscout/cc-enrich-worker/worker_test.go
git commit -m "feat(go-worker): parquet output + fetch/tech/embed/classify pipeline"
```

---

## Task 9: main.go, Dockerfile, full build

**Files:** Create/replace `main.go`, `Dockerfile`

- [ ] **Step 1: `main.go`** — flags/env: `--worklist`, `--out`, `--crawl-id`, `--k`, `--concurrency`, `EMBED base URL`, `CLICKHOUSE DSN`, S3 region. Load reference (CH) once, build S3 getter + embed client, `ProcessShard`, write/upload, exit.

- [ ] **Step 2: Dockerfile** (static binary):
```dockerfile
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /cc-enrich-worker .

FROM gcr.io/distroless/static-debian12
COPY --from=build /cc-enrich-worker /cc-enrich-worker
ENTRYPOINT ["/cc-enrich-worker"]
```

- [ ] **Step 3: Build + vet + full test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass.

- [ ] **Step 4: Commit**
```bash
git add corpscout/cc-enrich-worker/main.go corpscout/cc-enrich-worker/Dockerfile
git commit -m "feat(go-worker): main + Dockerfile (static binary)"
```

- [ ] **Step 5: (manual, EC2) end-to-end** — build the binary on the EC2, point it at a small worklist shard (from the DuckDB/Athena worklist, top-K), and confirm it writes domain + tech Parquet, classifications look sane, and the DGX log shows high `Running:` with the CPU pegged on tech while the GPU embeds. Record domains/sec.

---

## Notes for the implementer
- Unit tests are offline (httptest embed, fake rangeGetter, synthetic CH rows, real wappalyzergo). Only Task 9 step 5 needs live S3/DGX/CH.
- Keep thresholds (`consts.go`) equal to the validated Python values so Go and Python agree; if tuned, change both (or load from CH/config).
- The worklist query becomes **top-K per domain** (`rn <= K`, `rn=1` primary) — update the Athena/DuckDB query when generating the production worklist; the Go worker groups by `root_domain`.
- `git add` by explicit path under `corpscout/cc-enrich-worker/`.
