# cc-enrich-worker Package Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the flat 19-file `package main` in `commoncrawl/cc-enrich-worker/` into focused `internal/` packages, with `main.go` at the root as the only `package main`, without changing any behavior.

**Architecture:** Go requires one package per directory, so "organize into folders" = extract `internal/<pkg>` packages. Shared data structs move to a dependency-free `internal/model` hub; each concern (fetch, embed, parse, output, tech, extract, classify, worker) becomes its own package. Dependency-ordered extraction (leaf packages first) keeps `go build ./... && go test ./...` green after every task. This is a **pure refactor** — the existing tests are the safety net; no test should change except its `package` line and qualified references.

**Tech Stack:** Go 1.26; module `cc-enrich-worker`; deps `clickhouse-go/v2`, `aws-sdk-go-v2`, `parquet-go/parquet-go`, `projectdiscovery/wappalyzergo`, `petar-dambovaliev/aho-corasick`, `golang.org/x/net/html`.

## Global Constraints

- Module path is `cc-enrich-worker` (see `go.mod`); internal imports are `cc-enrich-worker/internal/<pkg>`.
- All work happens in `commoncrawl/cc-enrich-worker/`. Run every `go` command from there.
- `main.go` stays at the repo's package root (so `go build -o cc-enrich-worker .` and the Dockerfile are unchanged).
- After **every** task: `go vet ./...`, `go build ./...`, `go test ./...` must all pass before committing.
- Commit by explicit path (the working tree carries unrelated WIP — never `git add -A`).
- Use `git mv <old>.go internal/<pkg>/<old>.go` to preserve history; move `_test.go` with its source.
- Commit-message footer:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- No behavior changes. If a symbol is only used inside its new package, leave it unexported; only **export** (capitalize) symbols referenced from another package, and update every reference.

---

## Target structure

```
cc-enrich-worker/
  main.go                       package main — flags, env, clickhouse conn, readWorklist, orchestration
  internal/
    model/      types.go        Technology, Reference, Prototypes, DomainResult, Identifier,
                                 CompanyProfile, WorklistItem, PageFetch  (NO internal deps)
    fetch/      fetch.go         RangeGetter, FetchRecord, NewS3Getter, NewHTTPGetter
    embed/      embed.go         EmbedClient, NewEmbedClient  (+ embedMaxChars, embedBatch consts)
    parse/      parse.go         ParseHTML
    output/     output.go        DomainRow, TechRow, IdentifierRow, ProfileRow, Write*, UploadToS3
    tech/       tech.go techfast.go literal.go   DetectTech, FastMatcher  (-> model)
    extract/    lei.go vat.go profile.go         ExtractLEIs, ExtractVATs, ExtractProfile  (-> model)
    classify/   reference.go classify.go signals.go check.go
                                 LoadReference, Classify, MatchSignal, LoadReferenceMeta,
                                 VerifyReference, ReferenceMeta  (+ margin/pageType/confTopK/confTemp)
    worker/     worker.go        ProcessShard, ShardResult, ShardConfig  (+ ccBucket, techMaxBytes)
```

**Dependency graph (no cycles):**
```
model         ← (nothing internal)
fetch         ← (nothing internal)
embed         ← (nothing internal)
parse         ← (nothing internal)
output        ← (nothing internal)
tech          ← model
extract       ← model
classify      ← model
worker        ← model, fetch, parse, output, tech, extract, classify
main          ← model, fetch, embed, output, classify, worker
```

**Interface ownership (Go structural typing — define at the consumer, no shared interface type):**
- `fetch.RangeGetter` (was `rangeGetter`) — the byte-range getter interface, lives in `fetch`.
- `worker.embedderIface` and `classify.Embedder` — each consumer declares its own one-method
  `Embed([]string, string) ([][]float32, error)` interface; `*embed.EmbedClient` satisfies both.
  `embed` itself imports neither.

**Two gotchas to carry through:**
1. `classify` imports the `model` package, but `VerifyReference(meta ReferenceMeta, model string, emb Embedder)`
   has a param named `model` that shadows it. **Rename that param to `modelName`.**
2. `NewS3Getter`/`NewHTTPGetter` currently return concrete unexported types. Change them to return
   `RangeGetter` so the concrete getter types stay unexported.

---

### Task 1: Extract `internal/fetch`

**Files:**
- Move: `fetch.go` → `internal/fetch/fetch.go`, `fetch_test.go` → `internal/fetch/fetch_test.go`
- Modify: `worker.go`, `main.go` (qualify `fetch.` + import)

**Interfaces:**
- Produces: `fetch.RangeGetter` (interface `GetRange(ctx, bucket, key string, start, end int64) ([]byte, error)`),
  `fetch.FetchRecord(ctx, g RangeGetter, bucket, key string, offset, length int64) (http.Header, []byte, error)`,
  `fetch.NewS3Getter(ctx, region string) (RangeGetter, error)`, `fetch.NewHTTPGetter(base string, concurrency int) RangeGetter`.

- [ ] **Step 1: Move the files**
```bash
cd commoncrawl/cc-enrich-worker
mkdir -p internal/fetch
git mv fetch.go internal/fetch/fetch.go
git mv fetch_test.go internal/fetch/fetch_test.go
```

- [ ] **Step 2: Rename the package + export the interface**

In `internal/fetch/fetch.go` and `internal/fetch/fetch_test.go`, change the first line to `package fetch`.
In `fetch.go` rename the exported interface and return types:
- `type rangeGetter interface {` → `type RangeGetter interface {`
- `func FetchRecord(ctx context.Context, g rangeGetter, ...)` → `g RangeGetter`
- `func NewS3Getter(ctx context.Context, region string) (s3Getter, error)` → `(RangeGetter, error)`
- `func NewHTTPGetter(base string, concurrency int) httpRangeGetter` → `RangeGetter`
- In `fetch_test.go`, any reference to `rangeGetter` → `RangeGetter`; if the test referenced `ccBucket`,
  inline the literal `"commoncrawl"` (that const moves to `worker` in Task 9; `fetch` must not depend on it).

- [ ] **Step 3: Qualify references in the remaining `package main` files**

In `worker.go` and `main.go`, add `"cc-enrich-worker/internal/fetch"` to imports and replace:
- `rangeGetter` → `fetch.RangeGetter`
- `FetchRecord(` → `fetch.FetchRecord(`
- `NewS3Getter(` → `fetch.NewS3Getter(`, `NewHTTPGetter(` → `fetch.NewHTTPGetter(`
In `main.go`, the assignment `getter = g` now works because `NewS3Getter` returns `fetch.RangeGetter`.
The `var getter rangeGetter` declaration in `main.go` becomes `var getter fetch.RangeGetter`.
In `worker_test.go`, the fake getter type stays (it structurally implements `fetch.RangeGetter`); update the
`ProcessShard` param type reference if present.

- [ ] **Step 4: Build, vet, test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass (`ok cc-enrich-worker`, `ok cc-enrich-worker/internal/fetch`).

- [ ] **Step 5: Commit**
```bash
git add internal/fetch fetch.go fetch_test.go worker.go main.go worker_test.go
git commit -m "refactor(worker): extract internal/fetch

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Extract `internal/embed`

**Files:**
- Move: `embed.go` → `internal/embed/embed.go`, `embed_test.go` → `internal/embed/embed_test.go`
- Modify: `main.go` (qualify `embed.` + import), `consts.go` (move two consts)

**Interfaces:**
- Produces: `embed.EmbedClient` (method `Embed(texts []string, instruction string) ([][]float32, error)`),
  `embed.NewEmbedClient(baseURL, model string, batch, concurrency int) *EmbedClient`.

- [ ] **Step 1: Move the files + relocate the embed constants**
```bash
mkdir -p internal/embed
git mv embed.go internal/embed/embed.go
git mv embed_test.go internal/embed/embed_test.go
```
In `consts.go`, **delete** the `embedMaxChars` and `embedBatch` lines and add them to `internal/embed/embed.go`
(as package-level consts in `package embed`):
```go
const (
	embedMaxChars = 2000
	embedBatch    = 16
)
```
(Use the exact current values from `consts.go` — copy them verbatim before deleting.)

- [ ] **Step 2: Rename the package**

First line of both moved files → `package embed`. `embed.go` already uses `embedMaxChars`/`embedBatch`
internally, so no qualification needed there.

- [ ] **Step 3: Qualify references in `main.go`**

Add `"cc-enrich-worker/internal/embed"`; replace `NewEmbedClient(` → `embed.NewEmbedClient(` and the
`var emb embedderIface` stays (worker owns that interface — Task 9; for now `emb` is assigned
`embed.NewEmbedClient(...)` which returns `*embed.EmbedClient`). If `main.go` declares `emb` with a type,
make it `var emb *embed.EmbedClient` temporarily; it will be passed to `ProcessShard` and `VerifyReference`
which take their own interfaces.

- [ ] **Step 4: Build, vet, test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass, incl. `ok cc-enrich-worker/internal/embed`.

- [ ] **Step 5: Commit**
```bash
git add internal/embed embed.go embed_test.go consts.go main.go
git commit -m "refactor(worker): extract internal/embed

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Extract `internal/parse`

**Files:**
- Move: `parse.go` → `internal/parse/parse.go`, `parse_test.go` → `internal/parse/parse_test.go`
- Modify: `worker.go` (qualify `parse.` + import)

**Interfaces:**
- Produces: `parse.ParseHTML(htmlStr string) (text string, emails []string, socials []string)`.

- [ ] **Step 1: Move the files**
```bash
mkdir -p internal/parse
git mv parse.go internal/parse/parse.go
git mv parse_test.go internal/parse/parse_test.go
```

- [ ] **Step 2: Rename the package**

First line of both → `package parse`. `extractSocials`/`dedupe` stay unexported (used only inside `parse`).

- [ ] **Step 3: Qualify references in `worker.go`**

Add `"cc-enrich-worker/internal/parse"`; replace `ParseHTML(` → `parse.ParseHTML(`.

- [ ] **Step 4: Build, vet, test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass, incl. `ok cc-enrich-worker/internal/parse`.

- [ ] **Step 5: Commit**
```bash
git add internal/parse parse.go parse_test.go worker.go
git commit -m "refactor(worker): extract internal/parse

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Extract `internal/output`

**Files:**
- Move: `output.go` → `internal/output/output.go` (no `output_test.go` exists)
- Modify: `worker.go`, `main.go` (qualify `output.` + import)

**Interfaces:**
- Produces: `output.DomainRow`, `output.TechRow`, `output.IdentifierRow`, `output.ProfileRow` (struct types),
  `output.WriteDomains/WriteTech/WriteIdentifiers/WriteProfiles(path string, rows []T) error`,
  `output.UploadToS3(ctx, client *s3.Client, bucket, key, path string) error`.

- [ ] **Step 1: Move the file**
```bash
mkdir -p internal/output
git mv output.go internal/output/output.go
```

- [ ] **Step 2: Rename the package**

First line → `package output`. The row structs and `Write*`/`UploadToS3` are already exported.

- [ ] **Step 3: Qualify references in `worker.go` and `main.go`**

Add `"cc-enrich-worker/internal/output"`. In `worker.go`, `ShardResult` fields and the row construction
become `output.DomainRow`, `output.TechRow`, `output.IdentifierRow`, `output.ProfileRow` (everywhere those
type names appear). In `main.go`, `WriteDomains(` → `output.WriteDomains(` etc.; `var profiles []ProfileRow`
→ `[]output.ProfileRow`; likewise `domains []DomainRow`, `tech []TechRow`, `idents []IdentifierRow`.
In `worker_test.go`, `WriteDomains`/`WriteTech` and any `DomainRow`/`TechRow` references get the `output.` prefix.

- [ ] **Step 4: Build, vet, test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass (the `worker_test.go` parquet round-trip still passes via `output.DomainRow`).

- [ ] **Step 5: Commit**
```bash
git add internal/output output.go worker.go main.go worker_test.go
git commit -m "refactor(worker): extract internal/output

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Extract `internal/model` (shared types)

**Files:**
- Create: `internal/model/model.go`
- Modify: `types.go` (delete — its 5 structs move), `lei.go`, `profile.go`, `worker.go`, `classify.go`,
  `reference.go`, `tech.go`, `check.go` (qualify `model.` + import where they reference a moved type)

**Interfaces:**
- Produces: `model.Technology`, `model.Reference`, `model.Prototypes`, `model.DomainResult`, `model.PageFetch`,
  `model.Identifier`, `model.CompanyProfile`, `model.WorklistItem` (struct types only — no methods, no logic).

- [ ] **Step 1: Create the model package with the shared structs**

Create `internal/model/model.go` as `package model`. Move into it, **verbatim**, these struct definitions
(cut them from their current files):
- `Technology` (from `types.go`)
- `Reference` (from `types.go`)
- `Prototypes` (from `types.go`)
- `PageFetch` (from `types.go`)
- `DomainResult` (from `types.go`)
- `Identifier` (from `lei.go` — the `type Identifier struct {...}`)
- `CompanyProfile` (from `profile.go` — the `type CompanyProfile struct {...}`; **leave** the
  `func (p CompanyProfile) Empty()` method behind in `profile.go`/`extract` — a method needs its receiver
  type's package, so move `Empty()` to `model` too, OR keep it in `extract` as a free helper. Decision:
  **move `Empty()` into `model.go`** next to `CompanyProfile` so `extract` and `worker` can both call it.)
- `WorklistItem` (from `worker.go`)

Delete the now-empty `types.go` (`git rm types.go`).

- [ ] **Step 2: Add imports + qualify in every consumer**

Each file that still references one of the moved types adds `"cc-enrich-worker/internal/model"` and qualifies:
- `worker.go`: `Technology`→`model.Technology`, `Identifier`→`model.Identifier`,
  `CompanyProfile`→`model.CompanyProfile`, `DomainResult`→`model.DomainResult`, `Reference`→`model.Reference`,
  `Prototypes`→`model.Prototypes`, `WorklistItem`→`model.WorklistItem`. The `domainAgg` struct fields
  (`tech []Technology`, `identifiers []Identifier`, `profile CompanyProfile`) get the `model.` prefix.
- `lei.go`/`vat.go`: return `[]model.Identifier`; `Identifier{...}` literals → `model.Identifier{...}`.
- `profile.go`: returns `(model.CompanyProfile, []model.Identifier)`; literals qualified; the `Empty()` call
  sites use the value's method (still works after the method moved to `model`).
- `classify.go`: `Classify(... ref *Reference, protos *Prototypes) DomainResult` →
  `*model.Reference`, `*model.Prototypes`, `model.DomainResult`; build `model.DomainResult{...}`.
- `reference.go`: `BuildReference(...) *Reference` → `*model.Reference`; `BuildPrototypes` → `*model.Prototypes`;
  `LoadReference(...) (*model.Reference, *model.Prototypes, error)`.
- `tech.go`: `DetectTech(...) []Technology` → `[]model.Technology`; `techfast.go` `Detect(...) []Technology`
  and the `fpat`/build code that constructs `Technology{...}` → `model.Technology{...}`.
- `check.go`: no type move, but it lives next to classify — no change yet.
- `worker_test.go`, `classify_test.go`, `lei_test.go`, `vat_test.go`, `profile_test.go`, `reference_test.go`,
  `techfast_test.go`: qualify any moved-type references with `model.` (e.g. `Reference{...}` → `model.Reference{...}`,
  `Identifier`→`model.Identifier`). These are still in `package main` for now, so they import `model`.

- [ ] **Step 3: Build, vet, test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass. This is the highest-touch task; if the build fails, it is almost always a missed
qualification — `go build ./...` names the file:line.

- [ ] **Step 4: Commit**
```bash
git add internal/model types.go lei.go vat.go profile.go worker.go classify.go reference.go tech.go techfast.go \
        worker_test.go classify_test.go lei_test.go vat_test.go profile_test.go reference_test.go techfast_test.go
git commit -m "refactor(worker): extract internal/model shared types

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Extract `internal/tech`

**Files:**
- Move: `tech.go`, `techfast.go`, `literal.go` (+ `techfast_test.go`, `literal_test.go`) → `internal/tech/`
- Modify: `worker.go` (qualify `tech.` + import)

**Interfaces:**
- Produces: `tech.DetectTech(headers map[string][]string, body []byte) []model.Technology`,
  `tech.NewFastMatcher() (*FastMatcher, error)`, `tech.FastMatcher` (method `Detect(headers, body) []model.Technology`).

- [ ] **Step 1: Move the files**
```bash
mkdir -p internal/tech
git mv tech.go internal/tech/tech.go
git mv techfast.go internal/tech/techfast.go
git mv literal.go internal/tech/literal.go
git mv techfast_test.go internal/tech/techfast_test.go
git mv literal_test.go internal/tech/literal_test.go
```

- [ ] **Step 2: Rename the package + keep model qualification**

First line of all five → `package tech`. They already use `model.Technology` (from Task 5) and import `model`;
verify the import line is present in each `.go` that references it. `fpat`, `rawApp`, `parseImplies`,
`mustParse`, `longestRequiredLiteral`, `requiredLiteral` stay unexported (used only within `tech`).

- [ ] **Step 3: Qualify references in `worker.go`**

Add `"cc-enrich-worker/internal/tech"`; replace `DetectTech(` → `tech.DetectTech(`. If `worker.go` builds a
`FastMatcher` directly, qualify `NewFastMatcher` → `tech.NewFastMatcher`; otherwise `DetectTech` is the only
entry point.

- [ ] **Step 4: Build, vet, test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass, incl. `ok cc-enrich-worker/internal/tech`.

- [ ] **Step 5: Commit**
```bash
git add internal/tech tech.go techfast.go literal.go techfast_test.go literal_test.go worker.go
git commit -m "refactor(worker): extract internal/tech

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Extract `internal/extract`

**Files:**
- Move: `lei.go`, `vat.go`, `profile.go` (+ `lei_test.go`, `vat_test.go`, `profile_test.go`) → `internal/extract/`
- Modify: `worker.go` (qualify `extract.` + import)

**Interfaces:**
- Produces: `extract.ExtractLEIs(body []byte) []model.Identifier`,
  `extract.ExtractVATs(body []byte) []model.Identifier`,
  `extract.ExtractProfile(body []byte) (model.CompanyProfile, []model.Identifier)`.

- [ ] **Step 1: Move the files**
```bash
mkdir -p internal/extract
git mv lei.go internal/extract/lei.go
git mv vat.go internal/extract/vat.go
git mv profile.go internal/extract/profile.go
git mv lei_test.go internal/extract/lei_test.go
git mv vat_test.go internal/extract/vat_test.go
git mv profile_test.go internal/extract/profile_test.go
```

- [ ] **Step 2: Rename the package**

First line of all six → `package extract`. They already use `model.Identifier`/`model.CompanyProfile`
(from Task 5). `validLEI`, `validVAT`, `luhn`, `vatCheckDE`, `identValid`, `walkLD`, `isOrg`, `ldString`,
`ldStrings`, `ldNumber`, `parseYear`, `countryOf` stay unexported.

- [ ] **Step 3: Qualify references in `worker.go`**

Add `"cc-enrich-worker/internal/extract"`; replace `ExtractLEIs(` → `extract.ExtractLEIs(`,
`ExtractVATs(` → `extract.ExtractVATs(`, `ExtractProfile(` → `extract.ExtractProfile(`.

- [ ] **Step 4: Build, vet, test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass, incl. `ok cc-enrich-worker/internal/extract`.

- [ ] **Step 5: Commit**
```bash
git add internal/extract lei.go vat.go profile.go lei_test.go vat_test.go profile_test.go worker.go
git commit -m "refactor(worker): extract internal/extract (lei/vat/profile)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Extract `internal/classify`

**Files:**
- Move: `reference.go`, `classify.go`, `signals.go`, `check.go` (+ `classify_test.go`, `reference_test.go`,
  `check_test.go`, `signals_test.go` if present) → `internal/classify/`
- Modify: `worker.go`, `main.go` (qualify `classify.` + import), `consts.go` (move four consts)

**Interfaces:**
- Produces: `classify.LoadReference(ctx, conn driver.Conn) (*model.Reference, *model.Prototypes, error)`,
  `classify.Classify(page []float32, ref *model.Reference, protos *model.Prototypes) model.DomainResult`,
  `classify.MatchSignal(text string) string`,
  `classify.LoadReferenceMeta(ctx, conn driver.Conn) (ReferenceMeta, error)`,
  `classify.VerifyReference(meta ReferenceMeta, modelName string, emb Embedder) error`,
  `classify.ReferenceMeta` (struct), `classify.Embedder` (interface).

- [ ] **Step 1: Move the files + relocate the classify constants**
```bash
mkdir -p internal/classify
git mv reference.go internal/classify/reference.go
git mv classify.go internal/classify/classify.go
git mv signals.go internal/classify/signals.go
git mv check.go internal/classify/check.go
git mv classify_test.go internal/classify/classify_test.go
git mv reference_test.go internal/classify/reference_test.go
git mv check_test.go internal/classify/check_test.go
# signals_test.go: move only if it exists
git ls-files signals_test.go && git mv signals_test.go internal/classify/signals_test.go || true
```
In `consts.go`, **delete** `marginThreshold`, `pageTypeThreshold`, `confTopK`, `confTemp` and add them to
`internal/classify/classify.go` (verbatim values):
```go
const (
	marginThreshold   = 0.03
	pageTypeThreshold = 0.55
	confTopK          = 10
	confTemp          = 1.0
)
```

- [ ] **Step 2: Rename the package + fix the two gotchas**

First line of all moved files → `package classify`. In `check.go`:
- Define the consumer interface in this package:
  ```go
  type Embedder interface {
  	Embed(texts []string, instruction string) ([][]float32, error)
  }
  ```
- Change `VerifyReference(meta ReferenceMeta, model string, emb embedderIface)` to
  `VerifyReference(meta ReferenceMeta, modelName string, emb Embedder)` and replace the body's `model`
  references with `modelName` (this avoids shadowing the imported `model` package — which `reference.go`
  imports for `model.Reference`).
- In `check_test.go`, the fake embedder type stays (structurally implements `Embedder`); update the
  `VerifyReference(good, "qwen3-8b", emb8)` calls — the signature is unchanged at call sites (still
  `(meta, string, emb)`), only the param name changed, so no call-site edits needed.

- [ ] **Step 3: Qualify references in `worker.go` and `main.go`**

Add `"cc-enrich-worker/internal/classify"`:
- `worker.go`: `Classify(` → `classify.Classify(`, `MatchSignal(` → `classify.MatchSignal(`.
- `main.go`: `LoadReference(` → `classify.LoadReference(`, `LoadReferenceMeta(` → `classify.LoadReferenceMeta(`,
  `VerifyReference(` → `classify.VerifyReference(`; `meta, metaErr := classify.LoadReferenceMeta(...)`.

- [ ] **Step 4: Build, vet, test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass, incl. `ok cc-enrich-worker/internal/classify` (TestClassify*, TestConfidenceScore,
TestVerifyReference).

- [ ] **Step 5: Commit**
```bash
git add internal/classify reference.go classify.go signals.go check.go classify_test.go reference_test.go \
        check_test.go consts.go worker.go main.go
git commit -m "refactor(worker): extract internal/classify (reference/classify/signals/check)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Extract `internal/worker`

**Files:**
- Move: `worker.go` → `internal/worker/worker.go`, `worker_test.go` → `internal/worker/worker_test.go`
- Modify: `main.go` (qualify `worker.` + import); fold the now-tiny `consts.go` into `internal/worker`

**Interfaces:**
- Produces: `worker.ProcessShard(ctx, items []model.WorklistItem, getter fetch.RangeGetter, emb embedderIface,
  ref *model.Reference, protos *model.Prototypes, cfg ShardConfig) (ShardResult, error)`,
  `worker.ShardResult` (fields `Domains []output.DomainRow`, `Tech []output.TechRow`,
  `Identifiers []output.IdentifierRow`, `Profiles []output.ProfileRow`), `worker.ShardConfig`.

- [ ] **Step 1: Move worker + the remaining consts**
```bash
mkdir -p internal/worker
git mv worker.go internal/worker/worker.go
git mv worker_test.go internal/worker/worker_test.go
```
`consts.go` now holds only `ccBucket` and `techMaxBytes` (the others moved in Tasks 2 and 8). Move them into
`internal/worker/worker.go` and `git rm consts.go`:
```go
const (
	ccBucket     = "commoncrawl"
	techMaxBytes = 131072
)
```
(Copy the exact current values before removing `consts.go`.)

- [ ] **Step 2: Rename the package + keep the local embedder interface**

First line of `worker.go`/`worker_test.go` → `package worker`. Keep `embedderIface` defined here
(`Embed([]string, string) ([][]float32, error)`). `worker.go` already imports `model`, `fetch`, `output`
(Tasks 1/4/5); add `"cc-enrich-worker/internal/parse"`, `".../tech"`, `".../extract"`, `".../classify"` and
keep their qualifications. `subdomainOf` and `domainAgg` stay unexported.

- [ ] **Step 3: Qualify references in `main.go`**

Add `"cc-enrich-worker/internal/worker"`; replace `ProcessShard(` → `worker.ProcessShard(`,
`ShardConfig{` → `worker.ShardConfig{`. `res.Domains`/`res.Tech`/`res.Identifiers`/`res.Profiles` are unchanged
(fields on the returned `worker.ShardResult`).

- [ ] **Step 4: Build, vet, test**
```bash
go vet ./... && go build ./... && go test ./...
```
Expected: all pass, incl. `ok cc-enrich-worker/internal/worker` (TestProcessShard*).

- [ ] **Step 5: Commit**
```bash
git add internal/worker worker.go worker_test.go consts.go main.go
git commit -m "refactor(worker): extract internal/worker; main.go is the only package main

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Verify the final layout + update the README

**Files:**
- Modify: `README.md` (source-layout section)

**Interfaces:** none (verification + docs).

- [ ] **Step 1: Confirm only `main.go` is at the root and it is `package main`**
```bash
ls *.go                       # expect: main.go only
head -1 main.go               # expect: package main
ls internal/                  # expect: classify embed extract fetch model output parse tech worker
```

- [ ] **Step 2: Full build, vet, race-test, and a binary build**
```bash
go vet ./...
go build ./...
go test ./...
CGO_ENABLED=0 go build -o /tmp/ccw . && echo BUILD_OK
```
Expected: all pass; `BUILD_OK`.

- [ ] **Step 3: Update the README source-layout table**

In `README.md`, replace the flat file list in the source-layout section with the package layout:
```markdown
| `main.go` | entry point — flags, env, ClickHouse conn, `readWorklist`, orchestration |
| `internal/model/` | shared data structs (Technology, Reference, Identifier, CompanyProfile, …) |
| `internal/fetch/` | byte-range WARC fetch (S3 / HTTP CDN) |
| `internal/embed/` | OpenAI-compatible embedding client |
| `internal/parse/` | HTML → text / emails / socials |
| `internal/tech/` | Wappalyzer + the Aho-Corasick fast matcher |
| `internal/extract/` | LEI / VAT / schema.org-profile extraction |
| `internal/classify/` | NACE reference, classify + confidence, page-type signals, startup check |
| `internal/output/` | Parquet row structs + writers, S3 upload |
| `internal/worker/` | `ProcessShard` orchestration |
```

- [ ] **Step 4: Commit**
```bash
git add README.md
git commit -m "docs(worker): document the internal/ package layout

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review notes

- **Coverage:** every current `.go` file has a destination — fetch(1), embed(2), parse(3), output(4),
  types→model(5), tech+techfast+literal(6), lei+vat+profile→extract(7), reference+classify+signals+check(8),
  worker(9), main stays. `consts.go` is distributed (embed:2, classify:8, worker:9) and removed. Every test
  file moves with its source.
- **Cycle freedom:** verified against the dependency graph — `model` and the four leaf packages
  (fetch/embed/parse/output) import nothing internal; tech/extract/classify import only `model`; `worker`
  imports the leaves + model; `main` imports worker + leaves. No back-edges.
- **Gotchas captured:** the `model`-package shadowing in `VerifyReference` (param → `modelName`, Task 8); the
  getter constructors returning the `RangeGetter` interface (Task 1); the `CompanyProfile.Empty()` method
  moving to `model` with its receiver (Task 5).
- **Behavior unchanged:** no task adds/removes a test assertion; the existing suite is the gate after every task.
- **Right-sizing:** each task is one package with its tests, ends green, and is independently reviewable
  (a reviewer can reject "extract tech" without touching "extract extract").
