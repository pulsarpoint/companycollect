# Companysource Module Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `companies/companysource` the only Go code module under `companies` by moving shared helpers into it and adding module-local README/Makefile entry points.

**Architecture:** `companies/companysource` owns one binary at `cmd/companysource`, all source packages, and the shared helpers they use. `companies/data`, `companies/analysis`, and `companies/docs` stay outside the Go module because they are data, research, and documentation rather than active package code.

**Tech Stack:** Go 1.26, `go test`, `gofmt`, `github.com/cockroachdb/errors`, `parquet-go`, Make.

---

## File Structure

- Move `companies/common/companysource` to `companies/companysource/common/companysource`.
- Move `companies/common/countryimport` to `companies/companysource/common/countryimport`.
- Remove `companies/common/go.mod` and `companies/common/go.sum`.
- Modify all imports from `github.com/pulsarpoint/companycollect/companies/common/...` to `github.com/pulsarpoint/companycollect/companies/companysource/common/...`.
- Modify `companies/companysource/go.mod` to remove the `companies/common` module dependency and replace.
- Create `companies/companysource/README.md`.
- Create `companies/companysource/Makefile`.
- Modify `corpscout/docs/companysource.md` to point to `companies/companysource/Makefile`.

## Task 1: Move Shared Helpers Into Companysource

**Files:**
- Move: `companies/common/companysource/*` to `companies/companysource/common/companysource/*`
- Move: `companies/common/countryimport/*` to `companies/companysource/common/countryimport/*`
- Delete: `companies/common/go.mod`
- Delete: `companies/common/go.sum`

- [ ] **Step 1: Move directories**

```bash
mkdir -p companies/companysource/common
git mv companies/common/companysource companies/companysource/common/companysource
git mv companies/common/countryimport companies/companysource/common/countryimport
git rm companies/common/go.mod companies/common/go.sum
rmdir companies/common
```

- [ ] **Step 2: Verify moved files exist**

```bash
test -f companies/companysource/common/countryimport/manifest.go
test -f companies/companysource/common/companysource/runfolder.go
```

Expected: both commands exit 0.

## Task 2: Update Imports And Module Dependencies

**Files:**
- Modify: `companies/companysource/**/*.go`
- Modify: `companies/companysource/go.mod`
- Modify: `companies/companysource/go.sum`

- [ ] **Step 1: Replace imports**

```bash
rg -l 'github.com/pulsarpoint/companycollect/companies/common/' companies/companysource \
  | xargs perl -0pi -e 's#github.com/pulsarpoint/companycollect/companies/common/#github.com/pulsarpoint/companycollect/companies/companysource/common/#g'
```

- [ ] **Step 2: Tidy module**

```bash
cd companies/companysource
GOWORK=off go mod tidy
```

Expected: exits 0 and removes the `github.com/pulsarpoint/companycollect/companies/common` requirement and replace.

- [ ] **Step 3: Format and test**

```bash
cd companies/companysource
gofmt -w $(find . -name '*.go' -print)
GOWORK=off go test ./...
```

Expected: all packages pass.

## Task 3: Add Module README And Makefile

**Files:**
- Create: `companies/companysource/README.md`
- Create: `companies/companysource/Makefile`
- Modify: `corpscout/docs/companysource.md`

- [ ] **Step 1: Create README**

Create `companies/companysource/README.md` with:

```markdown
# Companysource

`companysource` is the single active company-source ingestion module under
`companies`. It builds one binary from `cmd/companysource` and contains all
source-specific packages plus shared ingestion helpers.

## Layout

```text
cmd/companysource/        CLI entry point for the single binary
common/                   shared source-agnostic helpers
internal/                 CLI, registry, source contract, ClickHouse support
sources/                  country/source implementations
```

Runtime data lives outside the Go module under `../data`.

## Commands

```bash
make test
make build
make list-sources
```

Use the binary directly:

```bash
bin/companysource list-sources
bin/companysource export-parquet --country finland --source prhytj --run-dir ../data/finland/sources/prhytj/runs/<run-id>
```
```

- [ ] **Step 2: Create Makefile**

Create `companies/companysource/Makefile` with:

```make
.PHONY: test build list-sources clean

BINARY ?= bin/companysource

test:
	GOWORK=off go test ./...

build:
	GOWORK=off go build -o $(BINARY) ./cmd/companysource

list-sources:
	GOWORK=off go run ./cmd/companysource list-sources

clean:
	rm -rf bin
```

- [ ] **Step 3: Update Corpscout docs**

In `corpscout/docs/companysource.md`, add:

```markdown
From `companies/companysource`, the common development commands are:

```bash
make test
make build
make list-sources
```
```

- [ ] **Step 4: Verify Makefile**

```bash
cd companies/companysource
make test
make build
make list-sources
```

Expected: tests pass, `bin/companysource` is created, source list includes Finland and US sources.

## Task 4: Final Verification And Commit

**Files:**
- All moved and modified files.

- [ ] **Step 1: Check old common module is gone**

```bash
test ! -d companies/common
test ! -f companies/common/go.mod
```

Expected: both commands exit 0.

- [ ] **Step 2: Check no old imports remain**

```bash
! rg 'github.com/pulsarpoint/companycollect/companies/common/' companies/companysource
```

Expected: exits 0.

- [ ] **Step 3: Check git status**

```bash
git status --short -uall
```

Expected: only planned move/docs/Makefile changes.

- [ ] **Step 4: Commit**

```bash
git add companies/companysource corpscout/docs/companysource.md
git commit -m "refactor: consolidate companysource module"
```
