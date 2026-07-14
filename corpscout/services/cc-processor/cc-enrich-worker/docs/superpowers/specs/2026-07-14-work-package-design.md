# internal/work — one API for "what remains to be done"

**Date:** 2026-07-14
**Status:** approved
**Scope:** new package `internal/work`; callers in `cmd/cc-enrich-worker` refactored onto it. No
behavior change — this is a consolidation refactor. Step (1) of the lean tech-mode restructure;
step (2) (fetch→channel→process pipeline) is a separate later spec.

## Problem

The knowledge of "what work remains for this run" is smeared across the CLI package:

- catalog reads: `catalog.LoadPartStats` + present/empty classification inline in `runRange`;
  `warcinput.LoadPlan` + `primaryPagesOnly` derivation inline in `openInput`
- completion state: `markers.Exists` checks inline in `runPartAttempt` and `runRange`'s skip logic
- preservation: `preserveStaleDir` + `completedEmbedding` + `parquetRows` in `cmd`
- path layout: `outDirFor` closure in `runRange`, the same join duplicated in `run()`, and the
  embedding-tree sibling derivation duplicated twice in `processInput`
- preflight: `requireLocalCatalog` in `cmd`

Every new consumer (the planned pipeline restructure, a future `status` unification) would have to
re-assemble these pieces. The command→tree mapping is fully determined at startup by
`(base, crawlID, selection, cmd)`, so one package can own the whole question.

## Design

### API (`internal/work`)

```go
// Package work answers "what remains to be done for this run" by composing the read-only catalog,
// the marker lifecycle, and the output-tree layout. It owns NO scheduling and NO mutation of
// outputs — the runner decides what to do with the answers.

type Status int

const (
    Pending   Status = iota // has pages, no marker: needs producing
    Empty                   // no pages in the catalog: nothing to do, ever
    Produced                // .produced marker present: done
    Preserved               // complete-but-unmarked output (.loaded, or a complete embed file): do not touch
)

type Part struct {
    Index  uint32
    Status Status
}

// Open binds a run identity. cmd is one of tech|industry|embed|both (anything else errors).
// It fails when the local catalog is absent, with an error naming the exact sync-db command —
// produce runs never sync the catalog (sync-db is the explicit step).
func Open(base, crawlID, selection, cmd string) (*Run, error)

func (r *Run) CatalogPath() string

// Parts classifies every part in [lo,hi]: one LoadPartStats query + one marker/output probe per
// present part. This is the PLANNING view (see Status below for the dispatch-time re-check).
func (r *Run) Parts(ctx context.Context, lo, hi uint32) ([]Part, error)

// Status re-checks one known non-empty part's completion state from the filesystem only (marker,
// then preservation probe) — the cheap, authoritative gate the runner calls just before producing,
// because another host may have produced the part after Parts() ran (rsync/NFS marker arrival).
// It never touches the catalog and never returns Empty.
func (r *Run) Status(part uint32) Status

// Plan loads one part's validated page selection. primaryPagesOnly is derived from cmd
// (industry|embed => true). Only meaningful for Pending parts.
func (r *Run) Plan(part uint32) (warcinput.Plan, error)

// OutDir is the single source of truth for the part's output directory:
//   <base>/<crawlID>/warc/<selection>/out_<cmd>_<part>
// (run()'s --out flag remains a caller-side override for single-part debug runs.)
func (r *Run) OutDir(part uint32) string

// EmbedDirFor is the sibling embedding tree for an ACTUAL output directory:
//   <base>/<crawlID>/embedding/warc/<selection>/<basename(outDir)>
// It takes the outDir (not the part) because single-part runs can override --out, and the
// embedding sibling follows the override's basename — exactly today's behavior.
func (r *Run) EmbedDirFor(outDir string) string

// CompletedEmbedding reports whether dir already holds a complete vector file (embeddings.parquet
// or embeddings_fp16.parquet, including the .empty-marker and undecodable-fp16 fallbacks). Moved
// verbatim from cmd's completedEmbedding/parquetRows; exported because run()'s embed
// verify-and-skip uses it directly.
func CompletedEmbedding(dir string) (path string, rows int64, complete bool)
```

### Status semantics (byte-identical to today's behavior)

- `Empty`: no `PartStats` row for the index — `runRange` skips it before pooling, exactly as the
  current present/empty classification does.
- `Produced`: `markers.Exists(markers.ProducedPath(outDir))`.
- `Preserved`: current `preserveStaleDir` logic moved verbatim — a `.loaded` marker preserves for
  every cmd; a complete embed file preserves for `cmd == "embed"` only. Evaluation order mirrors
  today's: the `.produced` marker is checked first, and the preservation probe runs only when the
  output directory exists.
- `Pending`: everything else. A Pending part with a non-empty output dir is crashed-produce
  debris; **wiping it stays in the runner** (mutation), keyed off `Pending`.

### Caller refactor (no behavior change)

- `runRange`: `work.Open` replaces `requireLocalCatalog` + inline `LoadPartStats` classification.
- `runRangePool`/`runPartAttempt`: the `outDirFor func(uint32) string` parameter is REPLACED by the
  `*work.Run` — one source of truth for paths and status. Tests build a real `work.Run` against
  their fixture catalogs (they already write them), so injection still works.
- `runPartAttempt`: marker + preserve checks become `r.Status(part)` (Produced → skip, Preserved →
  skip with today's log line, Pending → wipe-if-dirty then produce).
- `run()` (single `--part`): preflight via `work.Open`; default outDir via `r.OutDir`; embed
  verify-and-skip via `work.CompletedEmbedding`.
- `openInput`: `r.Plan(part)` replaces the inline `warcinput.LoadPlan` + `primaryPagesOnly`
  derivation.
- `processInput`: both duplicated embedding-dir joins become `r.EmbedDirFor(outDir)`.
- Deleted from `cmd`: `requireLocalCatalog`, `preserveStaleDir`, `completedEmbedding`,
  `parquetRows` (all moved), plus the duplicated path joins.
- `partDeps` gains the `*work.Run` so producePart/openInput/processInput reach it.

Out of scope (follow-ups, not this change): `status` command unification onto `work`;
`plancmd`-style sizing reports; the fetch→channel→process pipeline (step 2).

### Dependencies

`work` imports `catalog`, `markers`, `warcinput`, `parquet-go` (for CompletedEmbedding). `cmd`
imports `work`. `warcinput` already imports `catalog`; no cycles.

## Testing

- Unit tests for `work` using the existing DuckDB fixture pattern (`writeFixtureCatalog` shape) and
  real marker/parquet files in `t.TempDir()`: each Status arm (Pending / Empty / Produced /
  Preserved-via-.loaded / Preserved-via-embed-file scoped to cmd=embed), `Open` failing with the
  sync-db hint on a missing catalog, `Plan`'s primaryPagesOnly derivation per cmd, `OutDir`/
  `EmbedDir` exact paths, `CompletedEmbedding` fallbacks (.empty marker, undecodable fp16).
- Existing `cmd` tests must pass unchanged in assertion terms: `runrange_test.go` keeps proving
  skip/preserve/wipe/produce through the pool; `TestPreserveStaleDir` moves to `work` as the
  Status/Preserved tests.

## Non-goals

- No scheduling, retry, or breaker logic in `work`.
- No output mutation (wiping stale dirs stays in the runner).
- No change to marker file formats, path layouts, log lines, or exit codes.
