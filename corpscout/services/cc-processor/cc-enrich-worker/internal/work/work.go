// Package work answers "what remains to be done for this run" by composing the read-only catalog,
// the marker lifecycle, and the output-tree layout. It owns NO scheduling and NO mutation of
// outputs — the runner decides what to do with the answers.
package work

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/markers"
	"cc-enrich-worker/internal/warcinput"
)

// Status classifies one part's completion state for a run.
type Status int

const (
	Pending   Status = iota // has pages, no marker: needs producing
	Empty                   // no pages in the catalog: nothing to do, ever
	Produced                // .produced marker present: done
	Preserved               // complete-but-unmarked output (.loaded, or a complete embed file): do not touch
)

// Part is one classified part of a planning sweep.
type Part struct {
	Index  uint32
	Status Status
}

// Run binds one run identity — (base, crawlID, selection, cmd) — to the local catalog and the
// output tree. All methods are safe for concurrent use (the fields are immutable after Open).
type Run struct {
	base, crawlID, selection, cmd string
	primaryPagesOnly              bool
	catalogPath                   string
}

// Open binds a run identity. cmd is one of tech|industry|embed|both (anything else errors).
// It fails when the local catalog is absent, with an error naming the exact sync-db command —
// produce runs never sync the catalog (sync-db is the explicit step).
func Open(base, crawlID, selection, cmd string) (*Run, error) {
	var primaryPagesOnly bool
	switch cmd {
	case "industry", "embed":
		primaryPagesOnly = true
	case "tech", "both":
		primaryPagesOnly = false
	default:
		return nil, errors.Newf("unknown command %q (want tech|industry|embed|both)", cmd)
	}
	path := filepath.Join(base, crawlID, "warc-index", selection, "catalog.duckdb")
	if _, err := os.Stat(path); err != nil {
		return nil, errors.Wrapf(err,
			"local catalog missing at %s — run `cc-enrich-worker sync-db --crawl-id %s --selection %s --base %s` first",
			path, crawlID, selection, base)
	}
	return &Run{
		base: base, crawlID: crawlID, selection: selection, cmd: cmd,
		primaryPagesOnly: primaryPagesOnly, catalogPath: path,
	}, nil
}

// CatalogPath is the local read-only DuckDB file every catalog query uses.
func (r *Run) CatalogPath() string { return r.catalogPath }

// OutDir is the single source of truth for the part's output directory:
//
//	<base>/<crawlID>/warc/<selection>/out_<cmd>_<part>
//
// (A single --part run's --out flag remains a caller-side override.)
func (r *Run) OutDir(part uint32) string {
	return filepath.Join(r.base, r.crawlID, "warc", r.selection, fmt.Sprintf("out_%s_%d", r.cmd, part))
}

// EmbedDirFor is the sibling embedding tree for an ACTUAL output directory:
//
//	<base>/<crawlID>/embedding/warc/<selection>/<basename(outDir)>
//
// It takes the outDir (not the part) because single-part runs can override --out, and the
// embedding sibling follows the override's basename.
func (r *Run) EmbedDirFor(outDir string) string {
	return filepath.Join(r.base, r.crawlID, "embedding", "warc", r.selection, filepath.Base(outDir))
}

// Status re-checks one known non-empty part's completion state from the filesystem only (marker,
// then preservation probe) — the cheap, authoritative gate the runner calls just before producing,
// because another host may have produced the part after Parts ran (rsync/NFS marker arrival).
// It never touches the catalog and never returns Empty.
func (r *Run) Status(part uint32) Status {
	outDir := r.OutDir(part)
	if markers.Exists(markers.ProducedPath(outDir)) {
		return Produced
	}
	if info, err := os.Stat(outDir); err == nil && info.IsDir() && r.preserved(outDir) {
		return Preserved
	}
	return Pending
}

// preserved reports whether a NON-EMPTY output dir that lacks a .produced marker must be kept
// (and its part skipped) instead of wiped as crashed-produce debris:
//
//   - a sibling .loaded marker means the retired cc-crawl produce→verify→load lifecycle already
//     loaded this output into ClickHouse and wrote .loaded (it did not always leave .produced
//     behind). Historical output on disk still has that shape, and wiping it would delete data the
//     DB still references — disk would diverge from ClickHouse.
//   - for embed, an already-complete embeddings file (the single-part verify-and-skip predicate,
//     CompletedEmbedding) is the expensive GPU artifact.
//
// Either way the on-disk output is authoritative, so the caller skips the part rather than
// reproducing it.
func (r *Run) preserved(outDir string) bool {
	if markers.Exists(markers.LoadedPath(outDir)) {
		return true
	}
	if r.cmd == "embed" {
		if _, _, complete := CompletedEmbedding(outDir); complete {
			return true
		}
	}
	return false
}

// Parts classifies every part in [lo,hi]: one LoadPartStats query plus one filesystem Status probe
// per present part. This is the PLANNING view; Status is the dispatch-time re-check.
func (r *Run) Parts(ctx context.Context, lo, hi uint32) ([]Part, error) {
	stats, err := catalog.LoadPartStats(ctx, r.catalogPath, lo, hi)
	if err != nil {
		return nil, errors.Wrapf(err, "load part stats from %s", r.catalogPath)
	}
	present := make(map[uint32]struct{}, len(stats))
	for _, stat := range stats {
		present[stat.WarcIndex] = struct{}{}
	}
	parts := make([]Part, 0, int(hi-lo)+1)
	for i := lo; ; i++ {
		status := Empty
		if _, ok := present[i]; ok {
			status = r.Status(i)
		}
		parts = append(parts, Part{Index: i, Status: status})
		if i == hi {
			break
		}
	}
	return parts, nil
}

// Plan loads one part's validated page selection. primaryPagesOnly is derived from cmd
// (industry|embed => true). Only meaningful for Pending parts.
func (r *Run) Plan(part uint32) (warcinput.Plan, error) {
	return warcinput.LoadPlan(r.base, r.crawlID, r.selection, part, r.primaryPagesOnly)
}
