package planstore

import (
	"context"
	"path/filepath"
	"testing"

	"cc-download-worker/rangeplanner"
)

func TestStoreTracksHybridPlanAndCommittedChunks(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "plan.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	records := []rangeplanner.Record{
		{ID: 0, WARCFile: "a.warc.gz", Offset: 0, Length: 600},
		{ID: 1, WARCFile: "b.warc.gz", Offset: 0, Length: 200},
		{ID: 2, WARCFile: "b.warc.gz", Offset: 300, Length: 200},
	}
	worklist := rangeplanner.Worklist{
		Records:      records,
		OutputChunks: [][]rangeplanner.Record{records[:1], records[1:]},
		WARCFiles:    []string{"a.warc.gz", "b.warc.gz"},
	}
	reused, err := store.ImportPart(ctx, 87, "checksum", worklist)
	if err != nil {
		t.Fatal(err)
	}
	if reused {
		t.Fatal("new part was reported as reused")
	}
	reused, err = store.ImportPart(ctx, 87, "checksum", worklist)
	if err != nil {
		t.Fatal(err)
	}
	if !reused {
		t.Fatal("unchanged part was not reused")
	}
	if err := store.SetObjectSizes(ctx, map[string]int64{"a.warc.gz": 1_000, "b.warc.gz": 1_000}); err != nil {
		t.Fatal(err)
	}
	if err := store.RefreshStrategies(ctx, 50); err != nil {
		t.Fatal(err)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Parts != 1 || stats.Chunks != 2 || stats.PendingPages != 3 || stats.PendingBytes != 1_000 {
		t.Fatalf("unexpected plan stats %+v", stats)
	}
	if stats.WholeWARCObjects != 1 || stats.ExactWARCObjects != 1 || stats.EstimatedRequests != 3 || stats.EstimatedSourceBytes != 1_400 {
		t.Fatalf("unexpected hybrid stats %+v", stats)
	}
	if stats.WholeWARCPages != 1 || stats.ExactPages != 2 || stats.WholeWARCSelectedBytes != 600 || stats.ExactSelectedBytes != 400 || stats.WholeWARCDownloadBytes != 1_000 {
		t.Fatalf("unexpected strategy page stats %+v", stats)
	}
	buckets, err := store.UtilizationBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 1 || buckets[0].FromPercent != 60 || buckets[0].ToPercent != 70 || buckets[0].WARCObjects != 1 || buckets[0].Pages != 1 || buckets[0].JunkBytes != 400 {
		t.Fatalf("unexpected utilization buckets %+v", buckets)
	}
	warcs, err := store.WARCStats(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(warcs) != 2 || warcs[0].Filename != "a.warc.gz" || warcs[0].SelectedPercent != 60 || warcs[0].Strategy != "whole_warc" {
		t.Fatalf("unexpected WARC stats %+v", warcs)
	}

	if err := store.MarkChunkCommitted(ctx, CommittedChunk{Part: 87, Chunk: 0, ManifestKey: "manifest.json"}, 50); err != nil {
		t.Fatal(err)
	}
	stats, err = store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.CommittedChunks != 1 || stats.PendingPages != 2 || stats.PendingBytes != 400 || stats.WholeWARCObjects != 0 || stats.EstimatedRequests != 2 {
		t.Fatalf("unexpected committed stats %+v", stats)
	}
	pending, err := store.PendingPagesForChunk(ctx, 87, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].WorklistOrdinal != 1 || pending[0].Strategy != "exact_ranges" {
		t.Fatalf("unexpected pending pages %+v", pending)
	}
	if err := store.SetWARCCache(ctx, "b.warc.gz", "ready", "/cache/b.warc.gz"); err != nil {
		t.Fatal(err)
	}
	pending, err = store.PendingPagesForChunk(ctx, 87, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pending[0].CacheState != "ready" || pending[0].LocalPath != "/cache/b.warc.gz" {
		t.Fatalf("cache state was not retained %+v", pending[0])
	}
	if err := store.ReconcileCommittedChunks(ctx, nil, 50); err != nil {
		t.Fatal(err)
	}
	stats, err = store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.CommittedChunks != 0 || stats.PendingPages != 3 || stats.WholeWARCObjects != 1 {
		t.Fatalf("RustFS reconciliation did not reset local completion %+v", stats)
	}
}

func TestStoreRemovesPartsOutsideEffectiveSelection(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "plan.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record := rangeplanner.Record{ID: 0, WARCFile: "a.warc.gz", Offset: 0, Length: 100}
	worklist := rangeplanner.Worklist{
		Records:      []rangeplanner.Record{record},
		OutputChunks: [][]rangeplanner.Record{{record}},
		WARCFiles:    []string{"a.warc.gz"},
	}
	if _, err := store.ImportPart(ctx, 87, "checksum", worklist); err != nil {
		t.Fatal(err)
	}
	if err := store.SyncParts(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RefreshStrategies(ctx, 50); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Parts != 0 || stats.Pages != 0 || stats.WARCObjects != 0 {
		t.Fatalf("obsolete part remained %+v", stats)
	}
}

func TestOpenReadOnlyReportsWithoutAllowingStateChanges(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "plan.sqlite")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if _, err := readOnly.Stats(ctx); err != nil {
		t.Fatal(err)
	}
	if err := readOnly.SetWARCCache(ctx, "missing.warc.gz", "missing", ""); err == nil {
		t.Fatal("read-only plan accepted a state change")
	}
}
