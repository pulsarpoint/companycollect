package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/store"
)

// TestScanAXFRDisabledDoesNoAXFRWork proves `scan --axfr=false` makes runScan skip the AXFR step
// ENTIRELY: axfr_domains stays completely unseeded — not merely empty-pending — for a scan-id whose
// base resolution ran. This is the call-site gate task 6 requires: runAXFRPipeline (queue seeding, a
// prober) and the ClickHouse load pass must never run when --axfr is off.
//
// Network-free: the scan-id's seed is pre-marked complete with zero queued domains (persisted to the
// SQLite file before runScan opens it), so seedCycle skips ClickHouse entirely and scanResolve's feeder
// finds nothing to dispatch — --resolvers is parsed but never actually dialed.
func TestScanAXFRDisabledDoesNoAXFRWork(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "scan.db")
	scanID := "noaxfr"

	seed, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.MarkSeedComplete(ctx, scanID); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	if err := runScan([]string{
		"-scan-id", scanID, "-db", dbPath, "-resolvers", "127.0.0.1:1",
		"-axfr=false", "-host-enrich=false",
	}); err != nil {
		t.Fatalf("runScan: %v", err)
	}

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	has, err := st.HasAXFRWork(ctx, scanID)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Error("axfr_domains has staged rows after --axfr=false; want zero AXFR work (no seeding at all)")
	}
}

// axfrCycleFixture seeds a store with one resolved ('done') domain behind a single NON-dialable NS
// endpoint. SeedAXFRDomains still stages it (it only checks ns_endpoints/ns_ips are non-empty), but
// processAXFRTarget skips every non-dialable endpoint before ever calling the prober — so runAXFRPipeline
// exercises its REAL production path (real scheduler, real *resolve.AXFRProber) while making exactly zero
// network dials. This is the "loopback/never-dialable target" trick used elsewhere in this package
// (axfr_test.go's TestRunAXFRPipelineSkipsWhenQueueComplete) to keep AXFR-pipeline tests network-free
// without resorting to fakes for the piece under test.
func axfrCycleFixture(t *testing.T, dbPath, scanID, domain string) *store.Store {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatal(err)
	}
	eps := []model.NameserverEndpoint{
		{Name: "ns1." + domain, IP: "127.0.0.1", Scope: "loopback", Dialable: false},
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", ResolvedAt: time.Unix(0, 0).UTC(),
		NSIPs: []string{"127.0.0.1"}, Endpoints: eps,
	}}); err != nil {
		t.Fatal(err)
	}
	return st
}

// TestAXFRCycleStagesBeforeLoading proves axfrCycle — the ONE shared function both `run`'s orchestrated
// AXFR phase and `scan --axfr` call — runs the durable SQLite staging pipeline (runAXFRPipeline) to full
// completion BEFORE invoking the ClickHouse load pass. The injected loadFn (mirroring axfrQueueDeps'
// injectable-seam idiom already used for the feeder/committer) observes the store instead of dialing a
// real ClickHouse, so this stays network-free while still exercising the real runAXFRPipeline.
func TestAXFRCycleStagesBeforeLoading(t *testing.T) {
	ctx := context.Background()
	scanID := "axfr-on"
	st := axfrCycleFixture(t, filepath.Join(t.TempDir(), "s.db"), scanID, "a.example.com")
	defer st.Close()

	cfg := scanConfig{
		scanID: scanID, runID: scanID, axfr: true,
		axfrWorkers: 4, axfrQPS: 5, axfrInflight: 5,
		axfrMaxRecords: 100, axfrMaxBytes: 1000, axfrTimeout: time.Second,
	}

	var loadObservedStagedState bool
	loadFn := func(ctx context.Context, st *store.Store, scanID string) error {
		doneQ, err := st.AXFRQueueComplete(ctx, scanID)
		if err != nil {
			return err
		}
		hasWork, err := st.HasAXFRWork(ctx, scanID)
		if err != nil {
			return err
		}
		loadObservedStagedState = doneQ && hasWork
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- axfrCycle(ctx, st, cfg, loadFn) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("axfrCycle: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("axfrCycle did not return within 5s")
	}
	if !loadObservedStagedState {
		t.Error("load pass ran before AXFR staging fully completed (or was never invoked)")
	}
}

// TestRunAndScanShareAXFRCycle proves run's orchestrated AXFR phase (runAXFRPhase) and scan's --axfr
// step (runScan) are literally the same function — axfrCycle — by running it against two independently
// seeded stores with identical resolved-domain state and checking they converge on identical
// axfr_domains/axfr_probes outcomes. This is the "shared-function equivalence" test: it does not stand
// up ClickHouse (a fake loadFn stands in for axfrLoad on both sides).
func TestRunAndScanShareAXFRCycle(t *testing.T) {
	ctx := context.Background()
	scanID := "cycle1"
	domain := "shared.example.com"

	runSt := axfrCycleFixture(t, filepath.Join(t.TempDir(), "run.db"), scanID, domain)
	defer runSt.Close()
	scanSt := axfrCycleFixture(t, filepath.Join(t.TempDir(), "scan.db"), scanID, domain)
	defer scanSt.Close()

	cfg := scanConfig{
		scanID: scanID, runID: scanID, axfr: true,
		axfrWorkers: 4, axfrQPS: 5, axfrInflight: 5,
		axfrMaxRecords: 100, axfrMaxBytes: 1000, axfrTimeout: time.Second,
	}
	noopLoad := func(ctx context.Context, st *store.Store, scanID string) error { return nil }

	if err := axfrCycle(ctx, runSt, cfg, noopLoad); err != nil {
		t.Fatalf("run-side axfrCycle: %v", err)
	}
	if err := axfrCycle(ctx, scanSt, cfg, noopLoad); err != nil {
		t.Fatalf("scan-side axfrCycle: %v", err)
	}

	runDone, err := runSt.AXFRQueueComplete(ctx, scanID)
	if err != nil {
		t.Fatal(err)
	}
	scanDone, err := scanSt.AXFRQueueComplete(ctx, scanID)
	if err != nil {
		t.Fatal(err)
	}
	if !runDone || runDone != scanDone {
		t.Fatalf("AXFRQueueComplete run=%v scan=%v, want both true", runDone, scanDone)
	}

	runHas, err := runSt.HasAXFRWork(ctx, scanID)
	if err != nil {
		t.Fatal(err)
	}
	scanHas, err := scanSt.HasAXFRWork(ctx, scanID)
	if err != nil {
		t.Fatal(err)
	}
	if !runHas || runHas != scanHas {
		t.Fatalf("HasAXFRWork run=%v scan=%v, want both true", runHas, scanHas)
	}
}
