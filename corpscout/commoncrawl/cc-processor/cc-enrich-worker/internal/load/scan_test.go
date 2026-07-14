package load

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"cc-enrich-worker/internal/markers"
	"cc-enrich-worker/internal/output"
)

// producedDir creates dir/<name> as a directory and writes its sibling .produced marker with rows.
func producedDir(t *testing.T, root, name string, rows map[string]int) string {
	t.Helper()
	out := filepath.Join(root, name)
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := markers.WriteProduced(out, markers.Produced{Rows: rows, FinishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFindPending(t *testing.T) {
	root := t.TempDir()

	// a: produced, no loaded -> pending.
	a := producedDir(t, root, "a", map[string]int{"domains": 1})
	// b: produced AND loaded -> ignored.
	b := producedDir(t, root, "b", map[string]int{"domains": 1})
	if err := markers.WriteLoaded(b); err != nil {
		t.Fatal(err)
	}
	// c: a bare directory, no markers -> ignored.
	if err := os.MkdirAll(filepath.Join(root, "c"), 0o755); err != nil {
		t.Fatal(err)
	}
	// nested/d: produced, no loaded -> pending (walk descends).
	d := producedDir(t, root, filepath.Join("nested", "d"), map[string]int{"tech": 3})

	got, err := findPending(root)
	if err != nil {
		t.Fatalf("findPending: %v", err)
	}
	sort.Strings(got)
	want := []string{a, d}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("findPending = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("findPending = %v, want %v", got, want)
		}
	}
}

func TestSweepLoadLoop(t *testing.T) {
	root := t.TempDir()

	ok := producedDir(t, root, "ok", map[string]int{"domains": 2})
	shortfall := producedDir(t, root, "short", map[string]int{"domains": 5})

	// Injected loader: "ok" loads 2 domains (meets marker), "short" loads only 2 (marker wants 5).
	loadFn := func(_ context.Context, dir string) ([]Result, error) {
		return []Result{{
			Path:  filepath.Join(dir, "domains.parquet"),
			Table: Tables["domains"],
			Rows:  2,
		}}, nil
	}

	res, err := sweep(context.Background(), root, 2, loadFn)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Pending != 2 {
		t.Errorf("Pending = %d, want 2", res.Pending)
	}
	if res.Loaded != 1 {
		t.Errorf("Loaded = %d, want 1", res.Loaded)
	}
	if res.Failed != 1 {
		t.Errorf("Failed = %d, want 1", res.Failed)
	}
	if !markers.Exists(markers.LoadedPath(ok)) {
		t.Errorf("ok should have .loaded")
	}
	if markers.Exists(markers.LoadedPath(shortfall)) {
		t.Errorf("short should NOT have .loaded (verify failed)")
	}
	if len(res.FailedDirs) != 1 || res.FailedDirs[0] != shortfall {
		t.Errorf("FailedDirs = %v, want [%s]", res.FailedDirs, shortfall)
	}
}

// TestSweepSkipsEmbed proves an embed-only produced dir (whose sole artifact, embeddings.parquet,
// is not a loadable kind) is skipped — never loaded, never failed — so `load --scan` does not exit
// 1 on it forever, while normal produced dirs alongside it still load. A second sweep is identical:
// the embed dir stays produced-but-unloaded and untouched.
func TestSweepSkipsEmbed(t *testing.T) {
	root := t.TempDir()

	normal := producedDir(t, root, "normal", map[string]int{"domains": 2})
	// An embed-only produced dir: marker Cmd == "embed".
	embedOut := filepath.Join(root, "emb")
	if err := os.MkdirAll(embedOut, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := markers.WriteProduced(embedOut, markers.Produced{Cmd: "embed", Rows: map[string]int{}, FinishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	loadFn := func(_ context.Context, dir string) ([]Result, error) {
		if filepath.Base(dir) == "emb" {
			t.Errorf("loader must never be called for the embed dir %s", dir)
		}
		return []Result{{Path: filepath.Join(dir, "domains.parquet"), Rows: 2}}, nil
	}

	res, err := sweep(context.Background(), root, 2, loadFn)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Loaded != 1 || res.Failed != 0 || res.Skipped != 1 || res.Pending != 1 {
		t.Fatalf("first sweep = %+v, want Loaded=1 Failed=0 Skipped=1 Pending=1", res)
	}
	if !markers.Exists(markers.LoadedPath(normal)) {
		t.Errorf("normal should be .loaded")
	}
	if markers.Exists(markers.LoadedPath(embedOut)) {
		t.Errorf("embed dir must NOT be .loaded")
	}

	// Second sweep: normal is done, embed dir is still produced-but-unloaded and skipped again.
	res2, err := sweep(context.Background(), root, 2, loadFn)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if res2.Loaded != 0 || res2.Failed != 0 || res2.Skipped != 1 || res2.Pending != 0 {
		t.Fatalf("second sweep = %+v, want Loaded=0 Failed=0 Skipped=1 Pending=0", res2)
	}
	if markers.Exists(markers.LoadedPath(embedOut)) {
		t.Errorf("embed dir must still NOT be .loaded after second sweep")
	}
}

func TestSweepLoaderError(t *testing.T) {
	root := t.TempDir()
	good := producedDir(t, root, "good", map[string]int{"domains": 1})
	bad := producedDir(t, root, "bad", map[string]int{"domains": 1})

	loadFn := func(_ context.Context, dir string) ([]Result, error) {
		if filepath.Base(dir) == "bad" {
			return nil, fmt.Errorf("boom")
		}
		return []Result{{Path: filepath.Join(dir, "domains.parquet"), Rows: 1}}, nil
	}

	res, err := sweep(context.Background(), root, 1, loadFn)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Loaded != 1 || res.Failed != 1 {
		t.Errorf("Loaded=%d Failed=%d, want 1/1", res.Loaded, res.Failed)
	}
	if !markers.Exists(markers.LoadedPath(good)) {
		t.Errorf("good should have .loaded")
	}
	if markers.Exists(markers.LoadedPath(bad)) {
		t.Errorf("bad should NOT have .loaded")
	}
}

// writeParquet drops a placeholder parquet file inside an output dir so prune has something to reclaim
// and tests can assert the DIR (not just markers) is gone.
func writeParquet(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("parquet-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestSweepDeleteLoadedAfterLoad: with --delete-loaded on, a dir that loads+verifies has its output
// directory removed AFTER .loaded is written; BOTH markers survive (they are siblings, not inside the
// dir) and Pruned is counted.
func TestSweepDeleteLoadedAfterLoad(t *testing.T) {
	root := t.TempDir()
	ok := producedDir(t, root, "ok", map[string]int{"domains": 2})
	writeParquet(t, ok, "domains.parquet")

	loadFn := func(_ context.Context, dir string) ([]Result, error) {
		return []Result{{Path: filepath.Join(dir, "domains.parquet"), Rows: 2}}, nil
	}

	res, err := sweepPrune(context.Background(), root, 2, true, loadFn)
	if err != nil {
		t.Fatalf("sweepPrune: %v", err)
	}
	if res.Loaded != 1 || res.Failed != 0 || res.Pruned != 1 {
		t.Fatalf("res = %+v, want Loaded=1 Failed=0 Pruned=1", res)
	}
	if _, statErr := os.Stat(ok); !os.IsNotExist(statErr) {
		t.Errorf("output dir %s should be removed, stat err=%v", ok, statErr)
	}
	if !markers.Exists(markers.ProducedPath(ok)) {
		t.Errorf(".produced marker must survive prune")
	}
	if !markers.Exists(markers.LoadedPath(ok)) {
		t.Errorf(".loaded marker must survive prune")
	}
}

// TestSweepCatchUpPrune: a dir already carrying BOTH .produced and .loaded (a leftover from before the
// flag, or from a crash between WriteLoaded and delete) is removed on a flagged sweep, markers kept.
// The same fixture WITHOUT the flag is untouched.
func TestSweepCatchUpPrune(t *testing.T) {
	noopLoad := func(_ context.Context, dir string) ([]Result, error) {
		t.Errorf("loader must not be called for an already-loaded dir: %s", dir)
		return nil, nil
	}

	// With the flag: catch-up prune removes the dir, keeps both markers.
	t.Run("flag_on_prunes", func(t *testing.T) {
		root := t.TempDir()
		done := producedDir(t, root, "done", map[string]int{"domains": 1})
		writeParquet(t, done, "domains.parquet")
		if err := markers.WriteLoaded(done); err != nil {
			t.Fatal(err)
		}
		res, err := sweepPrune(context.Background(), root, 2, true, noopLoad)
		if err != nil {
			t.Fatalf("sweepPrune: %v", err)
		}
		if res.Pruned != 1 || res.Loaded != 0 || res.Pending != 0 {
			t.Fatalf("res = %+v, want Pruned=1 Loaded=0 Pending=0", res)
		}
		if _, statErr := os.Stat(done); !os.IsNotExist(statErr) {
			t.Errorf("dir %s should be pruned", done)
		}
		if !markers.Exists(markers.ProducedPath(done)) || !markers.Exists(markers.LoadedPath(done)) {
			t.Errorf("both markers must survive catch-up prune")
		}
		// Second sweep is a no-op: dir already gone, nothing re-pruned.
		res2, err := sweepPrune(context.Background(), root, 2, true, noopLoad)
		if err != nil {
			t.Fatalf("second sweepPrune: %v", err)
		}
		if res2.Pruned != 0 {
			t.Fatalf("second sweep Pruned = %d, want 0 (already reclaimed)", res2.Pruned)
		}
	})

	// Without the flag: the already-loaded dir is left in place.
	t.Run("flag_off_keeps", func(t *testing.T) {
		root := t.TempDir()
		done := producedDir(t, root, "done", map[string]int{"domains": 1})
		writeParquet(t, done, "domains.parquet")
		if err := markers.WriteLoaded(done); err != nil {
			t.Fatal(err)
		}
		res, err := sweepPrune(context.Background(), root, 2, false, noopLoad)
		if err != nil {
			t.Fatalf("sweepPrune: %v", err)
		}
		if res.Pruned != 0 {
			t.Fatalf("Pruned = %d, want 0 when flag off", res.Pruned)
		}
		if _, statErr := os.Stat(done); statErr != nil {
			t.Errorf("dir %s must be untouched when flag off, stat err=%v", done, statErr)
		}
	})
}

// TestSweepDeleteLoadedKeepsShortfall: a verify-shortfall dir keeps its parquet — it is still pending,
// not loaded, so nothing is pruned even with the flag on.
func TestSweepDeleteLoadedKeepsShortfall(t *testing.T) {
	root := t.TempDir()
	short := producedDir(t, root, "short", map[string]int{"domains": 5})
	writeParquet(t, short, "domains.parquet")

	loadFn := func(_ context.Context, dir string) ([]Result, error) {
		return []Result{{Path: filepath.Join(dir, "domains.parquet"), Rows: 2}}, nil // marker wants 5
	}

	res, err := sweepPrune(context.Background(), root, 2, true, loadFn)
	if err != nil {
		t.Fatalf("sweepPrune: %v", err)
	}
	if res.Failed != 1 || res.Loaded != 0 || res.Pruned != 0 {
		t.Fatalf("res = %+v, want Failed=1 Loaded=0 Pruned=0", res)
	}
	if _, statErr := os.Stat(short); statErr != nil {
		t.Errorf("shortfall dir %s must keep its parquet, stat err=%v", short, statErr)
	}
	if markers.Exists(markers.LoadedPath(short)) {
		t.Errorf("shortfall dir must NOT be .loaded")
	}
}

// TestSweepDeleteLoadedSkipsEmbed: an embed-only produced dir (Cmd == "embed", never gets .loaded) is
// untouched by a flagged sweep — its embeddings.parquet is the expensive GPU artifact and is never a
// prune target because catch-up prune requires .loaded. Also holds with the flag off.
func TestSweepDeleteLoadedSkipsEmbed(t *testing.T) {
	for _, deleteLoaded := range []bool{true, false} {
		deleteLoaded := deleteLoaded
		t.Run(fmt.Sprintf("delete=%v", deleteLoaded), func(t *testing.T) {
			root := t.TempDir()
			embedOut := filepath.Join(root, "emb")
			if err := os.MkdirAll(embedOut, 0o755); err != nil {
				t.Fatal(err)
			}
			writeParquet(t, embedOut, "embeddings.parquet")
			if err := markers.WriteProduced(embedOut, markers.Produced{Cmd: "embed", Rows: map[string]int{}, FinishedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
			loadFn := func(_ context.Context, dir string) ([]Result, error) {
				t.Errorf("loader must never run for embed dir %s", dir)
				return nil, nil
			}
			res, err := sweepPrune(context.Background(), root, 2, deleteLoaded, loadFn)
			if err != nil {
				t.Fatalf("sweepPrune: %v", err)
			}
			if res.Skipped != 1 || res.Pruned != 0 || res.Loaded != 0 {
				t.Fatalf("res = %+v, want Skipped=1 Pruned=0 Loaded=0", res)
			}
			if _, statErr := os.Stat(embedOut); statErr != nil {
				t.Errorf("embed dir %s must survive, stat err=%v", embedOut, statErr)
			}
			if _, statErr := os.Stat(filepath.Join(embedOut, "embeddings.parquet")); statErr != nil {
				t.Errorf("embeddings.parquet must survive, stat err=%v", statErr)
			}
		})
	}
}

// TestSweepMarkersNeverDeleted pins the invariant that RemoveAll(outDir) cannot reach the sibling
// markers (outDir+".produced"/".loaded"), because they live in the PARENT dir, not inside outDir.
func TestSweepMarkersNeverDeleted(t *testing.T) {
	root := t.TempDir()
	ok := producedDir(t, root, "ok", map[string]int{"domains": 1})
	writeParquet(t, ok, "domains.parquet")
	loadFn := func(_ context.Context, dir string) ([]Result, error) {
		return []Result{{Path: filepath.Join(dir, "domains.parquet"), Rows: 1}}, nil
	}
	if _, err := sweepPrune(context.Background(), root, 1, true, loadFn); err != nil {
		t.Fatalf("sweepPrune: %v", err)
	}
	// Markers are literally siblings: prefix of outDir, in the parent dir.
	if filepath.Dir(markers.ProducedPath(ok)) != filepath.Dir(ok) {
		t.Fatalf(".produced marker is not a sibling of outDir")
	}
	if !markers.Exists(markers.ProducedPath(ok)) || !markers.Exists(markers.LoadedPath(ok)) {
		t.Errorf("both markers must remain after prune")
	}
}

// --- Integration: real ClickHouse when reachable ---

func chTestConn(t *testing.T) driver.Conn {
	t.Helper()
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set; skipping real-ClickHouse loader sweep integration test")
	}
	port := os.Getenv("CLICKHOUSE_NATIVE_PORT")
	if port == "" {
		port = "9000"
	}
	db := os.Getenv("CLICKHOUSE_DATABASE")
	if db == "" {
		db = "corpscout"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{
			Database: db,
			Username: envOrTest("CLICKHOUSE_USER", "default"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
	})
	if err != nil {
		t.Skipf("CLICKHOUSE_HOST set but connect failed (%v); skipping integration test", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Skipf("CLICKHOUSE_HOST set but ping failed (%v); skipping integration test", err)
	}
	return conn
}

func envOrTest(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestSweepIntegration(t *testing.T) {
	conn := chTestConn(t)
	defer conn.Close()
	ctx := context.Background()
	root := t.TempDir()

	writeDomains := func(name string, n int) string {
		out := filepath.Join(root, name)
		if err := os.MkdirAll(out, 0o755); err != nil {
			t.Fatal(err)
		}
		rows := make([]output.DomainRow, n)
		for i := range rows {
			rows[i] = output.DomainRow{
				CrawlID:    "CC-TEST",
				URL:        fmt.Sprintf("https://%s-%d.example", name, i),
				RootDomain: fmt.Sprintf("%s-%d.example", name, i),
				ResolvedAt: time.Now(),
			}
		}
		if err := output.WriteDomains(filepath.Join(out, "domains.parquet"), rows); err != nil {
			t.Fatal(err)
		}
		return out
	}

	// Two produced parts with truthful counts, plus one already-loaded part.
	p1 := writeDomains("part1", 3)
	if err := markers.WriteProduced(p1, markers.Produced{Rows: map[string]int{"domains": 3}, FinishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	p2 := writeDomains("part2", 2)
	if err := markers.WriteProduced(p2, markers.Produced{Rows: map[string]int{"domains": 2}, FinishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	done := writeDomains("done", 1)
	if err := markers.WriteProduced(done, markers.Produced{Rows: map[string]int{"domains": 1}, FinishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := markers.WriteLoaded(done); err != nil {
		t.Fatal(err)
	}

	res, err := Sweep(ctx, conn, root, 2, false)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Loaded != 2 || res.Failed != 0 || res.Pending != 2 {
		t.Fatalf("first sweep = %+v, want Loaded=2 Failed=0 Pending=2", res)
	}
	if !markers.Exists(markers.LoadedPath(p1)) || !markers.Exists(markers.LoadedPath(p2)) {
		t.Fatalf("both parts should be marked .loaded")
	}

	// Second sweep: nothing pending.
	res2, err := Sweep(ctx, conn, root, 2, false)
	if err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	if res2.Loaded != 0 || res2.Pending != 0 {
		t.Fatalf("second sweep = %+v, want Loaded=0 Pending=0", res2)
	}

	// A marker whose count EXCEEDS the parquet rows -> Failed, no .loaded; a sibling still loads.
	root2 := t.TempDir()
	bad := filepath.Join(root2, "bad")
	if err := os.MkdirAll(bad, 0o755); err != nil {
		t.Fatal(err)
	}
	rows := []output.DomainRow{{CrawlID: "CC", URL: "https://bad.example", RootDomain: "bad.example", ResolvedAt: time.Now()}}
	if err := output.WriteDomains(filepath.Join(bad, "domains.parquet"), rows); err != nil {
		t.Fatal(err)
	}
	// Claim 99 rows though the file has 1.
	if err := markers.WriteProduced(bad, markers.Produced{Rows: map[string]int{"domains": 99}, FinishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(root2, "good")
	if err := os.MkdirAll(good, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := output.WriteDomains(filepath.Join(good, "domains.parquet"), rows); err != nil {
		t.Fatal(err)
	}
	if err := markers.WriteProduced(good, markers.Produced{Rows: map[string]int{"domains": 1}, FinishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	res3, err := Sweep(ctx, conn, root2, 2, false)
	if err != nil {
		t.Fatalf("third Sweep: %v", err)
	}
	if res3.Loaded != 1 || res3.Failed != 1 {
		t.Fatalf("third sweep = %+v, want Loaded=1 Failed=1", res3)
	}
	if markers.Exists(markers.LoadedPath(bad)) {
		t.Errorf("bad (verify shortfall) should NOT be .loaded")
	}
	if !markers.Exists(markers.LoadedPath(good)) {
		t.Errorf("good should still be .loaded despite bad sibling")
	}

	// --delete-loaded against real ClickHouse: one flagged sweep loads a fresh part, writes .loaded,
	// then reclaims the OUTPUT DIR while keeping both markers.
	root3 := t.TempDir()
	pd := writeDomains("prune", 2)
	// writeDomains wrote under root; move the fixture into root3 for an isolated sweep.
	pruneDir := filepath.Join(root3, "prune")
	if err := os.Rename(pd, pruneDir); err != nil {
		t.Fatal(err)
	}
	if err := markers.WriteProduced(pruneDir, markers.Produced{Rows: map[string]int{"domains": 2}, FinishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	res4, err := Sweep(ctx, conn, root3, 1, true)
	if err != nil {
		t.Fatalf("flagged Sweep: %v", err)
	}
	if res4.Loaded != 1 || res4.Pruned != 1 {
		t.Fatalf("flagged sweep = %+v, want Loaded=1 Pruned=1", res4)
	}
	if _, statErr := os.Stat(pruneDir); !os.IsNotExist(statErr) {
		t.Errorf("output dir %s should be pruned after flagged load", pruneDir)
	}
	if !markers.Exists(markers.ProducedPath(pruneDir)) || !markers.Exists(markers.LoadedPath(pruneDir)) {
		t.Errorf("both markers must survive the flagged prune")
	}
}
