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

	res, err := Sweep(ctx, conn, root, 2)
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
	res2, err := Sweep(ctx, conn, root, 2)
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

	res3, err := Sweep(ctx, conn, root2, 2)
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
}
