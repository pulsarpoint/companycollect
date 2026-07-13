package main

import (
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cc-enrich-worker/internal/markers"
)

// markerEntry is one discovered .produced marker: which command produced its part, when the part
// finished, and whether the sibling .loaded marker exists.
type markerEntry struct {
	Cmd        string
	FinishedAt time.Time
	Loaded     bool
}

// scanMarkers walks root for every "<dir>.produced" marker (a sibling of the output dir it
// describes — see markers.ProducedPath) and returns one markerEntry per marker found. A bare
// output directory with no .produced marker (a crashed produce that was never retried, or one
// that simply hasn't run yet) contributes nothing: it is neither produced, loaded, nor pending.
func scanMarkers(root string) ([]markerEntry, error) {
	var entries []markerEntry
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".produced") {
			return nil
		}
		outDir := strings.TrimSuffix(path, ".produced")
		produced, rerr := markers.ReadProduced(outDir)
		if rerr != nil {
			return fmt.Errorf("read %s: %w", path, rerr)
		}
		entries = append(entries, markerEntry{
			Cmd:        produced.Cmd,
			FinishedAt: produced.FinishedAt,
			Loaded:     markers.Exists(markers.LoadedPath(outDir)),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// cmdCounts is one command's marker tally: how many parts have finished producing, how many of
// those are also loaded into ClickHouse, and how many are still pending (produced, not yet
// loaded). OldestPendingAge is how long the longest-waiting pending part has been sitting,
// measured against the "now" the caller supplies; it is the zero value when Pending == 0.
type cmdCounts struct {
	Produced, Loaded, Pending int
	OldestPendingAge          time.Duration
}

// statusReport summarizes marker state across every command found under a root, keyed by the
// Produced.Cmd recorded in each marker (e.g. "tech", "industry"). Cmds is sorted so String()
// prints deterministically.
type statusReport struct {
	Root  string
	Cmds  []string
	ByCmd map[string]cmdCounts
}

// buildStatusReport aggregates markerEntry values into per-command counts. It is pure — no
// filesystem I/O and no wall-clock read — so it is fully unit-testable: callers (runStatusCmd)
// inject `now` rather than the function calling time.Now() itself, which keeps tests immune to
// scheduling jitter between marker creation and assertion.
func buildStatusReport(root string, entries []markerEntry, now time.Time) statusReport {
	byCmd := make(map[string]cmdCounts)
	oldestPending := make(map[string]time.Time)
	for _, e := range entries {
		c := byCmd[e.Cmd]
		c.Produced++
		if e.Loaded {
			c.Loaded++
		} else {
			c.Pending++
			if t, ok := oldestPending[e.Cmd]; !ok || e.FinishedAt.Before(t) {
				oldestPending[e.Cmd] = e.FinishedAt
			}
		}
		byCmd[e.Cmd] = c
	}

	cmds := make([]string, 0, len(byCmd))
	for cmd, c := range byCmd {
		if t, ok := oldestPending[cmd]; ok {
			c.OldestPendingAge = now.Sub(t)
			byCmd[cmd] = c
		}
		cmds = append(cmds, cmd)
	}
	sort.Strings(cmds)

	return statusReport{Root: root, Cmds: cmds, ByCmd: byCmd}
}

// String renders one table: per-command produced/loaded/pending counts and the oldest pending
// marker's age ("-" when nothing is pending for that command).
func (r statusReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "status: %s\n", r.Root)
	if len(r.Cmds) == 0 {
		b.WriteString("  (no .produced markers found)\n")
		return b.String()
	}
	fmt.Fprintf(&b, "  %-10s %-10s %-10s %-10s %s\n", "cmd", "produced", "loaded", "pending", "oldest pending age")
	for _, cmd := range r.Cmds {
		c := r.ByCmd[cmd]
		age := "-"
		if c.Pending > 0 {
			age = c.OldestPendingAge.Round(time.Second).String()
		}
		fmt.Fprintf(&b, "  %-10s %-10d %-10d %-10d %s\n", cmd, c.Produced, c.Loaded, c.Pending, age)
	}
	return b.String()
}

// runStatusCmd implements the `status` command: walk --root for .produced/.loaded markers left by
// the range runner and the loader, and print one per-command table of produced/loaded/pending
// counts plus the oldest pending part's age. Read-only — it never touches ClickHouse or writes
// any marker.
func runStatusCmd(args []string) {
	flagSet := flag.NewFlagSet("cc-enrich-worker status", flag.ExitOnError)
	root := flagSet.String("root", "", "producer output root to scan for .produced/.loaded markers (required)")
	_ = flagSet.Parse(args)

	if *root == "" {
		fmt.Fprintln(os.Stderr, "error: --root is required")
		flagSet.Usage()
		os.Exit(2)
	}
	rootAbs, err := filepath.Abs(*root)
	if err != nil {
		log.Fatalf("resolve root %s: %v", *root, err)
	}

	entries, err := scanMarkers(rootAbs)
	if err != nil {
		log.Fatalf("scan markers under %s: %v", rootAbs, err)
	}

	report := buildStatusReport(rootAbs, entries, time.Now())
	fmt.Print(report.String())
}
