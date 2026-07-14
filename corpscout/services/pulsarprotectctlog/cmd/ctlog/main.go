// Command ctlog processes Certificate Transparency log shards.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/config"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/ctclient"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/ingest"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/loglist"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/source"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/store/clickhouse"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/store/control"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/tileclient"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "list":
		err = cmdList(os.Args[2:])
	case "process":
		err = cmdProcess(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		slog.Error("ctlog failed", "error", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  ctlog list    [--source NAME] [--json]
  ctlog process  --source NAME --ctlog ID [--watch] [--watch-interval D] [--dry-run] [--limit N]`)
}

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	src := fs.String("source", "", "source name (empty = all sources)")
	asJSON := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	hc := tunedHTTPClient(cfg.HTTPTimeout, cfg.FetchParallel)

	sources, err := loglist.LoadSources(cfg.SourcesFile)
	if err != nil {
		return err
	}
	if *src != "" {
		s, ok := loglist.Find(sources, *src)
		if !ok {
			return fmt.Errorf("unknown source %q", *src)
		}
		sources = []loglist.Source{s}
	}

	processed := map[string]control.WorkUnitStatus{}
	if st, err := control.OpenReadOnly(ctx, cfg.ControlDBPath); err != nil {
		return err
	} else if st != nil {
		defer st.Close()
		rows, err := st.ListWorkUnits(ctx)
		if err != nil {
			return err
		}
		for _, r := range rows {
			processed[r.ID] = r
		}
	}

	type item struct {
		source string
		ctlog  loglist.CTLog
	}
	var items []item
	for _, s := range sources {
		ctlogs, err := loglist.CTLogs(ctx, hc, cfg.ShardListURL, s, deriveCTLogIDFromLog)
		if err != nil {
			return err
		}
		for _, c := range ctlogs {
			items = append(items, item{source: s.Name, ctlog: c})
		}
	}

	heads := make([]uint64, len(items))
	reach := make([]bool, len(items))
	sem := make(chan struct{}, max(cfg.FetchParallel, 1))
	var wg sync.WaitGroup
	for i := range items {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			heads[i], reach[i] = loglist.Head(ctx, hc, items[i].ctlog, cfg.MaxRetries)
		}(i)
	}
	wg.Wait()

	now := control.Now()
	var out []loglist.CTLogStatus
	for i, it := range items {
		c := it.ctlog
		wu, tracked := processed[workUnitID(it.source, c.ID)]
		out = append(out, loglist.CTLogStatus{
			CTLog: c, Phase: string(c.Phase(now)), Reachable: reach[i], Head: int64(heads[i]),
			Tracked: tracked, Status: statusOr(wu, tracked), Cursor: wu.NextIndex,
			CertsWritten: wu.CertsWritten, SANsWritten: wu.SANsWritten,
			PercentDone: loglist.PercentDone(wu.NextIndex, int64(heads[i])),
		})
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	printCTLogTable(out)
	return nil
}

// deriveCTLogIDFromLog computes the friendly id from a ctlog's monitoring URL.
func deriveCTLogIDFromLog(c loglist.CTLog) string { return deriveCTLogID(c.MonitoringURL) }

// statusOr returns "not-started" when the log is not tracked, else wu.Status.
func statusOr(wu control.WorkUnitStatus, tracked bool) string {
	if !tracked {
		return "not-started"
	}
	return wu.Status
}

// printCTLogTable prints a human-readable table of CT log statuses.
func printCTLogTable(rows []loglist.CTLogStatus) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SOURCE\tCTLOG\tINTERVAL\tPHASE\tREACH\tHEAD\tCURSOR\tDONE%\tSTATUS")
	for _, c := range rows {
		interval := c.Start.Format("2006-01-02") + ".." + c.End.Format("2006-01-02")
		reach := "no"
		if c.Reachable {
			reach = "yes"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%.1f\t%s\n",
			c.Source, c.ID, interval, c.Phase, reach,
			c.Head, c.Cursor, c.PercentDone, c.Status,
		)
	}
	w.Flush()
}

func cmdProcess(args []string) error {
	fs := flag.NewFlagSet("process", flag.ContinueOnError)
	srcName := fs.String("source", "", "source name (required)")
	ctlogID := fs.String("ctlog", "", "ctlog friendly id (required)")
	watch := fs.Bool("watch", false, "tail an active ctlog's delta after draining to head")
	interval := fs.Duration("watch-interval", 15*time.Minute, "poll interval for --watch")
	dryRun := fs.Bool("dry-run", false, "do not write to ClickHouse")
	limit := fs.Int("limit", 0, "max entries (0 = unlimited)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *srcName == "" {
		return fmt.Errorf("--source is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	hc := tunedHTTPClient(cfg.HTTPTimeout, cfg.FetchParallel)

	sources, err := loglist.LoadSources(cfg.SourcesFile)
	if err != nil {
		return err
	}
	s, ok := loglist.Find(sources, *srcName)
	if !ok {
		return fmt.Errorf("unknown source %q", *srcName)
	}
	ctlogs, err := loglist.CTLogs(ctx, hc, cfg.ShardListURL, s, deriveCTLogIDFromLog)
	if err != nil {
		return err
	}

	var target *loglist.CTLog
	if *ctlogID != "" {
		for i := range ctlogs {
			if ctlogs[i].ID == *ctlogID {
				target = &ctlogs[i]
				break
			}
		}
		if target == nil {
			return fmt.Errorf("ctlog %q not found in source %q", *ctlogID, *srcName)
		}
	}

	var chStore *clickhouse.Store
	var ctrl *control.Store
	if !*dryRun {
		chStore, err = clickhouse.Open(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDatabase, cfg.ClickHouseUser, cfg.ClickHousePassword)
		if err != nil {
			return err
		}
		defer chStore.Close()
		if err := chStore.EnsureSchema(ctx, cfg.ClickHouseStorage); err != nil {
			return err
		}
		ctrl, err = control.Open(ctx, cfg.ControlDBPath)
		if err != nil {
			return err
		}
		defer ctrl.Close()
	}

	if *ctlogID == "" {
		// Orchestrate the whole source.
		reachable := func(c loglist.CTLog) bool {
			head, ok := loglist.Head(ctx, hc, c, cfg.MaxRetries)
			if ok {
				slog.Info("processing ctlog", "source", *srcName, "ctlog", c.ID, "phase", phaseName(!control.Now().Before(c.End)), "head", head)
			}
			return ok
		}
		drain := func(c loglist.CTLog, frozen bool) error {
			return drainCTLog(ctx, cfg, hc, chStore, ctrl, *srcName, c, frozen, *limit)
		}
		return drainAll(ctx, ctlogs, reachable, drain)
	}

	// Single ctlog (target already resolved above).
	if !*watch {
		return drainCTLog(ctx, cfg, hc, chStore, ctrl, *srcName, *target, true, *limit)
	}
	slog.Info("watch: tailing ctlog delta", "ctlog", target.ID, "interval", *interval)
	for {
		frozen := !control.Now().Before(target.End)
		if err := drainCTLog(ctx, cfg, hc, chStore, ctrl, *srcName, *target, frozen, *limit); err != nil {
			return err
		}
		if frozen {
			slog.Info("ctlog window closed; finalized", "ctlog", target.ID)
			return nil
		}
		select {
		case <-time.After(*interval):
		case <-ctx.Done():
			return nil
		}
	}
}

// drainAll drains every ctlog of a source in sequence. Unreachable ctlogs are
// skipped and a failed shard is logged and skipped — one flaky shard must not
// starve the rest of the source — but any failure surfaces in the returned
// error so the systemd unit exits non-zero. A cancelled context (SIGINT) stops
// cleanly with a nil error.
func drainAll(ctx context.Context, ctlogs []loglist.CTLog, reachable func(loglist.CTLog) bool, drain func(c loglist.CTLog, frozen bool) error) error {
	now := control.Now()
	var failed []string
	for _, c := range ctlogs {
		if ctx.Err() != nil {
			return nil
		}
		if !reachable(c) {
			slog.Warn("ctlog unreachable; skipping", "ctlog", c.ID)
			continue
		}
		if err := drain(c, !now.Before(c.End)); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("ctlog drain failed; continuing with next shard", "ctlog", c.ID, "error", err)
			failed = append(failed, c.ID)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d of %d ctlogs failed: %s", len(failed), len(ctlogs), strings.Join(failed, ", "))
	}
	return nil
}

// buildSource constructs a Source for a ctlog.
func buildSource(target loglist.CTLog, hc *http.Client, cfg *config.Config) (source.Source, error) {
	if target.Type == "rfc6962" {
		cl, err := ctclient.New(target.MonitoringURL, target.ID, hc, cfg.MaxRetries)
		if err != nil {
			return nil, err
		}
		return source.NewRFC6962(cl, cfg.BatchSize), nil
	}
	return source.NewTile(tileclient.New(target.MonitoringURL, target.ID, hc, cfg.MaxRetries), cfg.FetchParallel), nil
}

// drainCTLog drains one ctlog to head. finalize marks it done when complete.
func drainCTLog(ctx context.Context, cfg *config.Config, hc *http.Client, ch *clickhouse.Store, ctrl *control.Store, srcName string, target loglist.CTLog, finalize bool, limit int) error {
	src, err := buildSource(target, hc, cfg)
	if err != nil {
		return err
	}
	head, err := src.TreeSize(ctx)
	if err != nil {
		return err
	}
	wuID := workUnitID(srcName, target.ID)
	if ctrl != nil {
		_ = ctrl.SetEnd(ctx, wuID, int64(head))
	}
	unit := model.WorkUnit{ID: wuID, LogName: target.ID, StartIndex: 0, EndIndex: head, WindowFrom: target.Start, WindowTo: target.End}
	began := time.Now()
	stats, err := ingest.New(src, ch, ctrl, cfg.WriteBatchSize).WithFinalize(finalize).WithLimit(limit).Run(ctx, unit)
	if err != nil {
		return err
	}
	slog.Info("drain cycle", "ctlog", target.ID, "head", head, "entries", stats.EntriesProcessed,
		"certs", stats.CertsWritten, "sans", stats.SANsWritten, "parse_errors", stats.ParseErrors, "elapsed", time.Since(began).Round(time.Second))
	return nil
}

// phaseName returns a human-readable phase label for logging.
func phaseName(frozen bool) string {
	if frozen {
		return "frozen"
	}
	return "active"
}

// workUnitID is the control-DB key for a ctlog, qualified by source so ids
// derived from different sources can never collide.
func workUnitID(source, ctlogID string) string { return source + "/" + ctlogID }

// deriveCTLogID builds a stable id/name from a monitoring URL, e.g.
// https://mon.sycamore.ct.letsencrypt.org/2025h2d/ -> sycamore-2025h2d.
func deriveCTLogID(monURL string) string {
	u, err := url.Parse(monURL)
	if err != nil {
		return strings.NewReplacer("https://", "", "http://", "", "/", "-").Replace(strings.Trim(monURL, "/"))
	}
	host := u.Hostname()
	labels := strings.Split(host, ".")
	hostLabel := labels[0]
	if (hostLabel == "mon" || hostLabel == "www") && len(labels) > 1 {
		hostLabel = labels[1]
	}
	segs := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })
	last := ""
	if len(segs) > 0 {
		last = segs[len(segs)-1]
	}
	if last == "" {
		return hostLabel
	}
	return hostLabel + "-" + last
}

// tunedHTTPClient returns an HTTP client whose transport allows up to parallel
// concurrent connections per host, so parallel tile fetches are not throttled
// by the default keep-alive cap of 2 connections per host.
func tunedHTTPClient(timeout time.Duration, parallel int) *http.Client {
	if parallel < 1 {
		parallel = 1
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = parallel * 2
	tr.MaxIdleConnsPerHost = parallel
	tr.MaxConnsPerHost = parallel
	tr.IdleConnTimeout = 90 * time.Second
	return &http.Client{Timeout: timeout, Transport: tr}
}
