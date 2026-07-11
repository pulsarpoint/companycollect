package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cc-download-worker/internal/rawdownload"
	"cc-download-worker/internal/worklistbuilder"
	"cc-raw/fetch"
	"cc-raw/rawstore"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

type options struct {
	baseDirectory     string
	crawlID           string
	parts             string
	pagesPerDomain    int
	worklistDirectory string
	rebuildWorklists  bool
	python            string
	sourceBucket      string
	sourceRegion      string
	sourceAnonymous   bool
	concurrency       int
	maxPackBytes      int64
	maxRecords        int
	maxFailureRate    float64
	recordAttempts    int
	recordTimeout     time.Duration
	tempDirectory     string
	rustfsEndpoint    string
	rustfsRegion      string
	rustfsBucket      string
	forceRedownload   bool
}

type totals struct {
	parts             int
	skippedParts      int
	chunks            int
	requestedRecords  int64
	downloadedRecords int64
	failedRecords     int64
	rawBytes          int64
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger, os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		logger.Error("download failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, args []string) error {
	options, err := parseOptions(args)
	if err != nil {
		return err
	}
	parts, err := parseParts(options.parts)
	if err != nil {
		return err
	}
	selection := worklistbuilder.Selection(options.pagesPerDomain)
	for _, part := range parts {
		if err := rawstore.ValidatePartIdentity(options.crawlID, selection, part); err != nil {
			return err
		}
	}
	worklistDirectory := options.worklistDirectory
	if worklistDirectory == "" {
		worklistDirectory = worklistbuilder.DefaultDirectory(options.baseDirectory, options.crawlID, options.pagesPerDomain)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store, err := rawstore.NewStore(ctx, rawstore.StoreConfig{
		Endpoint:  options.rustfsEndpoint,
		Region:    options.rustfsRegion,
		Bucket:    options.rustfsBucket,
		AccessKey: os.Getenv("CORPSCOUT_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("CORPSCOUT_S3_SECRET_KEY"),
	})
	if err != nil {
		return err
	}

	var source fetch.RangeGetter
	if options.sourceAnonymous {
		source = fetch.NewHTTPGetter(os.Getenv("CC_BASE_URL"), options.concurrency)
	} else {
		source, err = fetch.NewS3Getter(ctx, options.sourceRegion, options.concurrency)
		if err != nil {
			return errors.Wrap(err, "configure Common Crawl source")
		}
	}
	hostname, err := os.Hostname()
	if err != nil {
		return errors.Wrap(err, "read worker hostname")
	}
	runID, err := newRunID()
	if err != nil {
		return err
	}

	logger.Info("download run started",
		"crawl", options.crawlID,
		"selection", selection,
		"pages_per_domain", options.pagesPerDomain,
		"parts", options.parts,
		"worklist_directory", worklistDirectory,
		"rustfs_bucket", options.rustfsBucket,
		"concurrency", options.concurrency,
		"max_pack_bytes", options.maxPackBytes,
		"max_records", options.maxRecords,
		"record_attempts", options.recordAttempts,
		"run_id", runID,
	)

	var runTotals totals
	for _, part := range parts {
		worklist, err := worklistbuilder.Ensure(ctx, worklistbuilder.Config{
			Python:         options.python,
			CrawlID:        options.crawlID,
			PagesPerDomain: options.pagesPerDomain,
			Part:           part,
			OutputPath:     worklistbuilder.Path(worklistDirectory, part),
			Rebuild:        options.rebuildWorklists,
		})
		if err != nil {
			return errors.Wrapf(err, "prepare worklist part %d", part)
		}
		logger.Info("worklist ready",
			"crawl", options.crawlID,
			"selection", selection,
			"part", part,
			"path", worklist.Path,
			"rows", worklist.Rows,
			"reused", worklist.Reused,
		)

		downloader := rawdownload.Downloader{
			Source: source,
			Store:  store,
			Logger: logger,
			Config: rawdownload.Config{
				WorklistPath:    worklist.Path,
				WorklistKey:     worklistbuilder.Key(options.pagesPerDomain, part),
				CrawlID:         options.crawlID,
				Selection:       selection,
				Part:            part,
				SourceBucket:    options.sourceBucket,
				Concurrency:     options.concurrency,
				MaxPackBytes:    options.maxPackBytes,
				MaxRecords:      options.maxRecords,
				MaxFailureRate:  options.maxFailureRate,
				RecordAttempts:  options.recordAttempts,
				RecordTimeout:   options.recordTimeout,
				TempDir:         options.tempDirectory,
				RunID:           runID,
				WorkerHost:      hostname,
				GitCommit:       buildCommit(),
				ForceRedownload: options.forceRedownload,
			},
		}
		result, err := downloader.Run(ctx)
		if err != nil {
			return errors.Wrapf(err, "download part %d", part)
		}
		runTotals.add(result)
		logger.Info("download part complete",
			"crawl", options.crawlID,
			"selection", selection,
			"part", part,
			"skipped", result.Skipped,
			"chunks", result.ChunkCount,
			"requested_records", result.RequestedRecords,
			"downloaded_records", result.DownloadedRecords,
			"failed_records", result.FailedRecords,
			"raw_bytes", result.RawBytes,
			"raw_size", humanize.IBytes(uint64(result.RawBytes)),
			"run_id", runID,
		)
	}
	if getter, ok := source.(*fetch.S3Getter); ok {
		stats := getter.Stats()
		logger.Info("source S3 stats",
			"get_object_calls", stats.GetObjectCalls,
			"http_attempts", stats.HTTPAttempts,
			"http_503", stats.HTTP503s,
			"body_read_attempts", stats.BodyReadAttempts,
			"body_read_errors", stats.BodyReadErrors,
			"body_read_retries", stats.BodyReadRetries,
			"body_bytes", stats.BodyBytes,
		)
	}
	logger.Info("download run complete",
		"crawl", options.crawlID,
		"selection", selection,
		"parts", runTotals.parts,
		"skipped_parts", runTotals.skippedParts,
		"chunks", runTotals.chunks,
		"requested_records", runTotals.requestedRecords,
		"downloaded_records", runTotals.downloadedRecords,
		"failed_records", runTotals.failedRecords,
		"raw_bytes", runTotals.rawBytes,
		"raw_size", humanize.IBytes(uint64(runTotals.rawBytes)),
		"run_id", runID,
	)
	return nil
}

func parseOptions(args []string) (options, error) {
	var options options
	flags := flag.NewFlagSet("cc-download-worker", flag.ContinueOnError)
	flags.StringVar(&options.baseDirectory, "base", envOrDefault("OUT_BASE_DIR", "data"), "data root; worklists default to <base>/<crawl>/download/worklists/pagesN")
	flags.StringVar(&options.crawlID, "crawl", "", "Common Crawl ID, for example CC-MAIN-2026-25 (required)")
	flags.StringVar(&options.parts, "parts", "", "part number or inclusive range, for example 0 or 0-10 (required)")
	flags.IntVar(&options.pagesPerDomain, "pages-per-domain", 25, "selected pages per domain; 1 selects the best homepage")
	flags.StringVar(&options.worklistDirectory, "worklist-dir", "", "worklist cache directory override")
	flags.BoolVar(&options.rebuildWorklists, "rebuild-worklists", false, "replace valid cached worklists")
	flags.StringVar(&options.sourceBucket, "source-bucket", "commoncrawl", "Common Crawl source bucket")
	flags.StringVar(&options.sourceRegion, "source-region", "us-east-1", "Common Crawl source AWS region")
	flags.BoolVar(&options.sourceAnonymous, "source-anonymous", false, "read Common Crawl through its anonymous HTTPS endpoint")
	flags.IntVar(&options.concurrency, "concurrency", 64, "concurrent WARC record downloads")
	flags.Int64Var(&options.maxPackBytes, "max-pack-bytes", 256<<20, "target maximum advertised bytes per raw pack")
	flags.IntVar(&options.maxRecords, "max-records", 16384, "maximum worklist records per raw pack")
	flags.Float64Var(&options.maxFailureRate, "max-failure-rate", 0.01, "maximum terminal record failure ratio allowed per committed chunk")
	flags.IntVar(&options.recordAttempts, "record-attempts", 3, "logical attempts for transient record failures")
	flags.DurationVar(&options.recordTimeout, "record-timeout", 30*time.Second, "deadline for each logical record attempt")
	flags.StringVar(&options.tempDirectory, "temp-dir", "", "parent directory for temporary pack files (default system temp)")
	flags.StringVar(&options.rustfsEndpoint, "rustfs-endpoint", os.Getenv("CORPSCOUT_S3_ENDPOINT"), "RustFS S3-compatible endpoint")
	flags.StringVar(&options.rustfsRegion, "rustfs-region", "us-east-1", "RustFS signing region")
	flags.StringVar(&options.rustfsBucket, "rustfs-bucket", os.Getenv("CORPSCOUT_S3_BUCKET"), "RustFS bucket")
	flags.BoolVar(&options.forceRedownload, "force-redownload", false, "recreate explicitly reclaimed raw parts")
	if err := flags.Parse(args); err != nil {
		return options, err
	}
	if flags.NArg() != 0 {
		return options, errors.Newf("unexpected arguments: %v", flags.Args())
	}
	options.python = envOrDefault("CC_DOWNLOAD_PYTHON", "python3")
	if strings.TrimSpace(options.baseDirectory) == "" || strings.TrimSpace(options.crawlID) == "" || strings.TrimSpace(options.parts) == "" {
		return options, errors.New("base, crawl, and parts are required")
	}
	if options.pagesPerDomain < 1 {
		return options, errors.New("pages-per-domain must be at least 1")
	}
	if options.concurrency <= 0 || options.maxPackBytes <= 0 || options.maxRecords <= 0 || options.recordTimeout <= 0 {
		return options, errors.New("concurrency, pack limits, and record timeout must be positive")
	}
	if options.maxFailureRate < 0 || options.maxFailureRate > 1 {
		return options, errors.New("max-failure-rate must be between 0 and 1")
	}
	if options.recordAttempts < 1 || options.recordAttempts > 10 {
		return options, errors.New("record-attempts must be between 1 and 10")
	}
	if _, err := parseParts(options.parts); err != nil {
		return options, err
	}
	return options, nil
}

func parseParts(value string) ([]int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("parts are required")
	}
	lowText, highText, ranged := strings.Cut(value, "-")
	if !ranged {
		highText = lowText
	}
	low, err := strconv.Atoi(lowText)
	if err != nil || low < 0 {
		return nil, errors.Newf("invalid parts %q: expected a non-negative number or range N-M", value)
	}
	high, err := strconv.Atoi(highText)
	if err != nil || high < low {
		return nil, errors.Newf("invalid parts %q: expected an ascending range N-M", value)
	}
	if high-low > 10_000 {
		return nil, errors.Newf("invalid parts %q: range exceeds 10001 parts", value)
	}
	parts := make([]int, high-low+1)
	for index := range parts {
		parts[index] = low + index
	}
	return parts, nil
}

func (totals *totals) add(result rawdownload.Result) {
	totals.parts++
	if result.Skipped {
		totals.skippedParts++
	}
	totals.chunks += result.ChunkCount
	totals.requestedRecords += result.RequestedRecords
	totals.downloadedRecords += result.DownloadedRecords
	totals.failedRecords += result.FailedRecords
	totals.rawBytes += result.RawBytes
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func newRunID() (string, error) {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", errors.Wrap(err, "generate run ID")
	}
	return fmt.Sprintf("%s-%x", time.Now().UTC().Format("20060102T150405.000000000Z"), random), nil
}

func buildCommit() string {
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	commit := "unknown"
	modified := false
	for _, setting := range build.Settings {
		switch setting.Key {
		case "vcs.revision":
			commit = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if modified {
		return commit + "-dirty"
	}
	return commit
}
