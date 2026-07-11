package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"cc-enrich-worker/internal/fetch"
	"cc-enrich-worker/internal/rawdownload"
	"cc-enrich-worker/internal/rawstore"
	"github.com/cockroachdb/errors"
)

type options struct {
	worklist, worklistKey string
	crawlID, selection    string
	part                  int
	sourceBucket          string
	sourceRegion          string
	sourceAnonymous       bool
	concurrency           int
	maxPackBytes          int64
	maxRecords            int
	maxFailureRate        float64
	recordTimeout         time.Duration
	tempDir               string
	rustfsEndpoint        string
	rustfsRegion          string
	rustfsBucket          string
	forceRedownload       bool
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
	worklistKey := options.worklistKey
	if worklistKey == "" {
		worklistKey = filepath.Base(options.worklist)
	}

	logger.Info("download started",
		"crawl", options.crawlID,
		"selection", options.selection,
		"part", options.part,
		"worklist", options.worklist,
		"rustfs_bucket", options.rustfsBucket,
		"concurrency", options.concurrency,
		"max_pack_bytes", options.maxPackBytes,
		"max_records", options.maxRecords,
		"run_id", runID,
	)
	downloader := rawdownload.Downloader{
		Source: source,
		Store:  store,
		Logger: logger,
		Config: rawdownload.Config{
			WorklistPath:    options.worklist,
			WorklistKey:     worklistKey,
			CrawlID:         options.crawlID,
			Selection:       options.selection,
			Part:            options.part,
			SourceBucket:    options.sourceBucket,
			Concurrency:     options.concurrency,
			MaxPackBytes:    options.maxPackBytes,
			MaxRecords:      options.maxRecords,
			MaxFailureRate:  options.maxFailureRate,
			RecordTimeout:   options.recordTimeout,
			TempDir:         options.tempDir,
			RunID:           runID,
			WorkerHost:      hostname,
			GitCommit:       buildCommit(),
			ForceRedownload: options.forceRedownload,
		},
	}
	result, err := downloader.Run(ctx)
	if err != nil {
		return err
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
	logger.Info("download complete",
		"crawl", options.crawlID,
		"selection", options.selection,
		"part", options.part,
		"skipped", result.Skipped,
		"chunks", result.ChunkCount,
		"requested_records", result.RequestedRecords,
		"downloaded_records", result.DownloadedRecords,
		"failed_records", result.FailedRecords,
		"raw_bytes", result.RawBytes,
		"run_id", runID,
	)
	return nil
}

func parseOptions(args []string) (options, error) {
	var options options
	flags := flag.NewFlagSet("cc-download-worker", flag.ContinueOnError)
	flags.StringVar(&options.worklist, "worklist", "", "worklist Parquet shard (required)")
	flags.StringVar(&options.worklistKey, "worklist-key", "", "stable logical worklist key (default file name)")
	flags.StringVar(&options.crawlID, "crawl-id", "", "Common Crawl ID, for example CC-MAIN-2026-25 (required)")
	flags.StringVar(&options.selection, "selection", "", "worklist selection identity, for example tech25 (required)")
	flags.IntVar(&options.part, "part", -1, "worklist part number (required)")
	flags.StringVar(&options.sourceBucket, "source-bucket", "commoncrawl", "Common Crawl source bucket")
	flags.StringVar(&options.sourceRegion, "source-region", "us-east-1", "Common Crawl source AWS region")
	flags.BoolVar(&options.sourceAnonymous, "source-anonymous", false, "read Common Crawl through its anonymous HTTPS endpoint")
	flags.IntVar(&options.concurrency, "concurrency", 64, "concurrent WARC record downloads")
	flags.Int64Var(&options.maxPackBytes, "max-pack-bytes", 256<<20, "target maximum advertised bytes per raw pack")
	flags.IntVar(&options.maxRecords, "max-records", 16384, "maximum worklist records per raw pack")
	flags.Float64Var(&options.maxFailureRate, "max-failure-rate", 0.01, "maximum terminal record failure ratio allowed per committed chunk")
	flags.DurationVar(&options.recordTimeout, "record-timeout", 30*time.Second, "deadline for one WARC record")
	flags.StringVar(&options.tempDir, "temp-dir", "", "parent directory for temporary pack files (default system temp)")
	flags.StringVar(&options.rustfsEndpoint, "rustfs-endpoint", os.Getenv("CORPSCOUT_S3_ENDPOINT"), "RustFS S3-compatible endpoint")
	flags.StringVar(&options.rustfsRegion, "rustfs-region", "us-east-1", "RustFS signing region")
	flags.StringVar(&options.rustfsBucket, "rustfs-bucket", os.Getenv("CORPSCOUT_S3_BUCKET"), "RustFS bucket")
	flags.BoolVar(&options.forceRedownload, "force-redownload", false, "recreate an explicitly reclaimed raw part")
	if err := flags.Parse(args); err != nil {
		return options, err
	}
	if flags.NArg() != 0 {
		return options, errors.Newf("unexpected arguments: %v", flags.Args())
	}
	if options.worklist == "" || options.crawlID == "" || options.selection == "" || options.part < 0 {
		return options, errors.New("worklist, crawl-id, selection, and a non-negative part are required")
	}
	return options, nil
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
