package main

import (
	"context"
	"flag"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"cc-download-worker/internal/partspec"
	"cc-download-worker/internal/warcsize"
	"cc-download-worker/internal/worklistbuilder"
	"cc-download-worker/planstore"
	"cc-download-worker/rangeplanner"
	"cc-raw/rawstore"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

type options struct {
	baseDirectory       string
	crawlID             string
	parts               string
	mode                string
	skipLoaded          bool
	pagesPerDomain      int
	worklistDirectory   string
	rebuildWorklists    bool
	python              string
	maxPackBytes        int64
	maxRecords          int
	maxRangeBytes       int64
	junkThresholds      string
	wholeWARCThresholds string
	wholeWARCSizes      bool
	warcSizeBaseURL     string
	warcSizeCache       string
	warcHeadConc        int
	planDatabase        string
	planThreshold       float64
	planStatsLimit      int
	check               bool
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(context.Background(), logger, os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		logger.Error("WARC analysis failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, args []string) error {
	options, err := parseOptions(args)
	if err != nil {
		return err
	}
	parts, err := partspec.Parse(options.parts)
	if err != nil {
		return err
	}
	requestedParts := partspec.Format(parts)
	selection := worklistbuilder.Selection(options.pagesPerDomain)
	planDatabasePath := resolvedPlanDatabasePath(options, selection, requestedParts)
	if options.check {
		return checkPlan(ctx, logger, options, requestedParts, planDatabasePath)
	}
	skippedLoadedParts := []int(nil)
	if options.skipLoaded {
		parts, skippedLoadedParts, err = filterLoadedParts(options.baseDirectory, options.crawlID, options.mode, parts)
		if err != nil {
			return err
		}
	}
	analyzedParts := partspec.Format(parts)
	if len(parts) == 0 {
		logger.Info("WARC analysis complete: all requested parts are already loaded",
			"crawl", options.crawlID,
			"mode", options.mode,
			"requested_parts", requestedParts,
			"skipped_loaded_parts", partspec.Format(skippedLoadedParts),
		)
		return nil
	}
	thresholds, err := parseThresholds(options.junkThresholds)
	if err != nil {
		return err
	}
	wholeWARCThresholds, err := parseThresholds(options.wholeWARCThresholds)
	if err != nil {
		return err
	}
	worklistDirectory := options.worklistDirectory
	if worklistDirectory == "" {
		worklistDirectory = worklistbuilder.DefaultDirectory(options.baseDirectory, options.crawlID, options.pagesPerDomain)
	}
	plannerStore, err := planstore.Open(planDatabasePath)
	if err != nil {
		return err
	}
	defer plannerStore.Close()
	if err := plannerStore.SyncParts(ctx, parts); err != nil {
		return err
	}

	logger.Info("WARC analysis started",
		"crawl", options.crawlID,
		"selection", selection,
		"mode", options.mode,
		"requested_parts", requestedParts,
		"parts", analyzedParts,
		"skipped_loaded_parts", partspec.Format(skippedLoadedParts),
		"skipped_loaded_count", len(skippedLoadedParts),
		"max_range_bytes", options.maxRangeBytes,
		"max_range_size", humanize.IBytes(uint64(options.maxRangeBytes)),
		"junk_thresholds", thresholds,
		"whole_warc_thresholds", wholeWARCThresholds,
		"plan_database", planDatabasePath,
		"plan_whole_warc_threshold", options.planThreshold,
	)

	var allRecords []rangeplanner.Record
	var outputChunkGroups [][]rangeplanner.Record
	var partGroups [][]rangeplanner.Record
	warcFiles := make(map[string]struct{})
	for _, part := range parts {
		result, err := worklistbuilder.Ensure(ctx, worklistbuilder.Config{
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
		worklist, err := rangeplanner.ReadWorklist(result.Path, options.maxPackBytes, options.maxRecords)
		if err != nil && result.Reused {
			result, err = worklistbuilder.Ensure(ctx, worklistbuilder.Config{
				Python:         options.python,
				CrawlID:        options.crawlID,
				PagesPerDomain: options.pagesPerDomain,
				Part:           part,
				OutputPath:     worklistbuilder.Path(worklistDirectory, part),
				Rebuild:        true,
			})
			if err != nil {
				return errors.Wrapf(err, "rebuild invalid worklist part %d", part)
			}
			worklist, err = rangeplanner.ReadWorklist(result.Path, options.maxPackBytes, options.maxRecords)
		}
		if err != nil {
			return errors.Wrapf(err, "read worklist part %d", part)
		}
		checksum, _, err := rawstore.ChecksumFile(result.Path)
		if err != nil {
			return errors.Wrapf(err, "checksum worklist part %d", part)
		}
		planReused, err := plannerStore.ImportPart(ctx, part, string(checksum), worklist)
		if err != nil {
			return errors.Wrapf(err, "import worklist part %d into plan database", part)
		}
		firstID := int64(len(allRecords))
		for index := range worklist.Records {
			worklist.Records[index].ID = firstID + int64(index)
		}
		allRecords = append(allRecords, worklist.Records...)
		partGroups = append(partGroups, worklist.Records)
		outputChunkGroups = append(outputChunkGroups, worklist.OutputChunks...)
		for _, filename := range worklist.WARCFiles {
			warcFiles[filename] = struct{}{}
		}
		logger.Info("analysis worklist ready",
			"crawl", options.crawlID,
			"selection", selection,
			"part", part,
			"rows", len(worklist.Records),
			"output_chunks", len(worklist.OutputChunks),
			"warc_objects", len(worklist.WARCFiles),
			"reused", result.Reused,
			"plan_reused", planReused,
		)
	}
	if err := plannerStore.RefreshStrategies(ctx, options.planThreshold); err != nil {
		return err
	}

	logEstimate(logger, options, analyzedParts, rangeplanner.ExactEstimate(allRecords))
	policies := analysisPolicies(options.maxRangeBytes, thresholds)
	scopes := []struct {
		name   string
		groups [][]rangeplanner.Record
	}{
		{name: "output_chunk", groups: outputChunkGroups},
		{name: "part", groups: partGroups},
		{name: "part_block", groups: [][]rangeplanner.Record{allRecords}},
	}
	for _, scope := range scopes {
		for _, policy := range policies {
			estimate, err := rangeplanner.EstimateGroups(scope.name, scope.groups, policy)
			if err != nil {
				return errors.Wrapf(err, "estimate %s in scope %s", policy.Name, scope.name)
			}
			logEstimate(logger, options, analyzedParts, estimate)
		}
	}

	if options.wholeWARCSizes {
		filenames := make([]string, 0, len(warcFiles))
		for filename := range warcFiles {
			filenames = append(filenames, filename)
		}
		sort.Strings(filenames)
		cachePath := options.warcSizeCache
		if cachePath == "" {
			cachePath = filepath.Join(options.baseDirectory, options.crawlID, "analysis", "warc_sizes.json")
		}
		cache, err := warcsize.LoadCache(cachePath)
		if err != nil {
			return err
		}
		logger.Info("whole WARC size measurement started",
			"warc_objects", len(filenames),
			"cached_objects", cachedObjects(filenames, cache),
			"concurrency", options.warcHeadConc,
		)
		summary, err := warcsize.Measure(ctx, filenames, options.warcSizeBaseURL, options.warcHeadConc, cache)
		if err != nil {
			if saveErr := warcsize.SaveCache(cachePath, cache); saveErr != nil {
				return errors.CombineErrors(err, saveErr)
			}
			return err
		}
		if err := warcsize.SaveCache(cachePath, cache); err != nil {
			return err
		}
		if err := plannerStore.SetObjectSizes(ctx, cache); err != nil {
			return err
		}
		if err := plannerStore.RefreshStrategies(ctx, options.planThreshold); err != nil {
			return err
		}
		exact := rangeplanner.ExactEstimate(allRecords)
		for _, threshold := range wholeWARCThresholds {
			hybrid, err := rangeplanner.PlanWholeWARCHybrid(allRecords, cache, threshold)
			if err != nil {
				return errors.Wrapf(err, "plan hybrid whole-WARC threshold %g", threshold)
			}
			logEstimate(logger, options, analyzedParts, hybrid.Estimate("part_block"))
		}
		logEstimate(logger, options, analyzedParts, rangeplanner.Estimate{
			Algorithm:       "whole_warc_objects",
			Scope:           "part_block",
			WARCObjects:     summary.Objects,
			SelectedRecords: exact.SelectedRecords,
			SelectedBytes:   exact.SelectedBytes,
			SourceRequests:  int64(summary.Objects),
			SourceBytes:     summary.Bytes,
		})
		logger.Info("whole WARC sizes cached",
			"cache", cachePath,
			"cache_hits", summary.CacheHits,
			"http_checks", summary.HTTPChecks,
		)
	}
	if err := logPlanStatistics(ctx, logger, plannerStore, options, requestedParts, analyzedParts); err != nil {
		return err
	}

	logger.Info("WARC analysis complete",
		"crawl", options.crawlID,
		"selection", selection,
		"requested_parts", requestedParts,
		"parts", analyzedParts,
		"skipped_loaded_parts", partspec.Format(skippedLoadedParts),
		"selected_records", len(allRecords),
		"warc_objects", len(warcFiles),
	)
	return nil
}

func resolvedPlanDatabasePath(options options, selection, requestedParts string) string {
	if options.planDatabase != "" {
		return options.planDatabase
	}
	return filepath.Join(
		options.baseDirectory,
		options.crawlID,
		"download",
		"plans",
		selection,
		options.mode+"_parts_"+strings.ReplaceAll(requestedParts, ",", "_")+".sqlite",
	)
}

func checkPlan(ctx context.Context, logger *slog.Logger, options options, requestedParts, databasePath string) error {
	if _, err := os.Stat(databasePath); err != nil {
		return errors.Wrap(err, "open existing SQLite plan for check")
	}
	store, err := planstore.OpenReadOnly(databasePath)
	if err != nil {
		return err
	}
	defer store.Close()
	parts, err := store.Parts(ctx)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return errors.New("SQLite plan contains no parts")
	}
	if err := logPlanStatistics(ctx, logger, store, options, requestedParts, partspec.Format(parts)); err != nil {
		return err
	}
	logger.Info("SQLite WARC plan check complete",
		"crawl", options.crawlID,
		"requested_parts", requestedParts,
		"parts", partspec.Format(parts),
		"database", databasePath,
	)
	return nil
}

func logPlanStatistics(
	ctx context.Context,
	logger *slog.Logger,
	store *planstore.Store,
	options options,
	requestedParts string,
	analyzedParts string,
) error {
	stats, err := store.Stats(ctx)
	if err != nil {
		return err
	}
	junkBytes := max(int64(0), stats.EstimatedSourceBytes-stats.PendingBytes)
	logger.Info("SQLite WARC plan stats",
		"crawl", options.crawlID,
		"mode", options.mode,
		"requested_parts", requestedParts,
		"parts", analyzedParts,
		"database", store.Path(),
		"whole_warc_threshold_percent", stats.WholeWARCThresholdPercent,
		"planned_parts", stats.Parts,
		"chunks", stats.Chunks,
		"committed_chunks", stats.CommittedChunks,
		"pages", stats.Pages,
		"committed_pages", stats.Pages-stats.PendingPages,
		"pending_pages", stats.PendingPages,
		"pending_bytes", stats.PendingBytes,
		"pending_size", humanize.IBytes(uint64(stats.PendingBytes)),
		"warc_objects", stats.WARCObjects,
		"whole_warc_objects", stats.WholeWARCObjects,
		"whole_warc_pages", stats.WholeWARCPages,
		"whole_warc_selected_bytes", stats.WholeWARCSelectedBytes,
		"whole_warc_selected_size", humanize.IBytes(uint64(stats.WholeWARCSelectedBytes)),
		"whole_warc_download_bytes", stats.WholeWARCDownloadBytes,
		"whole_warc_download_size", humanize.IBytes(uint64(stats.WholeWARCDownloadBytes)),
		"exact_warc_objects", stats.ExactWARCObjects,
		"individual_pages", stats.ExactPages,
		"individual_bytes", stats.ExactSelectedBytes,
		"individual_size", humanize.IBytes(uint64(stats.ExactSelectedBytes)),
		"estimated_requests", stats.EstimatedRequests,
		"estimated_source_bytes", stats.EstimatedSourceBytes,
		"estimated_source_size", humanize.IBytes(uint64(stats.EstimatedSourceBytes)),
		"junk_bytes", junkBytes,
		"junk_size", humanize.IBytes(uint64(junkBytes)),
	)
	buckets, err := store.UtilizationBuckets(ctx)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		logger.Info("SQLite WARC utilization bucket",
			"utilization_from_percent", bucket.FromPercent,
			"utilization_to_percent", bucket.ToPercent,
			"warc_objects", bucket.WARCObjects,
			"pages", bucket.Pages,
			"selected_bytes", bucket.SelectedBytes,
			"selected_size", humanize.IBytes(uint64(bucket.SelectedBytes)),
			"object_bytes", bucket.ObjectBytes,
			"object_size", humanize.IBytes(uint64(bucket.ObjectBytes)),
			"junk_bytes", bucket.JunkBytes,
			"junk_size", humanize.IBytes(uint64(bucket.JunkBytes)),
		)
	}
	warcs, err := store.WARCStats(ctx, options.planStatsLimit)
	if err != nil {
		return err
	}
	for _, warc := range warcs {
		logger.Info("SQLite WARC plan object",
			"warc_filename", warc.Filename,
			"object_bytes", warc.ObjectBytes,
			"object_size", humanize.IBytes(uint64(warc.ObjectBytes)),
			"pending_pages", warc.PendingPages,
			"pending_bytes", warc.PendingBytes,
			"pending_size", humanize.IBytes(uint64(warc.PendingBytes)),
			"selected_percent", warc.SelectedPercent,
			"strategy", warc.Strategy,
			"cache_state", warc.CacheState,
			"local_path", warc.LocalPath,
		)
	}
	return nil
}

func analysisPolicies(maxRangeBytes int64, thresholds []float64) []rangeplanner.Policy {
	policies := []rangeplanner.Policy{
		{Name: "bounded_gap_64k", MaxGapBytes: 64 << 10, MaxRangeBytes: maxRangeBytes, MaxJunkPercent: 100},
		{Name: "bounded_gap_256k", MaxGapBytes: 256 << 10, MaxRangeBytes: maxRangeBytes, MaxJunkPercent: 100},
		{Name: "bounded_gap_1m", MaxGapBytes: 1 << 20, MaxRangeBytes: maxRangeBytes, MaxJunkPercent: 100},
	}
	for _, threshold := range thresholds {
		policies = append(policies, rangeplanner.Policy{
			Name:           "junk_threshold_" + strconv.FormatFloat(threshold, 'f', -1, 64),
			MaxGapBytes:    maxRangeBytes,
			MaxRangeBytes:  maxRangeBytes,
			MaxJunkPercent: threshold,
		})
	}
	return append(policies, rangeplanner.Policy{
		Name: "selected_warc_span_lower_bound", MaxGapBytes: 1 << 40, MaxRangeBytes: 1 << 40, MaxJunkPercent: 100,
	})
}

func logEstimate(logger *slog.Logger, options options, analyzedParts string, estimate rangeplanner.Estimate) {
	junkBytes := max(int64(0), estimate.SourceBytes-estimate.SelectedBytes)
	requestReductionPercent := percent(estimate.SelectedRecords-estimate.SourceRequests, estimate.SelectedRecords)
	selectedRecordsPerRequest := ratio(estimate.SelectedRecords, estimate.SourceRequests)
	selectedDensityPercent := percent(estimate.SelectedBytes, estimate.SourceBytes)
	logger.Info("WARC download estimate",
		"crawl", options.crawlID,
		"requested_parts", options.parts,
		"parts", analyzedParts,
		"algorithm", estimate.Algorithm,
		"scope", estimate.Scope,
		"warc_objects", estimate.WARCObjects,
		"max_gap_bytes", estimate.Policy.MaxGapBytes,
		"max_range_bytes", estimate.Policy.MaxRangeBytes,
		"max_junk_percent", estimate.Policy.MaxJunkPercent,
		"whole_warc_threshold_percent", estimate.WholeWARCThresholdPercent,
		"whole_warc_objects", estimate.WholeWARCObjects,
		"exact_warc_objects", estimate.ExactWARCObjects,
		"exact_record_requests", estimate.ExactRecordRequests,
		"selected_records", estimate.SelectedRecords,
		"selected_bytes", estimate.SelectedBytes,
		"selected_size", humanize.IBytes(uint64(estimate.SelectedBytes)),
		"source_requests", estimate.SourceRequests,
		"multi_record_requests", estimate.MultiRecordRequests,
		"max_records_per_request", estimate.MaxRecordsPerRequest,
		"selected_records_per_request", selectedRecordsPerRequest,
		"request_reduction_percent", requestReductionPercent,
		"source_bytes", estimate.SourceBytes,
		"source_size", humanize.IBytes(uint64(estimate.SourceBytes)),
		"junk_bytes", junkBytes,
		"junk_size", humanize.IBytes(uint64(junkBytes)),
		"junk_percent_of_source", 100-selectedDensityPercent,
		"selected_byte_density_percent", selectedDensityPercent,
	)
}

func parseOptions(args []string) (options, error) {
	var options options
	flags := flag.NewFlagSet("cc-warc-analyzer", flag.ContinueOnError)
	flags.StringVar(&options.baseDirectory, "base", envOrDefault("OUT_BASE_DIR", "data"), "data root containing generated worklists and analysis cache")
	flags.StringVar(&options.crawlID, "crawl", "", "Common Crawl ID (required)")
	flags.StringVar(&options.parts, "parts", "", "part number or inclusive range, for example 85 or 85-114 (required)")
	flags.StringVar(&options.mode, "mode", "tech", "loaded-marker mode used to exclude completed parts: tech or industry")
	flags.BoolVar(&options.skipLoaded, "skip-loaded", true, "exclude parts carrying out_<mode>_<part>.loaded")
	flags.IntVar(&options.pagesPerDomain, "pages-per-domain", 25, "selected pages per domain")
	flags.StringVar(&options.worklistDirectory, "worklist-dir", "", "worklist cache directory override")
	flags.BoolVar(&options.rebuildWorklists, "rebuild-worklists", false, "replace valid cached worklists")
	flags.Int64Var(&options.maxPackBytes, "max-pack-bytes", 256<<20, "logical output pack size used for chunk-scope estimates")
	flags.IntVar(&options.maxRecords, "max-records", 16384, "logical output pack record limit")
	flags.Int64Var(&options.maxRangeBytes, "max-range-bytes", 16<<20, "maximum candidate coalesced source range")
	flags.StringVar(&options.junkThresholds, "junk-thresholds", "10,25,50,75", "comma-separated maximum junk percentages to compare")
	flags.StringVar(&options.wholeWARCThresholds, "whole-warc-thresholds", "25,50,75", "compressed selected-byte percentages that trigger a complete WARC download")
	flags.BoolVar(&options.wholeWARCSizes, "whole-warc-sizes", true, "measure exact sizes of referenced complete WARC objects")
	flags.StringVar(&options.warcSizeBaseURL, "warc-base-url", "https://data.commoncrawl.org", "base URL used only for WARC size metadata checks")
	flags.StringVar(&options.warcSizeCache, "warc-size-cache", "", "WARC size JSON cache path override")
	flags.IntVar(&options.warcHeadConc, "warc-head-concurrency", 32, "concurrent WARC size metadata checks")
	flags.StringVar(&options.planDatabase, "plan-db", "", "SQLite plan database path override")
	flags.Float64Var(&options.planThreshold, "plan-whole-warc-threshold", 50, "selected-byte percentage used for the persisted SQLite strategy")
	flags.IntVar(&options.planStatsLimit, "plan-stats-limit", 10, "highest-utilization WARC rows logged from the SQLite plan")
	flags.BoolVar(&options.check, "check", false, "read and report an existing SQLite plan without changing it")
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
	if options.mode != "tech" && options.mode != "industry" {
		return options, errors.New("mode must be tech or industry")
	}
	if options.pagesPerDomain < 1 || options.maxPackBytes <= 0 || options.maxRecords <= 0 || options.maxRangeBytes <= 0 || options.warcHeadConc <= 0 || options.planStatsLimit <= 0 {
		return options, errors.New("page, pack, range, record, and concurrency limits must be positive")
	}
	if math.IsNaN(options.planThreshold) || math.IsInf(options.planThreshold, 0) || options.planThreshold < 0 || options.planThreshold > 100 {
		return options, errors.New("plan whole-WARC threshold must be between 0 and 100")
	}
	if _, err := partspec.Parse(options.parts); err != nil {
		return options, err
	}
	if _, err := parseThresholds(options.junkThresholds); err != nil {
		return options, err
	}
	if _, err := parseThresholds(options.wholeWARCThresholds); err != nil {
		return options, err
	}
	return options, nil
}

func filterLoadedParts(baseDirectory, crawlID, mode string, parts []int) ([]int, []int, error) {
	pending := make([]int, 0, len(parts))
	loaded := make([]int, 0)
	markerDirectory := filepath.Join(baseDirectory, crawlID, "crawl")
	for _, part := range parts {
		marker := filepath.Join(markerDirectory, "out_"+mode+"_"+strconv.Itoa(part)+".loaded")
		_, err := os.Stat(marker)
		switch {
		case err == nil:
			loaded = append(loaded, part)
		case errors.Is(err, os.ErrNotExist):
			pending = append(pending, part)
		default:
			return nil, nil, errors.Wrapf(err, "check loaded marker for part %d", part)
		}
	}
	return pending, loaded, nil
}

func parseThresholds(value string) ([]float64, error) {
	fields := strings.Split(value, ",")
	thresholds := make([]float64, 0, len(fields))
	for _, field := range fields {
		threshold, err := strconv.ParseFloat(strings.TrimSpace(field), 64)
		if err != nil || math.IsNaN(threshold) || math.IsInf(threshold, 0) || threshold < 0 || threshold > 100 {
			return nil, errors.Newf("invalid junk threshold %q: expected percentages from 0 to 100", field)
		}
		thresholds = append(thresholds, threshold)
	}
	if len(thresholds) == 0 {
		return nil, errors.New("at least one junk threshold is required")
	}
	return thresholds, nil
}

func cachedObjects(files []string, cache warcsize.Cache) int {
	count := 0
	for _, filename := range files {
		if cache[filename] > 0 {
			count++
		}
	}
	return count
}

func percent(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return 100 * float64(numerator) / float64(denominator)
}

func ratio(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
