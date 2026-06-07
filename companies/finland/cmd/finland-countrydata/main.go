package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/companycollect/companies/finland"
	"github.com/pulsarpoint/companycollect/companies/finland/prhytj"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

type cliConfig struct {
	command      string
	source       string
	envPath      string
	dataDir      string
	runID        string
	snapshotPath string
	maxPages     int
	chunkSize    int
	buildExport  bool
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		slog.Error("parse Finland countrydata command", "error", err)
		os.Exit(2)
	}
	result, err := run(context.Background(), cfg)
	if err != nil {
		slog.Error("run Finland countrydata command", "command", cfg.command, "source", cfg.source, "error_kind", countryimport.Classify(err), "error", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		slog.Error("write Finland countrydata result", "error", err)
		os.Exit(1)
	}
}

func parseArgs(args []string) (cliConfig, error) {
	if len(args) == 0 {
		return cliConfig{}, fmt.Errorf("missing command")
	}

	command := args[0]
	switch command {
	case "sync-source", "status-source", "export-source", "status", "build-export", "sync":
	default:
		return cliConfig{}, fmt.Errorf("unknown command %q", command)
	}

	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	cfg := cliConfig{command: command}
	flags.StringVar(&cfg.envPath, "env", "", "path to env file")
	flags.StringVar(&cfg.source, "source", "", "source slug")
	flags.StringVar(&cfg.dataDir, "data-dir", "", "Finland countrydata directory")
	flags.StringVar(&cfg.runID, "run-id", "", "export run ID")
	flags.StringVar(&cfg.snapshotPath, "snapshot-path", "", "source snapshot path")
	flags.IntVar(&cfg.maxPages, "max-pages", 0, "maximum pages to download")
	flags.IntVar(&cfg.chunkSize, "chunk-size", 0, "records per chunk")
	flags.BoolVar(&cfg.buildExport, "build-export", false, "build final export after sync")

	if err := flags.Parse(args[1:]); err != nil {
		return cliConfig{}, err
	}

	if cfg.source != "" && cfg.source != finland.SourcePRHYTJ {
		return cliConfig{}, fmt.Errorf("unknown source %q", cfg.source)
	}
	if requiresSource(command) {
		if cfg.source == "" {
			return cliConfig{}, fmt.Errorf("missing --source")
		}
	}

	return cfg, nil
}

func run(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	if cfg.envPath != "" {
		if err := countryimport.LoadEnvFile(cfg.envPath); err != nil {
			return nil, errors.Wrapf(err, "load env file %s", cfg.envPath)
		}
	}

	switch cfg.command {
	case "sync-source":
		return runSyncSource(ctx, cfg)
	case "export-source":
		return runExportSource(ctx, cfg)
	case "status-source":
		return runStatusSource(cfg)
	case "status":
		return runStatus(cfg)
	case "build-export":
		return runBuildExport(ctx, cfg)
	case "sync":
		result, err := runSyncSource(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if cfg.buildExport {
			buildResult, err := buildFinalExport(ctx, cfg)
			if err != nil {
				return nil, errors.Wrap(err, "build Finland final export")
			}
			result["final_manifest_path"] = buildResult.ManifestPath
			result["final_records_exported"] = buildResult.RecordsExported
			result["final_run_id"] = buildResult.RunID
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unknown command %q", cfg.command)
	}
}

func runSyncSource(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	source := newPRHSource()
	sourceDataDir := sourceDataDir(cfg)
	downloadResult, err := source.Download(ctx, countryimport.DownloadOptions{
		DataDir:  sourceDataDir,
		MaxPages: cfg.maxPages,
	})
	if err != nil {
		return nil, errors.Wrap(err, "download PRH YTJ snapshot")
	}

	exportResult, err := source.Export(ctx, prhytj.ExportOptions{
		DataDir:      sourceDataDir,
		SnapshotPath: downloadResult.SnapshotPath,
		RunID:        cfg.runID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "export PRH YTJ source")
	}

	return sourceResultMap(cfg.command, exportResult, map[string]any{
		"snapshot_path":      downloadResult.SnapshotPath,
		"pages_downloaded":   downloadResult.PagesDownloaded,
		"records_downloaded": downloadResult.RecordsSeen,
	}), nil
}

func runExportSource(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	result, err := newPRHSource().Export(ctx, prhytj.ExportOptions{
		DataDir:      sourceDataDir(cfg),
		SnapshotPath: cfg.snapshotPath,
		RunID:        cfg.runID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "export PRH YTJ source")
	}

	return sourceResultMap(cfg.command, result, map[string]any{
		"snapshot_path": cfg.snapshotPath,
	}), nil
}

func runStatusSource(cfg cliConfig) (map[string]any, error) {
	status, err := finland.SourceStatusFromLatestManifest(cfg.dataDir, cfg.source)
	if err != nil {
		return nil, errors.Wrap(err, "load PRH YTJ source status")
	}

	return map[string]any{
		"command":              cfg.command,
		"country_iso2":         finland.CountryISO2,
		"source":               cfg.source,
		"status":               status.Status,
		"source_manifest_path": status.LastExportManifestPath,
		"source_status":        status,
	}, nil
}

func runStatus(cfg cliConfig) (map[string]any, error) {
	prhStatus, err := finland.SourceStatusFromLatestManifest(cfg.dataDir, finland.SourcePRHYTJ)
	if err != nil {
		return nil, errors.Wrap(err, "load PRH YTJ source status")
	}

	return map[string]any{
		"command":                     cfg.command,
		"country_iso2":                finland.CountryISO2,
		"status":                      "ok",
		"prhytj_source_manifest_path": prhStatus.LastExportManifestPath,
		"sources": map[string]finland.SourceStatus{
			finland.SourcePRHYTJ: prhStatus,
		},
	}, nil
}

func runBuildExport(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	result, err := buildFinalExport(ctx, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "build Finland final export")
	}

	return map[string]any{
		"command":             cfg.command,
		"country_iso2":        finland.CountryISO2,
		"status":              "ok",
		"run_id":              result.RunID,
		"manifest_path":       result.ManifestPath,
		"records_exported":    result.RecordsExported,
		"final_manifest_path": result.ManifestPath,
	}, nil
}

func requiresSource(command string) bool {
	switch command {
	case "sync-source", "status-source", "export-source", "sync":
		return true
	default:
		return false
	}
}

func newPRHSource() *prhytj.Source {
	return prhytj.NewSource(prhytj.ConfigFromEnv())
}

func sourceDataDir(cfg cliConfig) string {
	return finland.LayoutForDataDir(cfg.dataDir).SourceDir(finland.SourcePRHYTJ)
}

func buildFinalExport(ctx context.Context, cfg cliConfig) (finland.BuildExportResult, error) {
	return finland.BuildFinalExport(ctx, finland.BuildExportOptions{
		DataDir: cfg.dataDir,
		RunID:   cfg.runID,
	})
}

func sourceResultMap(command string, result prhytj.ExportResult, extra map[string]any) map[string]any {
	response := map[string]any{
		"command":              command,
		"country_iso2":         finland.CountryISO2,
		"source":               finland.SourcePRHYTJ,
		"status":               "ok",
		"run_id":               result.RunID,
		"manifest_path":        result.ManifestPath,
		"source_manifest_path": result.ManifestPath,
		"records_seen":         result.RecordsSeen,
		"records_exported":     result.RecordsExported,
		"decode_errors":        result.DecodeErrors,
	}
	for key, value := range extra {
		response[key] = value
	}
	return response
}
