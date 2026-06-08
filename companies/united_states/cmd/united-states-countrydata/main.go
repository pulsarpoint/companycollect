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
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
	unitedstates "github.com/pulsarpoint/companycollect/companies/united_states"
	"github.com/pulsarpoint/companycollect/companies/united_states/coloradoentities"
	"github.com/pulsarpoint/companycollect/companies/united_states/irseobmf"
	"github.com/pulsarpoint/companycollect/companies/united_states/secedgar"
)

type cliConfig struct {
	command      string
	source       string
	envPath      string
	dataDir      string
	runID        string
	snapshotPath string
	chunkSize    int
	maxPages     int
}

// supportedSources are the source slugs the CLI can sync, export, and report.
var supportedSources = map[string]bool{
	unitedstates.SourceSECEdgar:         true,
	unitedstates.SourceIRSEOBMF:         true,
	unitedstates.SourceColoradoEntities: true,
}

// sourceExport is a source-agnostic view of a source parquet export result.
type sourceExport struct {
	source          string
	runID           string
	manifestPath    string
	recordsSeen     int64
	recordsExported int64
	decodeErrors    int64
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		command, source := commandAndSourceFromArgs(os.Args[1:])
		slog.Error("parse United States countrydata command", "command", command, "source", source, "error_kind", countryimport.ErrorKindInvalidConfig, "error", err)
		os.Exit(2)
	}
	result, err := run(context.Background(), cfg)
	if err != nil {
		slog.Error("run United States countrydata command", "command", cfg.command, "source", cfg.source, "error_kind", countryimport.Classify(err), "error", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		slog.Error("write United States countrydata result", "command", cfg.command, "source", cfg.source, "error_kind", countryimport.Classify(err), "error", err)
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
	flags.StringVar(&cfg.dataDir, "data-dir", "", "United States countrydata directory")
	flags.StringVar(&cfg.runID, "run-id", "", "export run ID")
	flags.StringVar(&cfg.snapshotPath, "snapshot-path", "", "source snapshot path")
	flags.IntVar(&cfg.chunkSize, "chunk-size", 0, "records per chunk")
	flags.IntVar(&cfg.maxPages, "max-pages", 0, "bounded remote sync page/file count")

	if err := flags.Parse(args[1:]); err != nil {
		return cliConfig{}, err
	}

	if requiresSource(command) {
		if cfg.source == "" {
			return cliConfig{}, fmt.Errorf("missing --source")
		}
		if !supportedSources[cfg.source] {
			return cliConfig{}, fmt.Errorf("source %q is not implemented for United States countrydata CLI source commands", cfg.source)
		}
	} else if cfg.source != "" && !supportedSources[cfg.source] {
		return cliConfig{}, fmt.Errorf("unknown source %q", cfg.source)
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
	case "sync-source", "sync":
		return runSyncSource(ctx, cfg)
	case "export-source":
		return runExportSource(ctx, cfg)
	case "status-source":
		return runStatusSource(cfg)
	case "status":
		return runStatus(cfg)
	case "build-export":
		return runBuildExport(ctx, cfg)
	default:
		return nil, fmt.Errorf("unknown command %q", cfg.command)
	}
}

func runSyncSource(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	sourceDataDir := sourceDataDir(cfg)
	downloadOpts := countryimport.DownloadOptions{DataDir: sourceDataDir, MaxPages: cfg.maxPages}
	processOpts := countryimport.ProcessOptions{DataDir: sourceDataDir, ChunkSize: cfg.chunkSize}

	var snapshotPath string
	var recordsDownloaded int64
	var recordsProcessed int64
	var export sourceExport

	switch cfg.source {
	case unitedstates.SourceSECEdgar:
		src := secedgar.NewSource(secedgar.ConfigFromEnv())
		download, err := src.Download(ctx, downloadOpts)
		if err != nil {
			return nil, errors.Wrap(err, "download SEC EDGAR snapshot")
		}
		processOpts.SnapshotPath = download.SnapshotPath
		process, err := src.Process(ctx, processOpts)
		if err != nil {
			return nil, errors.Wrap(err, "process SEC EDGAR snapshot")
		}
		result, err := src.Export(ctx, secedgar.ExportOptions{DataDir: sourceDataDir, SnapshotPath: download.SnapshotPath, RunID: cfg.runID})
		if err != nil {
			return nil, errors.Wrap(err, "export SEC EDGAR source")
		}
		snapshotPath, recordsDownloaded, recordsProcessed = download.SnapshotPath, download.RecordsSeen, process.RecordsProcessed
		export = secExport(result)
	case unitedstates.SourceIRSEOBMF:
		src := irseobmf.NewSource(irseobmf.ConfigFromEnv())
		download, err := src.Download(ctx, downloadOpts)
		if err != nil {
			return nil, errors.Wrap(err, "download IRS EO BMF snapshot")
		}
		processOpts.SnapshotPath = download.SnapshotPath
		process, err := src.Process(ctx, processOpts)
		if err != nil {
			return nil, errors.Wrap(err, "process IRS EO BMF snapshot")
		}
		result, err := src.Export(ctx, irseobmf.ExportOptions{DataDir: sourceDataDir, SnapshotPath: download.SnapshotPath, RunID: cfg.runID})
		if err != nil {
			return nil, errors.Wrap(err, "export IRS EO BMF source")
		}
		snapshotPath, recordsDownloaded, recordsProcessed = download.SnapshotPath, download.RecordsSeen, process.RecordsProcessed
		export = irsExport(result)
	case unitedstates.SourceColoradoEntities:
		src := coloradoentities.NewSource(coloradoentities.ConfigFromEnv())
		download, err := src.Download(ctx, downloadOpts)
		if err != nil {
			return nil, errors.Wrap(err, "download Colorado snapshot")
		}
		processOpts.SnapshotPath = download.SnapshotPath
		process, err := src.Process(ctx, processOpts)
		if err != nil {
			return nil, errors.Wrap(err, "process Colorado snapshot")
		}
		result, err := src.Export(ctx, coloradoentities.ExportOptions{DataDir: sourceDataDir, SnapshotPath: download.SnapshotPath, RunID: cfg.runID})
		if err != nil {
			return nil, errors.Wrap(err, "export Colorado source")
		}
		snapshotPath, recordsDownloaded, recordsProcessed = download.SnapshotPath, download.RecordsSeen, process.RecordsProcessed
		export = coloradoExport(result)
	default:
		return nil, fmt.Errorf("unsupported source %q", cfg.source)
	}

	return sourceResultMap(cfg.command, export, map[string]any{
		"snapshot_path":      snapshotPath,
		"records_downloaded": recordsDownloaded,
		"records_processed":  recordsProcessed,
	}), nil
}

func runExportSource(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	sourceDataDir := sourceDataDir(cfg)
	var export sourceExport

	switch cfg.source {
	case unitedstates.SourceSECEdgar:
		result, err := secedgar.NewSource(secedgar.ConfigFromEnv()).Export(ctx, secedgar.ExportOptions{
			DataDir: sourceDataDir, SnapshotPath: cfg.snapshotPath, RunID: cfg.runID,
		})
		if err != nil {
			return nil, errors.Wrap(err, "export SEC EDGAR source")
		}
		export = secExport(result)
	case unitedstates.SourceIRSEOBMF:
		result, err := irseobmf.NewSource(irseobmf.ConfigFromEnv()).Export(ctx, irseobmf.ExportOptions{
			DataDir: sourceDataDir, SnapshotPath: cfg.snapshotPath, RunID: cfg.runID,
		})
		if err != nil {
			return nil, errors.Wrap(err, "export IRS EO BMF source")
		}
		export = irsExport(result)
	case unitedstates.SourceColoradoEntities:
		result, err := coloradoentities.NewSource(coloradoentities.ConfigFromEnv()).Export(ctx, coloradoentities.ExportOptions{
			DataDir: sourceDataDir, SnapshotPath: cfg.snapshotPath, RunID: cfg.runID,
		})
		if err != nil {
			return nil, errors.Wrap(err, "export Colorado source")
		}
		export = coloradoExport(result)
	default:
		return nil, fmt.Errorf("unsupported source %q", cfg.source)
	}

	return sourceResultMap(cfg.command, export, map[string]any{
		"snapshot_path": cfg.snapshotPath,
	}), nil
}

func runStatusSource(cfg cliConfig) (map[string]any, error) {
	status, err := unitedstates.SourceStatusFromLatestManifest(cfg.dataDir, cfg.source)
	if err != nil {
		return nil, errors.Wrapf(err, "load %s source status", cfg.source)
	}

	return map[string]any{
		"command":              cfg.command,
		"country_iso2":         unitedstates.CountryISO2,
		"source":               cfg.source,
		"status":               status.Status,
		"source_manifest_path": status.LastExportManifestPath,
		"source_status":        status,
	}, nil
}

func runStatus(cfg cliConfig) (map[string]any, error) {
	sources := map[string]any{}
	for _, slug := range []string{
		unitedstates.SourceSECEdgar,
		unitedstates.SourceIRSEOBMF,
		unitedstates.SourceColoradoEntities,
	} {
		status, err := unitedstates.SourceStatusFromLatestManifest(cfg.dataDir, slug)
		if err != nil {
			return nil, errors.Wrapf(err, "load %s source status", slug)
		}
		sources[slug] = status
	}

	return map[string]any{
		"command":      cfg.command,
		"country_iso2": unitedstates.CountryISO2,
		"status":       "ok",
		"sources":      sources,
	}, nil
}

func runBuildExport(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Wrap(err, "build United States final export")
	}
	return nil, errors.New("United States final export is not implemented yet")
}

func requiresSource(command string) bool {
	switch command {
	case "sync-source", "status-source", "export-source", "sync":
		return true
	default:
		return false
	}
}

func sourceDataDir(cfg cliConfig) string {
	return unitedstates.LayoutForDataDir(cfg.dataDir).SourceDir(cfg.source)
}

func secExport(result secedgar.ExportResult) sourceExport {
	return sourceExport{
		source:          unitedstates.SourceSECEdgar,
		runID:           result.RunID,
		manifestPath:    result.ManifestPath,
		recordsSeen:     result.RecordsSeen,
		recordsExported: result.RecordsExported,
		decodeErrors:    result.DecodeErrors,
	}
}

func irsExport(result irseobmf.ExportResult) sourceExport {
	return sourceExport{
		source:          unitedstates.SourceIRSEOBMF,
		runID:           result.RunID,
		manifestPath:    result.ManifestPath,
		recordsSeen:     result.RecordsSeen,
		recordsExported: result.RecordsExported,
		decodeErrors:    result.DecodeErrors,
	}
}

func coloradoExport(result coloradoentities.ExportResult) sourceExport {
	return sourceExport{
		source:          unitedstates.SourceColoradoEntities,
		runID:           result.RunID,
		manifestPath:    result.ManifestPath,
		recordsSeen:     result.RecordsSeen,
		recordsExported: result.RecordsExported,
		decodeErrors:    result.DecodeErrors,
	}
}

func sourceResultMap(command string, export sourceExport, extra map[string]any) map[string]any {
	response := map[string]any{
		"command":              command,
		"country_iso2":         unitedstates.CountryISO2,
		"source":               export.source,
		"status":               "ok",
		"run_id":               export.runID,
		"manifest_path":        export.manifestPath,
		"source_manifest_path": export.manifestPath,
		"records_seen":         export.recordsSeen,
		"records_exported":     export.recordsExported,
		"decode_errors":        export.decodeErrors,
	}
	for key, value := range extra {
		response[key] = value
	}
	return response
}

func commandAndSourceFromArgs(args []string) (string, string) {
	var command string
	if len(args) > 0 {
		command = args[0]
	}

	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == "--source" && i+1 < len(args) {
			return command, args[i+1]
		}
		if len(arg) > len("--source=") && arg[:len("--source=")] == "--source=" {
			return command, arg[len("--source="):]
		}
	}
	return command, ""
}
