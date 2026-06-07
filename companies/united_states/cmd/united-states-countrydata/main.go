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

	if err := flags.Parse(args[1:]); err != nil {
		return cliConfig{}, err
	}

	if requiresSource(command) {
		if cfg.source == "" {
			return cliConfig{}, fmt.Errorf("missing --source")
		}
		if cfg.source != unitedstates.SourceSECEdgar {
			return cliConfig{}, fmt.Errorf("source %q is not implemented for United States countrydata CLI source commands; supported source: %q", cfg.source, unitedstates.SourceSECEdgar)
		}
	} else if cfg.source != "" && cfg.source != unitedstates.SourceSECEdgar {
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
		return runSyncSource(ctx, cfg)
	default:
		return nil, fmt.Errorf("unknown command %q", cfg.command)
	}
}

func runSyncSource(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	source := newSECEDGARSource()
	sourceDataDir := sourceDataDir(cfg)
	downloadResult, err := source.Download(ctx, countryimport.DownloadOptions{
		DataDir: sourceDataDir,
	})
	if err != nil {
		return nil, errors.Wrap(err, "download SEC EDGAR snapshot")
	}

	processResult, err := source.Process(ctx, countryimport.ProcessOptions{
		DataDir:      sourceDataDir,
		SnapshotPath: downloadResult.SnapshotPath,
		ChunkSize:    cfg.chunkSize,
	})
	if err != nil {
		return nil, errors.Wrap(err, "process SEC EDGAR snapshot")
	}

	exportResult, err := source.Export(ctx, secedgar.ExportOptions{
		DataDir:      sourceDataDir,
		SnapshotPath: downloadResult.SnapshotPath,
		RunID:        cfg.runID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "export SEC EDGAR source")
	}

	return sourceResultMap(cfg.command, exportResult, map[string]any{
		"snapshot_path":      downloadResult.SnapshotPath,
		"records_downloaded": downloadResult.RecordsSeen,
		"records_processed":  processResult.RecordsProcessed,
	}), nil
}

func runExportSource(ctx context.Context, cfg cliConfig) (map[string]any, error) {
	result, err := newSECEDGARSource().Export(ctx, secedgar.ExportOptions{
		DataDir:      sourceDataDir(cfg),
		SnapshotPath: cfg.snapshotPath,
		RunID:        cfg.runID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "export SEC EDGAR source")
	}

	return sourceResultMap(cfg.command, result, map[string]any{
		"snapshot_path": cfg.snapshotPath,
	}), nil
}

func runStatusSource(cfg cliConfig) (map[string]any, error) {
	status, err := unitedstates.SourceStatusFromLatestManifest(cfg.dataDir, cfg.source)
	if err != nil {
		return nil, errors.Wrap(err, "load SEC EDGAR source status")
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
	secStatus, err := unitedstates.SourceStatusFromLatestManifest(cfg.dataDir, unitedstates.SourceSECEdgar)
	if err != nil {
		return nil, errors.Wrap(err, "load SEC EDGAR source status")
	}

	return map[string]any{
		"command":                       cfg.command,
		"country_iso2":                  unitedstates.CountryISO2,
		"status":                        "ok",
		"secedgar_source_manifest_path": secStatus.LastExportManifestPath,
		"sources": map[string]any{
			unitedstates.SourceSECEdgar: secStatus,
			unitedstates.SourceIRSEOBMF: unitedstates.SourceStatus{
				SourceSlug: unitedstates.SourceIRSEOBMF,
				Status:     "not_implemented",
			},
			unitedstates.SourceColoradoEntities: unitedstates.SourceStatus{
				SourceSlug: unitedstates.SourceColoradoEntities,
				Status:     "not_implemented",
			},
		},
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

func newSECEDGARSource() *secedgar.Source {
	return secedgar.NewSource(secedgar.ConfigFromEnv())
}

func sourceDataDir(cfg cliConfig) string {
	return unitedstates.LayoutForDataDir(cfg.dataDir).SourceDir(unitedstates.SourceSECEdgar)
}

func sourceResultMap(command string, result secedgar.ExportResult, extra map[string]any) map[string]any {
	response := map[string]any{
		"command":              command,
		"country_iso2":         unitedstates.CountryISO2,
		"source":               unitedstates.SourceSECEdgar,
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
