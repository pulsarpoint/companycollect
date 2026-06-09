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
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/finland/prhytj"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/unitedstates/coloradoentities"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/unitedstates/irseobmf"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/unitedstates/secedgar"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(os.Args[1:], os.Stdout); err != nil {
		logger.Error("corpscout source command failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("command is required")
	}
	registry := defaultRegistry()
	switch args[0] {
	case "list-sources":
		for _, key := range registry.Keys() {
			if _, err := fmt.Fprintln(output, key); err != nil {
				return errors.Wrap(err, "write source list")
			}
		}
		return nil
	case "import-run":
		cfg, err := parseImportRun(args[1:])
		if err != nil {
			return err
		}
		result, err := companysources.ImportRun(context.Background(), registry, companysources.ImportRunRequest{
			Country:             cfg.Country,
			Source:              cfg.Source,
			RunDir:              cfg.RunDir,
			ClickHouseNativeURL: cfg.ClickHouseNativeURL,
			BatchSize:           cfg.BatchSize,
			Limit:               cfg.Limit,
		})
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(result)
	case "import-runs":
		cfg, err := parseImportRuns(args[1:])
		if err != nil {
			return err
		}
		result, err := companysources.ImportChangedRuns(context.Background(), registry, companysources.ImportChangedRunsRequest{
			RunsRoot:            cfg.RunsRoot,
			RunIndexPath:        cfg.RunIndexPath,
			ClickHouseNativeURL: cfg.ClickHouseNativeURL,
			BatchSize:           cfg.BatchSize,
			Limit:               cfg.Limit,
			ChangedOnly:         cfg.ChangedOnly,
		})
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(result)
	default:
		return errors.Errorf("unknown command %s", args[0])
	}
}

func defaultRegistry() companysources.Registry {
	return companysources.NewRegistry(
		prhytj.Source{},
		coloradoentities.Source{},
		irseobmf.Source{},
		secedgar.Source{},
	)
}

type importRunConfig struct {
	Country             string
	Source              string
	RunDir              string
	ClickHouseNativeURL string
	BatchSize           int
	Limit               int64
}

type importRunsConfig struct {
	RunsRoot            string
	RunIndexPath        string
	ClickHouseNativeURL string
	BatchSize           int
	ChangedOnly         bool
	Limit               int64
}

func parseImportRun(args []string) (importRunConfig, error) {
	var cfg importRunConfig
	fs := flag.NewFlagSet("import-run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.Country, "country", "", "source country")
	fs.StringVar(&cfg.Source, "source", "", "source key")
	fs.StringVar(&cfg.RunDir, "run-dir", "", "raw source run directory")
	fs.StringVar(&cfg.ClickHouseNativeURL, "clickhouse-native-url", os.Getenv("CLICKHOUSE_NATIVE_URL"), "ClickHouse native URL")
	fs.IntVar(&cfg.BatchSize, "batch-size", 1000, "rows per ClickHouse insert batch")
	fs.Int64Var(&cfg.Limit, "limit", 0, "maximum records to import")
	if err := fs.Parse(args); err != nil {
		return importRunConfig{}, err
	}
	if cfg.Country == "" {
		return importRunConfig{}, errors.New("country is required")
	}
	if cfg.Source == "" {
		return importRunConfig{}, errors.New("source is required")
	}
	if cfg.RunDir == "" {
		return importRunConfig{}, errors.New("run dir is required")
	}
	if cfg.ClickHouseNativeURL == "" {
		return importRunConfig{}, errors.New("clickhouse native url is required")
	}
	return cfg, nil
}

func parseImportRuns(args []string) (importRunsConfig, error) {
	var cfg importRunsConfig
	fs := flag.NewFlagSet("import-runs", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.RunsRoot, "runs-root", "", "root directory containing raw source runs")
	fs.StringVar(&cfg.RunIndexPath, "run-index", "../clickhouse/run-index.lock.yaml", "run import index path")
	fs.StringVar(&cfg.ClickHouseNativeURL, "clickhouse-native-url", os.Getenv("CLICKHOUSE_NATIVE_URL"), "ClickHouse native URL")
	fs.IntVar(&cfg.BatchSize, "batch-size", 1000, "rows per ClickHouse insert batch")
	fs.BoolVar(&cfg.ChangedOnly, "changed-only", false, "skip runs already imported with the same manifest and file hashes")
	fs.Int64Var(&cfg.Limit, "limit", 0, "maximum records to import per source")
	if err := fs.Parse(args); err != nil {
		return importRunsConfig{}, err
	}
	if cfg.RunsRoot == "" {
		return importRunsConfig{}, errors.New("runs root is required")
	}
	if cfg.ClickHouseNativeURL == "" {
		return importRunsConfig{}, errors.New("clickhouse native url is required")
	}
	return cfg, nil
}
