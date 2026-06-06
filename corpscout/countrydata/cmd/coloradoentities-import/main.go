package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/coloradoentities"
)

type cliConfig struct {
	command   string
	envPath   string
	dataDir   string
	maxPages  int
	chunkSize int
	limit     int64
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		slog.Error("parse Colorado entities import command", "error", err)
		os.Exit(2)
	}

	if err := run(context.Background(), cfg); err != nil {
		slog.Error("run Colorado entities import command",
			"command", cfg.command,
			"error_kind", countryimport.Classify(err),
			"error", err,
		)
		os.Exit(1)
	}
}

func parseArgs(args []string) (cliConfig, error) {
	if len(args) == 0 {
		return cliConfig{}, fmt.Errorf("missing command")
	}

	command := args[0]
	switch command {
	case "download", "process", "run":
	default:
		return cliConfig{}, fmt.Errorf("unknown command %q", command)
	}

	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	cfg := cliConfig{command: command}
	flags.StringVar(&cfg.envPath, "env", "", "path to env file")
	flags.StringVar(&cfg.dataDir, "data-dir", "", "Colorado entities data directory")
	flags.IntVar(&cfg.maxPages, "max-pages", 0, "maximum pages to download (0 = all)")
	flags.IntVar(&cfg.chunkSize, "chunk-size", 0, "records per processing chunk")
	flags.Int64Var(&cfg.limit, "limit", 0, "maximum records to process (0 = all)")

	if err := flags.Parse(args[1:]); err != nil {
		return cliConfig{}, err
	}

	return cfg, nil
}

func run(ctx context.Context, cli cliConfig) error {
	if cli.envPath != "" {
		if err := countryimport.LoadEnvFile(cli.envPath); err != nil {
			return errors.Wrapf(err, "load env file %s", cli.envPath)
		}
	}

	cfg := coloradoentities.ConfigFromEnv()
	if cli.dataDir != "" {
		cfg.DataDir = cli.dataDir
	}
	source := coloradoentities.NewSource(cfg)

	switch cli.command {
	case "download":
		result, err := source.Download(ctx, countryimport.DownloadOptions{
			DataDir:  cli.dataDir,
			MaxPages: cli.maxPages,
		})
		if err != nil {
			return errors.Wrap(err, "download Colorado entities snapshot")
		}
		slog.Info("downloaded Colorado entities snapshot",
			"snapshot_path", result.SnapshotPath,
			"pages", result.PagesDownloaded,
			"records", result.RecordsSeen,
		)
	case "process":
		result, err := source.Process(ctx, countryimport.ProcessOptions{
			DataDir:   cli.dataDir,
			ChunkSize: cli.chunkSize,
			Limit:     cli.limit,
		})
		if err != nil {
			return errors.Wrap(err, "process Colorado entities snapshot")
		}
		slog.Info("processed Colorado entities snapshot",
			"snapshot_path", result.SnapshotPath,
			"records", result.RecordsProcessed,
			"chunks", result.ChunksProcessed,
		)
	case "run":
		downloadResult, err := source.Download(ctx, countryimport.DownloadOptions{
			DataDir:  cli.dataDir,
			MaxPages: cli.maxPages,
		})
		if err != nil {
			return errors.Wrap(err, "download Colorado entities snapshot")
		}

		processResult, err := source.Process(ctx, countryimport.ProcessOptions{
			DataDir:      cli.dataDir,
			SnapshotPath: downloadResult.SnapshotPath,
			ChunkSize:    cli.chunkSize,
			Limit:        cli.limit,
		})
		if err != nil {
			return errors.Wrap(err, "process Colorado entities snapshot")
		}
		slog.Info("ran Colorado entities import",
			"snapshot_path", downloadResult.SnapshotPath,
			"pages", downloadResult.PagesDownloaded,
			"records_downloaded", downloadResult.RecordsSeen,
			"records_processed", processResult.RecordsProcessed,
			"chunks", processResult.ChunksProcessed,
		)
	default:
		return fmt.Errorf("unknown command %q", cli.command)
	}

	return nil
}
