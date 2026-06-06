package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type cliConfig struct {
	command   string
	envPath   string
	dataDir   string
	maxPages  int
	chunkSize int
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		slog.Error("parse PRH YTJ import command", "error", err)
		os.Exit(2)
	}

	if err := run(context.Background(), cfg); err != nil {
		slog.Error("run PRH YTJ import command",
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
	flags.StringVar(&cfg.dataDir, "data-dir", "", "PRH YTJ data directory")
	flags.IntVar(&cfg.maxPages, "max-pages", 0, "maximum pages to download")
	flags.IntVar(&cfg.chunkSize, "chunk-size", 0, "records per processing chunk")

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

	cfg := prhytj.ConfigFromEnv()
	if cli.dataDir != "" {
		cfg.DataDir = cli.dataDir
	}
	source := prhytj.NewSource(cfg)

	switch cli.command {
	case "download":
		result, err := source.Download(ctx, countryimport.DownloadOptions{
			DataDir:  cli.dataDir,
			MaxPages: cli.maxPages,
		})
		if err != nil {
			return errors.Wrap(err, "download PRH YTJ snapshot")
		}
		slog.Info("downloaded PRH YTJ snapshot",
			"snapshot_path", result.SnapshotPath,
			"pages", result.PagesDownloaded,
			"records", result.RecordsSeen,
		)
	case "process":
		result, err := source.Process(ctx, countryimport.ProcessOptions{
			DataDir:   cli.dataDir,
			ChunkSize: cli.chunkSize,
		})
		if err != nil {
			return errors.Wrap(err, "process PRH YTJ snapshot")
		}
		slog.Info("processed PRH YTJ snapshot",
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
			return errors.Wrap(err, "download PRH YTJ snapshot")
		}

		processResult, err := source.Process(ctx, countryimport.ProcessOptions{
			DataDir:      cli.dataDir,
			SnapshotPath: downloadResult.SnapshotPath,
			ChunkSize:    cli.chunkSize,
		})
		if err != nil {
			return errors.Wrap(err, "process PRH YTJ snapshot")
		}
		slog.Info("ran PRH YTJ import",
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
