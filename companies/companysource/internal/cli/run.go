package cli

import (
	"context"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
	"github.com/pulsarpoint/companycollect/companies/companysource/internal/registry"
	"github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

func Run(ctx context.Context, args []string) (any, error) {
	cfg, err := parseArgs(args)
	if err != nil {
		return nil, err
	}
	if cfg.EnvPath != "" {
		if err := countryimport.LoadEnvFile(cfg.EnvPath); err != nil {
			return nil, errors.Wrapf(err, "load env file %s", cfg.EnvPath)
		}
	}

	reg := registry.Default()
	if cfg.Command == "list-sources" {
		return map[string]any{"sources": reg.Keys()}, nil
	}
	selectedSource, err := reg.Get(cfg.Country, cfg.Source)
	if err != nil {
		return nil, err
	}
	switch cfg.Command {
	case "download":
		return selectedSource.Download(ctx, source.DownloadOptions{
			RunDir: cfg.RunDir, RunID: cfg.RunID, MaxPages: cfg.MaxPages,
		})
	case "export-parquet":
		return selectedSource.ExportParquet(ctx, source.ExportParquetOptions{
			RunDir: cfg.RunDir, Limit: cfg.Limit,
		})
	case "status":
		return selectedSource.Status(ctx, cfg.RunDir)
	default:
		return nil, errors.Errorf("unknown command %q", cfg.Command)
	}
}
