package cli

import (
	"flag"
	"fmt"
	"io"
)

type Config struct {
	Command             string
	Country             string
	Source              string
	EnvPath             string
	RunDir              string
	Database            string
	Out                 string
	DownOut             string
	ClickHouseNativeURL string
	SourceExportID      string
	ClickHouseImage     string
	DockerMount         string
	RunID               string
	MaxPages            int
	Limit               int64
}

func parseArgs(args []string) (Config, error) {
	if len(args) == 0 {
		return Config{}, fmt.Errorf("missing command")
	}
	command := args[0]
	switch command {
	case "download", "export-parquet", "generate-clickhouse-migration", "import-clickhouse", "list-sources", "status":
	default:
		return Config{}, fmt.Errorf("unknown command %q", command)
	}

	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	cfg := Config{Command: command}
	flags.StringVar(&cfg.Country, "country", "", "country slug")
	flags.StringVar(&cfg.Source, "source", "", "source slug")
	flags.StringVar(&cfg.EnvPath, "env", "", "path to env file")
	flags.StringVar(&cfg.RunDir, "run-dir", "", "flat source run directory")
	flags.StringVar(&cfg.Database, "database", "", "ClickHouse database")
	flags.StringVar(&cfg.Out, "out", "", "ClickHouse migration up path")
	flags.StringVar(&cfg.DownOut, "down-out", "", "ClickHouse migration down path")
	flags.StringVar(&cfg.ClickHouseNativeURL, "clickhouse-native-url", "", "ClickHouse native URL")
	flags.StringVar(&cfg.SourceExportID, "source-export-id", "", "source export UUID")
	flags.StringVar(&cfg.ClickHouseImage, "clickhouse-image", "", "ClickHouse Docker image")
	flags.StringVar(&cfg.DockerMount, "docker-mount", "", "host path mounted into Docker")
	flags.StringVar(&cfg.RunID, "run-id", "", "run ID")
	flags.IntVar(&cfg.MaxPages, "max-pages", 0, "maximum pages/files to download")
	flags.Int64Var(&cfg.Limit, "limit", 0, "maximum records to export")
	if err := flags.Parse(args[1:]); err != nil {
		return Config{}, err
	}
	return validateConfig(cfg)
}

func validateConfig(cfg Config) (Config, error) {
	if cfg.Command == "list-sources" {
		return cfg, nil
	}
	if cfg.Country == "" {
		return Config{}, fmt.Errorf("missing --country")
	}
	if cfg.Source == "" {
		return Config{}, fmt.Errorf("missing --source")
	}
	if cfg.RunDir == "" {
		return Config{}, fmt.Errorf("missing --run-dir")
	}
	switch cfg.Command {
	case "generate-clickhouse-migration":
		if cfg.Database == "" {
			return Config{}, fmt.Errorf("missing --database")
		}
		if cfg.Out == "" {
			return Config{}, fmt.Errorf("missing --out")
		}
		if cfg.DownOut == "" {
			return Config{}, fmt.Errorf("missing --down-out")
		}
	case "import-clickhouse":
		if cfg.Database == "" {
			return Config{}, fmt.Errorf("missing --database")
		}
		if cfg.ClickHouseNativeURL == "" {
			return Config{}, fmt.Errorf("missing --clickhouse-native-url")
		}
		if cfg.SourceExportID == "" {
			return Config{}, fmt.Errorf("missing --source-export-id")
		}
	}
	return cfg, nil
}
