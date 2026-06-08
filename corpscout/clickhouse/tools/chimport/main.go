package main

import (
	"flag"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Database string                 `yaml:"database"`
	Tables   map[string]TableConfig `yaml:"tables"`
}

func main() {
	if err := run(); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	var database string
	var sourceExportID string
	var exportDir string
	var configPath string
	var nativeURL string
	var clickHouseImage string
	var dockerMount string
	flag.StringVar(&database, "database", "", "ClickHouse database override")
	flag.StringVar(&sourceExportID, "source-export-id", "", "Postgres registry source export UUID")
	flag.StringVar(&exportDir, "export-dir", "", "source export directory")
	flag.StringVar(&configPath, "config", "", "source config YAML")
	flag.StringVar(&nativeURL, "clickhouse-native-url", "", "ClickHouse native URL")
	flag.StringVar(&clickHouseImage, "clickhouse-image", defaultClickHouseImage, "ClickHouse image used for clickhouse-local")
	flag.StringVar(&dockerMount, "docker-mount", "", "host path mounted into the clickhouse-local container")
	flag.Parse()

	if sourceExportID == "" {
		return errors.New("source-export-id is required")
	}
	if exportDir == "" {
		return errors.New("export-dir is required")
	}
	if configPath == "" {
		return errors.New("config is required")
	}
	if nativeURL == "" {
		return errors.New("clickhouse-native-url is required")
	}

	body, err := os.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, "read config")
	}
	var cfg Config
	if err := yaml.Unmarshal(body, &cfg); err != nil {
		return errors.Wrap(err, "parse config")
	}
	if database != "" {
		cfg.Database = database
	}
	if cfg.Database == "" {
		return errors.New("database is required")
	}
	if len(cfg.Tables) == 0 {
		return errors.New("at least one table is required")
	}

	for _, name := range sortedTableNames(cfg.Tables) {
		table := cfg.Tables[name]
		if table.Parquet == "" {
			return errors.Errorf("table %s parquet is required", name)
		}
		if table.Table == "" {
			return errors.Errorf("table %s table name is required", name)
		}
		if err := truncateTargetTable(nativeURL, clickHouseImage, cfg.Database, table.Table); err != nil {
			return errors.Wrapf(err, "truncate table %s", table.Table)
		}
		if err := executeNativeImport(NativeImportOptions{
			Database:        cfg.Database,
			Table:           table,
			ParquetPath:     filepath.Join(exportDir, table.Parquet),
			SourceExportID:  sourceExportID,
			NativeURL:       nativeURL,
			DockerMount:     dockerMount,
			ClickHouseImage: clickHouseImage,
		}); err != nil {
			return errors.Wrapf(err, "import table %s", table.Table)
		}
	}
	return nil
}

func sortedTableNames(tables map[string]TableConfig) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
