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
	var clickhouseURL string
	var database string
	var sourceExportID string
	var exportDir string
	var configPath string
	flag.StringVar(&clickhouseURL, "clickhouse-url", "", "ClickHouse HTTP URL")
	flag.StringVar(&database, "database", "", "ClickHouse database override")
	flag.StringVar(&sourceExportID, "source-export-id", "", "Postgres registry source export UUID")
	flag.StringVar(&exportDir, "export-dir", "", "source export directory")
	flag.StringVar(&configPath, "config", "", "source config YAML")
	flag.Parse()

	if clickhouseURL == "" {
		return errors.New("clickhouse-url is required")
	}
	if sourceExportID == "" {
		return errors.New("source-export-id is required")
	}
	if exportDir == "" {
		return errors.New("export-dir is required")
	}
	if configPath == "" {
		return errors.New("config is required")
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
		sql, err := buildInsertSQL(cfg.Database, table, sourceExportID)
		if err != nil {
			return err
		}
		structure, err := fetchExternalTableStructure(clickhouseURL, cfg.Database, table.Table, table.InjectColumns)
		if err != nil {
			return errors.Wrapf(err, "prepare import table %s", table.Table)
		}
		importURL, err := withExternalTableStructure(clickhouseURL, structure)
		if err != nil {
			return err
		}
		if err := executeParquetImport(importURL, sql, filepath.Join(exportDir, table.Parquet)); err != nil {
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
