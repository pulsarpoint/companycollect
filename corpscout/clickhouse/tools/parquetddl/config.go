package main

import (
	"sort"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Database     string                 `yaml:"database"`
	SourcePrefix string                 `yaml:"source_prefix"`
	Tables       map[string]TableConfig `yaml:"tables"`
}

type TableConfig struct {
	Parquet       string            `yaml:"parquet"`
	Table         string            `yaml:"table"`
	Engine        string            `yaml:"engine"`
	OrderBy       []string          `yaml:"order_by"`
	PartitionBy   string            `yaml:"partition_by"`
	InjectColumns map[string]string `yaml:"inject_columns"`
}

func parseConfig(body []byte) (Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(body, &cfg); err != nil {
		return Config{}, errors.Wrap(err, "parse parquet ddl config")
	}
	if cfg.Database == "" {
		return Config{}, errors.New("database is required")
	}
	if len(cfg.Tables) == 0 {
		return Config{}, errors.New("at least one table is required")
	}
	for name, table := range cfg.Tables {
		if table.Parquet == "" {
			return Config{}, errors.Errorf("table %s parquet is required", name)
		}
		if table.Table == "" {
			return Config{}, errors.Errorf("table %s table name is required", name)
		}
		if table.Engine == "" {
			return Config{}, errors.Errorf("table %s engine is required", name)
		}
		if len(table.OrderBy) == 0 {
			return Config{}, errors.Errorf("table %s order_by is required", name)
		}
	}
	return cfg, nil
}

func sortedTableNames(tables map[string]TableConfig) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
