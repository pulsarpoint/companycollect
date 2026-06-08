package main

import (
	"flag"
	"os"

	"github.com/cockroachdb/errors"
)

func main() {
	if err := run(); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	var source string
	var database string
	var exportDir string
	var configPath string
	var outPath string
	var downOutPath string
	var clickhouseLocal string
	flag.StringVar(&source, "source", "", "source name for logs")
	flag.StringVar(&database, "database", "", "ClickHouse database override")
	flag.StringVar(&exportDir, "export-dir", "", "source export directory")
	flag.StringVar(&configPath, "config", "", "source config YAML")
	flag.StringVar(&outPath, "out", "", "up migration output path")
	flag.StringVar(&downOutPath, "down-out", "", "down migration output path")
	flag.StringVar(&clickhouseLocal, "clickhouse-local", "clickhouse-local", "clickhouse-local binary")
	flag.Parse()

	if source == "" {
		return errors.New("source is required")
	}
	if exportDir == "" {
		return errors.New("export-dir is required")
	}
	if configPath == "" {
		return errors.New("config is required")
	}
	if outPath == "" {
		return errors.New("out is required")
	}
	if downOutPath == "" {
		return errors.New("down-out is required")
	}

	body, err := os.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, "read config")
	}
	cfg, err := parseConfig(body)
	if err != nil {
		return err
	}
	if database != "" {
		cfg.Database = database
	}

	up, down, err := generateMigrations(cfg, exportDir, ClickHouseLocalDescriber{Binary: clickhouseLocal})
	if err != nil {
		return err
	}
	if err := os.WriteFile(outPath, []byte(up), 0o644); err != nil {
		return errors.Wrap(err, "write up migration")
	}
	if err := os.WriteFile(downOutPath, []byte(down), 0o644); err != nil {
		return errors.Wrap(err, "write down migration")
	}
	return nil
}
