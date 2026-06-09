package prhytj

import (
	"context"
	_ "embed"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
	chimport "github.com/pulsarpoint/companycollect/companies/companysource/internal/clickhouse"
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

//go:embed clickhouse.yaml
var clickHouseConfigYAML []byte

type Source struct {
	cfg        Config
	httpClient *http.Client
}

func NewSource(cfg Config) *Source {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.PageDelay == 0 {
		cfg.PageDelay = countryimport.DefaultPageDelay
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = countryimport.DefaultRequestTimeout
	}
	if strings.TrimSpace(cfg.UserAgent) == "" {
		cfg.UserAgent = countryimport.DefaultUserAgent
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	cfg.HTTPClient = client

	return &Source{
		cfg:        cfg,
		httpClient: client,
	}
}

func (s *Source) Key() sourcespec.Key {
	return sourcespec.Key{Country: "finland", Source: SourceKey}
}

func (s *Source) DisplayName() string {
	return SourceName
}

func (s *Source) GenerateClickHouseMigration(ctx context.Context, opts sourcespec.ClickHouseMigrationOptions) (sourcespec.ClickHouseMigrationResult, error) {
	result, err := chimport.GenerateMigrationFiles(chimport.MigrationOptions{
		RunDir:     opts.RunDir,
		Database:   opts.Database,
		Out:        opts.Out,
		DownOut:    opts.DownOut,
		ConfigYAML: clickHouseConfigYAML,
	})
	if err != nil {
		return sourcespec.ClickHouseMigrationResult{}, errors.Wrap(err, "generate finland/prhytj ClickHouse migration")
	}
	return sourcespec.ClickHouseMigrationResult{UpPath: result.UpPath, DownPath: result.DownPath}, nil
}

func (s *Source) ImportClickHouse(ctx context.Context, opts sourcespec.ClickHouseImportOptions) (sourcespec.ClickHouseImportResult, error) {
	result, err := chimport.ImportRun(chimport.ImportOptions{
		RunDir:              opts.RunDir,
		Database:            opts.Database,
		ClickHouseNativeURL: opts.ClickHouseNativeURL,
		SourceExportID:      opts.SourceExportID,
		ClickHouseImage:     opts.ClickHouseImage,
		DockerMount:         opts.DockerMount,
		ConfigYAML:          clickHouseConfigYAML,
	})
	if err != nil {
		return sourcespec.ClickHouseImportResult{}, errors.Wrap(err, "import finland/prhytj ClickHouse tables")
	}
	return sourcespec.ClickHouseImportResult{ImportedTables: result.ImportedTables, Tables: result.Tables}, nil
}

func (s *Source) Status(ctx context.Context, runDir string) (sourcespec.StatusResult, error) {
	manifestPath := filepath.Join(strings.TrimSpace(runDir), "manifest.json")
	if _, err := os.Stat(manifestPath); err != nil {
		if os.IsNotExist(err) {
			return sourcespec.StatusResult{Status: "missing", RunDir: runDir}, nil
		}
		return sourcespec.StatusResult{}, errors.Wrap(err, "stat PRH YTJ manifest")
	}
	return sourcespec.StatusResult{Status: "ok", RunDir: runDir, ManifestPath: manifestPath}, nil
}
