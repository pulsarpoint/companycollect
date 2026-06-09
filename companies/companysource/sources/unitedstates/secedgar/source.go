package secedgar

import (
	"context"
	_ "embed"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
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
	if strings.TrimSpace(cfg.DownloadURL) == "" {
		cfg.DownloadURL = DefaultDownloadURL
	}
	if strings.TrimSpace(cfg.UserAgent) == "" {
		cfg.UserAgent = DefaultUserAgent
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = countryimport.DefaultRequestTimeout
	}
	if strings.TrimSpace(cfg.SourceSlug) == "" {
		cfg.SourceSlug = SourceSlug
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
	return sourcespec.Key{Country: "united_states", Source: SourceKey}
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
		return sourcespec.ClickHouseMigrationResult{}, errors.Wrap(err, "generate united_states/secedgar ClickHouse migration")
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
		return sourcespec.ClickHouseImportResult{}, errors.Wrap(err, "import united_states/secedgar ClickHouse tables")
	}
	return sourcespec.ClickHouseImportResult{ImportedTables: result.ImportedTables, Tables: result.Tables}, nil
}

func (s *Source) Status(ctx context.Context, runDir string) (sourcespec.StatusResult, error) {
	manifestPath := filepath.Join(strings.TrimSpace(runDir), "manifest.json")
	if _, err := os.Stat(manifestPath); err != nil {
		if os.IsNotExist(err) {
			return sourcespec.StatusResult{Status: "missing", RunDir: runDir}, nil
		}
		return sourcespec.StatusResult{}, errors.Wrap(err, "stat SEC EDGAR manifest")
	}
	return sourcespec.StatusResult{Status: "ok", RunDir: runDir, ManifestPath: manifestPath}, nil
}
