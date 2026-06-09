package irseobmf

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

// Source collects IRS EO BMF data into source snapshots and parquet exports.
type Source struct {
	cfg        Config
	httpClient *http.Client
}

// NewSource constructs a Source, applying public defaults for any unset field.
func NewSource(cfg Config) *Source {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if len(cfg.Files) == 0 {
		cfg.Files = DefaultFiles
	}
	if strings.TrimSpace(cfg.UserAgent) == "" {
		cfg.UserAgent = countryimport.DefaultUserAgent
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = countryimport.DefaultRequestTimeout
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
	return sourcespec.ClickHouseMigrationResult{}, errors.New("ClickHouse migration generation is not wired for united_states/irseobmf")
}

func (s *Source) ImportClickHouse(ctx context.Context, opts sourcespec.ClickHouseImportOptions) (sourcespec.ClickHouseImportResult, error) {
	return sourcespec.ClickHouseImportResult{}, errors.New("ClickHouse import is not wired for united_states/irseobmf")
}

func (s *Source) Status(ctx context.Context, runDir string) (sourcespec.StatusResult, error) {
	manifestPath := filepath.Join(strings.TrimSpace(runDir), "manifest.json")
	if _, err := os.Stat(manifestPath); err != nil {
		if os.IsNotExist(err) {
			return sourcespec.StatusResult{Status: "missing", RunDir: runDir}, nil
		}
		return sourcespec.StatusResult{}, errors.Wrap(err, "stat IRS EO BMF manifest")
	}
	return sourcespec.StatusResult{Status: "ok", RunDir: runDir, ManifestPath: manifestPath}, nil
}
