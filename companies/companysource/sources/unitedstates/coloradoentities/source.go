package coloradoentities

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

// Source collects Colorado Business Entities data into source snapshots and
// parquet exports.
type Source struct {
	cfg        Config
	httpClient *http.Client
}

// NewSource constructs a Source, applying public defaults for any unset field.
func NewSource(cfg Config) *Source {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = DefaultPageSize
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
	return sourcespec.Key{Country: "united_states", Source: SourceKey}
}

func (s *Source) DisplayName() string {
	return SourceName
}

func (s *Source) Status(ctx context.Context, runDir string) (sourcespec.StatusResult, error) {
	manifestPath := filepath.Join(strings.TrimSpace(runDir), "manifest.json")
	if _, err := os.Stat(manifestPath); err != nil {
		if os.IsNotExist(err) {
			return sourcespec.StatusResult{Status: "missing", RunDir: runDir}, nil
		}
		return sourcespec.StatusResult{}, errors.Wrap(err, "stat Colorado entities manifest")
	}
	return sourcespec.StatusResult{Status: "ok", RunDir: runDir, ManifestPath: manifestPath}, nil
}
