package irseobmf

import (
	"context"
	"net/http"
	"strings"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

// Source collects IRS EO BMF data into source snapshots and parquet exports.
type Source struct {
	cfg            Config
	httpClient     *http.Client
	metadataStore  countryimport.MetadataStore
	latestDownload *countryimport.DownloadMetadata
	latestProcess  *countryimport.ProcessMetadata
	storeFunc      func(context.Context, []IrsEoBmfRecord) (countryimport.StoreResult, error)
}

// NewSource constructs a Source, applying public defaults for any unset field.
func NewSource(cfg Config) *Source {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if len(cfg.Files) == 0 {
		cfg.Files = DefaultFiles
	}
	if strings.TrimSpace(cfg.DataDir) == "" {
		cfg.DataDir = defaultDataDir
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

	metadataStore := cfg.MetadataStore
	if metadataStore == nil {
		metadataStore = countryimport.NoopMetadataStore{}
	}
	cfg.MetadataStore = metadataStore

	return &Source{
		cfg:           cfg,
		httpClient:    client,
		metadataStore: metadataStore,
	}
}
