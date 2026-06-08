package coloradoentities

import (
	"context"
	"net/http"
	"strings"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

// Source collects Colorado Business Entities data into source snapshots and
// parquet exports.
type Source struct {
	cfg            Config
	httpClient     *http.Client
	metadataStore  countryimport.MetadataStore
	latestDownload *countryimport.DownloadMetadata
	latestProcess  *countryimport.ProcessMetadata
	storeFunc      func(context.Context, []ColoradoEntityRecord) (countryimport.StoreResult, error)
}

// NewSource constructs a Source, applying public defaults for any unset field.
func NewSource(cfg Config) *Source {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if strings.TrimSpace(cfg.DataDir) == "" {
		cfg.DataDir = defaultDataDir
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
