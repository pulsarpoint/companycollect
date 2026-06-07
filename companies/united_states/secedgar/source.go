package secedgar

import (
	"context"
	"net/http"
	"strings"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

type Source struct {
	cfg            Config
	httpClient     *http.Client
	metadataStore  countryimport.MetadataStore
	latestDownload *countryimport.DownloadMetadata
	latestProcess  *countryimport.ProcessMetadata
	storeFunc      func(context.Context, []CompanyTickerRecord) (countryimport.StoreResult, error)
}

func NewSource(cfg Config) *Source {
	if strings.TrimSpace(cfg.DataDir) == "" {
		cfg.DataDir = defaultDataDir
	}
	if strings.TrimSpace(cfg.DownloadURL) == "" {
		cfg.DownloadURL = DefaultDownloadURL
	}
	if strings.TrimSpace(cfg.UserAgent) == "" {
		cfg.UserAgent = countryimport.DefaultUserAgent
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
