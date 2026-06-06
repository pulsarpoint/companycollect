package irseobmf

import (
	"context"
	"net/http"
	"strings"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

var _ countryimport.BulkSource[IrsEoBmfRecord] = (*Source)(nil)

type Source struct {
	cfg            Config
	httpClient     *http.Client
	metadataStore  countryimport.MetadataStore
	latestDownload *countryimport.DownloadMetadata
	latestProcess  *countryimport.ProcessMetadata
	StoreFunc      func(context.Context, []IrsEoBmfRecord) (countryimport.StoreResult, error)
}

func NewSource(cfg Config) *Source {
	if len(cfg.DownloadURLs) == 0 {
		cfg.DownloadURLs = append([]string(nil), DefaultDownloadURLs...)
	}
	if strings.TrimSpace(cfg.DataDir) == "" {
		cfg.DataDir = defaultDataDir
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
