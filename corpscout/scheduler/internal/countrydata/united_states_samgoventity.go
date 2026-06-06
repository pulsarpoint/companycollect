package countrydata

import (
	"context"
	"net/http"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/samgoventity"
)

type UnitedStatesSamGovEntityImporter struct {
	HTTPClient *http.Client
}

type UnitedStatesSamGovEntityImportInput struct {
	BaseURL        string
	APIKey         string
	SamRegistered  string
	DataDir        string
	PageSize       int
	MaxPages       int
	ChunkSize      int
	Limit          int64
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	MetadataStore  countryimport.MetadataStore
	StoreFunc      func(context.Context, []samgoventity.SamEntityRecord) (countryimport.StoreResult, error)
}

type UnitedStatesSamGovEntityImportResult struct {
	Download countryimport.DownloadResult
	Process  countryimport.ProcessResult
}

func (i UnitedStatesSamGovEntityImporter) Run(ctx context.Context, input UnitedStatesSamGovEntityImportInput) (UnitedStatesSamGovEntityImportResult, error) {
	source := samgoventity.NewSource(samgoventity.Config{
		BaseURL:        input.BaseURL,
		APIKey:         input.APIKey,
		SamRegistered:  input.SamRegistered,
		DataDir:        input.DataDir,
		PageSize:       input.PageSize,
		PageDelay:      input.PageDelay,
		RequestTimeout: input.RequestTimeout,
		UserAgent:      input.UserAgent,
		HTTPClient:     i.HTTPClient,
		MetadataStore:  input.MetadataStore,
	})
	source.StoreFunc = input.StoreFunc

	download, err := source.Download(ctx, countryimport.DownloadOptions{
		MaxPages:       input.MaxPages,
		PageDelay:      input.PageDelay,
		RequestTimeout: input.RequestTimeout,
	})
	result := UnitedStatesSamGovEntityImportResult{Download: download}
	if err != nil {
		return result, err
	}

	process, err := source.Process(ctx, countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize:    input.ChunkSize,
		Limit:        input.Limit,
	})
	result.Process = process
	if err != nil {
		return result, err
	}

	return result, nil
}
