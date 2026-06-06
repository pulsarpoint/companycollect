package countrydata

import (
	"context"
	"net/http"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/secedgar"
)

type UnitedStatesSECEDGARImporter struct {
	HTTPClient *http.Client
}

type UnitedStatesSECEDGARImportInput struct {
	DownloadURL    string
	DataDir        string
	ChunkSize      int
	Limit          int64
	RequestTimeout time.Duration
	UserAgent      string
	MetadataStore  countryimport.MetadataStore
	StoreFunc      func(context.Context, []secedgar.SecTickerRecord) (countryimport.StoreResult, error)
}

type UnitedStatesSECEDGARImportResult struct {
	Download countryimport.DownloadResult
	Process  countryimport.ProcessResult
}

func (i UnitedStatesSECEDGARImporter) Run(ctx context.Context, input UnitedStatesSECEDGARImportInput) (UnitedStatesSECEDGARImportResult, error) {
	source := secedgar.NewSource(secedgar.Config{
		DownloadURL:    input.DownloadURL,
		DataDir:        input.DataDir,
		RequestTimeout: input.RequestTimeout,
		UserAgent:      input.UserAgent,
		HTTPClient:     i.HTTPClient,
		MetadataStore:  input.MetadataStore,
	})
	source.StoreFunc = input.StoreFunc

	download, err := source.Download(ctx, countryimport.DownloadOptions{
		RequestTimeout: input.RequestTimeout,
		UserAgent:      input.UserAgent,
	})
	result := UnitedStatesSECEDGARImportResult{Download: download}
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
