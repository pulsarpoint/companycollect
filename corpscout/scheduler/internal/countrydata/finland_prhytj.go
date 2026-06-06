package countrydata

import (
	"context"
	"net/http"
	"time"

	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type FinlandPRHYTJImporter struct {
	HTTPClient *http.Client
}

type FinlandPRHYTJImportInput struct {
	BaseURL       string
	DataDir       string
	MaxPages      int
	ChunkSize     int
	PageDelay     time.Duration
	MetadataStore countryimport.MetadataStore
}

type FinlandPRHYTJImportResult struct {
	Download countryimport.DownloadResult
	Process  countryimport.ProcessResult
}

func (i FinlandPRHYTJImporter) Run(ctx context.Context, input FinlandPRHYTJImportInput) (FinlandPRHYTJImportResult, error) {
	source := prhytj.NewSource(prhytj.Config{
		BaseURL:       input.BaseURL,
		DataDir:       input.DataDir,
		PageDelay:     input.PageDelay,
		HTTPClient:    i.HTTPClient,
		MetadataStore: input.MetadataStore,
	})

	download, err := source.Download(ctx, countryimport.DownloadOptions{
		MaxPages:  input.MaxPages,
		PageDelay: input.PageDelay,
	})
	result := FinlandPRHYTJImportResult{Download: download}
	if err != nil {
		return result, err
	}

	process, err := source.Process(ctx, countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize:    input.ChunkSize,
	})
	result.Process = process
	if err != nil {
		return result, err
	}

	return result, nil
}
