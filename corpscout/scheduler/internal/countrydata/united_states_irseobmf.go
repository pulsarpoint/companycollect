package countrydata

import (
	"context"
	"net/http"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/irseobmf"
)

type UnitedStatesIRSEoBmfImporter struct {
	HTTPClient *http.Client
}

type UnitedStatesIRSEoBmfImportInput struct {
	DownloadURLs   []string
	DataDir        string
	MaxFiles       int
	ChunkSize      int
	Limit          int64
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	MetadataStore  countryimport.MetadataStore
}

type UnitedStatesIRSEoBmfImportResult struct {
	Download countryimport.DownloadResult
	Process  countryimport.ProcessResult
}

func (i UnitedStatesIRSEoBmfImporter) Run(ctx context.Context, input UnitedStatesIRSEoBmfImportInput) (UnitedStatesIRSEoBmfImportResult, error) {
	source := irseobmf.NewSource(irseobmf.Config{
		DownloadURLs:   input.DownloadURLs,
		DataDir:        input.DataDir,
		PageDelay:      input.PageDelay,
		RequestTimeout: input.RequestTimeout,
		UserAgent:      input.UserAgent,
		HTTPClient:     i.HTTPClient,
		MetadataStore:  input.MetadataStore,
	})

	download, err := source.Download(ctx, countryimport.DownloadOptions{
		MaxPages:       input.MaxFiles,
		PageDelay:      input.PageDelay,
		RequestTimeout: input.RequestTimeout,
	})
	result := UnitedStatesIRSEoBmfImportResult{Download: download}
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
