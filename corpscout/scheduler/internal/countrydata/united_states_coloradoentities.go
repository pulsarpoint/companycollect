package countrydata

import (
	"context"
	"net/http"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/coloradoentities"
)

type UnitedStatesColoradoEntitiesImporter struct {
	HTTPClient *http.Client
}

type UnitedStatesColoradoEntitiesImportInput struct {
	BaseURL        string
	AppToken       string
	DataDir        string
	PageSize       int
	MaxPages       int
	ChunkSize      int
	Limit          int64
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	MetadataStore  countryimport.MetadataStore
	StoreFunc      func(context.Context, []coloradoentities.ColoradoEntityRecord) (countryimport.StoreResult, error)
}

type UnitedStatesColoradoEntitiesImportResult struct {
	Download countryimport.DownloadResult
	Process  countryimport.ProcessResult
}

func (i UnitedStatesColoradoEntitiesImporter) Run(ctx context.Context, input UnitedStatesColoradoEntitiesImportInput) (UnitedStatesColoradoEntitiesImportResult, error) {
	source := coloradoentities.NewSource(coloradoentities.Config{
		BaseURL:        input.BaseURL,
		AppToken:       input.AppToken,
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
	result := UnitedStatesColoradoEntitiesImportResult{Download: download}
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
