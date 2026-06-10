package companysources

import (
	"context"
)

func ImportRun(ctx context.Context, registry Registry, req ImportRunRequest) (ImportResult, error) {
	source, err := registry.Get(req.Country, req.Source)
	if err != nil {
		return ImportResult{}, err
	}
	return source.Import(ctx, ImportOptions{
		RunDir:              req.RunDir,
		Files:               req.Files,
		ClickHouseNativeURL: req.ClickHouseNativeURL,
		BatchSize:           req.BatchSize,
		Limit:               req.Limit,
		Truncate:            req.Truncate,
	})
}
