package companysources

import "context"

type Key struct {
	Country string
	Source  string
}

type Source interface {
	Key() Key
	DisplayName() string
	Import(ctx context.Context, opts ImportOptions) (ImportResult, error)
}

type ImportOptions struct {
	RunDir              string
	ClickHouseNativeURL string
	BatchSize           int
	Limit               int64
	Truncate            bool
}

type ImportResult struct {
	RunDir         string
	ImportedTables []string
	ImportedRows   int64
}

type ImportRunRequest struct {
	Country             string
	Source              string
	RunDir              string
	ClickHouseNativeURL string
	BatchSize           int
	Limit               int64
	Truncate            bool
}

type ImportChangedRunsRequest struct {
	RunsRoot            string
	RunIndexPath        string
	ClickHouseNativeURL string
	BatchSize           int
	Limit               int64
	ChangedOnly         bool
	Truncate            bool
}

type ImportChangedRunsResult struct {
	Sources []ImportChangedSourceResult
}

type ImportChangedSourceResult struct {
	Source       string
	RunID        string
	Status       string
	ImportedRows int64
}
