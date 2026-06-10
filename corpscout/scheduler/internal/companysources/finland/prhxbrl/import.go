package prhxbrl

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

func (Source) Import(ctx context.Context, opts companysources.ImportOptions) (companysources.ImportResult, error) {
	_ = ctx
	_ = opts
	return companysources.ImportResult{}, errors.New("Finland PRH financial XBRL ClickHouse import is not implemented")
}
