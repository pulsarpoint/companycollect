package prhxbrl

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	_ = ctx
	_ = opts
	return companysources.DownloadedFile{}, errors.New("Finland PRH financial XBRL download requires the source-specific Temporal action")
}
